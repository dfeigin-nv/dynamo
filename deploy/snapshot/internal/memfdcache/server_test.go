package memfdcache

import (
	"testing"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sys/unix"
)

// makeSealedMemfd creates a memfd of the given size sealed F_SEAL_FUTURE_WRITE,
// i.e. exactly what CRIU donates: a populated, future-write-sealed inode.
func makeSealedMemfd(t *testing.T, size int64) int {
	t.Helper()
	fd, err := unix.MemfdCreate("test", unix.MFD_ALLOW_SEALING)
	if err != nil {
		t.Fatalf("MemfdCreate: %v", err)
	}
	if err := unix.Ftruncate(fd, size); err != nil {
		unix.Close(fd)
		t.Fatalf("Ftruncate: %v", err)
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, fSealFutureWrite); err != nil {
		unix.Close(fd)
		t.Fatalf("F_ADD_SEALS: %v", err)
	}
	return fd
}

func makeUnsealedMemfd(t *testing.T, size int64) int {
	t.Helper()
	fd, err := unix.MemfdCreate("test-unsealed", unix.MFD_ALLOW_SEALING)
	if err != nil {
		t.Fatalf("MemfdCreate: %v", err)
	}
	if err := unix.Ftruncate(fd, size); err != nil {
		unix.Close(fd)
		t.Fatalf("Ftruncate: %v", err)
	}
	return fd
}

func newTestServer(maxBytes int64, ttl time.Duration) (*Server, *fakeClock) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := &Server{
		entries:  make(map[cacheKey]*entry),
		maxBytes: maxBytes,
		idleTTL:  ttl,
		log:      logr.Discard(),
		now:      clk.now,
		stop:     make(chan struct{}),
	}
	return s, clk
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func req(id string, shmid uint64, size uint64) request {
	return request{op: opDonate, seals: fSealFutureWrite, size: size, id: id, shmid: shmid, uid: 1000, gid: 1000}
}

func TestDonateThenBorrowHit(t *testing.T) {
	s, _ := newTestServer(0, 0)
	defer s.Close()

	r := req("ckpt:v1", 0x100, 4096)
	if st := s.donate(r, makeSealedMemfd(t, 4096)); st != statusOK {
		t.Fatalf("donate status = %d, want OK", st)
	}
	if n, b := s.Stats(); n != 1 || b != 4096 {
		t.Fatalf("Stats = (%d,%d), want (1,4096)", n, b)
	}

	key := cacheKey{id: "ckpt:v1", shmid: 0x100, uid: 1000, gid: 1000}
	fd, ok := s.borrow(key, 4096)
	if !ok {
		t.Fatal("borrow miss, want hit")
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil || st.Size != 4096 {
		t.Fatalf("borrowed fd fstat = %v size %d", err, st.Size)
	}
	s.release(key)
}

func TestBorrowMissWrongSizeAndWrongKey(t *testing.T) {
	s, _ := newTestServer(0, 0)
	defer s.Close()
	s.donate(req("ckpt:v1", 0x100, 4096), makeSealedMemfd(t, 4096))

	// Wrong size -> miss (stale entry guard).
	if _, ok := s.borrow(cacheKey{id: "ckpt:v1", shmid: 0x100, uid: 1000, gid: 1000}, 8192); ok {
		t.Fatal("borrow hit on size mismatch, want miss")
	}
	// Different owner -> miss (copy per owner).
	if _, ok := s.borrow(cacheKey{id: "ckpt:v1", shmid: 0x100, uid: 2000, gid: 2000}, 4096); ok {
		t.Fatal("borrow hit on different owner, want miss")
	}
}

func TestDonateUnsealedDeclined(t *testing.T) {
	s, _ := newTestServer(0, 0)
	defer s.Close()
	if st := s.donate(req("ckpt:v1", 0x100, 4096), makeUnsealedMemfd(t, 4096)); st != statusDecline {
		t.Fatalf("unsealed donate status = %d, want DECLINE", st)
	}
	if n, _ := s.Stats(); n != 0 {
		t.Fatalf("unsealed inode cached (%d entries), want 0", n)
	}
}

func TestBudgetLRUEviction(t *testing.T) {
	s, clk := newTestServer(8192, 0) // room for two 4096 entries
	defer s.Close()

	s.donate(req("ckpt:v1", 1, 4096), makeSealedMemfd(t, 4096))
	clk.advance(time.Second)
	s.donate(req("ckpt:v1", 2, 4096), makeSealedMemfd(t, 4096))
	if n, b := s.Stats(); n != 2 || b != 8192 {
		t.Fatalf("after 2 donates Stats=(%d,%d), want (2,8192)", n, b)
	}

	// Third donation evicts the LRU (shmid 1).
	clk.advance(time.Second)
	s.donate(req("ckpt:v1", 3, 4096), makeSealedMemfd(t, 4096))
	if n, b := s.Stats(); n != 2 || b != 8192 {
		t.Fatalf("after 3rd donate Stats=(%d,%d), want (2,8192)", n, b)
	}
	if _, ok := s.borrow(cacheKey{id: "ckpt:v1", shmid: 1, uid: 1000, gid: 1000}, 4096); ok {
		t.Fatal("LRU entry (shmid 1) survived, want evicted")
	}
	if _, ok := s.borrow(cacheKey{id: "ckpt:v1", shmid: 3, uid: 1000, gid: 1000}, 4096); !ok {
		t.Fatal("newest entry (shmid 3) missing")
	}
	s.release(cacheKey{id: "ckpt:v1", shmid: 3, uid: 1000, gid: 1000})
}

func TestBorrowedNotEvictedOverBudgetDeclines(t *testing.T) {
	s, _ := newTestServer(4096, 0) // room for exactly one entry
	defer s.Close()
	s.donate(req("ckpt:v1", 1, 4096), makeSealedMemfd(t, 4096))

	// Borrow it so it cannot be evicted.
	key := cacheKey{id: "ckpt:v1", shmid: 1, uid: 1000, gid: 1000}
	if _, ok := s.borrow(key, 4096); !ok {
		t.Fatal("borrow miss")
	}

	// New donation can't make room (only candidate is borrowed) -> decline.
	if st := s.donate(req("ckpt:v1", 2, 4096), makeSealedMemfd(t, 4096)); st != statusDecline {
		t.Fatalf("donate status = %d, want DECLINE (borrowed entry pins budget)", st)
	}

	// Release and retry -> now there's room.
	s.release(key)
	if st := s.donate(req("ckpt:v1", 2, 4096), makeSealedMemfd(t, 4096)); st != statusOK {
		t.Fatalf("donate after release = %d, want OK", st)
	}
}

func TestIdleTTLSweep(t *testing.T) {
	s, clk := newTestServer(0, time.Minute)
	defer s.Close()
	s.donate(req("ckpt:v1", 1, 4096), makeSealedMemfd(t, 4096))

	clk.advance(30 * time.Second)
	s.sweep()
	if n, _ := s.Stats(); n != 1 {
		t.Fatalf("entry swept too early (%d), want 1", n)
	}
	clk.advance(2 * time.Minute)
	s.sweep()
	if n, _ := s.Stats(); n != 0 {
		t.Fatalf("cold entry not swept (%d), want 0", n)
	}
}

func TestVersionInvalidation(t *testing.T) {
	s, _ := newTestServer(0, 0)
	defer s.Close()
	s.donate(req("modelA:v1", 1, 4096), makeSealedMemfd(t, 4096))
	s.donate(req("modelA:v1", 2, 4096), makeSealedMemfd(t, 4096))

	// New version of the same checkpoint supersedes the old inodes.
	s.donate(req("modelA:v2", 1, 4096), makeSealedMemfd(t, 4096))
	if _, ok := s.borrow(cacheKey{id: "modelA:v1", shmid: 1, uid: 1000, gid: 1000}, 4096); ok {
		t.Fatal("old-version entry survived new-version donate")
	}
	if _, ok := s.borrow(cacheKey{id: "modelA:v2", shmid: 1, uid: 1000, gid: 1000}, 4096); !ok {
		t.Fatal("new-version entry missing")
	}
	s.release(cacheKey{id: "modelA:v2", shmid: 1, uid: 1000, gid: 1000})
}

// TestSessionRoundTrip exercises the full wire protocol over a real socketpair:
// the test plays the CRIU client against a live serve() goroutine.
func TestSessionRoundTrip(t *testing.T) {
	s := New(0, 0, logr.Discard())
	defer s.Close()

	criuEnd, err := s.NewSession()
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	defer criuEnd.Close()
	cfd := int(criuEnd.Fd())

	r := request{seals: fSealFutureWrite, size: 4096, id: "ckpt:v1", shmid: 0x55, uid: 1000, gid: 1000}

	// MISS before anything is donated.
	if status, _ := clientGet(t, cfd, r); status != statusMiss {
		t.Fatalf("pre-donate GET status = %d, want MISS", status)
	}

	// DONATE a sealed memfd.
	memfd := makeSealedMemfd(t, 4096)
	if status := clientDonate(t, cfd, r, memfd); status != statusOK {
		t.Fatalf("DONATE status = %d, want OK", status)
	}
	unix.Close(memfd)

	// GET now HITs and returns a usable fd of the right size.
	status, gotFd := clientGet(t, cfd, r)
	if status != statusHit {
		t.Fatalf("post-donate GET status = %d, want HIT", status)
	}
	var st unix.Stat_t
	if err := unix.Fstat(gotFd, &st); err != nil || st.Size != 4096 {
		t.Fatalf("HIT fd fstat = %v size %d, want 4096", err, st.Size)
	}
	unix.Close(gotFd)

	// Different shmid -> MISS.
	other := r
	other.shmid = 0x99
	if status, _ := clientGet(t, cfd, other); status != statusMiss {
		t.Fatalf("other-shmid GET status = %d, want MISS", status)
	}
}

func clientGet(t *testing.T, sock int, r request) (uint32, int) {
	t.Helper()
	r.op = opGet
	if err := sendMsg(sock, encodeRequest(r)); err != nil {
		t.Fatalf("client GET send: %v", err)
	}
	buf := make([]byte, respSize)
	n, err := recvMsg(sock, buf)
	if err != nil {
		t.Fatalf("client GET resp: %v", err)
	}
	status, err := decodeRespStatus(buf[:n])
	if err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	if status == statusHit {
		fd, err := recvFD(sock)
		if err != nil {
			t.Fatalf("client GET recvFD: %v", err)
		}
		return status, fd
	}
	return status, -1
}

func clientDonate(t *testing.T, sock int, r request, fd int) uint32 {
	t.Helper()
	r.op = opDonate
	if err := sendMsg(sock, encodeRequest(r)); err != nil {
		t.Fatalf("client DONATE send: %v", err)
	}
	if err := sendFD(sock, fd); err != nil {
		t.Fatalf("client DONATE sendFD: %v", err)
	}
	buf := make([]byte, respSize)
	n, err := recvMsg(sock, buf)
	if err != nil {
		t.Fatalf("client DONATE resp: %v", err)
	}
	status, err := decodeRespStatus(buf[:n])
	if err != nil {
		t.Fatalf("decode resp: %v", err)
	}
	return status
}
