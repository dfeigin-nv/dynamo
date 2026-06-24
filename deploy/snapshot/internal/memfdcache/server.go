package memfdcache

import (
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sys/unix"
)

// cacheKey scopes one cached inode. id is "checkpointID:version"; a re-checkpoint
// bumps version -> a new key -> a clean miss. uid/gid are part of the key because
// a memfd inode has exactly one owner: a different owner is a miss that fills its
// own copy ("a copy per owner"); same-owner pods share one copy.
type cacheKey struct {
	id    string
	shmid uint64
	uid   uint32
	gid   uint32
}

// entry is one cached, populated, sealed memfd held open by the agent. Holding
// the fd keeps the inode and its physical pages resident for future borrowers.
type entry struct {
	fd       int
	size     int64
	seals    uint32
	refcount int // in-flight borrows; never evict while > 0
	lastUsed time.Time
}

// Server is the node-local memfd content cache. Safe for concurrent sessions.
type Server struct {
	mu       sync.Mutex
	entries  map[cacheKey]*entry
	curBytes int64
	maxBytes int64 // hard RAM budget; <= 0 means unlimited
	idleTTL  time.Duration
	log      logr.Logger
	now      func() time.Time // injectable for tests

	// allowUnsealed accepts F_SEAL_SEAL-only (writable, non-FUTURE_WRITE)
	// donations -- the "unsafe ceiling" (Option A): borrowers map the golden
	// MAP_SHARED writable. Cross-pod/re-sleep unsafe; gated by env, off by default.
	allowUnsealed bool

	stop     chan struct{}
	stopOnce sync.Once
}

// New constructs a cache server. maxBytes is the hard RAM budget for cached
// inodes (<= 0 = unlimited); idleTTL evicts cold (refcount==0) entries (<= 0 =
// no TTL sweep). The caller starts sessions with NewSession and must Close it.
func New(maxBytes int64, idleTTL time.Duration, log logr.Logger) *Server {
	s := &Server{
		entries: make(map[cacheKey]*entry),
		maxBytes: maxBytes,
		idleTTL:  idleTTL,
		log:      log,
		now:      time.Now,
		stop:     make(chan struct{}),
	}
	if v := os.Getenv("MEMFD_CACHE_ALLOW_UNSEALED"); v == "1" || strings.EqualFold(v, "true") {
		s.allowUnsealed = true
		log.Info("memfd-cache: MEMFD_CACHE_ALLOW_UNSEALED set -- accepting F_SEAL_SEAL (writable) donations; cross-pod/re-sleep UNSAFE")
	}
	if idleTTL > 0 {
		go s.sweepLoop()
	}
	return s
}

// NewSession creates a fresh socketpair, starts servicing the agent end, and
// returns the criu end to hand to nsrestore via ExtraFiles. The returned file
// should be closed by the caller after the restore process has been started
// (the child has its own dup). When criu closes its end, the session's serve
// goroutine releases every borrow taken on it.
func (s *Server) NewSession() (*os.File, error) {
	agentFd, criuEnd, err := newSocketpair()
	if err != nil {
		return nil, err
	}
	go s.serve(agentFd)
	return criuEnd, nil
}

// Close stops the TTL sweeper and closes every cached fd. In-flight sessions
// fail their next socket op and exit; their fds are closed here.
func (s *Server) Close() {
	s.stopOnce.Do(func() { close(s.stop) })
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.entries {
		_ = unix.Close(e.fd)
		delete(s.entries, k)
	}
	s.curBytes = 0
}

// Invalidate evicts all cold entries for a checkpoint (e.g. when the controller
// observes the checkpoint deleted). Borrowed entries are left for the borrow to
// finish; they expire by TTL/budget afterwards.
func (s *Server) Invalidate(checkpointID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.entries {
		if checkpointOf(k.id) == checkpointID && e.refcount == 0 {
			s.evictLocked(k, e)
		}
	}
}

// Stats reports the live entry count and total cached bytes (for tests/metrics).
func (s *Server) Stats() (entries int, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries), s.curBytes
}

func (s *Server) serve(agentFd int) {
	defer unix.Close(agentFd)

	var borrowed []cacheKey
	reqBuf := make([]byte, reqSize)

	for {
		n, err := recvMsg(agentFd, reqBuf)
		if err != nil || n == 0 {
			break // criu closed its end (restore done) or transport error
		}
		req, derr := decodeRequest(reqBuf[:n])
		if derr != nil {
			s.log.Error(derr, "memfd-cache: bad request")
			break
		}

		switch req.op {
		case opGet:
			key := cacheKey{id: req.id, shmid: req.shmid, uid: req.uid, gid: req.gid}
			fd, ok := s.borrow(key, req.size)
			if !ok {
				if err := sendMsg(agentFd, encodeResp(statusMiss)); err != nil {
					return
				}
				continue
			}
			// HIT: reply, then pass the fd. Refcount is already held, so the
			// entry can't be evicted in the window before the send completes.
			if err := sendMsg(agentFd, encodeResp(statusHit)); err != nil {
				s.release(key)
				return
			}
			if err := sendFD(agentFd, fd); err != nil {
				s.release(key)
				return
			}
			borrowed = append(borrowed, key)

		case opDonate:
			fd, rerr := recvFD(agentFd)
			if rerr != nil {
				s.log.Error(rerr, "memfd-cache: donate recv fd failed")
				return
			}
			status := s.donate(req, fd)
			if err := sendMsg(agentFd, encodeResp(status)); err != nil {
				return
			}

		default:
			s.log.Info("memfd-cache: unknown op", "op", req.op)
			return
		}
	}

	for _, k := range borrowed {
		s.release(k)
	}
}

// borrow hands out a cached fd for key if present and the size matches,
// incrementing its refcount so it can't be evicted while in use.
func (s *Server) borrow(key cacheKey, expectSize uint64) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.entries[key]
	if e == nil {
		return -1, false
	}
	if uint64(e.size) != expectSize {
		// Size disagreement means a stale or wrong entry: miss and refill.
		s.log.Info("memfd-cache: size mismatch on hit; treating as miss",
			"shmid", key.shmid, "cached", e.size, "want", expectSize)
		return -1, false
	}
	e.refcount++
	e.lastUsed = s.now()
	return e.fd, true
}

func (s *Server) release(key cacheKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.entries[key]; e != nil && e.refcount > 0 {
		e.refcount--
		e.lastUsed = s.now()
	}
}

// donate validates and stores a donated fd. The seal is verified on the actual
// fd (never trusting the request) since the seal is the entire safety property.
// Returns the status to send back: OK (cached or already present) or DECLINE
// (not sealed, or over budget with nothing evictable). The fd is consumed.
func (s *Server) donate(req request, fd int) uint32 {
	// The donated golden must be sealed F_SEAL_FUTURE_WRITE: the kernel then
	// forbids new writable mappings, so borrowers can only map it MAP_PRIVATE
	// (copy-on-write) and cannot corrupt the shared copy. CRIU's COW path seals
	// the writable weight-shadow goldens this way before donating. The seal is
	// verified on the actual fd, never trusting the request's declared seals.
	seals, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
	if err != nil {
		s.log.Info("memfd-cache: declining donation (seal read failed)", "shmid", req.shmid, "err", err)
		_ = unix.Close(fd)
		return statusDecline
	}
	if seals&fSealFutureWrite == 0 {
		if !s.allowUnsealed {
			s.log.Info("memfd-cache: declining unsealed donation", "shmid", req.shmid, "seals", seals)
			_ = unix.Close(fd)
			return statusDecline
		}
		s.log.Info("memfd-cache: accepting non-FUTURE_WRITE donation (unsafe ceiling)", "shmid", req.shmid, "seals", seals)
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		s.log.Error(err, "memfd-cache: fstat donated fd failed", "shmid", req.shmid)
		_ = unix.Close(fd)
		return statusDecline
	}
	size := st.Size
	key := cacheKey{id: req.id, shmid: req.shmid, uid: req.uid, gid: req.gid}

	s.mu.Lock()
	defer s.mu.Unlock()

	// A new version of the same checkpoint supersedes its older inodes.
	s.invalidateOtherVersionsLocked(req.id)

	if _, exists := s.entries[key]; exists {
		// Already cached (re-donate or a concurrent restore): keep the first.
		_ = unix.Close(fd)
		return statusOK
	}

	if !s.makeRoomLocked(size) {
		s.log.Info("memfd-cache: over budget, declining donation",
			"shmid", req.shmid, "size", size, "cur", s.curBytes, "max", s.maxBytes)
		_ = unix.Close(fd)
		return statusDecline
	}

	s.entries[key] = &entry{fd: fd, size: size, seals: uint32(seals), refcount: 0, lastUsed: s.now()}
	s.curBytes += size
	s.log.V(1).Info("memfd-cache: cached inode",
		"checkpoint", checkpointOf(req.id), "shmid", req.shmid, "size", size, "cur", s.curBytes)
	return statusOK
}

// makeRoomLocked evicts cold LRU entries until size fits the budget. Returns
// false if it cannot fit (only borrowed entries remain). Unlimited when maxBytes<=0.
func (s *Server) makeRoomLocked(size int64) bool {
	if s.maxBytes <= 0 {
		return true
	}
	for s.curBytes+size > s.maxBytes {
		var victimKey cacheKey
		var victim *entry
		for k, e := range s.entries {
			if e.refcount != 0 {
				continue
			}
			if victim == nil || e.lastUsed.Before(victim.lastUsed) {
				victim, victimKey = e, k
			}
		}
		if victim == nil {
			return false // nothing evictable
		}
		s.evictLocked(victimKey, victim)
	}
	return true
}

// invalidateOtherVersionsLocked evicts cold entries that share id's checkpoint
// prefix but carry a different version.
func (s *Server) invalidateOtherVersionsLocked(id string) {
	ckpt := checkpointOf(id)
	for k, e := range s.entries {
		if e.refcount == 0 && k.id != id && checkpointOf(k.id) == ckpt {
			s.evictLocked(k, e)
		}
	}
}

func (s *Server) evictLocked(k cacheKey, e *entry) {
	_ = unix.Close(e.fd)
	delete(s.entries, k)
	s.curBytes -= e.size
}

func (s *Server) sweepLoop() {
	interval := s.idleTTL / 2
	if interval <= 0 {
		interval = s.idleTTL
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.sweep()
		}
	}
}

func (s *Server) sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepLocked()
}

func (s *Server) sweepLocked() {
	if s.idleTTL <= 0 {
		return
	}
	cutoff := s.now().Add(-s.idleTTL)
	for k, e := range s.entries {
		if e.refcount == 0 && e.lastUsed.Before(cutoff) {
			s.evictLocked(k, e)
		}
	}
}

// checkpointOf splits "checkpointID:version" at the last ':'. With no ':' the
// whole id is the checkpoint (version "").
func checkpointOf(id string) string {
	if i := strings.LastIndexByte(id, ':'); i >= 0 {
		return id[:i]
	}
	return id
}
