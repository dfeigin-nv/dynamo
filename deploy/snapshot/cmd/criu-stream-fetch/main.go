// Pipeline C streamer (minimal skeleton).
//
// Reads a small manifest, allocates one memfd per "range" entry, fills
// each memfd from a local file (S3/NIXL swap is a follow-on commit),
// and hands the fds + per-range eventfds + a shared abort_fd to the
// two CRIU socket peers that the agent pre-created:
//
//   * CRIU lazy-pages daemon  — receives [abort_fd, ev_0..ev_{n-1}]
//     via a uint32 n_evfd header + SCM_RIGHTS payload. Matches
//     recv_streamer_daemon_fds() in criu/uffd.c on the
//     streaming-restore-pipeline-c branch.
//   * CRIU restore (mem.c)    — receives one private-VMA memfd per
//     task via SCM_RIGHTS, indexed by mem.c:recv_streamer_private_fd.
//     For the smoke we send a single memfd; multi-task wiring is a
//     follow-on.
//
// Once the handshake is in flight the streamer writes its inputs into
// each memfd (no copy from a staging buffer) and signals the matching
// eventfd. On any error the abort_fd is poisoned and PIE / daemon
// notice via the existing epoll paths.
//
// This binary is intentionally bytes-mover-agnostic — the first cut
// reads from local files so the wire protocol can be exercised
// independently of NIXL OBJ. The NIXL swap lives in pipes.go siblings.
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type rangeEntry struct {
	ID     uint32 `json:"id"`
	Size   uint64 `json:"size"`
	Source string `json:"source"`
	Bucket string `json:"bucket,omitempty"`
	Key    string `json:"key,omitempty"`
	// Shmid is the CRIU shmem inode id, set only on shmem ranges. It is
	// the key CRIU restore sends over CRIU_STREAMER_SHMEM_SOCK
	// (mem.c:recv_streamer_shmem_memfd writes mie->shmid) and the value
	// the lazy-pages daemon receives in the shmid table
	// (uffd.c:recv_streamer_daemon_fds) to resolve which VMAs each
	// eventfd gates.
	Shmid uint64 `json:"shmid,omitempty"`
}

type manifest struct {
	ShmemRanges   []rangeEntry `json:"shmem_ranges"`
	PrivateRanges []rangeEntry `json:"private_ranges"`
}

func loadManifest(path string) (*manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var m manifest
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

func memfdCreate(name string, size uint64) (int, error) {
	fd, err := unix.MemfdCreate(name, unix.MFD_CLOEXEC)
	if err != nil {
		return -1, fmt.Errorf("memfd_create(%s): %w", name, err)
	}
	if size > 0 {
		if err := unix.Ftruncate(fd, int64(size)); err != nil {
			unix.Close(fd)
			return -1, fmt.Errorf("ftruncate(%s, %d): %w", name, size, err)
		}
	}
	return fd, nil
}

func eventfdCreate() (int, error) {
	fd, err := unix.Eventfd(0, unix.EFD_CLOEXEC|unix.EFD_NONBLOCK)
	if err != nil {
		return -1, fmt.Errorf("eventfd: %w", err)
	}
	return fd, nil
}

// sendDaemonFds writes the uint32 n_evfd header, then the n-entry shmid
// table, then a single SCM_RIGHTS message with [abort_fd, ev_0..ev_{n-1}].
// Matches the wire protocol in criu/uffd.c:recv_streamer_daemon_fds, which
// reads the header, then recv()s n*sizeof(uint64) into streamer_evfd_shmids
// as a raw struct read, then recv_fds(n+1). Native endianness on both sides.
func sendDaemonFds(sock int, abortFd int, evfds []int, shmids []uint64) error {
	if len(shmids) != len(evfds) {
		return fmt.Errorf("shmid table has %d entries, want %d", len(shmids), len(evfds))
	}
	var hdr [4]byte
	binary.NativeEndian.PutUint32(hdr[:], uint32(len(evfds)))
	if _, err := unix.Write(sock, hdr[:]); err != nil {
		return fmt.Errorf("write n_evfd: %w", err)
	}
	tbl := make([]byte, 8*len(shmids))
	for i, s := range shmids {
		binary.NativeEndian.PutUint64(tbl[i*8:], s)
	}
	if err := writeAll(sock, tbl); err != nil {
		return fmt.Errorf("write shmid table: %w", err)
	}
	fds := make([]int, 0, 1+len(evfds))
	fds = append(fds, abortFd)
	fds = append(fds, evfds...)
	return sendFdsChunked(sock, fds)
}

// crScmMaxFD mirrors CR_SCM_MAX_FD in include/common/scm.h. The kernel caps
// one SCM_RIGHTS message at SCM_MAX_FD (253) descriptors, and CRIU's
// __recv_fds loops in chunks of 252 with a one-byte iov per chunk. A single
// sendmsg carrying all 424 fds of a gpt-oss-120b dump fails with EINVAL, so
// the sender has to chunk identically or the handshake never lands.
const crScmMaxFD = 252

func sendFdsChunked(sock int, fds []int) error {
	for i := 0; i < len(fds); i += crScmMaxFD {
		n := len(fds) - i
		if n > crScmMaxFD {
			n = crScmMaxFD
		}
		rights := unix.UnixRights(fds[i : i+n]...)
		if err := unix.Sendmsg(sock, []byte{0}, rights, nil, 0); err != nil {
			return fmt.Errorf("sendmsg fds [%d,%d): %w", i, i+n, err)
		}
	}
	return nil
}

// writeAll loops over write() until the whole buffer is out. A single
// unix.Write on a SOCK_STREAM socket may short-write.
func writeAll(fd int, b []byte) error {
	for len(b) > 0 {
		n, err := unix.Write(fd, b)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			return err
		}
		if n <= 0 {
			return fmt.Errorf("write returned %d", n)
		}
		b = b[n:]
	}
	return nil
}

// readAll loops over read() until n bytes are drained. Returns io.EOF if the
// peer closes mid-message.
func readAll(fd int, b []byte) error {
	for len(b) > 0 {
		n, err := unix.Read(fd, b)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			return err
		}
		if n == 0 {
			return io.EOF
		}
		b = b[n:]
	}
	return nil
}

// serveShmemSocket answers CRIU restore's shmem-memfd requests. CRIU writes
// an 8-byte shmid (mem.c:recv_streamer_shmem_memfd) and expects a single
// SCM_RIGHTS reply carrying that shmem range's memfd. There is no ack: the
// streamer fills asynchronously and the daemon gates visibility via the
// per-range eventfd, so this must reply immediately even while the fill for
// that range is still in flight.
//
// Runs until CRIU closes the socket. Every restored task shares one socket
// endpoint and serializes on streamer_shmem_sock_lock, so requests arrive
// one at a time.
func serveShmemSocket(sock int, byShmid map[uint64]int) {
	for {
		var buf [8]byte
		if err := readAll(sock, buf[:]); err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "criu-stream-fetch: shmem sock read: %v\n", err)
			}
			return
		}
		shmid := binary.NativeEndian.Uint64(buf[:])
		memfd, ok := byShmid[shmid]
		if !ok {
			// Replying with nothing would wedge CRIU in recv_fds. Log
			// loudly and close so the restore fails fast instead.
			fmt.Fprintf(os.Stderr,
				"criu-stream-fetch: no shmem range for shmid=%d (%#x)\n", shmid, shmid)
			return
		}
		if err := sendPrivateFd(sock, memfd); err != nil {
			fmt.Fprintf(os.Stderr, "criu-stream-fetch: send shmem memfd %d: %v\n", shmid, err)
			return
		}
	}
}

// sendPrivateFd sends pages_memfd over the per-task private-pages
// socket as a single SCM_RIGHTS message with a 1-byte dummy iov.
// Matches criu/mem.c:recv_streamer_private_fd, which calls
// __recv_fds(sock, &fd, 1, NULL, 0).
func sendPrivateFd(sock, pagesFd int) error {
	rights := unix.UnixRights(pagesFd)
	if err := unix.Sendmsg(sock, []byte{0}, rights, nil, 0); err != nil {
		return fmt.Errorf("sendmsg private fd: %w", err)
	}
	return nil
}

// sendPrivateAck writes a single 'A' byte after the memfd has been
// fully filled. CRIU's recv_streamer_private_fd blocks reading this
// byte before letting prepare_vma_ios return, so PIE only runs after
// the bytes are in place. Async cross-process futex coordination was
// replaced by this ack because the PIE futex array must live in
// shmalloc'd memory (preserved across unmap_old_vmas), and that
// region is not reachable by the external streamer process.
func sendPrivateAck(sock int) error {
	_, err := unix.Write(sock, []byte{'A'})
	if err != nil {
		return fmt.Errorf("sendAck: %w", err)
	}
	return nil
}

// fillMemfd reads source into the memfd. The caller must Ftruncate before.
// For the smoke this is the bytes-mover; the real bytes-mover will replace
// this with NIXL OBJ posts + check_xfer_state polling.
func fillMemfd(memfd int, source string) error {
	src, err := os.Open(source)
	if err != nil {
		return err
	}
	defer src.Close()
	// Write with raw pwrite rather than wrapping memfd in an *os.File.
	// os.NewFile takes ownership of the descriptor, so once the wrapper
	// becomes unreachable the runtime finalizer close()s it — which used
	// to be harmless because the caller closed the memfd immediately
	// after filling, but now the shmem memfds have to stay open for
	// serveShmemSocket to hand them to CRIU on demand. Losing them that
	// way shows up as an EBADF on send and then a hung restore:
	//   criu-stream-fetch: send shmem memfd <shmid>: sendmsg: bad file descriptor
	return copyReaderToFdAt(memfd, 0, src)
}

// copyFileToFdAt writes source's bytes into memfd starting at offset
// off (pwrite-style). Used for the aggregate private-pages memfd where
// each range lands at a distinct offset.
func copyFileToFdAt(memfd int, off int64, source string) error {
	src, err := os.Open(source)
	if err != nil {
		return err
	}
	defer src.Close()
	return copyReaderToFdAt(memfd, off, src)
}

// copyReaderToFdAt is the source-agnostic pwrite loop shared by file
// and S3-stdout fillers.
func copyReaderToFdAt(memfd int, off int64, src io.Reader) error {
	buf := make([]byte, 1<<20)
	cur := off
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			w, werr := unix.Pwrite(memfd, buf[:n], cur)
			if werr != nil {
				return werr
			}
			if w != n {
				return fmt.Errorf("short pwrite at %d: got %d want %d", cur, w, n)
			}
			cur += int64(n)
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return rerr
		}
	}
}

// fillMemfdFromS3 streams s5cmd cat <s3uri> stdout into memfd via pwrite.
// Stage 1 fallback: sequential single-stream cat. Stage 2 replaces this
// with NIXL OBJ postXfer (PreallocatedStreamBuf into the same memfd).
func fillMemfdFromS3(memfd int, s3uri string) error {
	cmd := exec.Command("s5cmd", "cat", s3uri)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("s5cmd cat stdout pipe: %w", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("s5cmd cat start: %w", err)
	}
	if err := copyReaderToFdAt(memfd, 0, stdout); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("s5cmd cat copy: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("s5cmd cat wait: %w", err)
	}
	return nil
}

// fillFromSource dispatches based on the manifest range's source URI scheme.
// s3:// sources go through fillMemfdFromS3 (Stage 1 sequential s5cmd cat),
// local paths use the existing copyFileToFdAt loop.
func fillFromSource(memfd int, r *rangeEntry) error {
	if strings.HasPrefix(r.Source, "s3://") {
		return fillMemfdFromS3(memfd, r.Source)
	}
	return copyFileToFdAt(memfd, 0, r.Source)
}


// signalReady writes 1 to the eventfd; daemon's handle_streamer_evfd
// reacts by issuing UFFDIO_CONTINUE / writing the PIE futex word.
func signalReady(evfd int) error {
	var buf [8]byte
	binary.NativeEndian.PutUint64(buf[:], 1)
	_, err := unix.Write(evfd, buf[:])
	return err
}

// poisonAbort writes 1 to the abort_fd. Daemon/PIE see POLLIN, bail.
func poisonAbort(abortFd int) {
	if abortFd < 0 {
		return
	}
	var buf [8]byte
	binary.NativeEndian.PutUint64(buf[:], 1)
	_, _ = unix.Write(abortFd, buf[:])
}

func socketFromEnv(name string) (int, error) {
	v := os.Getenv(name)
	if v == "" {
		return -1, fmt.Errorf("env %s is unset", name)
	}
	var fd int
	if _, err := fmt.Sscanf(v, "%d", &fd); err != nil || fd < 0 {
		return -1, fmt.Errorf("env %s=%q is not a valid fd", name, v)
	}
	return fd, nil
}

func main() {
	var manifestPath string
	flag.StringVar(&manifestPath, "manifest", "", "path to streamer manifest JSON")
	flag.Parse()

	if manifestPath == "" {
		manifestPath = os.Getenv("CRIU_STREAMER_MANIFEST")
	}
	if manifestPath == "" {
		fmt.Fprintln(os.Stderr, "criu-stream-fetch: --manifest or CRIU_STREAMER_MANIFEST required")
		os.Exit(2)
	}

	// Daemon socket is only needed when the dump has shmem ranges
	// (CRIU lazy-pages with --stream-restore). Private-only dumps
	// (e.g. a single sleep process) skip the daemon entirely.
	daemonSock, _ := socketFromEnv("CRIU_STREAMER_DAEMON_SOCK")
	shmemSock, _ := socketFromEnv("CRIU_STREAMER_SHMEM_SOCK")
	privateSock, err := socketFromEnv("CRIU_STREAMER_PRIVATE_SOCK")
	if err != nil {
		fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
		os.Exit(2)
	}

	m, err := loadManifest(manifestPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "criu-stream-fetch: load manifest: %v\n", err)
		os.Exit(1)
	}

	abortFd, err := eventfdCreate()
	if err != nil {
		fmt.Fprintf(os.Stderr, "criu-stream-fetch: abort_fd: %v\n", err)
		os.Exit(1)
	}

	// Allocate one memfd + one eventfd per shmem range.
	shmemMemfds := make([]int, len(m.ShmemRanges))
	shmemEvfds := make([]int, len(m.ShmemRanges))
	shmemShmids := make([]uint64, len(m.ShmemRanges))
	shmemByShmid := make(map[uint64]int, len(m.ShmemRanges))
	for i, r := range m.ShmemRanges {
		if r.Shmid == 0 {
			fmt.Fprintf(os.Stderr,
				"criu-stream-fetch: shmem range %d has no shmid; manifest is stale\n", r.ID)
			poisonAbort(abortFd)
			os.Exit(1)
		}
		mfd, err := memfdCreate(fmt.Sprintf("criu-shmem-%d", r.ID), r.Size)
		if err != nil {
			fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
			poisonAbort(abortFd)
			os.Exit(1)
		}
		evfd, err := eventfdCreate()
		if err != nil {
			fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
			poisonAbort(abortFd)
			os.Exit(1)
		}
		shmemMemfds[i] = mfd
		shmemEvfds[i] = evfd
		shmemShmids[i] = r.Shmid
		shmemByShmid[r.Shmid] = mfd
	}

	// Send abort_fd + eventfds to the daemon side via SCM_RIGHTS.
	// Skip entirely if no shmem ranges + no daemon socket inherited:
	// private-only dumps don't run a lazy-pages daemon. The agent only
	// spawns one when the manifest has shmem ranges, so a daemon socket
	// with an empty range list would be an agent bug — CRIU rejects
	// n_evfd == 0 as bogus, and the daemon would hang the restore.
	if daemonSock >= 0 && len(shmemEvfds) > 0 {
		if err := sendDaemonFds(daemonSock, abortFd, shmemEvfds, shmemShmids); err != nil {
			fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
			poisonAbort(abortFd)
			os.Exit(1)
		}
	} else if daemonSock >= 0 {
		fmt.Fprintln(os.Stderr,
			"criu-stream-fetch: daemon socket inherited but manifest has no shmem ranges")
		poisonAbort(abortFd)
		os.Exit(1)
	}

	// Serve CRIU restore's shmem-memfd requests concurrently with the
	// fills below: CRIU asks for the inode during open_shmem, long before
	// the bytes land, and gates visibility on the per-range eventfd.
	if shmemSock >= 0 && len(shmemMemfds) > 0 {
		go serveShmemSocket(shmemSock, shmemByShmid)
	}

	// Fill the shmem memfds in parallel, signalling each eventfd as its
	// range completes so the daemon can start answering CONTINUE faults
	// for that shmid without waiting for the rest.
	shmemFilled := make(chan struct{})
	if len(m.ShmemRanges) > 0 {
		go func() {
			defer close(shmemFilled)
			var wg sync.WaitGroup
			sem := make(chan struct{}, shmemFillConcurrency())
			for i := range m.ShmemRanges {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					sem <- struct{}{}
					defer func() { <-sem }()
					r := &m.ShmemRanges[i]
					start := time.Now()
					if skipShmemFill() {
						// Experiment knob: leave the memfd zero-filled but
						// still signal ready, to measure whether anything in
						// the restored workload ever reads shmem content.
						// Wrong data if it does — inference correctness is
						// the check. Never set this in production.
						if err := signalReady(shmemEvfds[i]); err != nil {
							poisonAbort(abortFd)
						}
						return
					}
					if err := fillMemfd(shmemMemfds[i], r.Source); err != nil {
						fmt.Fprintf(os.Stderr,
							"criu-stream-fetch: fill shmem shmid=%d: %v\n", r.Shmid, err)
						poisonAbort(abortFd)
						return
					}
					if err := signalReady(shmemEvfds[i]); err != nil {
						fmt.Fprintf(os.Stderr,
							"criu-stream-fetch: signal shmid=%d: %v\n", r.Shmid, err)
						poisonAbort(abortFd)
						return
					}
					fmt.Fprintf(os.Stderr, "[stream] shmem shmid=%d %d bytes in %s\n",
						r.Shmid, r.Size, time.Since(start))
				}(i)
			}
			wg.Wait()
			fmt.Fprintf(os.Stderr, "[stream] shmem fill done (%d ranges)\n", len(m.ShmemRanges))
		}()
	} else {
		close(shmemFilled)
	}

	// Private-VMA path: request-response over the private socket.
	// CRIU writes a 4-byte uint32 pages_img_id; streamer replies with
	// SCM_RIGHTS(memfd) for that id plus a 1-byte 'A' ack after the
	// memfd has been fully filled. Loop until CRIU closes the socket.
	//
	// Stage 1 overlap: pre-create one memfd per private range and
	// launch a fill goroutine immediately so the bytes-mover runs in
	// parallel with CRIU's non-PIE setup phase. CRIU's per-task request
	// then sends the already-prepped memfd and waits only on the
	// goroutine's done channel before acking.
	type privateState struct {
		memfd int
		done  chan error
	}
	states := make(map[uint32]*privateState, len(m.PrivateRanges))
	if len(m.PrivateRanges) > 0 {
		for i := range m.PrivateRanges {
			r := &m.PrivateRanges[i]
			pfd, err := memfdCreate(fmt.Sprintf("criu-private-%d", r.ID), r.Size)
			if err != nil {
				fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
				poisonAbort(abortFd)
				os.Exit(1)
			}
			st := &privateState{memfd: pfd, done: make(chan error, 1)}
			states[r.ID] = st
			fmt.Fprintf(os.Stderr, "[stream] prefetch start id=%d size=%d src=%s ts=%s\n",
				r.ID, r.Size, r.Source, time.Now().UTC().Format(time.RFC3339Nano))
			go func(r *rangeEntry, st *privateState) {
				t0 := time.Now()
				err := fillFromSource(st.memfd, r)
				elapsed := time.Since(t0)
				st.done <- err
				fmt.Fprintf(os.Stderr, "[stream] prefetch done id=%d elapsed_ms=%d err=%v ts=%s\n",
					r.ID, elapsed.Milliseconds(), err,
					time.Now().UTC().Format(time.RFC3339Nano))
			}(r, st)
		}

		for {
			var hdr [4]byte
			n, err := unix.Read(privateSock, hdr[:])
			if n == 0 || err == io.EOF {
				break // CRIU closed; no more tasks
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "criu-stream-fetch: read id: %v\n", err)
				poisonAbort(abortFd)
				os.Exit(1)
			}
			if n != 4 {
				fmt.Fprintf(os.Stderr, "criu-stream-fetch: short id read: %d\n", n)
				poisonAbort(abortFd)
				os.Exit(1)
			}
			id := binary.NativeEndian.Uint32(hdr[:])
			st := states[id]
			if st == nil {
				fmt.Fprintf(os.Stderr, "criu-stream-fetch: manifest missing pages_img_id=%d\n", id)
				poisonAbort(abortFd)
				os.Exit(1)
			}

			if err := sendPrivateFd(privateSock, st.memfd); err != nil {
				fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
				poisonAbort(abortFd)
				os.Exit(1)
			}
			if err := <-st.done; err != nil {
				fmt.Fprintf(os.Stderr, "criu-stream-fetch: fill id=%d: %v\n", id, err)
				poisonAbort(abortFd)
				os.Exit(1)
			}
			if err := sendPrivateAck(privateSock); err != nil {
				fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
				poisonAbort(abortFd)
				os.Exit(1)
			}
			unix.Close(st.memfd)
			delete(states, id)
		}
		// Drain any prefetch goroutines CRIU never claimed (e.g. the
		// task exited early). Best-effort: close memfds to release tmpfs.
		for id, st := range states {
			<-st.done
			unix.Close(st.memfd)
			delete(states, id)
		}
		fmt.Fprintf(os.Stderr, "[stream] private loop done\n")
	}

	// Shmem fills were launched before the private loop so they overlap
	// CRIU's setup. Wait for them here. The memfds stay open until exit:
	// serveShmemSocket hands them to CRIU on demand, and CRIU may ask for
	// any shmid at any point during restore.
	<-shmemFilled

	// Stay alive until the peer closes whichever socket we're using.
	// Private-only dumps watch privateSock; shmem dumps watch daemonSock.
	watchSock := daemonSock
	if watchSock < 0 {
		watchSock = privateSock
	}
	for {
		var buf [1]byte
		n, err := syscall.Read(watchSock, buf[:])
		if err != nil || n == 0 {
			break
		}
	}
	unix.Close(abortFd)
}

// shmemFillConcurrency bounds how many shmem ranges are filled at once.
// The gpt-oss-120b dump has 423 ranges totalling ~124 GiB; unbounded
// goroutines would thrash the page cache and the PVC. Override with
// SHMEM_FILL_CONCURRENCY.
func shmemFillConcurrency() int {
	if v := os.Getenv("SHMEM_FILL_CONCURRENCY"); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			return n
		}
	}
	return 16
}

// skipShmemFill gates the SHMEM_SKIP_FILL experiment described at its only
// call site.
func skipShmemFill() bool {
	return os.Getenv("SHMEM_SKIP_FILL") == "1"
}
