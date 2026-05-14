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
	"sync/atomic"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

type rangeEntry struct {
	ID     uint32 `json:"id"`
	Size   uint64 `json:"size"`
	Source string `json:"source"`
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

// sendDaemonFds writes the uint32 n_evfd header then a single SCM_RIGHTS
// message with [abort_fd, ev_0..ev_{n-1}]. Matches the wire protocol in
// criu/uffd.c:recv_streamer_daemon_fds.
func sendDaemonFds(sock int, abortFd int, evfds []int) error {
	var hdr [4]byte
	binary.NativeEndian.PutUint32(hdr[:], uint32(len(evfds)))
	if _, err := unix.Write(sock, hdr[:]); err != nil {
		return fmt.Errorf("write n_evfd: %w", err)
	}
	fds := make([]int, 0, 1+len(evfds))
	fds = append(fds, abortFd)
	fds = append(fds, evfds...)
	rights := unix.UnixRights(fds...)
	if err := unix.Sendmsg(sock, []byte{0}, rights, nil, 0); err != nil {
		return fmt.Errorf("sendmsg daemon fds: %w", err)
	}
	return nil
}

// sendPrivateFds sends [pages_memfd, futex_memfd] over the per-task
// private-pages socket as a single SCM_RIGHTS message with a 1-byte
// dummy iov. Matches criu/mem.c:recv_streamer_private_fds, which calls
// __recv_fds(sock, fds, 2, NULL, 0).
func sendPrivateFds(sock, pagesFd, futexFd int) error {
	rights := unix.UnixRights(pagesFd, futexFd)
	if err := unix.Sendmsg(sock, []byte{0}, rights, nil, 0); err != nil {
		return fmt.Errorf("sendmsg private fds: %w", err)
	}
	return nil
}

// signalFutexReady writes 1 to futex[idx] (MAP_SHARED with PIE) and
// futex-wakes any waiter. PIE's restorer.c:1895 path is
//   while (!ready) sys_futex(FUTEX_WAIT)
// so the write-then-wake matches a standard producer side.
func signalFutexReady(futex []uint32, idx int) error {
	if idx >= len(futex) {
		return fmt.Errorf("futex idx %d out of range %d", idx, len(futex))
	}
	// Atomic store-release before wake.
	atomicStore32(&futex[idx], 1)
	addr := uintptr(unsafe.Pointer(&futex[idx]))
	// FUTEX_WAKE = 1; not exposed in x/sys/unix on this version, so
	// pass the raw op code. Wake up to INT_MAX waiters.
	const futexWake = 1
	_, _, errno := unix.Syscall6(unix.SYS_FUTEX, addr,
		futexWake, 0x7fffffff, 0, 0, 0)
	if errno != 0 {
		return fmt.Errorf("futex_wake idx %d: %v", idx, errno)
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
	// Wrap the fd in *os.File for io.Copy; do NOT defer dst.Close() —
	// the caller continues to use memfd by raw fd. We seek to 0 and
	// io.Copy advances via Write which uses the memfd's offset; that's
	// fine for sequential fill, but if the caller does later writes
	// they must seek themselves.
	dst := os.NewFile(uintptr(memfd), "memfd")
	_, err = io.Copy(dst, src)
	return err
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
	buf := make([]byte, 64*1024)
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

// atomicStore32 store-releases v into *p. Used right before FUTEX_WAKE.
func atomicStore32(p *uint32, v uint32) {
	atomic.StoreUint32(p, v)
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
	for i, r := range m.ShmemRanges {
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
	}

	// Send abort_fd + eventfds to the daemon side via SCM_RIGHTS.
	// Skip entirely if no shmem ranges + no daemon socket inherited:
	// private-only dumps don't run a lazy-pages daemon.
	if daemonSock >= 0 && len(shmemEvfds) > 0 {
		if err := sendDaemonFds(daemonSock, abortFd, shmemEvfds); err != nil {
			fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
			poisonAbort(abortFd)
			os.Exit(1)
		}
	}

	// Private-VMA path: one pages memfd holding all private-range bytes
	// at the offsets pagemap_render_iovec computed, plus a futex memfd
	// sized n_private*sizeof(uint32) that PIE futex-waits on per range.
	// Multi-task fan-out is a follow-on once we have >1 task.
	var futex []uint32
	var futexBytes int
	if len(m.PrivateRanges) > 0 {
		nPriv := len(m.PrivateRanges)
		futexBytes = nPriv * 4

		// Aggregate pages memfd: sum the range sizes for now. Real
		// agent will size this to CRIU's vma_ios layout from the
		// manifest CRIU sees.
		var pagesSize uint64
		for _, r := range m.PrivateRanges {
			pagesSize += r.Size
		}
		pfd, err := memfdCreate("criu-private-pages", pagesSize)
		if err != nil {
			fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
			poisonAbort(abortFd)
			os.Exit(1)
		}
		ffd, err := memfdCreate("criu-private-futex", uint64(futexBytes))
		if err != nil {
			fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
			poisonAbort(abortFd)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "[stream] sending [pages_fd=%d, futex_fd=%d] to private sock %d\n", pfd, ffd, privateSock)
		if err := sendPrivateFds(privateSock, pfd, ffd); err != nil {
			fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
			poisonAbort(abortFd)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "[stream] private fds sent OK\n")

		// Map the futex memfd locally so we can signal PIE.
		raw, err := unix.Mmap(ffd, 0, futexBytes,
			unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		if err != nil {
			fmt.Fprintf(os.Stderr, "criu-stream-fetch: mmap futex: %v\n", err)
			poisonAbort(abortFd)
			os.Exit(1)
		}
		futex = unsafe.Slice((*uint32)(unsafe.Pointer(&raw[0])), nPriv)

		// Local-files smoke: fill pages memfd from concatenated
		// sources. Real version reads from S3 per-range via NIXL.
		var off int64
		for i, r := range m.PrivateRanges {
			if err := copyFileToFdAt(pfd, off, r.Source); err != nil {
				fmt.Fprintf(os.Stderr, "criu-stream-fetch: fill private %d: %v\n", i, err)
				poisonAbort(abortFd)
				os.Exit(1)
			}
			off += int64(r.Size)
			if err := signalFutexReady(futex, i); err != nil {
				fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
				poisonAbort(abortFd)
				os.Exit(1)
			}
		}
		unix.Close(pfd)
		unix.Close(ffd)
	}

	// Fill each shmem memfd, signal its eventfd as the last byte lands.
	// Sequential for the smoke; per-range goroutines once NIXL is wired.
	for i, r := range m.ShmemRanges {
		if err := fillMemfd(shmemMemfds[i], r.Source); err != nil {
			fmt.Fprintf(os.Stderr, "criu-stream-fetch: fill shmem %d: %v\n", r.ID, err)
			poisonAbort(abortFd)
			os.Exit(1)
		}
		if err := signalReady(shmemEvfds[i]); err != nil {
			fmt.Fprintf(os.Stderr, "criu-stream-fetch: signal %d: %v\n", r.ID, err)
			poisonAbort(abortFd)
			os.Exit(1)
		}
		unix.Close(shmemMemfds[i])
		unix.Close(shmemEvfds[i])
	}

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
