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
	"syscall"

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

// sendPrivateFd sends one memfd over the per-task private-pages socket.
// Matches criu/mem.c:recv_streamer_private_fd (single fd, no payload).
func sendPrivateFd(sock int, memfd int) error {
	rights := unix.UnixRights(memfd)
	if err := unix.Sendmsg(sock, []byte{0}, rights, nil, 0); err != nil {
		return fmt.Errorf("sendmsg private fd: %w", err)
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
	dst := os.NewFile(uintptr(memfd), "memfd")
	_, err = io.Copy(dst, src)
	return err
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

	daemonSock, err := socketFromEnv("CRIU_STREAMER_DAEMON_SOCK")
	if err != nil {
		fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
		os.Exit(2)
	}
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
	if err := sendDaemonFds(daemonSock, abortFd, shmemEvfds); err != nil {
		fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
		poisonAbort(abortFd)
		os.Exit(1)
	}

	// Send the (single) private-VMA memfd to the restore side. Multi-task
	// fan-out lives in a follow-on once we have >1 task in real workloads.
	if len(m.PrivateRanges) > 0 {
		pfd, err := memfdCreate("criu-private-0", m.PrivateRanges[0].Size)
		if err != nil {
			fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
			poisonAbort(abortFd)
			os.Exit(1)
		}
		if err := sendPrivateFd(privateSock, pfd); err != nil {
			fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
			poisonAbort(abortFd)
			os.Exit(1)
		}
		// Fill the private memfd from its source file. PIE waits on its
		// per-task streamer_private_ready_futex; we'll wire that signal
		// once the agent passes the shmalloc'd futex array address
		// through; for the smoke the PIE side falls back to the
		// existing pages-img path when the futex pointer is NULL.
		if err := fillMemfd(pfd, m.PrivateRanges[0].Source); err != nil {
			fmt.Fprintf(os.Stderr, "criu-stream-fetch: fill private: %v\n", err)
			poisonAbort(abortFd)
			os.Exit(1)
		}
		unix.Close(pfd)
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

	// Stay alive until the daemon closes the socket — abort_fd POLLHUP is
	// the daemon's signal that everything finished cleanly. Read returns
	// (0, nil) on stream EOF, so check n explicitly.
	for {
		var buf [1]byte
		n, err := syscall.Read(daemonSock, buf[:])
		if err != nil || n == 0 {
			break
		}
	}
	unix.Close(abortFd)
}
