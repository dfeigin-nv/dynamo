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

// sendPrivateFdsAsync sends [pages_memfd, pipe_rfd] as a single SCM_RIGHTS
// message (Step A async overlap). Matches criu/mem.c:recv_streamer_private_fd
// when opts.stream_restore_async != 0, which calls recv_fds(sock, fds, 2).
// fds[0] is the memfd, fds[1] is the read end of the readiness pipe. CRIU's
// PIE blocks on read(pipe_rfd) before consuming the memfd; the streamer
// writes one byte to the write end once the fill completes (or closes it on
// error, so PIE sees EOF and aborts).
func sendPrivateFdsAsync(sock, pagesFd, pipeRfd int) error {
	rights := unix.UnixRights(pagesFd, pipeRfd)
	if err := unix.Sendmsg(sock, []byte{0}, rights, nil, 0); err != nil {
		return fmt.Errorf("sendmsg private fds (async): %w", err)
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
	// Step A async overlap (default on). When enabled the streamer hands
	// CRIU [memfd, pipe_rfd] + an immediate 'A' ack so CRIU's prep/fork
	// overlaps the still-running S3 fill; a per-task goroutine writes the
	// pipe once the fill completes. Kill switch: STREAM_RESTORE_ASYNC=0
	// (must match criu's --no-stream-restore-async).
	asyncEnabled := os.Getenv("STREAM_RESTORE_ASYNC") != "0"
	var asyncWG sync.WaitGroup
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

			if asyncEnabled {
				// Overlap: send [memfd, pipe_rfd] + ack now, before the
				// fill finishes. A goroutine writes the pipe when the fill
				// completes so PIE (blocked on read(pipe_rfd)) proceeds.
				var p [2]int
				if err := unix.Pipe2(p[:], unix.O_CLOEXEC); err != nil {
					fmt.Fprintf(os.Stderr, "criu-stream-fetch: pipe2 id=%d: %v\n", id, err)
					poisonAbort(abortFd)
					os.Exit(1)
				}
				pipeR, pipeW := p[0], p[1]
				if err := sendPrivateFdsAsync(privateSock, st.memfd, pipeR); err != nil {
					fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
					poisonAbort(abortFd)
					os.Exit(1)
				}
				if err := sendPrivateAck(privateSock); err != nil {
					fmt.Fprintf(os.Stderr, "criu-stream-fetch: %v\n", err)
					poisonAbort(abortFd)
					os.Exit(1)
				}
				unix.Close(pipeR) // CRIU holds its own dup
				asyncWG.Add(1)
				go func(id uint32, st *privateState, pipeW int) {
					defer asyncWG.Done()
					err := <-st.done
					if err != nil {
						// Fill failed: close pipe without writing so PIE's
						// read returns EOF and it aborts cleanly.
						fmt.Fprintf(os.Stderr, "criu-stream-fetch: fill id=%d: %v\n", id, err)
						unix.Close(pipeW)
						unix.Close(st.memfd)
						poisonAbort(abortFd)
						return
					}
					if _, werr := unix.Write(pipeW, []byte{'1'}); werr != nil {
						fmt.Fprintf(os.Stderr, "criu-stream-fetch: pipe signal id=%d: %v\n", id, werr)
					}
					unix.Close(pipeW)
					unix.Close(st.memfd)
				}(id, st, pipeW)
				delete(states, id) // goroutine owns memfd + done from here
			} else {
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
		}
		// Drain any prefetch goroutines CRIU never claimed (e.g. the
		// task exited early). Best-effort: close memfds to release tmpfs.
		for id, st := range states {
			<-st.done
			unix.Close(st.memfd)
			delete(states, id)
		}
		// Async overlap: CRIU closes the private socket after handover,
		// but PIE reads the readiness pipes later. Stay alive until every
		// fill goroutine has signalled its pipe.
		asyncWG.Wait()
		fmt.Fprintf(os.Stderr, "[stream] private loop done\n")
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
