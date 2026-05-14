// Pipeline C orchestration: spawn the (streamer, criu lazy-pages,
// criu restore) triple with pre-created socket pairs and per-process
// env vars. Runs inside the placeholder namespace (called from
// nsrestore), so the criu binary on PATH is the streaming-pipeline-c
// build that ships with the snapshot-agent image.
//
// Wire protocol (matches CRIU side):
//
//   * Daemon socket (SOCK_STREAM): agent end -> criu-stream-fetch via
//     CRIU_STREAMER_DAEMON_SOCK; child end -> criu lazy-pages via the
//     same env var. Streamer writes [uint32 n_evfd | SCM_RIGHTS:
//     abort_fd, ev_0..ev_{n-1}].
//
//   * Private socket (SOCK_STREAM): agent end -> criu-stream-fetch via
//     CRIU_STREAMER_PRIVATE_SOCK; child end -> criu restore via the same
//     env var. Streamer writes SCM_RIGHTS [pages_memfd, futex_memfd].
//
// First-cut behaviour:
//   - Local-files bytes-mover only; the streamer reads from a manifest
//     JSON pointing at on-disk paths. NIXL OBJ + S3 swap is the next
//     follow-on commit; the orchestration code below does not change.
//   - Single-task fan-out; multi-task (multiple private fds) lands when
//     real vLLM workloads exercise it.
//   - The lazy-pages daemon and the restore are separate criu invocations
//     because lazy-pages mode is a daemon, not a swrk-RPC call; we can't
//     drive it through the existing go-criu client.

package executor

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sys/unix"
)

// PipelineCBinaries lists the executables this path needs on PATH or
// at the configured absolute locations. Overridable via env for tests.
type pipelineCBinaries struct {
	criu      string // criu binary (lazy-pages + restore)
	streamer  string // criu-stream-fetch binary
}

func resolvePipelineCBinaries() pipelineCBinaries {
	b := pipelineCBinaries{
		criu:     "/usr/local/sbin/criu",
		streamer: "/usr/local/sbin/criu-stream-fetch",
	}
	if v := os.Getenv("CRIU_BIN"); v != "" {
		b.criu = v
	}
	if v := os.Getenv("CRIU_STREAM_FETCH_BIN"); v != "" {
		b.streamer = v
	}
	return b
}

// PipelineCOptions configures a single Pipeline C restore.
type PipelineCOptions struct {
	// ImagesDir is the CRIU image directory the restore reads from
	// (rsti / mm-*.img / pagemap / pages-*.img). Pipeline C's
	// per-file objects are NOT read by CRIU; CRIU still reads the
	// metadata images. The streamer manifest below references the
	// per-file objects.
	ImagesDir string

	// WorkDir is CRIU's work directory (logs, lock files).
	WorkDir string

	// ManifestPath is the JSON file the streamer ingests: list of
	// shmem ranges + private ranges with local source paths.
	ManifestPath string

	// CgroupRoot is forwarded to criu restore's --cgroup-root.
	CgroupRoot string

	// ExtraRestoreArgs is appended to the criu restore argv after the
	// standard --stream-restore --lazy-pages set.
	ExtraRestoreArgs []string
}

// PipelineCResult holds timings + observed pids for the triple.
type PipelineCResult struct {
	StreamerPID       int           `json:"streamerPID"`
	LazyPagesPID      int           `json:"lazyPagesPID"`
	RestorePID        int           `json:"restorePID"`
	StreamerSetupDur  time.Duration `json:"streamerSetupDuration"`
	RestoreDur        time.Duration `json:"restoreDuration"`
	TotalDur          time.Duration `json:"totalDuration"`
}

// ExecutePipelineC spawns the (streamer, lazy-pages, restore) triple
// and waits for the restore to exit. Streamer and lazy-pages daemon
// are reaped after restore returns.
func ExecutePipelineC(ctx context.Context, opts PipelineCOptions, log logr.Logger) (*PipelineCResult, error) {
	bins := resolvePipelineCBinaries()
	if _, err := os.Stat(bins.streamer); err != nil {
		return nil, fmt.Errorf("streamer binary %s: %w", bins.streamer, err)
	}
	if _, err := os.Stat(bins.criu); err != nil {
		return nil, fmt.Errorf("criu binary %s: %w", bins.criu, err)
	}
	if _, err := os.Stat(opts.ManifestPath); err != nil {
		return nil, fmt.Errorf("manifest %s: %w", opts.ManifestPath, err)
	}
	if opts.ImagesDir == "" {
		return nil, fmt.Errorf("ImagesDir required for Pipeline C")
	}
	if opts.WorkDir == "" {
		opts.WorkDir = opts.ImagesDir
	}
	if err := os.MkdirAll(opts.WorkDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir workdir %s: %w", opts.WorkDir, err)
	}

	result := &PipelineCResult{}
	totalStart := time.Now()
	defer func() { result.TotalDur = time.Since(totalStart) }()

	// Create the two SOCK_STREAM pairs. Cross-namespace fd passing is
	// not needed because everything runs in the placeholder ns here.
	daemonPair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		return nil, fmt.Errorf("socketpair daemon: %w", err)
	}
	privatePair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		unix.Close(daemonPair[0])
		unix.Close(daemonPair[1])
		return nil, fmt.Errorf("socketpair private: %w", err)
	}

	// Make the "child end" non-CLOEXEC so it survives exec; set CLOEXEC
	// on the parent ends so the criu children don't inherit them and
	// hold the socket open after the streamer exits. (Discovered in
	// the wire_test harness — same discipline applies here.)
	setCLOEXEC(daemonPair[0], false)
	setCLOEXEC(privatePair[0], false)
	setCLOEXEC(daemonPair[1], true)
	setCLOEXEC(privatePair[1], true)

	closeFds := func() {
		for _, fd := range []int{daemonPair[0], daemonPair[1], privatePair[0], privatePair[1]} {
			if fd >= 0 {
				unix.Close(fd)
			}
		}
	}

	// Streamer gets daemonPair[0] + privatePair[0]. The fd number it
	// sees in its own table is the same int because we pass them as
	// ExtraFiles; offset by 3 below.
	streamerSetupStart := time.Now()
	streamerCmd := exec.CommandContext(ctx, bins.streamer,
		"--manifest", opts.ManifestPath,
	)
	streamerCmd.Stdout = os.Stdout
	streamerCmd.Stderr = os.Stderr
	streamerCmd.ExtraFiles = []*os.File{
		os.NewFile(uintptr(daemonPair[0]), "stream-daemon"),
		os.NewFile(uintptr(privatePair[0]), "stream-private"),
	}
	streamerCmd.Env = append(os.Environ(),
		"CRIU_STREAMER_DAEMON_SOCK=3",
		"CRIU_STREAMER_PRIVATE_SOCK=4",
	)
	if err := streamerCmd.Start(); err != nil {
		closeFds()
		return nil, fmt.Errorf("start streamer: %w", err)
	}
	result.StreamerPID = streamerCmd.Process.Pid
	log.Info("Pipeline C streamer started",
		"pid", result.StreamerPID,
		"manifest", opts.ManifestPath,
	)

	// Parent (this process) is done with the [0] ends — close so the
	// streamer is the sole owner and peer-close propagates correctly.
	unix.Close(daemonPair[0])
	unix.Close(privatePair[0])
	daemonPair[0] = -1
	privatePair[0] = -1
	result.StreamerSetupDur = time.Since(streamerSetupStart)

	// Lazy-pages daemon: criu lazy-pages --stream-restore. It will
	// recv abort_fd + eventfds from the streamer over fd 3 (daemon
	// peer end) on startup.
	lazyArgs := []string{
		"lazy-pages",
		"--stream-restore",
		"--work-dir", opts.WorkDir,
		"--address", filepath.Join(opts.WorkDir, "criu-lazy-pages.sock"),
		"--page-server",
	}
	lazyCmd := exec.CommandContext(ctx, bins.criu, lazyArgs...)
	lazyCmd.Stdout = os.Stdout
	lazyCmd.Stderr = os.Stderr
	lazyCmd.ExtraFiles = []*os.File{
		os.NewFile(uintptr(daemonPair[1]), "criu-daemon"),
	}
	lazyCmd.Env = append(os.Environ(), "CRIU_STREAMER_DAEMON_SOCK=3")
	if err := lazyCmd.Start(); err != nil {
		reapStreamer(streamerCmd, log)
		closeFds()
		return nil, fmt.Errorf("start lazy-pages daemon: %w", err)
	}
	result.LazyPagesPID = lazyCmd.Process.Pid
	log.Info("Pipeline C lazy-pages daemon started",
		"pid", result.LazyPagesPID,
		"work_dir", opts.WorkDir,
	)
	unix.Close(daemonPair[1])
	daemonPair[1] = -1

	// criu restore --stream-restore --lazy-pages.
	restoreStart := time.Now()
	restoreArgs := []string{
		"restore",
		"--stream-restore",
		"--lazy-pages",
		"--images-dir", opts.ImagesDir,
		"--work-dir", opts.WorkDir,
		"--address", filepath.Join(opts.WorkDir, "criu-lazy-pages.sock"),
	}
	if opts.CgroupRoot != "" {
		restoreArgs = append(restoreArgs, "--cgroup-root", opts.CgroupRoot)
	}
	restoreArgs = append(restoreArgs, opts.ExtraRestoreArgs...)
	restoreCmd := exec.CommandContext(ctx, bins.criu, restoreArgs...)
	restoreCmd.Stdout = os.Stdout
	restoreCmd.Stderr = os.Stderr
	restoreCmd.ExtraFiles = []*os.File{
		os.NewFile(uintptr(privatePair[1]), "criu-private"),
	}
	restoreCmd.Env = append(os.Environ(), "CRIU_STREAMER_PRIVATE_SOCK=3")
	if err := restoreCmd.Start(); err != nil {
		_ = lazyCmd.Process.Signal(syscall.SIGTERM)
		_ = lazyCmd.Wait()
		reapStreamer(streamerCmd, log)
		closeFds()
		return nil, fmt.Errorf("start restore: %w", err)
	}
	result.RestorePID = restoreCmd.Process.Pid
	log.Info("Pipeline C restore started",
		"pid", result.RestorePID,
		"images_dir", opts.ImagesDir,
	)
	unix.Close(privatePair[1])
	privatePair[1] = -1

	// Wait on restore first; it owns the lifetime.
	if err := restoreCmd.Wait(); err != nil {
		_ = lazyCmd.Process.Signal(syscall.SIGTERM)
		_ = lazyCmd.Wait()
		reapStreamer(streamerCmd, log)
		return result, fmt.Errorf("criu restore failed: %w", err)
	}
	result.RestoreDur = time.Since(restoreStart)

	// Restore exited cleanly. Lazy-pages should self-exit on UFFD
	// teardown; nudge it after a brief settle window.
	settleCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	go func() {
		<-settleCtx.Done()
		if lazyCmd.ProcessState == nil {
			_ = lazyCmd.Process.Signal(syscall.SIGTERM)
		}
	}()
	if err := lazyCmd.Wait(); err != nil {
		log.V(1).Info("lazy-pages daemon exited", "err", err.Error())
	}
	reapStreamer(streamerCmd, log)

	return result, nil
}

func setCLOEXEC(fd int, on bool) {
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil {
		return
	}
	if on {
		flags |= unix.FD_CLOEXEC
	} else {
		flags &^= unix.FD_CLOEXEC
	}
	_, _ = unix.FcntlInt(uintptr(fd), unix.F_SETFD, flags)
}

func reapStreamer(cmd *exec.Cmd, log logr.Logger) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
		return
	}
	// Streamer exits on its own once the daemon socket closes; give it
	// a beat, then SIGTERM if still alive.
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			log.V(1).Info("streamer exited", "err", err.Error())
		}
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Signal(syscall.SIGTERM)
		<-done
	}
}
