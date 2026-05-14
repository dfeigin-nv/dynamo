package criu

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	criulib "github.com/checkpoint-restore/go-criu/v8"
	criurpc "github.com/checkpoint-restore/go-criu/v8/rpc"
	"github.com/go-logr/logr"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"

	"github.com/ai-dynamo/dynamo/deploy/snapshot/internal/logging"
	"github.com/ai-dynamo/dynamo/deploy/snapshot/internal/types"
)

// writePipelineCManifest walks checkpointPath for pages-*.img files and
// emits a streamer-compatible JSON manifest at <workDir>/pipeline-c-
// manifest.json. Each entry is one pages_img_id with the on-disk path
// as source. Returns the manifest path.
func writePipelineCManifest(checkpointPath, workDir string) (string, error) {
	entries, err := os.ReadDir(checkpointPath)
	if err != nil {
		return "", fmt.Errorf("readdir %s: %w", checkpointPath, err)
	}
	type rangeEntry struct {
		ID     uint32 `json:"id"`
		Size   uint64 `json:"size"`
		Source string `json:"source"`
	}
	var priv []rangeEntry
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "pages-") || !strings.HasSuffix(name, ".img") {
			continue
		}
		idStr := strings.TrimSuffix(strings.TrimPrefix(name, "pages-"), ".img")
		var id uint32
		if _, err := fmt.Sscanf(idStr, "%d", &id); err != nil {
			continue
		}
		src := filepath.Join(checkpointPath, name)
		info, err := os.Stat(src)
		if err != nil {
			return "", fmt.Errorf("stat %s: %w", src, err)
		}
		priv = append(priv, rangeEntry{ID: id, Size: uint64(info.Size()), Source: src})
	}
	if workDir == "" {
		workDir = checkpointPath
	}
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return "", err
	}
	out := filepath.Join(workDir, "pipeline-c-manifest.json")
	body := []byte(`{"shmem_ranges":[],"private_ranges":[`)
	first := true
	for _, r := range priv {
		if !first {
			body = append(body, ',')
		}
		first = false
		body = append(body, []byte(fmt.Sprintf(
			`{"id":%d,"size":%d,"source":%q}`, r.ID, r.Size, r.Source))...)
	}
	body = append(body, []byte("]}")...)
	if err := os.WriteFile(out, body, 0o644); err != nil {
		return "", err
	}
	return out, nil
}

// spawnPipelineCStreamer forks the criu-stream-fetch binary with one end
// of a Unix SOCK_STREAM socketpair attached as inherited fd 3, and
// returns the swrk-side end for go-criu to pass to the spawned CRIU.
//
// Streamer fd 3 (its private socket end) is non-CLOEXEC so it survives
// exec; the swrk end has CLOEXEC stripped only on the way into criu
// (handled by go-criu's ExtraFiles plumbing).
func spawnPipelineCStreamer(manifest string, log logr.Logger) (*exec.Cmd, *os.File, error) {
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("socketpair: %w", err)
	}
	streamerEnd := os.NewFile(uintptr(pair[0]), "stream-priv-streamer")
	swrkEnd := os.NewFile(uintptr(pair[1]), "stream-priv-swrk")

	const streamerBin = "/usr/local/sbin/criu-stream-fetch"
	if _, err := os.Stat(streamerBin); err != nil {
		streamerEnd.Close()
		swrkEnd.Close()
		return nil, nil, fmt.Errorf("streamer binary missing: %w", err)
	}

	cmd := exec.Command(streamerBin, "--manifest", manifest)
	cmd.Env = append(os.Environ(), "CRIU_STREAMER_PRIVATE_SOCK=3")
	cmd.ExtraFiles = []*os.File{streamerEnd}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		streamerEnd.Close()
		swrkEnd.Close()
		return nil, nil, fmt.Errorf("start streamer: %w", err)
	}
	streamerEnd.Close() // parent's copy; child has the inherited fd 3
	log.Info("Pipeline C streamer started",
		"pid", cmd.Process.Pid, "manifest", manifest)
	return cmd, swrkEnd, nil
}

// RestoreLogFilename is the CRIU restore log filename (also used by executor/restore.go).
const RestoreLogFilename = "restore.log"

const (
	netNsPath        = "/proc/1/ns/net"
	placeholderFDDir = "/proc/1/fd"
)

// NetNsPath is the per-pod network namespace inode path the agent inherits
// across the placeholder boundary. Exported for internal/criu/streams3.
const NetNsPath = netNsPath

// RegisterInheritFDs is the exported alias of registerInheritFDs for cross-package
// callers (e.g. internal/criu/streams3) that share the same FD lifetime contract.
func RegisterInheritFDs(c *criulib.Criu, stdioFDs []string, log logr.Logger) []*os.File {
	return registerInheritFDs(c, stdioFDs, log)
}

// CloseFiles is the exported alias of closeFiles.
func CloseFiles(files []*os.File) {
	closeFiles(files)
}

// RestoreNotify is the exported alias of the unexported restoreNotify type for
// cross-package callers (e.g. internal/criu/streams3) that need to drive a
// go-criu Restore call directly.
type RestoreNotify = restoreNotify

// NewRestoreNotify constructs a RestoreNotify bound to log. Callers read the
// restored PID via the returned struct's RestoredPID method after Restore.
func NewRestoreNotify(log logr.Logger) *RestoreNotify {
	return &restoreNotify{log: log}
}

// RestoredPID returns the PID reported by CRIU's PostRestore callback.
func (n *restoreNotify) RestoredPID() int32 {
	return n.restoredPID
}

// ExecuteRestore opens the image/work directory FDs, configures inherited
// resources, and calls go-criu Restore. Returns the namespace-relative PID.
func ExecuteRestore(
	criuOpts *criurpc.CriuOpts,
	m *types.CheckpointManifest,
	checkpointPath string,
	log logr.Logger,
) (int32, error) {
	settings := m.CRIUDump.CRIU

	// Open image dir FD
	imageDir, imageDirFD, err := openPathForCRIU(checkpointPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open image directory: %w", err)
	}
	defer imageDir.Close()
	criuOpts.ImagesDirFd = proto.Int32(imageDirFD)

	// Open work dir FD
	if settings.WorkDir != "" {
		if err := os.MkdirAll(settings.WorkDir, 0755); err != nil {
			return 0, fmt.Errorf("failed to create CRIU work directory: %w", err)
		}
		workDirFile, workDirFD, err := openPathForCRIU(settings.WorkDir)
		if err != nil {
			return 0, fmt.Errorf("failed to open CRIU work directory: %w", err)
		}
		defer workDirFile.Close()
		criuOpts.WorkDirFd = proto.Int32(workDirFD)
	}

	c := criulib.MakeCriu()
	if _, err := os.Stat(settings.BinaryPath); err != nil {
		return 0, fmt.Errorf("criu binary not found at %s: %w", settings.BinaryPath, err)
	}
	c.SetCriuPath(settings.BinaryPath)

	netNsFile, err := os.Open(netNsPath)
	if err != nil {
		return 0, fmt.Errorf("failed to open net NS at %s: %w", netNsPath, err)
	}
	defer netNsFile.Close()
	c.AddInheritFd("extNetNs", netNsFile)

	inheritedFiles := registerInheritFDs(c, m.K8s.StdioFDs, log)
	defer closeFiles(inheritedFiles)

	// Pipeline C: auto-generate streamer manifest from the local CRIU
	// images directory, spawn the streamer, hand its swrk-side socket
	// end to go-criu. Setup runs inside the placeholder namespace so
	// cross-ns fd passing is unnecessary.
	var streamer *exec.Cmd
	if criuOpts.GetStreamRestore() {
		manifest, err := writePipelineCManifest(checkpointPath, settings.WorkDir)
		if err != nil {
			return 0, fmt.Errorf("Pipeline C manifest: %w", err)
		}
		s, sockSwrk, err := spawnPipelineCStreamer(manifest, log)
		if err != nil {
			return 0, fmt.Errorf("Pipeline C streamer spawn: %w", err)
		}
		streamer = s
		defer func() {
			if streamer != nil && streamer.ProcessState == nil {
				_ = streamer.Process.Signal(syscall.SIGTERM)
				_, _ = streamer.Process.Wait()
			}
		}()
		c.SetStreamPrivateSock(sockSwrk)
		defer sockSwrk.Close()
	}

	notify := &restoreNotify{log: log}
	log.V(1).Info("Executing go-criu Restore call", "stream_restore", criuOpts.GetStreamRestore())
	if err := c.Restore(criuOpts, notify); err != nil {
		log.Error(err, "go-criu Restore returned error")
		logging.LogRestoreErrors(checkpointPath, settings.WorkDir, log)
		return 0, fmt.Errorf("CRIU restore failed: %w", err)
	}

	return notify.restoredPID, nil
}

// BuildRestoreOpts assembles CriuOpts for a CRIU restore from the checkpoint manifest.
// ImagesDirFd and WorkDirFd are left unset — ExecuteRestore opens them at restore time.
func BuildRestoreOpts(m *types.CheckpointManifest, checkpointPath string, cgroupRoot string, log logr.Logger) (*criurpc.CriuOpts, error) {
	extMounts, err := buildRestoreExtMounts(m)
	if err != nil {
		return nil, err
	}
	log.V(1).Info("Generated external mount map set", "ext_mount_count", len(extMounts))

	settings := m.CRIUDump.CRIU
	criuOpts := &criurpc.CriuOpts{
		LogFile: proto.String(RestoreLogFilename),
		Root:    proto.String("/"),
		ExtMnt:  extMounts,
	}
	if err := applyCommonSettings(criuOpts, &settings); err != nil {
		return nil, err
	}

	// Restore-only options
	criuOpts.RstSibling = proto.Bool(settings.RstSibling)
	criuOpts.MntnsCompatMode = proto.Bool(settings.MntnsCompatMode)

	// Pipeline C opt-in via env. When STREAM_MODE=c the agent's
	// ExecuteRestore will spawn the streamer + hand its socket to
	// go-criu, and the swrk-mode CRIU's mem.c will recv per-task
	// pages memfds via the protocol installed in cr_restore_tasks.
	if os.Getenv("STREAM_MODE") == "c" {
		criuOpts.StreamRestore = proto.Bool(true)
	}
	criuOpts.EvasiveDevices = proto.Bool(settings.EvasiveDevices)
	criuOpts.ForceIrmap = proto.Bool(settings.ForceIrmap)

	if cgroupRoot != "" && shouldSetCgroupRoot(criuOpts.GetManageCgroupsMode()) {
		criuOpts.CgRoot = []*criurpc.CgroupRoot{
			{Path: proto.String(cgroupRoot)},
		}
	}

	criuConfPath := filepath.Join(checkpointPath, criuConfFilename)
	if _, err := os.Stat(criuConfPath); err == nil {
		criuOpts.ConfigFile = proto.String(criuConfPath)
	}

	return criuOpts, nil
}

func buildRestoreExtMounts(m *types.CheckpointManifest) ([]*criurpc.ExtMountMap, error) {
	if len(m.CRIUDump.ExtMnt) == 0 {
		return nil, fmt.Errorf("checkpoint manifest is missing criuDump.extMnt")
	}

	restoreMap := map[string]string{"/": "."}
	for _, val := range m.CRIUDump.ExtMnt {
		if val == "" || val == "/" {
			continue
		}
		restoreMap[val] = val
	}
	return toExtMountMaps(restoreMap), nil
}

func registerInheritFDs(c *criulib.Criu, stdioFDs []string, log logr.Logger) []*os.File {
	if len(stdioFDs) == 0 {
		log.V(1).Info("No stdio FD descriptors in manifest, skipping inherit-fd setup")
		return nil
	}

	var openFiles []*os.File
	for i, target := range stdioFDs {
		if !strings.Contains(target, "pipe:") {
			continue
		}
		// stdin (fd 0) is a read-end pipe; stdout/stderr (fd 1, 2) are write-end
		openMode := os.O_WRONLY
		if i == 0 {
			openMode = os.O_RDONLY
		}
		fdPath := fmt.Sprintf("%s/%d", placeholderFDDir, i)
		f, err := os.OpenFile(fdPath, openMode, 0)
		if err != nil {
			log.V(1).Info("Failed to open placeholder stdio FD, skipping", "fd", i, "target", target, "error", err)
			continue
		}
		openFiles = append(openFiles, f)
		c.AddInheritFd(target, f)
	}

	log.V(1).Info("Registered inherited stdio pipes", "count", len(openFiles))
	return openFiles
}

func closeFiles(files []*os.File) {
	for _, file := range files {
		if file != nil {
			file.Close()
		}
	}
}

type restoreNotify struct {
	criulib.NoNotify
	restoredPID int32
	log         logr.Logger
}

func (n *restoreNotify) PreRestore() error {
	n.log.V(1).Info("CRIU pre-restore")
	return nil
}

func (n *restoreNotify) PostRestore(pid int32) error {
	n.restoredPID = pid
	n.log.Info("CRIU post-restore: process restored", "pid", pid)
	return nil
}
