package criu

// stream_s3.go — S3 streaming checkpoint/restore using criu-image-streamer.
//
// These functions checkpoint/restore CRIU images, rootfs diff, deleted-files list,
// and manifest directly to/from S3 via criu-image-streamer pipes with NO local tmpdir
// for checkpoint data. All large data flows through Unix pipes.
//
// Small local directories used:
//   - socketDir (a few bytes — UNIX socket file + CRIU conf/log files)
//
// criu-image-streamer encodes external files (manifest.yaml, rootfs-diff.tar,
// deleted-files.json) inline in the image stream via --ext-file-fds.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	criulib "github.com/checkpoint-restore/go-criu/v8"
	criurpc "github.com/checkpoint-restore/go-criu/v8/rpc"
	"github.com/go-logr/logr"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	"github.com/ai-dynamo/dynamo/deploy/snapshot/pkg/common"
	"github.com/ai-dynamo/dynamo/deploy/snapshot/pkg/types"
)

const (
	// Name for criu-image-streamer binary (must be on PATH in agent image)
	streamerBin = "criu-image-streamer"

	// criuWorkDir is where CRIU writes restore.log; readable via kubectl exec.
	criuWorkDir = "/var/criu-work"

	// External file names embedded in the stream
	streamManifestName     = "manifest.yaml"
	streamRootfsDiffName   = "rootfs-diff.tar"
	streamDeletedFilesName = "deleted-files.json"

	// defaultS3Shards is the default number of parallel S3 upload/download shards.
	defaultS3Shards = 16
)

// ExecuteDumpS3 performs a CRIU dump and streams the images directly to S3
// via criu-image-streamer. The manifest, rootfs-diff, and deleted-files are
// embedded inline in the stream — no local checkpoint directory is created.
//
// Shard S3 URIs: s3URI/hash/img-0.lz4, img-1.lz4, ..., img-(numShards-1).lz4
func ExecuteDumpS3(
	criuOpts *criurpc.CriuOpts,
	settings *types.CRIUSettings,
	manifest *types.CheckpointManifest,
	upperDir string,
	s3URI, hash string,
	numShards int,
	log logr.Logger,
) (time.Duration, error) {
	if numShards <= 0 {
		numShards = defaultS3Shards
	}

	// Temporary dirs: socket dir (tiny — just holds the UNIX socket file)
	socketDir, err := os.MkdirTemp("", "criu-stream-socket-*")
	if err != nil {
		return 0, fmt.Errorf("failed to create socket dir: %w", err)
	}
	defer os.RemoveAll(socketDir)

	// Write criu.conf: libdir/allow-uprobes/skip-in-flight (settings) +
	// ext-mount-map entries (not valid as RPC fields, config file only).
	// WorkDirFd is not set; CRIU defaults to using images_dir as work dir.
	confContent := buildCRIUConf(settings) + criulib.BuildStreamingCRIUConf(criuOpts)
	if confContent != "" {
		confPath := filepath.Join(socketDir, criuConfFilename)
		if err := os.WriteFile(confPath, []byte(confContent), 0644); err != nil {
			return 0, fmt.Errorf("failed to write criu.conf: %w", err)
		}
		criuOpts.ConfigFile = proto.String(confPath)
	}
	criuOpts.LogFile = proto.String(dumpLogFilename)

	// Marshal manifest to bytes
	manifestBytes, err := yaml.Marshal(manifest)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal manifest: %w", err)
	}

	// --- Set up S3 shard upload pipes ---
	shardWriteFDs, shardCmds, shardFDStr, err := startS3UploadPipes(s3URI, hash, numShards)
	if err != nil {
		return 0, err
	}
	defer killCmds(shardCmds)

	// --- Set up external file pipes ---
	// manifest.yaml: written immediately (we have the bytes)
	manifestR, manifestW, err := os.Pipe()
	if err != nil {
		closeFDs(shardWriteFDs)
		return 0, fmt.Errorf("failed to create manifest pipe: %w", err)
	}

	// rootfs-diff.tar: written after checkpoint-start (app is stopped)
	rootfsDiffR, rootfsDiffW, err := os.Pipe()
	if err != nil {
		closeFDs(shardWriteFDs)
		manifestR.Close()
		manifestW.Close()
		return 0, fmt.Errorf("failed to create rootfs-diff pipe: %w", err)
	}

	// deleted-files.json: also written after checkpoint-start
	deletedFilesR, deletedFilesW, err := os.Pipe()
	if err != nil {
		closeFDs(shardWriteFDs)
		manifestR.Close()
		manifestW.Close()
		rootfsDiffR.Close()
		rootfsDiffW.Close()
		return 0, fmt.Errorf("failed to create deleted-files pipe: %w", err)
	}

	// progress pipe: read for socket-init and checkpoint-start signals
	progressR, progressW, err := os.Pipe()
	if err != nil {
		closeFDs(shardWriteFDs)
		manifestR.Close()
		manifestW.Close()
		rootfsDiffR.Close()
		rootfsDiffW.Close()
		deletedFilesR.Close()
		deletedFilesW.Close()
		return 0, fmt.Errorf("failed to create progress pipe: %w", err)
	}
	// progressR is read by the scanner goroutine below; close on return.
	defer progressR.Close()

	// --- Build ext-file-fds arg ---
	// streamerCmd.ExtraFiles layout: [shards(0..N-1), manifestR, rootfsDiffR, deletedFilesR, progressW]
	// fd numbers in child: shard i → 3+i; manifestR → 3+N; rootfsDiffR → 3+N+1; etc.
	extFileFDs := fmt.Sprintf(
		"%s:%d,%s:%d,%s:%d",
		streamManifestName, 3+len(shardWriteFDs),
		streamRootfsDiffName, 3+len(shardWriteFDs)+1,
		streamDeletedFilesName, 3+len(shardWriteFDs)+2,
	)

	// --- Start criu-image-streamer capture ---
	streamerArgs := []string{
		"--images-dir", socketDir,
		"--shard-fds", shardFDStr,
		"--ext-file-fds", extFileFDs,
		"--progress-fd", strconv.Itoa(3 + len(shardWriteFDs) + 3), // progressW
		"capture",
	}
	streamerCmd := exec.Command(streamerBin, streamerArgs...)
	// ExtraFiles: shards + manifest + rootfs-diff + deleted-files + progressW
	streamerCmd.ExtraFiles = append(shardWriteFDs,
		manifestR, rootfsDiffR, deletedFilesR, progressW)
	streamerCmd.Stdout = os.Stderr
	streamerCmd.Stderr = os.Stderr

	if err := streamerCmd.Start(); err != nil {
		// Close all FDs not yet closed (write ends not yet owned by goroutines).
		closeFDs(shardWriteFDs)
		manifestR.Close()
		manifestW.Close()
		rootfsDiffR.Close()
		rootfsDiffW.Close()
		deletedFilesR.Close()
		deletedFilesW.Close()
		progressW.Close()
		return 0, fmt.Errorf("failed to start criu-image-streamer: %w", err)
	}

	// Close parent copies of FDs we've handed to streamer (they are now owned by
	// the streamer child process; the parent must not hold them open).
	closeFDs(shardWriteFDs)
	manifestR.Close()
	rootfsDiffR.Close()
	deletedFilesR.Close()
	progressW.Close()

	// --- Write manifest immediately (small, fits in pipe buffer) ---
	go func() {
		defer manifestW.Close()
		if _, err := manifestW.Write(manifestBytes); err != nil {
			log.Error(err, "Failed to write manifest to pipe")
		}
	}()

	// --- Monitor progress pipe; start rootfs capture after checkpoint-start ---
	checkpointStarted := make(chan struct{})
	socketReady := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(progressR)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			switch line {
			case "socket-init":
				close(socketReady)
			case "checkpoint-start":
				close(checkpointStarted)
			default:
				if strings.HasPrefix(line, "{") {
					logShardStats(line, log)
				}
			}
		}
	}()

	// Wait for socket-init before starting CRIU
	select {
	case <-socketReady:
	case <-time.After(30 * time.Second):
		streamerCmd.Process.Kill()
		// rootfsDiffW and deletedFilesW are not yet owned by any goroutine.
		rootfsDiffW.Close()
		deletedFilesW.Close()
		return 0, fmt.Errorf("timeout waiting for criu-image-streamer socket-init")
	}

	// captureCancel is closed on the error path so the goroutines below do not
	// deadlock waiting on checkpointStarted if CRIU fails before emitting it.
	captureCancel := make(chan struct{})

	// After checkpoint-start: capture rootfs-diff.tar and deleted-files.json
	var captureWg sync.WaitGroup
	var captureErr error
	captureWg.Add(2)

	go func() {
		defer captureWg.Done()
		defer rootfsDiffW.Close()
		select {
		case <-checkpointStarted:
		case <-captureCancel:
			return
		}
		if upperDir == "" {
			return
		}
		if err := tarOverlayToWriter(upperDir, manifest.Overlay.Exclusions,
			manifest.Overlay.BindMountDests, rootfsDiffW); err != nil {
			log.Error(err, "Failed to stream rootfs-diff.tar")
			captureErr = err
		}
	}()

	go func() {
		defer captureWg.Done()
		defer deletedFilesW.Close()
		select {
		case <-checkpointStarted:
		case <-captureCancel:
			return
		}
		if upperDir == "" {
			return
		}
		data, err := common.CollectWhiteoutsJSON(upperDir)
		if err != nil {
			log.Error(err, "Failed to collect whiteouts")
			return
		}
		if len(data) > 0 {
			if _, err := deletedFilesW.Write(data); err != nil {
				log.Error(err, "Failed to write deleted-files to pipe")
			}
		}
	}()

	// --- Call CRIU dump via swrk Dump() with Stream=true ---
	// Stream=true tells CRIU swrk to call img_streamer_init() and connect to
	// streamer-capture.sock in images_dir, exactly like "--stream" in CLI mode.
	criuOpts.Stream = proto.Bool(true)
	criuOpts.ImagesDir = proto.String(socketDir)
	criuOpts.ImagesDirFd = proto.Int32(-1) // -1 = use ImagesDir string path

	criuClient := criulib.MakeCriu()
	if settings != nil && strings.TrimSpace(settings.BinaryPath) != "" {
		criuClient.SetCriuPath(settings.BinaryPath)
	}

	dumpStart := time.Now()
	if err := criuClient.Dump(criuOpts, nil); err != nil {
		close(captureCancel) // unblock goroutines waiting on checkpointStarted
		streamerCmd.Process.Kill()
		captureWg.Wait()
		return 0, fmt.Errorf("CRIU dump stream failed: %w", err)
	}
	dumpDuration := time.Since(dumpStart)

	// Wait for rootfs capture to complete
	captureWg.Wait()
	if captureErr != nil {
		streamerCmd.Process.Kill()
		return 0, captureErr
	}

	// Wait for streamer to finish uploading to S3
	if err := streamerCmd.Wait(); err != nil {
		return 0, fmt.Errorf("criu-image-streamer failed: %w", err)
	}

	// Wait for all S3 shard upload processes to finish.
	// criu-image-streamer exiting means all data was written to the shard pipes,
	// but aws s3 cp may still be finalizing multipart upload API calls with S3.
	// defer killCmds would race and kill them before the uploads complete.
	for i, cmd := range shardCmds {
		if err := cmd.Wait(); err != nil {
			return 0, fmt.Errorf("S3 shard %d upload failed: %w", i, err)
		}
	}

	// Also upload manifest.yaml as a standalone S3 object so the agent can read it
	// before starting streaming restore (for CUDA device map building).
	if err := UploadManifestS3(manifest, s3URI, hash); err != nil {
		return 0, fmt.Errorf("failed to upload manifest to S3: %w", err)
	}

	log.Info("CRIU dump S3 streaming completed", "duration", dumpDuration)
	return dumpDuration, nil
}

// ExecuteRestoreS3 downloads CRIU images from S3 via criu-image-streamer and
// restores the process. Returns the checkpoint manifest (read from embedded stream)
// and the restored init PID.
func ExecuteRestoreS3(
	s3URI, hash string,
	numShards int,
	cgroupRoot string,
	log logr.Logger,
) (*types.CheckpointManifest, int32, error) {
	if numShards <= 0 {
		numShards = defaultS3Shards
	}

	// Small local dirs for socket and CRIU work files
	socketDir, err := os.MkdirTemp("", "criu-stream-socket-*")
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create socket dir: %w", err)
	}
	defer os.RemoveAll(socketDir)

	// --- Set up S3 shard download pipes ---
	log.Info("Starting S3 shard downloads", "s3URI", s3URI, "hash", hash, "numShards", numShards)
	shardReadFDs, shardCmds, shardFDStr, err := startS3DownloadPipes(s3URI, hash, numShards)
	if err != nil {
		return nil, 0, err
	}
	defer killCmds(shardCmds)

	// --- Set up external file output pipes (streamer writes extracted files to these) ---
	// manifest.yaml: streamer writes → our code reads
	manifestPR, manifestPW, err := os.Pipe()
	if err != nil {
		closeFDs(shardReadFDs)
		return nil, 0, fmt.Errorf("failed to create manifest pipe: %w", err)
	}
	defer manifestPR.Close()

	// rootfs-diff.tar: streamer writes → we pipe to tar
	rootfsDiffPR, rootfsDiffPW, err := os.Pipe()
	if err != nil {
		closeFDs(shardReadFDs)
		manifestPR.Close()
		manifestPW.Close()
		return nil, 0, fmt.Errorf("failed to create rootfs-diff pipe: %w", err)
	}
	defer rootfsDiffPR.Close()

	// deleted-files.json: streamer writes → we read and apply
	deletedFilesPR, deletedFilesPW, err := os.Pipe()
	if err != nil {
		closeFDs(shardReadFDs)
		manifestPR.Close()
		manifestPW.Close()
		rootfsDiffPR.Close()
		rootfsDiffPW.Close()
		return nil, 0, fmt.Errorf("failed to create deleted-files pipe: %w", err)
	}
	defer deletedFilesPR.Close()

	// progress pipe
	progressPR, progressPW, err := os.Pipe()
	if err != nil {
		closeFDs(shardReadFDs)
		manifestPR.Close()
		manifestPW.Close()
		rootfsDiffPR.Close()
		rootfsDiffPW.Close()
		deletedFilesPR.Close()
		deletedFilesPW.Close()
		return nil, 0, fmt.Errorf("failed to create progress pipe: %w", err)
	}
	defer progressPR.Close()

	// --- ext-file-fds for serve side (streamer writes TO these FDs) ---
	extFileFDs := fmt.Sprintf(
		"%s:%d,%s:%d,%s:%d",
		streamManifestName, 3+len(shardReadFDs),
		streamRootfsDiffName, 3+len(shardReadFDs)+1,
		streamDeletedFilesName, 3+len(shardReadFDs)+2,
	)

	// --- Start criu-image-streamer serve ---
	streamerArgs := []string{
		"--images-dir", socketDir,
		"--shard-fds", shardFDStr,
		"--ext-file-fds", extFileFDs,
		"--progress-fd", strconv.Itoa(3 + len(shardReadFDs) + 3),
		"serve-memfd",
	}
	streamerCmd := exec.Command(streamerBin, streamerArgs...)
	streamerCmd.ExtraFiles = append(shardReadFDs,
		manifestPW, rootfsDiffPW, deletedFilesPW, progressPW)
	streamerCmd.Stdout = os.Stderr
	streamerCmd.Stderr = os.Stderr

	if err := streamerCmd.Start(); err != nil {
		// Close all FDs not yet closed.
		closeFDs(shardReadFDs)
		manifestPW.Close()
		rootfsDiffPW.Close()
		deletedFilesPW.Close()
		progressPW.Close()
		return nil, 0, fmt.Errorf("failed to start criu-image-streamer: %w", err)
	}

	// Close parent copies of FDs we've handed to streamer (they are now owned by
	// the streamer child process; the parent must not hold them open).
	closeFDs(shardReadFDs)
	manifestPW.Close()
	rootfsDiffPW.Close()
	deletedFilesPW.Close()
	progressPW.Close()

	// --- Read manifest from pipe (small, arrives quickly) ---
	var manifest *types.CheckpointManifest
	var manifestErr error
	manifestDone := make(chan struct{})
	go func() {
		defer close(manifestDone)
		data, err := io.ReadAll(manifestPR)
		if err != nil {
			manifestErr = fmt.Errorf("failed to read manifest from stream: %w", err)
			return
		}
		m := &types.CheckpointManifest{}
		if err := yaml.Unmarshal(data, m); err != nil {
			manifestErr = fmt.Errorf("failed to unmarshal manifest: %w", err)
			return
		}
		manifest = m
	}()

	// --- Apply rootfs-diff concurrently ---
	var rootfsWg sync.WaitGroup
	rootfsWg.Add(1)
	go func() {
		defer rootfsWg.Done()
		log.Info("Applying rootfs diff", "target", "/")
		tarCmd := exec.Command("tar", "--skip-old-files", "-C", "/", "-xf", "-")
		tarCmd.Stdin = rootfsDiffPR
		tarCmd.Stdout = os.Stderr
		tarCmd.Stderr = os.Stderr
		if err := tarCmd.Run(); err != nil {
			// tar may exit non-zero if some files fail (best-effort)
			log.Error(err, "rootfs-diff tar extraction had errors (best-effort)")
		}
		log.Info("Rootfs diff applied successfully")
	}()

	// --- Apply deleted files concurrently ---
	var deletedFilesData []byte
	var deletedFilesWg sync.WaitGroup
	deletedFilesWg.Add(1)
	go func() {
		defer deletedFilesWg.Done()
		var readErr error
		deletedFilesData, readErr = io.ReadAll(deletedFilesPR)
		if readErr != nil {
			log.Error(readErr, "Failed to read deleted-files from stream (best-effort)")
		}
	}()

	// --- Monitor progress for socket-init and shard transfer stats ---
	socketReady := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(progressPR)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "socket-init" {
				close(socketReady)
				continue
			}
			if strings.HasPrefix(line, "{") {
				logShardStats(line, log)
			}
		}
	}()

	// streamerWait captures the exit status of criu-image-streamer exactly once,
	// shared between the socket-init wait below and the final wait after CRIU restore.
	streamerWait := make(chan error, 1)
	go func() { streamerWait <- streamerCmd.Wait() }()

	// Wait for manifest to be available (need it for criuOpts)
	<-manifestDone
	if manifestErr != nil {
		streamerCmd.Process.Kill()
		return nil, 0, manifestErr
	}

	// Wait for socket-init before starting CRIU.
	// criu-image-streamer must buffer ALL shard data before emitting socket-init
	// (CRIU reads files in a different order than they were written, so random
	// access into the in-memory store is required). For large checkpoints this
	// can take many minutes; use no fixed timeout — just bail if the streamer dies.
	select {
	case <-socketReady:
		log.Info("S3 shard download + streamer drain complete (socket-init received)")
	case err := <-streamerWait:
		if err != nil {
			return nil, 0, fmt.Errorf("criu-image-streamer exited before socket-init: %w", err)
		}
		return nil, 0, fmt.Errorf("criu-image-streamer exited (success) before socket-init")
	}

	// Wait for rootfs-diff to be fully applied before CRIU restore
	rootfsWg.Wait()
	log.Info("Rootfs diff applied, starting CRIU restore")

	criuOpts, err := BuildRestoreOpts(manifest, socketDir, cgroupRoot, log)
	if err != nil {
		return nil, 0, err
	}

	// Write criu.conf: libdir/allow-uprobes/skip-in-flight + ext-mount-map (config file only)
	manifestSettings := &manifest.CRIUDump.CRIU
	confContent := buildCRIUConf(manifestSettings)
	if confContent != "" {
		confPath := filepath.Join(socketDir, criuConfFilename)
		if err := os.WriteFile(confPath, []byte(confContent), 0644); err != nil {
			return nil, 0, fmt.Errorf("failed to write criu.conf: %w", err)
		}
		criuOpts.ConfigFile = proto.String(confPath)
	}

	// Point CRIU's work dir at /var/criu-work so restore.log is readable via
	// "kubectl exec ... -- cat /var/criu-work/restore.log" for timing analysis.
	// openPathForCRIU clears CLOEXEC so the fd is directly inherited by CRIU.
	if err := os.MkdirAll(criuWorkDir, 0755); err != nil {
		log.Error(err, "failed to create CRIU work dir, restore.log unavailable")
	} else if workDirFile, workDirFD, err := openPathForCRIU(criuWorkDir); err != nil {
		log.Error(err, "failed to open CRIU work dir, restore.log unavailable")
	} else {
		defer workDirFile.Close()
		criuOpts.WorkDirFd = proto.Int32(workDirFD)
	}

	criuClient := criulib.MakeCriu()
	if strings.TrimSpace(manifestSettings.BinaryPath) != "" {
		criuClient.SetCriuPath(manifestSettings.BinaryPath)
	}
	inheritedFiles, err := registerNetNsForRestore(criuClient, manifest, log)
	if err != nil {
		return nil, 0, err
	}
	defer closeFiles(inheritedFiles)

	// --- CRIU restore via memfd symlinks (no streaming) ---
	// The streamer wrote image files into memfds and created symlinks in socketDir.
	// CRIU opens them as regular seekable files — preadv/AIO/parallel memfd all work.
	criuOpts.ImagesDir = proto.String(socketDir)
	criuOpts.ImagesDirFd = proto.Int32(-1) // required field; -1 = use ImagesDir string

	notify := &restoreNotify{log: log}
	log.Info("Executing go-criu Restore call")
	restoreStart := time.Now()
	if err := criuClient.Restore(criuOpts, notify); err != nil {
		// WorkDirFd points to criuWorkDir; fall back to socketDir if that failed.
		for _, dir := range []string{criuWorkDir, socketDir} {
			if logData, readErr := os.ReadFile(filepath.Join(dir, "restore.log")); readErr == nil {
				log.Info("CRIU restore log", "log", string(logData))
				break
			}
		}
		log.Error(err, "go-criu Restore returned error")
		streamerCmd.Process.Kill()
		return nil, 0, fmt.Errorf("CRIU restore failed: %w", err)
	}
	log.Info("CRIU restore completed", "duration", time.Since(restoreStart), "restored_pid", notify.restoredPID)
	restoredPID := notify.restoredPID

	// Kill the streamer — in memfd mode it parks after socket-init, waiting to be killed.
	// In pipe-serve mode it exits naturally, but killing is harmless.
	streamerCmd.Process.Kill()
	if err := <-streamerWait; err != nil && !strings.Contains(err.Error(), "signal: killed") {
		log.Error(err, "criu-image-streamer serve exited with error (restore complete, ignoring)")
	}

	// Apply deleted files
	deletedFilesWg.Wait()
	if len(deletedFilesData) > 0 {
		if err := common.ApplyDeletedFilesFromBytes(deletedFilesData, "/", log); err != nil {
			log.Error(err, "Failed to apply deleted files")
		}
	}

	log.Info("CRIU restore S3 streaming completed", "restored_pid", restoredPID)
	return manifest, restoredPID, nil
}

// --- Helpers ---

func startS3UploadPipes(s3URI, hash string, numShards int) ([]*os.File, []*exec.Cmd, string, error) {
	var writeFDs []*os.File
	var cmds []*exec.Cmd
	var fdNums []string

	useLZ4 := os.Getenv("S3_SHARD_LZ4") != "0"

	for i := 0; i < numShards; i++ {
		r, w, err := os.Pipe()
		if err != nil {
			closeFDs(writeFDs)
			killCmds(cmds)
			return nil, nil, "", fmt.Errorf("failed to create shard %d pipe: %w", i, err)
		}

		var s3Key, shellCmd string
		if useLZ4 {
			s3Key = fmt.Sprintf("%s/%s/img-%d.lz4", s3URI, hash, i)
			shellCmd = fmt.Sprintf("lz4 - - | aws s3 cp - '%s'", s3Key)
		} else {
			s3Key = fmt.Sprintf("%s/%s/img-%d", s3URI, hash, i)
			shellCmd = fmt.Sprintf("aws s3 cp - '%s'", s3Key)
		}
		cmd := exec.Command("sh", "-c", shellCmd)
		cmd.Stdin = r
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			r.Close()
			w.Close()
			closeFDs(writeFDs)
			killCmds(cmds)
			return nil, nil, "", fmt.Errorf("failed to start S3 upload for shard %d: %w", i, err)
		}
		r.Close() // parent doesn't need the read end

		writeFDs = append(writeFDs, w)
		cmds = append(cmds, cmd)
		fdNums = append(fdNums, strconv.Itoa(3+i)) // fd 3, 4, 5, ...
	}

	return writeFDs, cmds, strings.Join(fdNums, ","), nil
}

func startS3DownloadPipes(s3URI, hash string, numShards int) ([]*os.File, []*exec.Cmd, string, error) {
	var readFDs []*os.File
	var cmds []*exec.Cmd
	var fdNums []string

	useLZ4 := os.Getenv("S3_SHARD_LZ4") != "0"

	for i := 0; i < numShards; i++ {
		r, w, err := os.Pipe()
		if err != nil {
			closeFDs(readFDs)
			killCmds(cmds)
			return nil, nil, "", fmt.Errorf("failed to create shard %d pipe: %w", i, err)
		}

		var s3Key, shellCmd string
		if useLZ4 {
			s3Key = fmt.Sprintf("%s/%s/img-%d.lz4", s3URI, hash, i)
			shellCmd = fmt.Sprintf("s5cmd cat '%s' | lz4 -d - -", s3Key)
		} else {
			s3Key = fmt.Sprintf("%s/%s/img-%d", s3URI, hash, i)
			shellCmd = fmt.Sprintf("s5cmd cat '%s'", s3Key)
		}
		cmd := exec.Command("sh", "-c", shellCmd)
		cmd.Stdout = w
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			r.Close()
			w.Close()
			closeFDs(readFDs)
			killCmds(cmds)
			return nil, nil, "", fmt.Errorf("failed to start S3 download for shard %d: %w", i, err)
		}
		w.Close() // parent doesn't need the write end

		readFDs = append(readFDs, r)
		cmds = append(cmds, cmd)
		fdNums = append(fdNums, strconv.Itoa(3+i))
	}

	return readFDs, cmds, strings.Join(fdNums, ","), nil
}

// tarOverlayToWriter runs tar of the overlay upperDir and writes to w.
func tarOverlayToWriter(upperDir string, exclusions types.OverlaySettings,
	bindMountDests []string, w io.Writer) error {

	tarArgs := []string{"--xattrs"}
	for _, excl := range common.BuildExclusions(exclusions) {
		tarArgs = append(tarArgs, "--exclude="+excl)
	}
	for _, dest := range bindMountDests {
		tarArgs = append(tarArgs, "--exclude=."+dest)
	}
	tarArgs = append(tarArgs, "-C", upperDir, "-cf", "-", ".")

	cmd := exec.Command("tar", tarArgs...)
	cmd.Stdout = w
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("tar overlay failed: %w (stderr: %s)", err, stderr.String())
	}
	return nil
}

// shardStats is the JSON structure emitted by criu-image-streamer on the progress pipe
// after all shards have been transferred.
type shardStats struct {
	Shards []struct {
		Size                 uint64 `json:"size"`
		TransferDurationMs   uint64 `json:"transfer_duration_millis"`
	} `json:"shards"`
}

// logShardStats parses the shard transfer stats JSON and logs per-shard and aggregate throughput.
func logShardStats(line string, log logr.Logger) {
	var stats shardStats
	if err := json.Unmarshal([]byte(line), &stats); err != nil {
		log.V(1).Info("Failed to parse shard stats", "line", line, "err", err)
		return
	}
	var totalBytes, totalMs uint64
	for _, s := range stats.Shards {
		totalBytes += s.Size
		if s.TransferDurationMs > totalMs {
			totalMs = s.TransferDurationMs
		}
	}
	var aggMBps float64
	if totalMs > 0 {
		aggMBps = float64(totalBytes) / float64(totalMs) / 1024 / 1024 * 1000
	}
	log.Info("Shard transfer stats",
		"shards", len(stats.Shards),
		"total_mb", totalBytes/1024/1024,
		"wall_time_ms", totalMs,
		"throughput_mbps", fmt.Sprintf("%.0f", aggMBps),
	)
}

// UploadManifestS3 uploads the manifest.yaml as a standalone S3 object alongside the stream shards.
// The agent downloads this before starting nsrestore to build the CUDA device map.
func UploadManifestS3(manifest *types.CheckpointManifest, s3URI, hash string) error {
	manifestBytes, err := yaml.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("failed to marshal manifest: %w", err)
	}
	s3Key := fmt.Sprintf("%s/%s/%s", s3URI, hash, streamManifestName)
	cmd := exec.Command("sh", "-c", fmt.Sprintf("aws s3 cp - '%s'", s3Key))
	cmd.Stdin = bytes.NewReader(manifestBytes)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to upload manifest to S3: %w", err)
	}
	return nil
}

// DownloadManifestS3 downloads manifest.yaml from S3 and returns the parsed manifest.
// Called by the agent before nsrestore to get CUDA device map info.
func DownloadManifestS3(s3URI, hash string) (*types.CheckpointManifest, error) {
	s3Key := fmt.Sprintf("%s/%s/%s", s3URI, hash, streamManifestName)
	cmd := exec.Command("sh", "-c", fmt.Sprintf("s5cmd cat '%s'", s3Key))
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to download manifest from S3 (%s): %w", s3Key, err)
	}
	m := &types.CheckpointManifest{}
	if err := yaml.Unmarshal(out.Bytes(), m); err != nil {
		return nil, fmt.Errorf("failed to parse manifest: %w", err)
	}
	return m, nil
}

// registerNetNsForRestore registers the network namespace and stdio inherit FDs.
// Returns all opened files; caller must defer closeFiles on the returned slice.
func registerNetNsForRestore(c *criulib.Criu, m *types.CheckpointManifest, log logr.Logger) ([]*os.File, error) {
	netNsFile, err := os.Open(netNsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open net NS at %s: %w", netNsPath, err)
	}
	c.AddInheritFd("extNetNs", netNsFile)
	stdioFiles := registerInheritFDs(c, m.K8s.StdioFDs, log)
	return append([]*os.File{netNsFile}, stdioFiles...), nil
}

func closeFDs(fds []*os.File) {
	for _, f := range fds {
		if f != nil {
			f.Close()
		}
	}
}

func killCmds(cmds []*exec.Cmd) {
	for _, cmd := range cmds {
		if cmd != nil && cmd.Process != nil {
			cmd.Process.Kill()
		}
	}
}

// --- Direct S3 path (no streamer) ---
//
// These functions bypass criu-image-streamer entirely: checkpoint images are stored as
// individual S3 objects and downloaded to a tmpfs directory for restore. This eliminates
// the streamer's single-threaded demux bottleneck — s5cmd parallelizes across files AND
// within large files via byte-range GETs.

const (
	// directS3TmpfsMount is a dedicated tmpfs for S3 checkpoint images.
	// Must NOT be /dev/shm (unmounted before CRIU restore).
	directS3TmpfsMount = "/run/criu-s3-images"
)

// ExecuteDumpS3Direct performs a CRIU dump to a tmpfs directory, then uploads
// individual files to S3 using s5cmd. No streamer involved.
func ExecuteDumpS3Direct(
	criuOpts *criurpc.CriuOpts,
	settings *types.CRIUSettings,
	manifest *types.CheckpointManifest,
	upperDir string,
	s3URI, hash string,
	log logr.Logger,
) (time.Duration, error) {
	// Mount a dedicated tmpfs for the dump
	tmpDir := filepath.Join(directS3TmpfsMount, hash)
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create tmpfs dump dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	// Write criu.conf
	confContent := buildCRIUConf(settings)
	if confContent != "" {
		confPath := filepath.Join(tmpDir, criuConfFilename)
		if err := os.WriteFile(confPath, []byte(confContent), 0644); err != nil {
			return 0, fmt.Errorf("failed to write criu.conf: %w", err)
		}
		criuOpts.ConfigFile = proto.String(confPath)
	}
	criuOpts.LogFile = proto.String(dumpLogFilename)

	// Write manifest
	manifestBytes, err := yaml.Marshal(manifest)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal manifest: %w", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, streamManifestName), manifestBytes, 0644); err != nil {
		return 0, fmt.Errorf("failed to write manifest: %w", err)
	}

	// CRIU dump to tmpDir (same as PVC path)
	log.Info("Executing CRIU dump to tmpfs", "dir", tmpDir)
	dumpDuration, err := ExecuteDump(criuOpts, tmpDir, settings, log)
	if err != nil {
		return 0, fmt.Errorf("CRIU dump failed: %w", err)
	}
	log.Info("CRIU dump completed", "duration", dumpDuration)

	// Capture rootfs diff
	if upperDir != "" {
		log.Info("Capturing rootfs diff")
		if _, err := common.CaptureRootfsDiff(upperDir, tmpDir,
			manifest.Overlay.Exclusions, manifest.Overlay.BindMountDests); err != nil {
			return 0, fmt.Errorf("failed to capture rootfs diff: %w", err)
		}
		if _, err := common.CaptureDeletedFiles(upperDir, tmpDir); err != nil {
			log.Error(err, "Failed to capture deleted files (best-effort)")
		}
	}

	// Upload to S3
	s3Dest := fmt.Sprintf("%s/%s/", s3URI, hash)
	log.Info("Uploading checkpoint to S3", "dest", s3Dest)
	uploadStart := time.Now()
	cmd := exec.Command("s5cmd", "--numworkers", "16", "sync", tmpDir+"/", s3Dest)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return 0, fmt.Errorf("s5cmd upload failed: %w", err)
	}
	log.Info("S3 upload completed", "duration", time.Since(uploadStart))

	return dumpDuration, nil
}

// ExecuteRestoreS3Direct downloads individual checkpoint files from S3 to a tmpfs
// directory, then restores using the standard PVC-style directory restore path.
// No criu-image-streamer involved — s5cmd parallelizes the download.
func ExecuteRestoreS3Direct(
	s3URI, hash string,
	cgroupRoot string,
	log logr.Logger,
) (*types.CheckpointManifest, int32, error) {
	// Mount dedicated tmpfs (survives /dev/shm unmount)
	if err := os.MkdirAll(directS3TmpfsMount, 0755); err != nil {
		return nil, 0, fmt.Errorf("failed to create tmpfs mount point: %w", err)
	}
	if err := syscall.Mount("tmpfs", directS3TmpfsMount, "tmpfs", 0, "size=90%"); err != nil {
		return nil, 0, fmt.Errorf("failed to mount tmpfs at %s: %w", directS3TmpfsMount, err)
	}
	defer func() {
		syscall.Unmount(directS3TmpfsMount, 0)
		os.RemoveAll(directS3TmpfsMount)
	}()

	tmpDir := filepath.Join(directS3TmpfsMount, hash)
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return nil, 0, fmt.Errorf("failed to create restore dir: %w", err)
	}

	// Download all checkpoint files from S3
	s3Src := fmt.Sprintf("%s/%s/*", s3URI, hash)
	log.Info("Downloading checkpoint from S3", "src", s3Src, "dest", tmpDir,
		"cmd", "s5cmd --numworkers 256 cp -c 32")
	downloadStart := time.Now()
	cmd := exec.Command("s5cmd", "--numworkers", "256", "cp", "-c", "32", s3Src, tmpDir+"/")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return nil, 0, fmt.Errorf("s5cmd download failed: %w", err)
	}
	downloadDuration := time.Since(downloadStart)

	// Measure downloaded size
	var totalBytes int64
	filepath.Walk(tmpDir, func(_ string, info os.FileInfo, _ error) error {
		if info != nil && !info.IsDir() {
			totalBytes += info.Size()
		}
		return nil
	})
	throughputMBps := float64(totalBytes) / downloadDuration.Seconds() / 1024 / 1024
	log.Info("S3 download complete",
		"duration", downloadDuration,
		"total_mb", totalBytes/1024/1024,
		"throughput_mbps", fmt.Sprintf("%.0f", throughputMBps),
	)

	// Read manifest
	m, err := types.ReadManifest(tmpDir)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read manifest from %s: %w", tmpDir, err)
	}

	// Apply rootfs diff and deleted files
	if err := common.ApplyRootfsDiff(tmpDir, "/", log); err != nil {
		return nil, 0, fmt.Errorf("rootfs diff failed: %w", err)
	}
	if err := common.ApplyDeletedFiles(tmpDir, "/", log); err != nil {
		log.Error(err, "Failed to apply deleted files (best-effort)")
	}

	// Build CRIU opts from manifest
	criuOpts, err := BuildRestoreOpts(m, tmpDir, cgroupRoot, log)
	if err != nil {
		return nil, 0, err
	}

	// Point CRIU work dir at /var/criu-work for restore.log access
	if err := os.MkdirAll(criuWorkDir, 0755); err != nil {
		log.Error(err, "failed to create CRIU work dir")
	} else if workDirFile, workDirFD, err := openPathForCRIU(criuWorkDir); err != nil {
		log.Error(err, "failed to open CRIU work dir")
	} else {
		defer workDirFile.Close()
		criuOpts.WorkDirFd = proto.Int32(workDirFD)
	}

	// CRIU restore from tmpfs directory (same as PVC path)
	log.Info("Executing CRIU restore from tmpfs", "dir", tmpDir)
	restoredPID, err := ExecuteRestore(criuOpts, m, tmpDir, log)
	if err != nil {
		return nil, 0, err
	}

	log.Info("CRIU restore S3 direct completed", "restored_pid", restoredPID)
	return m, restoredPID, nil
}
