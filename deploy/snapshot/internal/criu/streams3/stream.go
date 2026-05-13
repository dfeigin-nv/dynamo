// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package streams3

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	criulib "github.com/checkpoint-restore/go-criu/v8"
	criurpc "github.com/checkpoint-restore/go-criu/v8/rpc"
	"github.com/go-logr/logr"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	"github.com/ai-dynamo/dynamo/deploy/snapshot/internal/criu"
	snapshotruntime "github.com/ai-dynamo/dynamo/deploy/snapshot/internal/runtime"
	"github.com/ai-dynamo/dynamo/deploy/snapshot/internal/types"
)

// ExecuteDumpS3 performs a CRIU dump and streams the images directly to S3
// via criu-image-streamer. The manifest, rootfs-diff tarball, and deleted-
// files list are embedded inline in the image stream via --ext-file-fds —
// no local checkpoint directory is created.
//
// Shard S3 URIs are: s3URI/hash/img-0.lz4, img-1.lz4, ..., img-(numShards-1).lz4
// (or without .lz4 when S3_SHARD_LZ4=0).
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

	// Temporary dirs: socket dir (tiny — just holds the UNIX socket file).
	socketDir, err := os.MkdirTemp("", "criu-stream-socket-*")
	if err != nil {
		return 0, fmt.Errorf("failed to create socket dir: %w", err)
	}
	defer os.RemoveAll(socketDir)

	// Write criu.conf: libdir/allow-uprobes/skip-in-flight (settings) +
	// ext-mount-map entries (not valid as RPC fields, config file only).
	// WorkDirFd is not set; CRIU defaults to using images_dir as work dir.
	confContent := criu.BuildCRIUConf(settings) + criulib.BuildStreamingCRIUConf(criuOpts)
	if confContent != "" {
		confPath := filepath.Join(socketDir, criu.CRIUConfFilename)
		if err := os.WriteFile(confPath, []byte(confContent), 0644); err != nil {
			return 0, fmt.Errorf("failed to write criu.conf: %w", err)
		}
		criuOpts.ConfigFile = proto.String(confPath)
	}
	criuOpts.LogFile = proto.String(criu.DumpLogFilename)

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
	// manifest.yaml: written immediately (we have the bytes).
	manifestR, manifestW, err := os.Pipe()
	if err != nil {
		closeFDs(shardWriteFDs)
		return 0, fmt.Errorf("failed to create manifest pipe: %w", err)
	}

	// rootfs-diff.tar: written after checkpoint-start (app is stopped).
	rootfsDiffR, rootfsDiffW, err := os.Pipe()
	if err != nil {
		closeFDs(shardWriteFDs)
		manifestR.Close()
		manifestW.Close()
		return 0, fmt.Errorf("failed to create rootfs-diff pipe: %w", err)
	}

	// deleted-files.json: also written after checkpoint-start.
	deletedFilesR, deletedFilesW, err := os.Pipe()
	if err != nil {
		closeFDs(shardWriteFDs)
		manifestR.Close()
		manifestW.Close()
		rootfsDiffR.Close()
		rootfsDiffW.Close()
		return 0, fmt.Errorf("failed to create deleted-files pipe: %w", err)
	}

	// progress pipe: read for socket-init and checkpoint-start signals.
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
	defer progressR.Close()

	// --- Build --ext-file-fds arg ---
	// streamerCmd.ExtraFiles layout:
	//   [shards(0..N-1), manifestR, rootfsDiffR, deletedFilesR, progressW]
	// fd numbers in the streamer child:
	//   shard i           → 3+i
	//   manifestR         → 3+N
	//   rootfsDiffR       → 3+N+1
	//   deletedFilesR     → 3+N+2
	//   progressW         → 3+N+3
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
		"--progress-fd", strconv.Itoa(3 + len(shardWriteFDs) + 3),
		"capture",
	}
	streamerCmd := exec.Command(streamerBin, streamerArgs...)
	streamerCmd.ExtraFiles = append(shardWriteFDs,
		manifestR, rootfsDiffR, deletedFilesR, progressW)
	streamerCmd.Stdout = os.Stderr
	streamerCmd.Stderr = os.Stderr

	if err := streamerCmd.Start(); err != nil {
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

	// Close parent copies of FDs handed to the streamer (the child owns them now).
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

	// Wait for socket-init before starting CRIU.
	select {
	case <-socketReady:
	case <-time.After(30 * time.Second):
		streamerCmd.Process.Kill()
		rootfsDiffW.Close()
		deletedFilesW.Close()
		return 0, fmt.Errorf("timeout waiting for criu-image-streamer socket-init")
	}

	// captureCancel is closed on the error path so the goroutines below do
	// not deadlock waiting on checkpointStarted if CRIU fails before emitting it.
	captureCancel := make(chan struct{})

	// After checkpoint-start: capture rootfs-diff.tar and deleted-files.json.
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
		data, err := snapshotruntime.CollectWhiteoutsJSON(upperDir)
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

	// Wait for rootfs capture to complete.
	captureWg.Wait()
	if captureErr != nil {
		streamerCmd.Process.Kill()
		return 0, captureErr
	}

	// Wait for streamer to finish flushing to the shard pipes.
	if err := streamerCmd.Wait(); err != nil {
		return 0, fmt.Errorf("criu-image-streamer failed: %w", err)
	}

	// Wait for all S3 shard upload processes to finish. The streamer exiting
	// means all data was written into the shard pipes, but `aws s3 cp` may
	// still be finalizing multipart upload API calls; defer killCmds would
	// race and kill them before the uploads complete.
	for i, cmd := range shardCmds {
		if err := cmd.Wait(); err != nil {
			return 0, fmt.Errorf("S3 shard %d upload failed: %w", i, err)
		}
	}

	// Also upload manifest.yaml as a standalone S3 object so the agent can
	// read it before starting streaming restore (for CUDA device map building).
	if err := UploadManifestS3(manifest, s3URI, hash); err != nil {
		return 0, fmt.Errorf("failed to upload manifest to S3: %w", err)
	}

	log.Info("CRIU dump S3 streaming completed", "duration", dumpDuration)
	return dumpDuration, nil
}

// ExecuteRestoreS3 downloads CRIU images from S3 via criu-image-streamer and
// restores the process. Returns the checkpoint manifest (read from embedded
// stream) and the restored init PID.
func ExecuteRestoreS3(
	s3URI, hash string,
	numShards int,
	cgroupRoot string,
	log logr.Logger,
) (*types.CheckpointManifest, int32, error) {
	if numShards <= 0 {
		numShards = defaultS3Shards
	}

	// Small local dirs for socket and CRIU work files.
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
	manifestPR, manifestPW, err := os.Pipe()
	if err != nil {
		closeFDs(shardReadFDs)
		return nil, 0, fmt.Errorf("failed to create manifest pipe: %w", err)
	}
	defer manifestPR.Close()

	rootfsDiffPR, rootfsDiffPW, err := os.Pipe()
	if err != nil {
		closeFDs(shardReadFDs)
		manifestPR.Close()
		manifestPW.Close()
		return nil, 0, fmt.Errorf("failed to create rootfs-diff pipe: %w", err)
	}
	defer rootfsDiffPR.Close()

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
		closeFDs(shardReadFDs)
		manifestPW.Close()
		rootfsDiffPW.Close()
		deletedFilesPW.Close()
		progressPW.Close()
		return nil, 0, fmt.Errorf("failed to start criu-image-streamer: %w", err)
	}

	// Close parent copies of FDs handed to the streamer (the child owns them now).
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
			// tar may exit non-zero if some files fail (best-effort).
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

	// streamerWait captures the exit status of criu-image-streamer exactly
	// once, shared between the socket-init wait below and the final wait
	// after CRIU restore.
	streamerWait := make(chan error, 1)
	go func() { streamerWait <- streamerCmd.Wait() }()

	// Wait for manifest to be available (need it for criuOpts).
	<-manifestDone
	if manifestErr != nil {
		streamerCmd.Process.Kill()
		return nil, 0, manifestErr
	}

	// Wait for socket-init before starting CRIU.
	// criu-image-streamer must buffer ALL shard data before emitting
	// socket-init (CRIU reads files in a different order than they were
	// written, so random access into the in-memory store is required). For
	// large checkpoints this can take many minutes; no fixed timeout — bail
	// only if the streamer dies.
	select {
	case <-socketReady:
		log.Info("S3 shard download + streamer drain complete (socket-init received)")
	case err := <-streamerWait:
		if err != nil {
			return nil, 0, fmt.Errorf("criu-image-streamer exited before socket-init: %w", err)
		}
		return nil, 0, fmt.Errorf("criu-image-streamer exited (success) before socket-init")
	}

	// Wait for rootfs-diff to be fully applied before CRIU restore.
	rootfsWg.Wait()
	log.Info("Rootfs diff applied, starting CRIU restore")

	criuOpts, err := criu.BuildRestoreOpts(manifest, socketDir, cgroupRoot, log)
	if err != nil {
		return nil, 0, err
	}

	// Write criu.conf: libdir/allow-uprobes/skip-in-flight + ext-mount-map (config file only).
	manifestSettings := &manifest.CRIUDump.CRIU
	confContent := criu.BuildCRIUConf(manifestSettings)
	if confContent != "" {
		confPath := filepath.Join(socketDir, criu.CRIUConfFilename)
		if err := os.WriteFile(confPath, []byte(confContent), 0644); err != nil {
			return nil, 0, fmt.Errorf("failed to write criu.conf: %w", err)
		}
		criuOpts.ConfigFile = proto.String(confPath)
	}

	// Point CRIU's work dir at /var/criu-work so restore.log is readable via
	// "kubectl exec ... -- cat /var/criu-work/restore.log" for timing analysis.
	// OpenPathForCRIU clears CLOEXEC so the fd is directly inherited by CRIU.
	if err := os.MkdirAll(criuWorkDir, 0755); err != nil {
		log.Error(err, "failed to create CRIU work dir, restore.log unavailable")
	} else if workDirFile, workDirFD, err := criu.OpenPathForCRIU(criuWorkDir); err != nil {
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
	defer criu.CloseFiles(inheritedFiles)

	// --- CRIU restore via memfd symlinks (no in-stream restore reads) ---
	// The streamer wrote image files into memfds and created symlinks in
	// socketDir. CRIU opens them as regular seekable files — preadv/AIO/
	// parallel memfd all work.
	criuOpts.ImagesDir = proto.String(socketDir)
	criuOpts.ImagesDirFd = proto.Int32(-1) // required field; -1 = use ImagesDir string

	notify := criu.NewRestoreNotify(log)
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
	restoredPID := notify.RestoredPID()
	log.Info("CRIU restore completed", "duration", time.Since(restoreStart), "restored_pid", restoredPID)

	// Kill the streamer — in memfd mode it parks after socket-init, waiting
	// to be killed. In pipe-serve mode it exits naturally, but killing is
	// harmless either way.
	streamerCmd.Process.Kill()
	if err := <-streamerWait; err != nil && !strings.Contains(err.Error(), "signal: killed") {
		log.Error(err, "criu-image-streamer serve exited with error (restore complete, ignoring)")
	}

	// Apply deleted files.
	deletedFilesWg.Wait()
	if len(deletedFilesData) > 0 {
		if err := snapshotruntime.ApplyDeletedFilesFromBytes(deletedFilesData, "/", log); err != nil {
			log.Error(err, "Failed to apply deleted files")
		}
	}

	log.Info("CRIU restore S3 streaming completed", "restored_pid", restoredPID)
	return manifest, restoredPID, nil
}

// tarOverlayToWriter runs `tar` over the overlay upperDir and writes the
// archive bytes to w. Used by ExecuteDumpS3 to stream rootfs-diff.tar into
// the streamer's ext-file pipe without ever materializing the tarball.
func tarOverlayToWriter(upperDir string, exclusions types.OverlaySettings,
	bindMountDests []string, w io.Writer) error {

	tarArgs := []string{"--xattrs"}
	for _, excl := range snapshotruntime.BuildExclusions(exclusions) {
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

// registerNetNsForRestore registers the network namespace and stdio inherit
// FDs with the go-criu client. Returns all opened files; caller must defer
// criu.CloseFiles on the returned slice.
func registerNetNsForRestore(c *criulib.Criu, m *types.CheckpointManifest, log logr.Logger) ([]*os.File, error) {
	netNsFile, err := os.Open(criu.NetNsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open net NS at %s: %w", criu.NetNsPath, err)
	}
	c.AddInheritFd("extNetNs", netNsFile)
	stdioFiles := criu.RegisterInheritFDs(c, m.K8s.StdioFDs, log)
	return append([]*os.File{netNsFile}, stdioFiles...), nil
}
