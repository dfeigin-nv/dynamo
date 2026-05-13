// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package streams3

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	criurpc "github.com/checkpoint-restore/go-criu/v8/rpc"
	"github.com/go-logr/logr"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"

	"github.com/ai-dynamo/dynamo/deploy/snapshot/internal/criu"
	snapshotruntime "github.com/ai-dynamo/dynamo/deploy/snapshot/internal/runtime"
	"github.com/ai-dynamo/dynamo/deploy/snapshot/internal/types"
)

const (
	// directS3TmpfsMount is a dedicated tmpfs for S3 checkpoint images.
	// Must NOT be /dev/shm — that gets unmounted before CRIU restore.
	directS3TmpfsMount = "/run/criu-s3-images"
)

// ExecuteDumpS3Direct performs a CRIU dump to a tmpfs directory, then uploads
// individual files to S3 using s5cmd. No criu-image-streamer involved.
//
// This path bypasses the streamer's single-threaded demux: s5cmd parallelizes
// across files AND within large files via byte-range GETs, so it can saturate
// available S3 bandwidth on a wide instance. The trade-off is a full local
// materialization of the dump in tmpfs before upload starts.
func ExecuteDumpS3Direct(
	criuOpts *criurpc.CriuOpts,
	settings *types.CRIUSettings,
	manifest *types.CheckpointManifest,
	upperDir string,
	s3URI, hash string,
	log logr.Logger,
) (time.Duration, error) {
	// Mount point already exists at agent boot; per-dump dir lives under it.
	tmpDir := filepath.Join(directS3TmpfsMount, hash)
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create tmpfs dump dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	// Write criu.conf with libdir/allow-uprobes/skip-in-flight options
	if confContent := criu.BuildCRIUConf(settings); confContent != "" {
		confPath := filepath.Join(tmpDir, criu.CRIUConfFilename)
		if err := os.WriteFile(confPath, []byte(confContent), 0644); err != nil {
			return 0, fmt.Errorf("failed to write criu.conf: %w", err)
		}
		criuOpts.ConfigFile = proto.String(confPath)
	}
	criuOpts.LogFile = proto.String(criu.DumpLogFilename)

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
	dumpDuration, err := criu.ExecuteDump(criuOpts, tmpDir, settings, log)
	if err != nil {
		return 0, fmt.Errorf("CRIU dump failed: %w", err)
	}
	log.Info("CRIU dump completed", "duration", dumpDuration)

	// Capture rootfs diff
	if upperDir != "" {
		log.Info("Capturing rootfs diff")
		if _, err := snapshotruntime.CaptureRootfsDiff(upperDir, tmpDir,
			manifest.Overlay.Exclusions, manifest.Overlay.BindMountDests); err != nil {
			return 0, fmt.Errorf("failed to capture rootfs diff: %w", err)
		}
		if _, err := snapshotruntime.CaptureDeletedFiles(upperDir, tmpDir); err != nil {
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

// ExecuteRestoreS3Direct downloads individual checkpoint files from S3 to a
// tmpfs directory, then restores using the standard PVC-style directory
// restore path. No criu-image-streamer involved — s5cmd parallelizes the
// download.
func ExecuteRestoreS3Direct(
	s3URI, hash string,
	cgroupRoot string,
	log logr.Logger,
) (*types.CheckpointManifest, int32, error) {
	// Mount dedicated tmpfs (must survive the /dev/shm unmount before CRIU restore)
	if err := os.MkdirAll(directS3TmpfsMount, 0755); err != nil {
		return nil, 0, fmt.Errorf("failed to create tmpfs mount point: %w", err)
	}
	if err := syscall.Mount("tmpfs", directS3TmpfsMount, "tmpfs", 0, "size=90%"); err != nil {
		return nil, 0, fmt.Errorf("failed to mount tmpfs at %s: %w", directS3TmpfsMount, err)
	}
	defer func() {
		_ = syscall.Unmount(directS3TmpfsMount, 0)
		_ = os.RemoveAll(directS3TmpfsMount)
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

	// Measure downloaded size for throughput logging
	var totalBytes int64
	_ = filepath.Walk(tmpDir, func(_ string, info os.FileInfo, _ error) error {
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
	if err := snapshotruntime.ApplyRootfsDiff(tmpDir, "/", log); err != nil {
		return nil, 0, fmt.Errorf("rootfs diff failed: %w", err)
	}
	if err := snapshotruntime.ApplyDeletedFiles(tmpDir, "/", log); err != nil {
		log.Error(err, "Failed to apply deleted files (best-effort)")
	}

	// Build CRIU opts from manifest
	criuOpts, err := criu.BuildRestoreOpts(m, tmpDir, cgroupRoot, log)
	if err != nil {
		return nil, 0, err
	}

	// Point CRIU work dir at /var/criu-work for restore.log access via kubectl exec
	if err := os.MkdirAll(criuWorkDir, 0755); err != nil {
		log.Error(err, "failed to create CRIU work dir")
	} else if workDirFile, workDirFD, err := criu.OpenPathForCRIU(criuWorkDir); err != nil {
		log.Error(err, "failed to open CRIU work dir")
	} else {
		defer workDirFile.Close()
		criuOpts.WorkDirFd = proto.Int32(workDirFD)
	}

	// CRIU restore from tmpfs directory (same as PVC path)
	log.Info("Executing CRIU restore from tmpfs", "dir", tmpDir)
	restoredPID, err := criu.ExecuteRestore(criuOpts, m, tmpDir, log)
	if err != nil {
		return nil, 0, err
	}

	log.Info("CRIU restore S3 direct completed", "restored_pid", restoredPID)
	return m, restoredPID, nil
}
