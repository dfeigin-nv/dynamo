package executor

import (
	"context"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	criurpc "github.com/checkpoint-restore/go-criu/v8/rpc"
	"github.com/go-logr/logr"

	"github.com/ai-dynamo/dynamo/deploy/snapshot/pkg/common"
	"github.com/ai-dynamo/dynamo/deploy/snapshot/pkg/criu"
	"github.com/ai-dynamo/dynamo/deploy/snapshot/pkg/cuda"
	"github.com/ai-dynamo/dynamo/deploy/snapshot/pkg/types"
)

// RestoreOptions holds configuration for an in-namespace restore.
type RestoreOptions struct {
	CheckpointPath        string
	CheckpointStorageType string // "pvc" (default) or "s3"
	CheckpointLocation    string // for s3: the S3 URI prefix (without hash)
	CheckpointHash        string // for s3: the checkpoint hash
	CUDADeviceMap         string
	CgroupRoot            string
}

// RestoreInNamespace performs a full restore from inside the target container's namespaces.
func RestoreInNamespace(ctx context.Context, opts RestoreOptions, log logr.Logger) (int, error) {
	restoreStart := time.Now()
	log.Info("Starting nsrestore workflow",
		"storage_type", opts.CheckpointStorageType,
		"checkpoint_path", opts.CheckpointPath,
		"has_cuda_map", opts.CUDADeviceMap != "",
		"cgroup_root", opts.CgroupRoot,
	)

	if opts.CheckpointStorageType == "s3" {
		if os.Getenv("S3_DIRECT") == "1" {
			log.Info("Using S3 direct restore path (S3_DIRECT=1)")
			return restoreInNamespaceS3Direct(ctx, opts, log, restoreStart)
		}
		return restoreInNamespaceS3(ctx, opts, log, restoreStart)
	}

	// --- PVC path ---
	m, err := types.ReadManifest(opts.CheckpointPath)
	if err != nil {
		return 0, fmt.Errorf("failed to read manifest: %w", err)
	}
	log.Info("Loaded checkpoint manifest",
		"ext_mounts", len(m.CRIUDump.ExtMnt),
		"criu_log_level", m.CRIUDump.CRIU.LogLevel,
		"manage_cgroups_mode", m.CRIUDump.CRIU.ManageCgroupsMode,
		"checkpoint_has_cuda", !m.CUDA.IsEmpty(),
	)
	// Phase 1: Configure — build CRIU opts from manifest
	criuOpts, err := criu.BuildRestoreOpts(m, opts.CheckpointPath, opts.CgroupRoot, log)
	if err != nil {
		return 0, err
	}

	// Phase 2: Execute — rootfs, CRIU restore, CUDA restore
	restoredPID, err := executeRestore(ctx, criuOpts, m, opts, log)
	if err != nil {
		return 0, err
	}

	log.Info("nsrestore completed", "restored_pid", restoredPID, "duration", time.Since(restoreStart))
	return restoredPID, nil
}

// restoreInNamespaceS3 performs a streaming S3 restore: all CRIU images, rootfs diff,
// manifest stream directly from S3 via criu-image-streamer. No local staging dir.
func restoreInNamespaceS3(ctx context.Context, opts RestoreOptions, log logr.Logger, restoreStart time.Time) (int, error) {
	// Unmount placeholder's /dev/shm before streaming restore
	if err := syscall.Unmount("/dev/shm", 0); err != nil {
		return 0, fmt.Errorf("failed to unmount /dev/shm before S3 restore: %w", err)
	}

	if err := common.RemountProcSys(true); err != nil {
		return 0, fmt.Errorf("failed to remount /proc/sys read-write for restore: %w", err)
	}
	defer func() {
		if err := common.RemountProcSys(false); err != nil {
			log.Error(err, "Failed to remount /proc/sys read-only after restore")
		}
	}()

	// CheckpointLocation includes the hash — strip it to get the prefix for ExecuteRestoreS3.
	s3Prefix := strings.TrimSuffix(opts.CheckpointLocation, "/"+opts.CheckpointHash)

	// Stream restore from S3 — manifest, rootfs-diff, and CRIU images all flow through pipes
	m, restoredPID, err := criu.ExecuteRestoreS3(s3Prefix, opts.CheckpointHash, 0, opts.CgroupRoot, log)
	if err != nil {
		return 0, fmt.Errorf("S3 streaming restore failed: %w", err)
	}

	// CUDA restore
	if !m.CUDA.IsEmpty() {
		processes, err := common.ReadProcessTable("/proc")
		if err != nil {
			return 0, fmt.Errorf("failed to read restored process table: %w", err)
		}
		restorePIDs, err := common.ResolveManifestPIDsToObservedPIDs(processes, int(restoredPID), m.CUDA.PIDs)
		if err != nil {
			return 0, fmt.Errorf("failed to resolve restored CUDA PIDs: %w", err)
		}
		if err := cuda.RestoreAndUnlockProcessTree(ctx, restorePIDs, opts.CUDADeviceMap, log); err != nil {
			return 0, fmt.Errorf("CUDA restore failed: %w", err)
		}
	}

	log.Info("S3 nsrestore completed", "restored_pid", restoredPID, "duration", time.Since(restoreStart))
	return int(restoredPID), nil
}

// restoreInNamespaceS3Direct downloads individual files from S3 to tmpfs, then
// restores using the standard directory-based CRIU restore (no streamer).
func restoreInNamespaceS3Direct(ctx context.Context, opts RestoreOptions, log logr.Logger, restoreStart time.Time) (int, error) {
	// Unmount placeholder's /dev/shm before restore (CRIU recreates it)
	if err := syscall.Unmount("/dev/shm", 0); err != nil {
		return 0, fmt.Errorf("failed to unmount /dev/shm: %w", err)
	}

	if err := common.RemountProcSys(true); err != nil {
		return 0, fmt.Errorf("failed to remount /proc/sys read-write: %w", err)
	}
	defer func() {
		if err := common.RemountProcSys(false); err != nil {
			log.Error(err, "Failed to remount /proc/sys read-only after restore")
		}
	}()

	s3Prefix := strings.TrimSuffix(opts.CheckpointLocation, "/"+opts.CheckpointHash)

	m, restoredPID, err := criu.ExecuteRestoreS3Direct(s3Prefix, opts.CheckpointHash, opts.CgroupRoot, log)
	if err != nil {
		return 0, fmt.Errorf("S3 direct restore failed: %w", err)
	}

	// CUDA restore
	if !m.CUDA.IsEmpty() {
		processes, err := common.ReadProcessTable("/proc")
		if err != nil {
			return 0, fmt.Errorf("failed to read restored process table: %w", err)
		}
		restorePIDs, err := common.ResolveManifestPIDsToObservedPIDs(processes, int(restoredPID), m.CUDA.PIDs)
		if err != nil {
			return 0, fmt.Errorf("failed to resolve restored CUDA PIDs: %w", err)
		}
		if err := cuda.RestoreAndUnlockProcessTree(ctx, restorePIDs, opts.CUDADeviceMap, log); err != nil {
			return 0, fmt.Errorf("CUDA restore failed: %w", err)
		}
	}

	log.Info("S3 direct nsrestore completed", "restored_pid", restoredPID, "duration", time.Since(restoreStart))
	return int(restoredPID), nil
}

func executeRestore(ctx context.Context, criuOpts *criurpc.CriuOpts, m *types.CheckpointManifest, opts RestoreOptions, log logr.Logger) (int, error) {
	// Apply rootfs diff inside the namespace (target root is /)
	if err := common.ApplyRootfsDiff(opts.CheckpointPath, "/", log); err != nil {
		return 0, fmt.Errorf("rootfs diff failed: %w", err)
	}
	if err := common.ApplyDeletedFiles(opts.CheckpointPath, "/", log); err != nil {
		log.Error(err, "Failed to apply deleted files")
	}

	// Unmount placeholder's /dev/shm so CRIU can recreate tmpfs with checkpointed content
	if err := syscall.Unmount("/dev/shm", 0); err != nil {
		return 0, fmt.Errorf("failed to unmount /dev/shm before restore: %w", err)
	}

	if err := common.RemountProcSys(true); err != nil {
		return 0, fmt.Errorf("failed to remount /proc/sys read-write for restore: %w", err)
	}
	defer func() {
		if err := common.RemountProcSys(false); err != nil {
			log.Error(err, "Failed to remount /proc/sys read-only after restore")
		}
	}()

	// CRIU restore
	restoredPID, err := criu.ExecuteRestore(criuOpts, m, opts.CheckpointPath, log)
	if err != nil {
		return 0, err
	}
	processes, err := common.ReadProcessTable("/proc")
	if err != nil {
		return 0, fmt.Errorf("failed to read restored process table: %w", err)
	}
	log.Info("Restored process table snapshot",
		"proc_root", "/proc",
		"criu_callback_pid", restoredPID,
		"process_count", len(processes),
		"manifest_cuda_pids", m.CUDA.PIDs,
	)
	for _, process := range processes {
		log.Info("Restored process entry",
			"observed_pid", process.ObservedPID,
			"parent_pid", process.ParentPID,
			"outermost_pid", process.OutermostPID,
			"innermost_pid", process.InnermostPID,
			"namespace_pids", process.NamespacePIDs,
			"cmdline", process.Cmdline,
		)
	}

	// CUDA restore — remap checkpoint-time innermost namespace PIDs onto the
	// current visible restored PIDs before invoking cuda-checkpoint.
	if !m.CUDA.IsEmpty() {
		restorePIDs, err := common.ResolveManifestPIDsToObservedPIDs(processes, int(restoredPID), m.CUDA.PIDs)
		if err != nil {
			return 0, fmt.Errorf("failed to resolve restored CUDA PIDs: %w", err)
		}
		log.Info("Resolved manifest CUDA PIDs to current restore PIDs",
			"manifest_cuda_pids", m.CUDA.PIDs,
			"restored_cuda_pids", restorePIDs,
			"criu_callback_pid", restoredPID,
		)
		if err := cuda.RestoreAndUnlockProcessTree(ctx, restorePIDs, opts.CUDADeviceMap, log); err != nil {
			return 0, fmt.Errorf("CUDA restore failed: %w", err)
		}
	}

	return int(restoredPID), nil
}
