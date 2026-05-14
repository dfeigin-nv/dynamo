// Thin dispatch helper that wires the Pipeline C orchestration
// (pipeline_c.go::ExecutePipelineC) into the existing
// restoreInNamespaceS3 entry point. Keeps the Pipeline A/B S3 path
// in nsrestore.go untouched so a runtime env-var flip (STREAM_MODE=c)
// is reversible.
//
// Inputs read from environment for the first cut; once captures emit
// the new manifest format these move into the manifest fields:
//
//   PIPELINE_C_MANIFEST     local JSON manifest path the streamer reads
//   PIPELINE_C_IMAGES_DIR   CRIU image directory (mm-*.img / pagemap)
//   PIPELINE_C_WORK_DIR     CRIU work dir (defaults to images dir)
//   STREAM_MODE=c           top-level enable
//
// Streamer + CRIU lazy-pages + CRIU restore run inside the placeholder
// namespace alongside this nsrestore invocation. Sockets are created
// here and inherited via ExtraFiles + numeric env vars matching the
// recv side in criu/uffd.c (P5a) and criu/mem.c (path-c).

package executor

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr"

	snapshotruntime "github.com/ai-dynamo/dynamo/deploy/snapshot/internal/runtime"
)

func restoreInNamespacePipelineC(ctx context.Context, opts RestoreOptions, log logr.Logger) (*RestoreInNamespaceResult, error) {
	manifest := os.Getenv("PIPELINE_C_MANIFEST")
	imagesDir := os.Getenv("PIPELINE_C_IMAGES_DIR")
	workDir := os.Getenv("PIPELINE_C_WORK_DIR")
	if manifest == "" {
		return nil, fmt.Errorf("STREAM_MODE=c requires PIPELINE_C_MANIFEST")
	}
	if imagesDir == "" {
		// Fall back to opts.CheckpointPath if no explicit dir given —
		// keeps the env-driven first cut symmetric with the PVC path.
		if opts.CheckpointPath == "" {
			return nil, fmt.Errorf("STREAM_MODE=c requires PIPELINE_C_IMAGES_DIR or --checkpoint-path")
		}
		imagesDir = opts.CheckpointPath
	}

	log.Info("Pipeline C restore dispatch",
		"manifest", manifest,
		"images_dir", imagesDir,
		"work_dir", workDir,
		"cgroup_root", opts.CgroupRoot,
	)

	// Match the PVC path's tmpfs / proc-sys dance so CRIU restore sees
	// the same mount state on entry. (Pipeline C uses the same kernel
	// surface as the existing restore for everything except the page
	// data, so the same prologue applies.)
	setupStart := time.Now()
	if err := unmountDevShmBestEffort(log); err != nil {
		return nil, fmt.Errorf("Pipeline C: unmount /dev/shm: %w", err)
	}
	if err := snapshotruntime.RemountProcSys(true); err != nil {
		return nil, fmt.Errorf("Pipeline C: remount /proc/sys rw: %w", err)
	}
	setupDur := time.Since(setupStart)
	defer func() {
		if err := snapshotruntime.RemountProcSys(false); err != nil {
			log.Error(err, "Pipeline C: failed to remount /proc/sys ro")
		}
	}()

	pcRes, err := ExecutePipelineC(ctx, PipelineCOptions{
		ImagesDir:    imagesDir,
		WorkDir:      workDir,
		ManifestPath: manifest,
		CgroupRoot:   opts.CgroupRoot,
	}, log)
	if err != nil {
		return nil, err
	}

	return &RestoreInNamespaceResult{
		// Pipeline C does not surface the restored PID through the
		// stream pipe yet — the agent already tracks the placeholder's
		// init pid externally, so a 0 here is informational. Set to
		// the restore criu's pid as a placeholder; agent reconciler
		// uses container introspection, not this field, for the
		// Pipeline C path.
		RestoredPID:            pcRes.RestorePID,
		NSRestoreSetupDuration: setupDur + pcRes.StreamerSetupDur,
		CRIURestoreDuration:    pcRes.RestoreDur,
	}, nil
}

func unmountDevShmBestEffort(log logr.Logger) error {
	// Same call the PVC and S3 paths make. Wrapped so the file
	// stays linter-clean without an extra syscall import in the
	// dispatch file.
	return tryUnmount("/dev/shm")
}
