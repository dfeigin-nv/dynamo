package cuda

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"

	snapshotruntime "github.com/ai-dynamo/dynamo/deploy/snapshot/internal/runtime"
)

const (
	cudaCheckpointHelperBinary = "/usr/local/bin/cuda-checkpoint-helper"

	// Custom-storage (stream) mode binaries. Each does the full two-phase tree
	// internally (lock-all -> checkpoint-all -> per-pid DtoH+WRITE, or
	// restore-all -> refill -> unlock-all) in ONE exec, so all pids are passed
	// comma-separated to a single invocation — do NOT loop per-pid (that would
	// break the TP>1 ordering invariant and collide every rank's blob at p0).
	ckptStreamCkptBinary    = "/usr/local/bin/ckpt-stream-ckpt"
	ckptStreamRestoreBinary = "/usr/local/bin/ckpt-stream-restore"

	actionLock       = "lock"
	actionCheckpoint = "checkpoint"
	actionRestore    = "restore"
	actionUnlock     = "unlock"
)

func lock(ctx context.Context, pid int, log logr.Logger) error {
	return runAction(ctx, pid, actionLock, "", log)
}

func checkpoint(ctx context.Context, pid int, log logr.Logger) error {
	return runAction(ctx, pid, actionCheckpoint, "", log)
}

func restoreProcess(ctx context.Context, pid int, deviceMap string, log logr.Logger) error {
	return runAction(ctx, pid, actionRestore, deviceMap, log)
}

func unlock(ctx context.Context, pid int, log logr.Logger) error {
	return runAction(ctx, pid, actionUnlock, "", log)
}

func getState(ctx context.Context, pid int) (string, error) {
	cmd := exec.CommandContext(ctx, cudaCheckpointHelperBinary, "--get-state", "--pid", strconv.Itoa(pid))
	output, err := cmd.CombinedOutput()
	state := strings.TrimSpace(string(output))
	if err != nil {
		return "", fmt.Errorf("cuda-checkpoint-helper --get-state failed for pid %d: %w (output: %s)", pid, err, state)
	}
	if state == "" {
		return "", fmt.Errorf("cuda-checkpoint-helper --get-state returned empty state for pid %d", pid)
	}
	return state, nil
}

// runStream execs a custom-storage stream binary (ckpt-stream-ckpt or
// ckpt-stream-restore) ONCE with all pids comma-joined and the S3 target. The
// binary handles the whole process tree two-phase internally. stderr is streamed
// through so the DtoH/NIXL throughput lines land in the agent log.
func runStream(ctx context.Context, binary string, pids []int, bucket, keyprefix string, log logr.Logger) error {
	if len(pids) == 0 {
		return fmt.Errorf("%s: no pids", binary)
	}
	pidStrs := make([]string, len(pids))
	for i, p := range pids {
		pidStrs[i] = strconv.Itoa(p)
	}
	pidArg := strings.Join(pidStrs, ",")
	cmd := exec.CommandContext(ctx, binary, pidArg, bucket, keyprefix)
	cmd.Env = os.Environ()
	cmd.Stderr = os.Stderr
	start := time.Now()
	output, err := cmd.Output()
	duration := time.Since(start)
	out := strings.TrimSpace(string(output))
	if err != nil {
		log.Error(err, "custom-storage stream command failed",
			"binary", binary, "pids", pidArg, "bucket", bucket, "keyprefix", keyprefix,
			"duration", duration, "stdout", out)
		return fmt.Errorf("%s %s %s %s failed after %s: %w (stdout: %s)", binary, pidArg, bucket, keyprefix, duration, err, out)
	}
	log.Info("custom-storage stream command succeeded",
		"binary", binary, "pids", pidArg, "bucket", bucket, "keyprefix", keyprefix, "duration", duration)
	return nil
}

func runAction(ctx context.Context, pid int, action, deviceMap string, log logr.Logger) error {
	args := []string{"--action", action, "--pid", strconv.Itoa(pid)}
	if action == actionRestore && deviceMap != "" {
		args = append(args, "--device-map", deviceMap)
	}
	cmd := exec.CommandContext(ctx, cudaCheckpointHelperBinary, args...)
	details := snapshotruntime.ProcessDetails{
		ObservedPID:   pid,
		OutermostPID:  pid,
		InnermostPID:  pid,
		NamespacePIDs: []int{pid},
	}
	if process, err := snapshotruntime.ReadProcessDetails("/proc", pid); err == nil {
		details = process
	}
	start := time.Now()
	output, err := cmd.CombinedOutput()
	duration := time.Since(start)
	out := strings.TrimSpace(string(output))
	if err != nil {
		log.Error(err, "cuda-checkpoint-helper command failed",
			"pid", pid,
			"outermost_pid", details.OutermostPID,
			"innermost_pid", details.InnermostPID,
			"cmdline", details.Cmdline,
			"action", action,
			"duration", duration,
			"output", out,
		)
		return fmt.Errorf("cuda-checkpoint-helper %v failed for pid %d after %s: %w (output: %s)", args, pid, duration, err, out)
	}
	log.V(1).Info("cuda-checkpoint-helper command succeeded",
		"pid", pid,
		"outermost_pid", details.OutermostPID,
		"innermost_pid", details.InnermostPID,
		"cmdline", details.Cmdline,
		"action", action,
		"duration", duration,
		"output", out,
	)
	return nil
}
