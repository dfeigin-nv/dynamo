package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"

	"github.com/go-logr/logr"

	"github.com/ai-dynamo/dynamo/deploy/snapshot/pkg/executor"
	"github.com/ai-dynamo/dynamo/deploy/snapshot/pkg/logging"
)

func main() {
	// Logs go to stderr so stdout is reserved for the structured result.
	log := logging.ConfigureLogger("stderr").WithName("nsrestore")

	checkpointPath := flag.String("checkpoint-path", "", "Path to checkpoint directory (pvc mode)")
	checkpointStorageType := flag.String("checkpoint-storage-type", "pvc", "Storage type: pvc or s3")
	checkpointLocation := flag.String("checkpoint-location", "", "S3 URI prefix for checkpoint (s3 mode)")
	checkpointHash := flag.String("checkpoint-hash", "", "Checkpoint hash (s3 mode)")
	cudaDeviceMap := flag.String("cuda-device-map", "", "CUDA device map for cuda-checkpoint restore")
	cgroupRoot := flag.String("cgroup-root", "", "CRIU cgroup root remap path")
	flag.Parse()

	if *checkpointStorageType == "s3" {
		if *checkpointLocation == "" {
			fatal(log, nil, "--checkpoint-location is required for s3 storage type")
		}
		if *checkpointHash == "" {
			fatal(log, nil, "--checkpoint-hash is required for s3 storage type")
		}
	} else {
		if *checkpointPath == "" {
			fatal(log, nil, "--checkpoint-path is required for pvc storage type")
		}
	}

	opts := executor.RestoreOptions{
		CheckpointPath:        *checkpointPath,
		CheckpointStorageType: *checkpointStorageType,
		CheckpointLocation:    *checkpointLocation,
		CheckpointHash:        *checkpointHash,
		CUDADeviceMap:         *cudaDeviceMap,
		CgroupRoot:            *cgroupRoot,
	}

	restoredPID, err := executor.RestoreInNamespace(context.Background(), opts, log)
	if err != nil {
		fatal(log, err, "restore failed")
	}

	result := struct {
		RestoredPID int `json:"restoredPID"`
	}{RestoredPID: restoredPID}
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		fatal(log, err, "Failed to write restore result")
	}
}

func fatal(log logr.Logger, err error, msg string, keysAndValues ...interface{}) {
	if err != nil {
		log.Error(err, msg, keysAndValues...)
	} else {
		log.Info(msg, keysAndValues...)
	}
	os.Exit(1)
}
