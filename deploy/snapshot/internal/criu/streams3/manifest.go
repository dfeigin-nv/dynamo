// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package streams3

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"

	"github.com/go-logr/logr"
	"gopkg.in/yaml.v3"

	"github.com/ai-dynamo/dynamo/deploy/snapshot/internal/types"
)

// UploadManifestS3 uploads the manifest.yaml as a standalone S3 object
// alongside the streamed shards. The agent downloads this before starting
// nsrestore to build the CUDA device map without yet bringing up the
// streamer pipeline.
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

// DownloadManifestS3 downloads manifest.yaml from S3 and returns the parsed
// manifest. Used by the agent before nsrestore to recover CUDA device map
// info without inspecting the (encrypted-on-disk) image stream.
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

// shardStats is the JSON structure criu-image-streamer emits on the progress
// pipe once all shards have finished transferring.
type shardStats struct {
	Shards []struct {
		Size               uint64 `json:"size"`
		TransferDurationMs uint64 `json:"transfer_duration_millis"`
	} `json:"shards"`
}

// logShardStats parses a single shard-stats JSON line and logs aggregate
// throughput. Lines that aren't valid JSON are dropped at V(1).
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
