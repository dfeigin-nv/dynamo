// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Package streams3 plumbs CRIU dump/restore data through criu-image-streamer
// directly to and from S3, with no shared PVC between dump and restore pods.
//
// Files:
//
//	pipes.go    — S3 upload/download shard pipes + low-level FD/cmd helpers.
//	manifest.go — Standalone manifest.yaml object handling (upload/download).
//	direct.go   — Non-streaming "direct" path: tmpfs dump + s5cmd sync.
//	stream.go   — Streaming dump/restore via criu-image-streamer.
package streams3

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

const (
	// streamerBin is the name of the criu-image-streamer binary; it must be on
	// PATH in the agent container image.
	streamerBin = "criu-image-streamer"

	// criuWorkDir is where CRIU writes restore.log; readable via kubectl exec.
	criuWorkDir = "/var/criu-work"

	// External file names embedded in the streamer's ext-file-fds stream.
	streamManifestName     = "manifest.yaml"
	streamRootfsDiffName   = "rootfs-diff.tar"
	streamDeletedFilesName = "deleted-files.json"

	// defaultS3Shards is the default number of parallel S3 upload/download shards.
	defaultS3Shards = 16
)

// startS3UploadPipes spawns one shell pipeline per shard that consumes
// stdin and uploads it to s3URI/hash/img-i(.lz4). Returns the parent-side
// write ends, the running child commands, and the comma-joined fd numbers
// (3..3+N-1) that streamer-capture should hand to --shard-fds.
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
		// Parent doesn't need the read end after the child has it.
		r.Close()

		writeFDs = append(writeFDs, w)
		cmds = append(cmds, cmd)
		fdNums = append(fdNums, strconv.Itoa(3+i))
	}

	return writeFDs, cmds, strings.Join(fdNums, ","), nil
}

// startS3DownloadPipes spawns one shell pipeline per shard that pulls
// s3URI/hash/img-i(.lz4) and writes it to its stdout pipe end. Returns the
// parent-side read ends, the running child commands, and the comma-joined
// fd numbers (3..3+N-1) for --shard-fds on the streamer serve side.
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

		// -c 32 matches the existing direct.go cp path and lifts s5cmd cat
		// off its default 5 parallel range-GETs per shard. With 16 shards
		// outer × 32 inner that's 512 concurrent parts in flight, well
		// within s5cmd's --numworkers (default 256 per process) and the
		// ~400 Gbps host fabric headroom on p4d-class instances.
		var s3Key, shellCmd string
		if useLZ4 {
			s3Key = fmt.Sprintf("%s/%s/img-%d.lz4", s3URI, hash, i)
			shellCmd = fmt.Sprintf("s5cmd cat -c 32 '%s' | lz4 -d - -", s3Key)
		} else {
			s3Key = fmt.Sprintf("%s/%s/img-%d", s3URI, hash, i)
			shellCmd = fmt.Sprintf("s5cmd cat -c 32 '%s'", s3Key)
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
		// Parent doesn't need the write end after the child has it.
		w.Close()

		readFDs = append(readFDs, r)
		cmds = append(cmds, cmd)
		fdNums = append(fdNums, strconv.Itoa(3+i))
	}

	return readFDs, cmds, strings.Join(fdNums, ","), nil
}

// closeFDs closes a slice of *os.File handles, ignoring nil entries.
func closeFDs(fds []*os.File) {
	for _, f := range fds {
		if f != nil {
			f.Close()
		}
	}
}

// killCmds best-effort kills a slice of running child commands.
func killCmds(cmds []*exec.Cmd) {
	for _, cmd := range cmds {
		if cmd != nil && cmd.Process != nil {
			cmd.Process.Kill()
		}
	}
}
