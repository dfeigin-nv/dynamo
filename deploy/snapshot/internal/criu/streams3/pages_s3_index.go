// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package streams3

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ai-dynamo/dynamo/deploy/snapshot/internal/criu"
	"github.com/go-logr/logr"
)

// pagesS3IndexEntry is the per-pages_img_id row in the sidecar.
type pagesS3IndexEntry struct {
	ID   uint32 `json:"id"`
	Size uint64 `json:"size"`
	Key  string `json:"key"`
}

// pagesS3Index is the sidecar schema consumed by criu.writePipelineCManifest
// to emit s3:// sources for each pages_img_id range under Pipeline C / Stage 1.
type pagesS3Index struct {
	Bucket string              `json:"bucket"`
	Prefix string              `json:"prefix"`
	Pages  []pagesS3IndexEntry `json:"pages"`
}

// parseS3URI splits "s3://bucket/key-prefix" into bucket and the trailing key
// prefix (no leading slash). Returns an error on malformed input.
func parseS3URI(s3URI string) (string, string, error) {
	rest, ok := strings.CutPrefix(s3URI, "s3://")
	if !ok {
		return "", "", fmt.Errorf("s3 URI must start with s3://: %q", s3URI)
	}
	parts := strings.SplitN(rest, "/", 2)
	if parts[0] == "" {
		return "", "", fmt.Errorf("s3 URI missing bucket: %q", s3URI)
	}
	if len(parts) == 1 {
		return parts[0], "", nil
	}
	return parts[0], parts[1], nil
}

// writePagesS3Index enumerates the pages-*.img objects under
// <s3URI>/<hash>/ via `s5cmd ls`, parses their sizes, and writes a JSON
// sidecar at <tmpDir>/<criu.PagesS3IndexFilename>. The sidecar is read by
// writePipelineCManifest to emit s3:// sources for each pages_img_id.
func writePagesS3Index(s3URI, hash, tmpDir string, log logr.Logger) error {
	bucket, basePrefix, err := parseS3URI(s3URI)
	if err != nil {
		return err
	}
	keyPrefix := path.Join(basePrefix, hash)
	if keyPrefix != "" && !strings.HasSuffix(keyPrefix, "/") {
		keyPrefix += "/"
	}
	listURI := fmt.Sprintf("s3://%s/%spages-*.img", bucket, keyPrefix)
	log.Info("Enumerating pages-*.img on S3", "uri", listURI)

	out, err := exec.Command("s5cmd", "ls", listURI).Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return fmt.Errorf("s5cmd ls %q: %w (stderr=%s)",
				listURI, err, string(ee.Stderr))
		}
		return fmt.Errorf("s5cmd ls %q: %w", listURI, err)
	}

	idx := pagesS3Index{Bucket: bucket, Prefix: strings.TrimSuffix(keyPrefix, "/")}
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		// `s5cmd ls` line: "<date> <time> <size> <name>"
		if len(fields) < 4 {
			continue
		}
		sizeStr := fields[len(fields)-2]
		name := fields[len(fields)-1]
		if !strings.HasPrefix(name, "pages-") || !strings.HasSuffix(name, ".img") {
			continue
		}
		idStr := strings.TrimSuffix(strings.TrimPrefix(name, "pages-"), ".img")
		id, err := strconv.ParseUint(idStr, 10, 32)
		if err != nil {
			continue
		}
		size, err := strconv.ParseUint(sizeStr, 10, 64)
		if err != nil {
			return fmt.Errorf("s5cmd ls size parse %q: %w", sizeStr, err)
		}
		idx.Pages = append(idx.Pages, pagesS3IndexEntry{
			ID:   uint32(id),
			Size: size,
			Key:  keyPrefix + name,
		})
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("s5cmd ls scan: %w", err)
	}
	if len(idx.Pages) == 0 {
		return fmt.Errorf("s5cmd ls %q returned no pages-*.img entries", listURI)
	}

	body, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("marshal pages index: %w", err)
	}
	dst := filepath.Join(tmpDir, criu.PagesS3IndexFilename)
	if err := os.WriteFile(dst, body, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", dst, err)
	}
	log.Info("Wrote pages S3 index sidecar",
		"path", dst, "count", len(idx.Pages))
	return nil
}
