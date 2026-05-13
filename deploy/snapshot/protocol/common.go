// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package protocol

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	CheckpointSourceLabel = "nvidia.com/snapshot-is-checkpoint-source"

	// Restore pods carry CheckpointIDLabel without CheckpointSourceLabel.
	CheckpointIDLabel  = "nvidia.com/snapshot-checkpoint-id"
	RestoreTargetLabel = "nvidia.com/snapshot-is-restore-target"

	CheckpointArtifactVersionAnnotation = "nvidia.com/snapshot-artifact-version"

	// Required comma-separated checkpoint/restore target container list.
	TargetContainersAnnotation = "nvidia.com/snapshot-target-containers"

	CheckpointStatusAnnotation = "nvidia.com/snapshot-checkpoint-status"

	// Full keys are nvidia.com/snapshot-restore-status.<containerName>.
	RestoreStatusAnnotationPrefix = "nvidia.com/snapshot-restore-status."

	// Full keys are nvidia.com/snapshot-restore-container-id.<containerName>.
	RestoreContainerIDAnnotationPrefix = "nvidia.com/snapshot-restore-container-id."

	// Legacy unscoped restore status keys, cleared when stamping fresh metadata.
	RestoreStatusAnnotation      = "nvidia.com/snapshot-restore-status"
	RestoreContainerIDAnnotation = "nvidia.com/snapshot-restore-container-id"

	CheckpointStorageTypeAnnotation     = "nvidia.com/snapshot-storage-type"
	CheckpointStorageBasePathAnnotation = "nvidia.com/snapshot-storage-base-path"
	CheckpointStorageS3URIAnnotation    = "nvidia.com/snapshot-storage-s3-uri"
	CheckpointVolumeName                = "checkpoint-storage"
	DefaultCheckpointArtifactVersion    = "1"
	DefaultCheckpointJobTTLSeconds      = int32(300)
	DefaultSeccompLocalhostProfile      = "profiles/block-iouring.json"
	StorageTypePVC                      = "pvc"
	StorageTypeS3                       = "s3"

	CheckpointStatusCompleted = "completed"
	CheckpointStatusFailed    = "failed"
	RestoreStatusInProgress   = "in_progress"
	RestoreStatusCompleted    = "completed"
	RestoreStatusFailed       = "failed"
)

type Storage struct {
	Type     string
	Location string
	PVCName  string
	BasePath string

	// S3URI is the s3://bucket/prefix root for S3 storage. The per-checkpoint
	// shard prefix is derived by appending the checkpoint ID at resolve time.
	S3URI string
}

type RestoreStatusAnnotationKeys struct {
	Status      string
	ContainerID string
}

func ArtifactVersion(version string) string {
	version = strings.TrimSpace(version)
	if version == "" {
		return DefaultCheckpointArtifactVersion
	}
	return version
}

func ResolveCheckpointStorage(checkpointID string, version string, storage Storage) (Storage, error) {
	resolved, err := resolveStorageConfig(storage)
	if err != nil {
		return Storage{}, err
	}
	if resolved.Type == StorageTypeS3 {
		// S3 layout: <s3URI>/<checkpointID>/versions/<artifactVersion>
		resolved.Location = strings.TrimRight(resolved.S3URI, "/") + "/" + checkpointID + "/versions/" + ArtifactVersion(version)
		return resolved, nil
	}
	resolved.Location = strings.TrimRight(resolved.BasePath, "/") + "/" + checkpointID + "/versions/" + ArtifactVersion(version)
	return resolved, nil
}

func ResolveRestoreStorage(checkpointID string, version string, location string, storage Storage) (Storage, error) {
	resolved, err := resolveStorageConfig(storage)
	if err != nil {
		return Storage{}, err
	}
	location = strings.TrimSpace(location)
	if location == "" {
		return ResolveCheckpointStorage(checkpointID, version, storage)
	}
	resolved.Location = location
	return resolved, nil
}

// FormatTargetContainers renders the canonical annotation value.
func FormatTargetContainers(names []string) string {
	cleaned := make([]string, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		cleaned = append(cleaned, name)
	}
	return strings.Join(cleaned, ",")
}

// ParseTargetContainers trims names and rejects empty or duplicate entries.
func ParseTargetContainers(value string) ([]string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, ",")
	seen := make(map[string]struct{}, len(parts))
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name == "" {
			return nil, fmt.Errorf("empty container name in %s=%q", TargetContainersAnnotation, value)
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("duplicate container name %q in %s=%q", name, TargetContainersAnnotation, value)
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	return out, nil
}

// TargetContainersFromAnnotations requires the target list and enforces bounds.
func TargetContainersFromAnnotations(annotations map[string]string, minCount, maxCount int) ([]string, error) {
	raw, ok := annotations[TargetContainersAnnotation]
	if !ok || strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("missing required %s annotation", TargetContainersAnnotation)
	}
	names, err := ParseTargetContainers(raw)
	if err != nil {
		return nil, err
	}
	if minCount > 0 && len(names) < minCount {
		return nil, fmt.Errorf("%s must list at least %d container name(s), got %d", TargetContainersAnnotation, minCount, len(names))
	}
	if maxCount > 0 && len(names) > maxCount {
		return nil, fmt.Errorf("%s must list at most %d container name(s), got %d", TargetContainersAnnotation, maxCount, len(names))
	}
	return names, nil
}

func RestoreStatusAnnotationKeysFor(containerName string) (RestoreStatusAnnotationKeys, error) {
	keys := RestoreStatusAnnotationKeys{
		Status:      RestoreStatusAnnotationPrefix + containerName,
		ContainerID: RestoreContainerIDAnnotationPrefix + containerName,
	}
	for _, annotationKey := range []string{keys.Status, keys.ContainerID} {
		if errs := validation.IsQualifiedName(annotationKey); len(errs) > 0 {
			return RestoreStatusAnnotationKeys{}, fmt.Errorf("container name %q cannot be used in restore status annotation key %q: %s", containerName, annotationKey, strings.Join(errs, "; "))
		}
	}
	return keys, nil
}

func RestoreStatusAnnotations(containerName, status, containerID string) (map[string]string, error) {
	keys, err := RestoreStatusAnnotationKeysFor(containerName)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		keys.Status:      status,
		keys.ContainerID: containerID,
	}, nil
}

func clearRestoreStatusKeys(annotations map[string]string) {
	delete(annotations, "nvidia.com/snapshot-restore-status")
	delete(annotations, "nvidia.com/snapshot-restore-container-id")
	for key := range annotations {
		if strings.HasPrefix(key, RestoreStatusAnnotationPrefix) ||
			strings.HasPrefix(key, RestoreContainerIDAnnotationPrefix) {
			delete(annotations, key)
		}
	}
}

// ApplyRestoreTargetMetadata resets restore metadata and stamps checkpoint ID.
// The caller owns TargetContainersAnnotation.
func ApplyRestoreTargetMetadata(labels map[string]string, annotations map[string]string, enabled bool, checkpointID string, artifactVersion string) {
	delete(labels, CheckpointSourceLabel)
	delete(labels, RestoreTargetLabel)
	delete(labels, CheckpointIDLabel)
	delete(annotations, CheckpointArtifactVersionAnnotation)
	delete(annotations, CheckpointStatusAnnotation)
	clearRestoreStatusKeys(annotations)

	if !enabled {
		return
	}

	labels[RestoreTargetLabel] = "true"
	if checkpointID != "" {
		labels[CheckpointIDLabel] = checkpointID
	}
	annotations[CheckpointArtifactVersionAnnotation] = ArtifactVersion(artifactVersion)
}

func ApplyCheckpointStorageMetadata(annotations map[string]string, storage Storage) {
	if annotations == nil {
		return
	}
	delete(annotations, CheckpointStorageTypeAnnotation)
	delete(annotations, CheckpointStorageBasePathAnnotation)
	delete(annotations, CheckpointStorageS3URIAnnotation)
	storageType := strings.TrimSpace(storage.Type)
	if storageType != "" {
		annotations[CheckpointStorageTypeAnnotation] = storageType
	}
	basePath := strings.TrimSpace(storage.BasePath)
	if basePath != "" {
		basePath = strings.TrimRight(basePath, "/")
		if basePath == "" {
			basePath = "/"
		}
		annotations[CheckpointStorageBasePathAnnotation] = basePath
	}
	s3URI := strings.TrimSpace(storage.S3URI)
	if s3URI != "" {
		annotations[CheckpointStorageS3URIAnnotation] = strings.TrimRight(s3URI, "/")
	}
}

func applyCheckpointSourceMetadata(labels map[string]string, annotations map[string]string, checkpointID string, artifactVersion string) {
	delete(labels, RestoreTargetLabel)
	delete(labels, CheckpointIDLabel)
	delete(annotations, CheckpointArtifactVersionAnnotation)

	labels[CheckpointSourceLabel] = "true"
	if checkpointID != "" {
		labels[CheckpointIDLabel] = checkpointID
	}
	annotations[CheckpointArtifactVersionAnnotation] = ArtifactVersion(artifactVersion)
}

func resolveStorageConfig(storage Storage) (Storage, error) {
	storageType := strings.TrimSpace(storage.Type)
	if storageType == "" {
		storageType = StorageTypePVC
	}
	switch storageType {
	case StorageTypePVC:
		basePath := strings.TrimSpace(storage.BasePath)
		if basePath == "" {
			return Storage{}, fmt.Errorf("checkpoint base path is required")
		}
		if !strings.HasPrefix(basePath, "/") {
			return Storage{}, fmt.Errorf("checkpoint base path %q must be absolute", basePath)
		}
		basePath = strings.TrimRight(basePath, "/")
		if basePath == "" {
			basePath = "/"
		}
		return Storage{
			Type:     storageType,
			PVCName:  strings.TrimSpace(storage.PVCName),
			BasePath: basePath,
		}, nil
	case StorageTypeS3:
		s3URI := strings.TrimSpace(storage.S3URI)
		if s3URI == "" {
			return Storage{}, fmt.Errorf("storage.s3.uri is required when storage type is s3")
		}
		if !strings.HasPrefix(s3URI, "s3://") {
			return Storage{}, fmt.Errorf("storage.s3.uri %q must begin with s3://", s3URI)
		}
		return Storage{
			Type:  storageType,
			S3URI: strings.TrimRight(s3URI, "/"),
		}, nil
	default:
		return Storage{}, fmt.Errorf("checkpoint storage type %q is not supported", storageType)
	}
}
