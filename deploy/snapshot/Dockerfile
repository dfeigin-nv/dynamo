# SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Unified Dockerfile for snapshot-agent and placeholder images.
#
# Build targets:
#   docker build --platform linux/amd64 --target agent -t snapshot-agent:latest .
#   docker build --platform linux/amd64 --target placeholder --build-arg BASE_IMAGE=<app-image> -t placeholder:latest .
#
# Local CRIU targets (context = checkpoints repo root, CHREK_PATH=dynamo/deploy/snapshot):
#   snapshot-testing/build-images.sh --local
#   docker build -f dynamo/deploy/snapshot/Dockerfile --build-arg CHREK_PATH=dynamo/deploy/snapshot --target agent-local -t snapshot-agent .
#   docker build -f dynamo/deploy/snapshot/Dockerfile --build-arg CHREK_PATH=dynamo/deploy/snapshot --target placeholder-local -t snapshot-placeholder .
#
# Optional targets for CI:
#   docker build --target linter .   # Run linting
#   docker build --target tester .   # Run tests

# =============================================================================
# Build Arguments
# =============================================================================
ARG DOCKER_PROXY
ARG GO_VERSION=1.25
ARG CRIU_REPO=https://github.com/dfeigin-nv/criu.git
ARG CRIU_COMMIT=777baaf27f6a76f743c9bf24b64886297dc0129b
ARG AGENT_BASE_IMAGE=nvcr.io/nvidia/cuda-dl-base:25.11-cuda13.0-devel-ubuntu24.04

# For placeholder target only - this default allows agent builds to succeed,
# but placeholder builds MUST override it with --build-arg BASE_IMAGE=<image>
ARG BASE_IMAGE=placeholder-requires-base-image-arg

# When building from checkpoints repo root, set to dynamo/deploy/chrek
ARG CHREK_PATH=.

# =============================================================================
# Stage: Go base - Common setup for Go builds
# =============================================================================
FROM ${DOCKER_PROXY}golang:${GO_VERSION} AS go-base

ARG CHREK_PATH
ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN echo "Building for ${TARGETOS}/${TARGETARCH}"

RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Copy go-criu (needed for the replace directive in go.mod)
COPY go-criu /go-criu

COPY ${CHREK_PATH}/go.mod ${CHREK_PATH}/go.sum ./
RUN sed -i 's|=> .*go-criu|=> /go-criu|' go.mod
RUN go mod download

# COPY overwrites go.mod — re-apply the path fix
COPY ${CHREK_PATH}/ .
RUN sed -i 's|=> .*go-criu|=> /go-criu|' go.mod

# =============================================================================
# Stage: Linter - Run golangci-lint
# =============================================================================
FROM go-base AS linter

RUN go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.62.2
RUN golangci-lint run --timeout=5m

# =============================================================================
# Stage: Tester - Run tests
# =============================================================================
FROM go-base AS tester

RUN go test ./... -v

# =============================================================================
# Stage: Builder - Build Go binaries
# =============================================================================
FROM go-base AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-w -s" -o /snapshot-agent ./cmd/agent
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-w -s" -o /nsrestore ./cmd/nsrestore

# =============================================================================
# Stage: criu-image-streamer Builder - Build Rust streaming binary
# =============================================================================
FROM rust:1.82-slim AS criu-image-streamer-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

COPY criu-image-streamer /tmp/criu-image-streamer
# prost-build uses proto/criu as the only include path, so well-known types must be there
RUN mkdir -p /tmp/criu-image-streamer/proto/criu/google/protobuf && \
    cp /usr/include/google/protobuf/descriptor.proto \
       /tmp/criu-image-streamer/proto/criu/google/protobuf/descriptor.proto
RUN cd /tmp/criu-image-streamer && cargo build --release

# =============================================================================
# Stage: CRIU Builder - Build CRIU with CUDA plugin
# =============================================================================
FROM ubuntu:24.04 AS criu-builder

ARG CRIU_REPO
ARG CRIU_COMMIT

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    build-essential \
    pkg-config \
    libbsd-dev \
    libcap-dev \
    libnet1-dev \
    libnl-3-dev \
    libnl-route-3-dev \
    libprotobuf-dev \
    libprotobuf-c-dev \
    protobuf-c-compiler \
    protobuf-compiler \
    python3 \
    python3-protobuf \
    libgnutls28-dev \
    libnftables-dev \
    uuid-dev \
    libaio-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git init /tmp/criu \
    && cd /tmp/criu \
    && git remote add origin ${CRIU_REPO} \
    && git fetch --depth 1 origin ${CRIU_COMMIT} \
    && git checkout FETCH_HEAD \
    && make -j$(nproc) \
    && make DESTDIR=/criu-install install-criu install-lib install-cuda_plugin

RUN git clone https://github.com/NVIDIA/cuda-checkpoint.git /tmp/cuda-checkpoint

# =============================================================================
# Stage: CRIU Builder (local) - Use local CRIU source from repo root
# =============================================================================
FROM ubuntu:24.04 AS criu-builder-local

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    build-essential \
    pkg-config \
    libbsd-dev \
    libcap-dev \
    libnet1-dev \
    libnl-3-dev \
    libnl-route-3-dev \
    libprotobuf-dev \
    libprotobuf-c-dev \
    protobuf-c-compiler \
    protobuf-compiler \
    python3 \
    python3-protobuf \
    libgnutls28-dev \
    libnftables-dev \
    uuid-dev \
    libselinux1-dev \
    libaio-dev \
    && rm -rf /var/lib/apt/lists/*

COPY criu /tmp/criu
RUN cd /tmp/criu \
    && find . -name '*.o' -delete \
    && find . -name '*.a' -delete \
    && find . -name '.*.cmd' -delete \
    && find . -name '.*.d' -delete \
    && find . -name 'built-in.o' -delete \
    && rm -rf lib/c/libcriu.* compel/compel-host-bin compel/plugins/*.lib.a compel/libcompel.a soccr/libsoccr.a \
    && rm -f include/criu/config.h include/version.h compel/include/asm include/common/asm 2>/dev/null || true \
    && make -j$(nproc) \
    && make DESTDIR=/criu-install install-criu install-lib install-cuda_plugin

RUN git clone https://github.com/NVIDIA/cuda-checkpoint.git /tmp/cuda-checkpoint

# =============================================================================
# Stage: Agent - Final snapshot-agent image
# =============================================================================
FROM ${AGENT_BASE_IMAGE} AS agent

ARG TARGETARCH=amd64

RUN if [ "${TARGETARCH}" != "amd64" ]; then \
      echo "ERROR: Dynamo Snapshot requires x86_64 (cuda-checkpoint has no ${TARGETARCH} binary)" >&2; exit 1; \
    fi

# Install CRIU runtime dependencies + S3 streaming tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    libaio1t64 \
    libbsd0 \
    libcap2 \
    libnet1 \
    libnl-3-200 \
    libnl-route-3-200 \
    libprotobuf-c1 \
    libgnutls30t64 \
    libnftables1 \
    iproute2 \
    iptables \
    procps \
    uuid-runtime \
    tar \
    rsync \
    ca-certificates \
    util-linux \
    lz4 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Copy CRIU from builder
COPY --from=criu-builder /criu-install/usr/local /usr/local
RUN criu --version

# Copy cuda-checkpoint binary
COPY --from=criu-builder /tmp/cuda-checkpoint/bin/x86_64_Linux/cuda-checkpoint /usr/local/sbin/cuda-checkpoint
RUN chmod +x /usr/local/sbin/cuda-checkpoint

# Copy the built binaries
COPY --from=builder /snapshot-agent /usr/local/bin/snapshot-agent
COPY --from=builder /nsrestore /usr/local/bin/nsrestore
COPY --from=criu-image-streamer-builder /tmp/criu-image-streamer/target/release/criu-image-streamer /usr/local/bin/criu-image-streamer

# Install AWS CLI for S3 streaming upload support
RUN pip3 install -q awscli --break-system-packages 2>/dev/null || pip3 install -q awscli
# Install s5cmd for fast S3 downloads (Go-based, ~20% faster than aws cli)
RUN curl -sL https://github.com/peak/s5cmd/releases/download/v2.3.0/s5cmd_2.3.0_Linux-64bit.tar.gz \
    | tar xz -C /usr/local/bin/ s5cmd && s5cmd version

# Create directories
RUN mkdir -p /checkpoints /var/run/snapshot

USER root

ENTRYPOINT ["/usr/local/bin/snapshot-agent"]

# =============================================================================
# Stage: Agent (local CRIU)
# =============================================================================
FROM ${AGENT_BASE_IMAGE} AS agent-local

RUN apt-get update && apt-get install -y --no-install-recommends \
    libaio1t64 \
    libbsd0 \
    libcap2 \
    libnet1 \
    libnl-3-200 \
    libnl-route-3-200 \
    libprotobuf-c1 \
    libgnutls30t64 \
    libnftables1 \
    iproute2 \
    iptables \
    procps \
    uuid-runtime \
    tar \
    rsync \
    ca-certificates \
    util-linux \
    lz4 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

COPY --from=criu-builder-local /criu-install/usr/local /usr/local
RUN criu --version

COPY --from=criu-builder-local /tmp/cuda-checkpoint/bin/x86_64_Linux/cuda-checkpoint /usr/local/sbin/cuda-checkpoint
RUN chmod +x /usr/local/sbin/cuda-checkpoint

COPY --from=builder /snapshot-agent /usr/local/bin/snapshot-agent
COPY --from=builder /nsrestore /usr/local/bin/nsrestore
COPY --from=criu-image-streamer-builder /tmp/criu-image-streamer/target/release/criu-image-streamer /usr/local/bin/criu-image-streamer

RUN pip3 install -q awscli --break-system-packages 2>/dev/null || pip3 install -q awscli
RUN curl -sL https://github.com/peak/s5cmd/releases/download/v2.3.0/s5cmd_2.3.0_Linux-64bit.tar.gz \
    | tar xz -C /usr/local/bin/ s5cmd

RUN mkdir -p /checkpoints /var/run/snapshot

USER root

ENTRYPOINT ["/usr/local/bin/snapshot-agent"]

# =============================================================================
# Stage: Placeholder - Runtime-compatible restore image (requires BASE_IMAGE arg)
# This image is a superset of the runtime image: same default execution contract
# (entrypoint/cmd/user), plus CRIU/cuda-checkpoint tooling for external restore.
# The operator may still override command to "sleep infinity" for restore pods.
# =============================================================================
FROM ${BASE_IMAGE} AS placeholder

ARG BASE_IMAGE
ARG TARGETARCH=amd64
ENV ORIGINAL_BASE_IMAGE=${BASE_IMAGE}

USER root

RUN if [ "${TARGETARCH}" != "amd64" ]; then \
      echo "ERROR: Dynamo Snapshot requires x86_64 (cuda-checkpoint has no ${TARGETARCH} binary)" >&2; exit 1; \
    fi

# Install runtime dependencies for CRIU restore (nsrestore runs here via nsenter)
# lz4 and ca-certificates are needed for S3 streaming restore (aws+lz4+criu-image-streamer
# are called from inside the placeholder namespace by nsrestore).
RUN apt-get update && apt-get install -y --no-install-recommends \
    libaio1t64 \
    libbsd0 \
    libcap2 \
    libnet1 \
    libnl-3-200 \
    libnl-route-3-200 \
    libprotobuf-c1 \
    libgnutls30t64 \
    libnftables1 \
    iproute2 \
    iptables \
    procps \
    uuid-runtime \
    tar \
    rsync \
    ca-certificates \
    lz4 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Copy CRIU from builder (needed by nsrestore running inside these namespaces)
COPY --from=criu-builder /criu-install/usr/local /usr/local
RUN criu --version && echo "CRIU installed successfully"

# Copy cuda-checkpoint binary (used for external CUDA state checkpoint/restore)
COPY --from=criu-builder /tmp/cuda-checkpoint/bin/x86_64_Linux/cuda-checkpoint /usr/local/sbin/cuda-checkpoint
RUN chmod +x /usr/local/sbin/cuda-checkpoint

# Copy nsrestore binary (invoked by DaemonSet via nsenter)
COPY --from=builder /nsrestore /usr/local/bin/nsrestore
RUN chmod +x /usr/local/bin/nsrestore

# Copy criu-image-streamer (needed for S3 streaming restore inside placeholder namespace)
COPY --from=criu-image-streamer-builder /tmp/criu-image-streamer/target/release/criu-image-streamer /usr/local/bin/criu-image-streamer
RUN chmod +x /usr/local/bin/criu-image-streamer

# Copy AWS CLI v2 bundle (needed for S3 streaming restore inside placeholder namespace)
RUN pip3 install -q awscli --break-system-packages 2>/dev/null || pip3 install -q awscli
RUN curl -sL https://github.com/peak/s5cmd/releases/download/v2.3.0/s5cmd_2.3.0_Linux-64bit.tar.gz \
    | tar xz -C /usr/local/bin/ s5cmd
# aws installs to the venv (/opt/dynamo/venv/bin/aws) which has an absolute shebang.
# Symlink to /usr/local/bin so nsrestore (running with agent PATH) can find it.
RUN ln -sf /opt/dynamo/venv/bin/aws /usr/local/bin/aws

# Create directories
RUN mkdir -p /checkpoints /var/run/criu /var/criu-work

# =============================================================================
# Stage: Placeholder (local CRIU)
# =============================================================================
FROM ${BASE_IMAGE} AS placeholder-local

ARG BASE_IMAGE
ENV ORIGINAL_BASE_IMAGE=${BASE_IMAGE}

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    libaio1t64 \
    libbsd0 \
    libcap2 \
    libnet1 \
    libnl-3-200 \
    libnl-route-3-200 \
    libprotobuf-c1 \
    libgnutls30t64 \
    libnftables1 \
    iproute2 \
    iptables \
    procps \
    uuid-runtime \
    tar \
    rsync \
    ca-certificates \
    lz4 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=criu-builder-local /criu-install/usr/local /usr/local
RUN criu --version && echo "CRIU (local) installed successfully"

COPY --from=criu-builder-local /tmp/cuda-checkpoint/bin/x86_64_Linux/cuda-checkpoint /usr/local/sbin/cuda-checkpoint
RUN chmod +x /usr/local/sbin/cuda-checkpoint

COPY --from=builder /nsrestore /usr/local/bin/nsrestore
RUN chmod +x /usr/local/bin/nsrestore

COPY --from=criu-image-streamer-builder /tmp/criu-image-streamer/target/release/criu-image-streamer /usr/local/bin/criu-image-streamer
RUN chmod +x /usr/local/bin/criu-image-streamer

RUN pip3 install -q awscli --break-system-packages 2>/dev/null || pip3 install -q awscli
RUN curl -sL https://github.com/peak/s5cmd/releases/download/v2.3.0/s5cmd_2.3.0_Linux-64bit.tar.gz \
    | tar xz -C /usr/local/bin/ s5cmd
RUN ln -sf /opt/dynamo/venv/bin/aws /usr/local/bin/aws

RUN mkdir -p /checkpoints /var/run/criu /var/criu-work
