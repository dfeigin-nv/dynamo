# SPDX-FileCopyrightText: Copyright (c) 2024-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Unified Dockerfile for snapshot-agent and placeholder images.
#
# Build targets (the `operator` build context is required — the snapshot module depends on the
# operator module via `replace => ../operator`):
#   docker build --platform linux/amd64 --target agent --build-context operator=../operator -t snapshot-agent:latest .
#   docker build --platform linux/amd64 --target placeholder --build-context operator=../operator --build-arg BASE_IMAGE=<app-image> -t placeholder:latest .
#
# Optional targets for CI:
#   docker build --target linter --build-context operator=../operator .   # Run linting
#   docker build --target tester --build-context operator=../operator .   # Run tests

# =============================================================================
# Build Arguments
# =============================================================================
ARG DOCKER_PROXY
ARG GO_VERSION=1.26.3
# Default to the dynamo-snapshot CRIU fork that carries the streaming +
# AIO + parallel-memfd restore work. Custom forks can override both args at
# build time, for example:
#   --build-arg CRIU_REPO=<git-remote-url>
#   --build-arg CRIU_REF=<fork-branch-or-sha>
ARG CRIU_REPO=https://github.com/dfeigin-nv/criu.git
# memfd-cache-ceiling-test is the shipped memfd-cache HIT path: node-local
# content cache (donate/borrow of writable MAP_SHARED memfds over SCM_RIGHTS)
# + coordinator-parallel restore. No streaming commits. Warm HIT restores in
# ~4.5s; cold MISS ~13s. Streaming (Pipeline C) stays dormant on the
# snapshot-s3-memfd-juicefs-streaming branch for later reconciliation.
ARG CRIU_REF=memfd-cache-ceiling-test

ARG AGENT_BASE_IMAGE=nvcr.io/nvidia/cuda-dl-base:25.11-cuda13.0-devel-ubuntu24.04

# For placeholder target only - this default allows agent builds to succeed,
# but placeholder builds MUST override it with --build-arg BASE_IMAGE=<image>
ARG BASE_IMAGE=placeholder-requires-base-image-arg

# =============================================================================
# Stage: Go base - Common setup for Go builds
# =============================================================================
FROM ${DOCKER_PROXY}library/golang:${GO_VERSION} AS go-base

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN echo "Building for ${TARGETOS}/${TARGETARCH}"

RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# The snapshot module depends on the operator module via `replace => ../operator`, so the operator
# module must sit at /operator (a sibling of /workspace) for `go mod download` and the Go build to
# resolve it. Provided via the `operator` named build context (see build-deploy-component action).
COPY --from=operator . /operator/

COPY go.mod go.sum ./
RUN go mod download

COPY . .

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
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-w -s" -o /criu-stream-fetch ./cmd/criu-stream-fetch

# Emit a CycloneDX SBOM for snapshot-agent's Go module set. nsrestore's
# deps are a subset of agent's, so a single SBOM keyed on ./cmd/agent is
# sufficient. The git-init dance satisfies cyclonedx-gomod's main-module
# version probe (Docker build context doesn't carry .git). Pinned to
# keep the SBOM tool itself reproducible.
RUN --mount=type=cache,target=/go/pkg/mod,sharing=shared \
    --mount=type=cache,target=/root/.cache/go-build,sharing=shared \
    GOBIN=/usr/local/bin go install github.com/CycloneDX/cyclonedx-gomod/cmd/cyclonedx-gomod@v1.7.0 && \
    git init -q && \
    git -c user.email=build@dynamo -c user.name=build add -A && \
    git -c user.email=build@dynamo -c user.name=build commit -q -m sbom && \
    cyclonedx-gomod app -licenses -json -main ./cmd/agent -output /sbom-go.cdx.json .

# Compliance: harvest each Go module's real LICENSE files from the module cache
# so the go NOTICES generator inlines upstream license text instead of canonical
# SPDX fallback. Preserve the cache's escaped "<module-path>@<version>/" layout —
# generators/go.py escapes module names the same way to look up. Non-fatal.
RUN --mount=type=cache,target=/go/pkg/mod,sharing=shared \
    mkdir -p /sbom-go-licenses && cd /go/pkg/mod && \
    find . -path ./cache -prune -o -type f \
        \( -iname 'LICENSE*' -o -iname 'LICENCE*' -o -iname 'COPYING*' \
           -o -iname 'NOTICE*' -o -iname 'UNLICENSE*' \) -print | \
    while IFS= read -r lf; do \
        rel="${lf#./}"; dest="/sbom-go-licenses/$(dirname "$rel")"; \
        mkdir -p "$dest" && cp "$lf" "$dest/" 2>/dev/null || true; \
    done; \
    echo "go license harvest: $(find /sbom-go-licenses -type f 2>/dev/null | wc -l) files"; true

# Compliance source archival: vendor third-party Go modules. Same pattern
# as operator's builder. First-party (github.com/ai-dynamo/) pruned later
# in collect_sources.py.
ARG ENABLE_SOURCE_ARCHIVAL=false
RUN --mount=type=cache,target=/go/pkg/mod,sharing=shared \
    if [ "$ENABLE_SOURCE_ARCHIVAL" = "true" ]; then \
        go mod vendor -o /tmp/go-vendor ; \
    else \
        mkdir -p /tmp/go-vendor ; \
    fi

# =============================================================================
# Stage: CUDA checkpoint helper builder
# =============================================================================
FROM ${AGENT_BASE_IMAGE} AS cuda-helper-builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY cmd/cuda-checkpoint-helper/main.c ./cmd/cuda-checkpoint-helper/main.c

RUN gcc -O2 -Wall -Wextra -o /cuda-checkpoint-helper \
    ./cmd/cuda-checkpoint-helper/main.c \
    -I/usr/local/cuda/include \
    -L/usr/local/cuda/lib64/stubs \
    -lcuda

# =============================================================================
# Stage: CRIU Builder - Build CRIU with CUDA plugin
# =============================================================================
FROM ubuntu:24.04 AS criu-builder

ARG CRIU_REPO
ARG CRIU_REF
# Pass --build-arg CRIU_CACHE_BUST=$(date +%s) to force a fresh git fetch
# when the upstream branch has new commits but ${CRIU_REF} string is
# unchanged.
ARG CRIU_CACHE_BUST=12

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
    && rm -rf /var/lib/apt/lists/*

RUN echo "criu cache-bust: ${CRIU_CACHE_BUST}" \
    && git init /tmp/criu \
    && cd /tmp/criu \
    && git remote add origin ${CRIU_REPO} \
    && git fetch --depth 1 origin ${CRIU_REF} \
    && git checkout FETCH_HEAD \
    && git --no-pager log --oneline -3 \
    && make -j$(nproc) \
    && make DESTDIR=/criu-install install-criu install-lib install-cuda_plugin

COPY plugins/inet-remap/snapshot_inet_remap.c /tmp/snapshot_inet_remap.c
RUN mkdir -p /criu-install/usr/local/lib/snapshot/criu-plugins \
    && gcc -O2 -Wall -Wextra -fPIC -shared \
      -idirafter /tmp/criu/criu/include \
      -o /criu-install/usr/local/lib/snapshot/criu-plugins/snapshot_inet_remap.so \
      /tmp/snapshot_inet_remap.c

RUN git clone https://github.com/NVIDIA/cuda-checkpoint.git /tmp/cuda-checkpoint

# =============================================================================
# Stage: nixl-obj-builder - build aws-sdk-cpp + NIXL (OBJ plugin) + the C++
# criu-stream-fetch-cpp streamer (has the NIXL_WRITE upload path, uploadMain).
# ubuntu:24.04 base => ABI-compatible with the ubuntu24.04 agent base.
# =============================================================================
FROM ubuntu:24.04 AS nixl-obj-builder
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake git ninja-build meson pkg-config ca-certificates \
    libcurl4-openssl-dev libssl-dev uuid-dev zlib1g-dev libasio-dev \
    && rm -rf /var/lib/apt/lists/*

# aws-sdk-cpp: s3 (standard client) + s3-crt (Common Runtime, high throughput)
RUN git clone --recurse-submodules --depth 1 --shallow-submodules \
        https://github.com/aws/aws-sdk-cpp.git --branch 1.11.581 /src/aws-sdk-cpp \
    && cmake -S /src/aws-sdk-cpp -B /src/aws_build -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_ONLY="s3;s3-crt" -DENABLE_TESTING=OFF -DBUILD_SHARED_LIBS=ON \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
    && cmake --build /src/aws_build -j"$(nproc)" \
    && cmake --install /src/aws_build \
    && rm -rf /src/aws-sdk-cpp /src/aws_build

# pybind11 for NIXL's always-built python bindings (separate layer keeps the
# aws-sdk build above cached).
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-dev pybind11-dev \
    && rm -rf /var/lib/apt/lists/*

# NIXL fork (POSIX + OBJ plugins only). Vendored source under third_party/nixl-src;
# abseil/asio resolve from vendored subprojects offline.
COPY third_party/nixl-src /src/nixl
RUN cd /src/nixl \
    && meson setup build --prefix=/usr/local --libdir=lib/x86_64-linux-gnu \
        -Denable_plugins=POSIX,OBJ -Dbuild_tests=false -Dbuild_examples=false \
        -Dbuildtype=release -Drust=false \
    && ninja -C build \
    && meson install -C build \
    && ldconfig

# criu-stream-fetch against the freshly built, self-consistent NIXL
COPY cmd/criu-stream-fetch-cpp/ /src/streamer/
RUN cd /src/streamer \
    && cmake -S . -B build \
        -DNIXL_INCLUDE_DIR=/usr/local/include/nixl \
        -DNIXL_LIBRARY=/usr/local/lib/x86_64-linux-gnu/libnixl.so \
        -DNIXL_BUILD_LIBRARY=/usr/local/lib/x86_64-linux-gnu/libnixl_build.so \
    && cmake --build build -j"$(nproc)" \
    && cp build/criu-stream-fetch /criu-stream-fetch-cpp \
    && echo "=== streamer links ===" && ldd /criu-stream-fetch-cpp | grep -iE "nixl|aws|not found" \
    && echo "=== OBJ plugin path ===" && find /usr/local -name libplugin_OBJ.so

# Fused checkpoint->S3 streamer (no local file/fsync). CUDA via dlopen at
# runtime, so no CUDA toolkit needed here; only NIXL headers/libs.
RUN cd /src/streamer \
    && g++ -O2 -std=c++17 -Wall -I/usr/local/include/nixl ckpt_stream.cpp -o /ckpt-stream \
        /usr/local/lib/x86_64-linux-gnu/libnixl.so \
        /usr/local/lib/x86_64-linux-gnu/libnixl_build.so \
        -ldl -lpthread \
    && echo "=== ckpt-stream links ===" && ldd /ckpt-stream | grep -iE "nixl|not found"

# Phase-0 split binaries: checkpoint half and restore half in SEPARATE processes
# (a CRIU dump/restore runs between them). Share ckpt_common.h. Same NIXL/CUDA
# link recipe as /ckpt-stream above.
RUN cd /src/streamer \
    && g++ -O2 -std=c++17 -Wall -I/usr/local/include/nixl ckpt_stream_ckpt.cpp -o /ckpt-stream-ckpt \
        /usr/local/lib/x86_64-linux-gnu/libnixl.so \
        /usr/local/lib/x86_64-linux-gnu/libnixl_build.so -ldl -lpthread \
    && g++ -O2 -std=c++17 -Wall -I/usr/local/include/nixl ckpt_stream_restore.cpp -o /ckpt-stream-restore \
        /usr/local/lib/x86_64-linux-gnu/libnixl.so \
        /usr/local/lib/x86_64-linux-gnu/libnixl_build.so -ldl -lpthread \
    && echo "=== split-binary links ===" \
    && ldd /ckpt-stream-ckpt | grep -iE "nixl|not found" \
    && ldd /ckpt-stream-restore | grep -iE "nixl|not found"

# =============================================================================
# Stage: Agent (pre-compliance) - Everything that ships, minus /legal
# =============================================================================
# Split out so the licenses stage can run dpkg-query against the actual
# installed package state (CRIU runtime libs included), then the final
# `agent` stage is a thin downstream that COPYs /legal in.
FROM ${AGENT_BASE_IMAGE} AS agent_pre

ARG TARGETARCH=amd64

RUN if [ "${TARGETARCH}" != "amd64" ]; then \
      echo "ERROR: Dynamo Snapshot requires x86_64 (cuda-checkpoint has no ${TARGETARCH} binary)" >&2; exit 1; \
    fi

# Install CRIU runtime dependencies + S3 streaming tooling
# (lz4 for per-shard compression, s5cmd for parallel S3 transfers,
# wget/curl for fetching tarballs, unzip+gnupg for the AWS CLI v2 bundled
# installer + signature verification).
# NOTE: the apt package `awscli` is not in Ubuntu 24.04 noble universe,
# so AWS CLI v2 is installed below from the official bundled installer.
RUN apt-get update && apt-get install -y --no-install-recommends \
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
    ca-certificates \
    util-linux \
    lz4 \
    curl \
    wget \
    unzip \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install s5cmd (parallel S3 client used by the dump/direct restore paths).
ARG S5CMD_VERSION=2.3.0
RUN curl -fsSL -o /tmp/s5cmd.tar.gz \
      "https://github.com/peak/s5cmd/releases/download/v${S5CMD_VERSION}/s5cmd_${S5CMD_VERSION}_Linux-64bit.tar.gz" \
    && tar -xzf /tmp/s5cmd.tar.gz -C /usr/local/bin s5cmd \
    && rm -f /tmp/s5cmd.tar.gz \
    && s5cmd version

# JuiceFS CE CLI: used by ExecuteRestoreS3Direct when RESTORE_SOURCE=juicefs:...
# to run `juicefs warmup` against the FUSE-mounted checkpoint dir before
# CRIU restore. Pre-populates the JFS local cache with parallel S3 GETs so
# CRIU's serial mmap reads hit cache instead of going through FUSE per-page.
ARG JUICEFS_VERSION=1.3.1
RUN curl -fsSL -o /tmp/juicefs.tar.gz \
      "https://github.com/juicedata/juicefs/releases/download/v${JUICEFS_VERSION}/juicefs-${JUICEFS_VERSION}-linux-amd64.tar.gz" \
    && tar -xzf /tmp/juicefs.tar.gz -C /usr/local/bin juicefs \
    && rm -f /tmp/juicefs.tar.gz \
    && juicefs version

# Install AWS CLI v2 from the official bundled installer, with GPG signature
# verification against the AWS CLI Team public key (key ID A6310ACC4672475C,
# fingerprint FB5DB77FD5C118B80511ADA8A6310ACC4672475C, valid through 2026-07).
# Versioned URL is used so builds are reproducible; bump AWSCLI_VERSION when
# upgrading. See https://docs.aws.amazon.com/cli/latest/userguide/getting-started-version.html
ARG AWSCLI_VERSION=2.34.46
RUN set -eu \
    && cat > /tmp/aws-cli.pub <<'EOF'
-----BEGIN PGP PUBLIC KEY BLOCK-----

mQINBF2Cr7UBEADJZHcgusOJl7ENSyumXh85z0TRV0xJorM2B/JL0kHOyigQluUG
ZMLhENaG0bYatdrKP+3H91lvK050pXwnO/R7fB/FSTouki4ciIx5OuLlnJZIxSzx
PqGl0mkxImLNbGWoi6Lto0LYxqHN2iQtzlwTVmq9733zd3XfcXrZ3+LblHAgEt5G
TfNxEKJ8soPLyWmwDH6HWCnjZ/aIQRBTIQ05uVeEoYxSh6wOai7ss/KveoSNBbYz
gbdzoqI2Y8cgH2nbfgp3DSasaLZEdCSsIsK1u05CinE7k2qZ7KgKAUIcT/cR/grk
C6VwsnDU0OUCideXcQ8WeHutqvgZH1JgKDbznoIzeQHJD238GEu+eKhRHcz8/jeG
94zkcgJOz3KbZGYMiTh277Fvj9zzvZsbMBCedV1BTg3TqgvdX4bdkhf5cH+7NtWO
lrFj6UwAsGukBTAOxC0l/dnSmZhJ7Z1KmEWilro/gOrjtOxqRQutlIqG22TaqoPG
fYVN+en3Zwbt97kcgZDwqbuykNt64oZWc4XKCa3mprEGC3IbJTBFqglXmZ7l9ywG
EEUJYOlb2XrSuPWml39beWdKM8kzr1OjnlOm6+lpTRCBfo0wa9F8YZRhHPAkwKkX
XDeOGpWRj4ohOx0d2GWkyV5xyN14p2tQOCdOODmz80yUTgRpPVQUtOEhXQARAQAB
tCFBV1MgQ0xJIFRlYW0gPGF3cy1jbGlAYW1hem9uLmNvbT6JAlQEEwEIAD4CGwMF
CwkIBwIGFQoJCAsCBBYCAwECHgECF4AWIQT7Xbd/1cEYuAURraimMQrMRnJHXAUC
aGveYQUJDMpiLAAKCRCmMQrMRnJHXKBYD/9Ab0qQdGiO5hObchG8xh8Rpb4Mjyf6
0JrVo6m8GNjNj6BHkSc8fuTQJ/FaEhaQxj3pjZ3GXPrXjIIVChmICLlFuRXYzrXc
Pw0lniybypsZEVai5kO0tCNBCCFuMN9RsmmRG8mf7lC4FSTbUDmxG/QlYK+0IV/l
uJkzxWa+rySkdpm0JdqumjegNRgObdXHAQDWlubWQHWyZyIQ2B4U7AxqSpcdJp6I
S4Zds4wVLd1WE5pquYQ8vS2cNlDm4QNg8wTj58e3lKN47hXHMIb6CHxRnb947oJa
pg189LLPR5koh+EorNkA1wu5mAJtJvy5YMsppy2y/kIjp3lyY6AmPT1posgGk70Z
CmToEZ5rbd7ARExtlh76A0cabMDFlEHDIK8RNUOSRr7L64+KxOUegKBfQHb9dADY
qqiKqpCbKgvtWlds909Ms74JBgr2KwZCSY1HaOxnIr4CY43QRqAq5YHOay/mU+6w
hhmdF18vpyK0vfkvvGresWtSXbag7Hkt3XjaEw76BzxQH21EBDqU8WJVjHgU6ru+
DJTs+SxgJbaT3hb/vyjlw0lK+hFfhWKRwgOXH8vqducF95NRSUxtS4fpqxWVaw3Q
V2OWSjbne99A5EPEySzryFTKbMGwaTlAwMCwYevt4YT6eb7NmFhTx0Fis4TalUs+
j+c7Kg92pDx2uQ==
=OBAt
-----END PGP PUBLIC KEY BLOCK-----
EOF
RUN set -eu \
    && gpg --import /tmp/aws-cli.pub \
    && curl -fsSL -o /tmp/awscliv2.zip "https://awscli.amazonaws.com/awscli-exe-linux-x86_64-${AWSCLI_VERSION}.zip" \
    && curl -fsSL -o /tmp/awscliv2.zip.sig "https://awscli.amazonaws.com/awscli-exe-linux-x86_64-${AWSCLI_VERSION}.zip.sig" \
    && gpg --verify /tmp/awscliv2.zip.sig /tmp/awscliv2.zip \
    && unzip -q /tmp/awscliv2.zip -d /tmp \
    && { if [ -d /usr/local/aws-cli ]; then /tmp/aws/install --update; else /tmp/aws/install; fi; } \
    && aws --version \
    && rm -rf /tmp/awscliv2.zip /tmp/awscliv2.zip.sig /tmp/aws /tmp/aws-cli.pub /root/.gnupg

# Copy CRIU from builder
COPY --from=criu-builder /criu-install/usr/local /usr/local
COPY --from=criu-builder /tmp/criu/COPYING /legal/CRIU/COPYING
RUN criu --version

# Copy CUDA checkpoint binaries
COPY --from=criu-builder /tmp/cuda-checkpoint/bin/x86_64_Linux/cuda-checkpoint /usr/local/sbin/cuda-checkpoint
COPY --from=criu-builder /tmp/cuda-checkpoint/LICENSE /legal/cuda-checkpoint/LICENSE
COPY --from=cuda-helper-builder /cuda-checkpoint-helper /usr/local/bin/cuda-checkpoint-helper
RUN chmod +x /usr/local/sbin/cuda-checkpoint /usr/local/bin/cuda-checkpoint-helper

# Copy the built binaries
COPY --from=builder /snapshot-agent /usr/local/bin/snapshot-agent
COPY --from=builder /nsrestore /usr/local/bin/nsrestore
COPY --from=builder /criu-stream-fetch /usr/local/sbin/criu-stream-fetch
RUN chmod +x /usr/local/sbin/criu-stream-fetch

# C++ NIXL OBJ streamer (has the NIXL_WRITE upload path, --upload/uploadMain)
# + its self-consistent NIXL/aws-sdk runtime. Under /opt/nixl-runtime so it
# can't collide with the base's fragmented NIXL.
COPY --from=nixl-obj-builder /criu-stream-fetch-cpp /usr/local/sbin/criu-stream-fetch-cpp
COPY --from=nixl-obj-builder /ckpt-stream /usr/local/sbin/ckpt-stream
COPY --from=nixl-obj-builder /usr/local/lib /opt/nixl-runtime/lib
RUN chmod +x /usr/local/sbin/criu-stream-fetch-cpp /usr/local/sbin/ckpt-stream \
    && printf '%s\n' /opt/nixl-runtime/lib /opt/nixl-runtime/lib/x86_64-linux-gnu \
        > /etc/ld.so.conf.d/nixl-runtime.conf \
    && ldconfig \
    && echo "=== OBJ plugin ===" && find /opt/nixl-runtime -name libplugin_OBJ.so \
    && echo "=== streamer ldd ===" \
    && ( ldd /usr/local/sbin/criu-stream-fetch-cpp | grep -qi "not found" \
         && echo "WARN: unresolved NIXL libs" || echo "NIXL libs resolved" )

# Create directories
RUN mkdir -p /checkpoints /var/run/snapshot

USER root

# =============================================================================
# Stage: Compliance: licenses
# =============================================================================
# Runs in agent_pre's actual filesystem so dpkg-query enumerates the real
# runtime package set (cuda-dl-base + everything we apt-get installed in
# agent_pre). cyclonedx-gomod produced /sbom-go.cdx.json upstream in the
# Go builder; we COPY it in here for the go generator.
#
# `compliance` build context is plumbed by .github/actions/build-deploy-component/action.yml
# as `--build-context compliance=../../container/compliance`.
FROM agent_pre AS licenses

USER root
RUN mkdir -p /legal /sboms
# Freeze the shipped dpkg database and copyright files before installing
# compliance-only helpers. The final image is FROM agent_pre, so packages
# installed below must not appear in dpkg NOTICES/OSRB output.
RUN mkdir -p /runtime-root/var/lib /runtime-root/usr/share \
    && cp -a /var/lib/dpkg /runtime-root/var/lib/ \
    && cp -a /usr/share/doc /runtime-root/usr/share/
# Install python3 + python3-yaml for the compliance tooling only. PyYAML is
# required for license_overrides.yaml; dpkg scanning reads /runtime-root so
# these helper packages are not attributed as shipped content.
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-yaml \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
COPY --from=compliance . /opt/compliance
ENV PYTHONPATH=/opt
COPY --from=builder /sbom-go.cdx.json /tmp/sbom-go.cdx.json
COPY --from=builder /sbom-go-licenses /tmp/go-licenses

# go + dpkg cover the runtime image's apt + Go module set. native
# enumerates from-source components (CRIU, cuda-checkpoint,
# cuda-checkpoint-helper) — these are attributed by the manual
# `COPY /tmp/criu/COPYING /legal/CRIU/COPYING` lines in agent_pre, which
# the native generator reads via license_text_path to inline in NOTICES.
# BASELINE_SBOM_FILE (optional, empty default): the per-arch baseline SBOM
# *stem* under /opt/compliance/base_sboms/. We append -${TARGETARCH}.cdx.json
# so the matching-arch floor is subtracted (snapshot ships amd64-only today).
ARG BASELINE_SBOM_FILE=""
ARG TARGETARCH
RUN python3 -m compliance.generators \
    --ecosystem go,dpkg,native \
    --go-sbom /tmp/sbom-go.cdx.json \
    --go-licenses-dir /tmp/go-licenses \
    --native-yaml /opt/compliance/native_packages.yaml \
    --native-image snapshot-agent \
    --output-dir /legal \
    --policy /opt/compliance/policy/licenses.toml \
    --dpkg-root /runtime-root \
    ${BASELINE_SBOM_FILE:+--subtract-sbom /opt/compliance/base_sboms/${BASELINE_SBOM_FILE}-${TARGETARCH}.cdx.json} \
    -v
# Policy gate on the single unified CSV (its `ecosystem` column scopes each row).
RUN python3 -m compliance.policy.validate \
        --policy /opt/compliance/policy/licenses.toml \
        --input /legal/osrb-deps.csv


# =============================================================================
# Stage: Compliance: artifact (single unified -compliance tree for CI extraction)
# =============================================================================
FROM scratch AS compliance_artifact
COPY --from=licenses /legal/ /


# =============================================================================
# Stage: Compliance: sources
# =============================================================================
# Collects source archives for dpkg deltas, third-party Go modules, and
# preserved native source trees for OSRB submission. Output is a separate
# sources zip cross-referenced in the OSRB bundle's build-provenance.json;
# never COPY'd into the runtime.
FROM agent_pre AS sources_collect
WORKDIR /
RUN mkdir -p /sources /opt/compliance /opt/go-vendor /opt/native-sources /runtime-root/var/lib \
    && cp -a /var/lib/dpkg /runtime-root/var/lib/
# Python is needed only to run compliance.collect_sources. The dpkg package
# set comes from /runtime-root, captured before this install.
RUN apt-get update && apt-get install -y --no-install-recommends python3 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
COPY --from=compliance . /opt/compliance
ENV PYTHONPATH=/opt
COPY --from=builder /tmp/go-vendor/ /opt/go-vendor/
# Native source trees (CRIU sources + cuda-checkpoint clone) from criu-builder.
#
# These COPYs are unconditional (not gated on ENABLE_SOURCE_ARCHIVAL) by
# design: sources_collect is its own stage that is NEVER COPY'd into the
# runtime agent image — the runtime only consumes /sources.zip via the
# `sources_archive` stage. So copying the source trees in here costs nothing
# in the shipped image. Gating them would risk archive completeness (an ARG
# flip would silently drop CRIU+cuda-checkpoint from the archive).
COPY --from=criu-builder /tmp/criu/ /opt/native-sources/criu/
COPY --from=criu-builder /tmp/cuda-checkpoint/ /opt/native-sources/cuda-checkpoint/

ARG ENABLE_SOURCE_ARCHIVAL=false
ARG BASELINE_SBOM_FILE=""
ARG TARGETARCH
RUN if [ "$ENABLE_SOURCE_ARCHIVAL" = "true" ]; then \
        python3 -m compliance.collect_sources \
            --ecosystem dpkg --ecosystem go --ecosystem native \
            --output-zip /sources.zip \
            --sources-root /sources \
            --dpkg-root /runtime-root \
            --go-vendor-dir /opt/go-vendor \
            --native-source-dir /opt/native-sources \
            ${BASELINE_SBOM_FILE:+--baseline-sbom /opt/compliance/base_sboms/${BASELINE_SBOM_FILE}-${TARGETARCH}.cdx.json} \
            -v ; \
    else \
        : > /sources.zip ; \
    fi


FROM scratch AS sources_archive
COPY --from=sources_collect /sources.zip /sources.zip


# =============================================================================
# Stage: Agent - Final snapshot-agent image
# =============================================================================
FROM agent_pre AS agent
COPY --from=licenses /legal /legal

ENTRYPOINT ["/usr/local/bin/snapshot-agent"]

# =============================================================================
# Stage: Placeholder - Runtime-compatible restore image (requires BASE_IMAGE arg)
# This image is a superset of the runtime image: same default execution contract
# (entrypoint/cmd/user), plus CRIU/cuda-checkpoint tooling for external restore.
# Dynamo backend entrypoints run a restore standby path when the operator
# injects DYN_SNAPSHOT_RESTORE_STANDBY=1, then sleep while the
# snapshot-agent restores into the container.
# =============================================================================
FROM ${BASE_IMAGE} AS placeholder

ARG BASE_IMAGE
ARG TARGETARCH=amd64
ENV ORIGINAL_BASE_IMAGE=${BASE_IMAGE}

USER root

RUN if [ "${TARGETARCH}" != "amd64" ]; then \
      echo "ERROR: Dynamo Snapshot requires x86_64 (cuda-checkpoint has no ${TARGETARCH} binary)" >&2; exit 1; \
    fi

# Install minimal runtime dependencies for CRIU restore (nsrestore runs here via nsenter)
# plus S3 streaming tooling (lz4 + s5cmd + AWS CLI v2) since
# nsrestore --checkpoint-storage-type=s3 executes inside this namespace.
# unzip+gnupg are required by the AWS CLI v2 bundled installer + sig verify
# (see agent stage above for rationale).
RUN apt-get update && apt-get install -y --no-install-recommends \
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
    ca-certificates \
    lz4 \
    curl \
    wget \
    unzip \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

ARG S5CMD_VERSION=2.3.0
RUN curl -fsSL -o /tmp/s5cmd.tar.gz \
      "https://github.com/peak/s5cmd/releases/download/v${S5CMD_VERSION}/s5cmd_${S5CMD_VERSION}_Linux-64bit.tar.gz" \
    && tar -xzf /tmp/s5cmd.tar.gz -C /usr/local/bin s5cmd \
    && rm -f /tmp/s5cmd.tar.gz \
    && s5cmd version

# JuiceFS CE CLI: used by ExecuteRestoreS3Direct when RESTORE_SOURCE=juicefs:...
# to run `juicefs warmup` against the FUSE-mounted checkpoint dir before
# CRIU restore. Pre-populates the JFS local cache with parallel S3 GETs so
# CRIU's serial mmap reads hit cache instead of going through FUSE per-page.
ARG JUICEFS_VERSION=1.3.1
RUN curl -fsSL -o /tmp/juicefs.tar.gz \
      "https://github.com/juicedata/juicefs/releases/download/v${JUICEFS_VERSION}/juicefs-${JUICEFS_VERSION}-linux-amd64.tar.gz" \
    && tar -xzf /tmp/juicefs.tar.gz -C /usr/local/bin juicefs \
    && rm -f /tmp/juicefs.tar.gz \
    && juicefs version

# Install AWS CLI v2 from the official bundled installer with GPG sig verify
# (same approach + key block as the agent stage above).
ARG AWSCLI_VERSION=2.34.46
RUN set -eu \
    && cat > /tmp/aws-cli.pub <<'EOF'
-----BEGIN PGP PUBLIC KEY BLOCK-----

mQINBF2Cr7UBEADJZHcgusOJl7ENSyumXh85z0TRV0xJorM2B/JL0kHOyigQluUG
ZMLhENaG0bYatdrKP+3H91lvK050pXwnO/R7fB/FSTouki4ciIx5OuLlnJZIxSzx
PqGl0mkxImLNbGWoi6Lto0LYxqHN2iQtzlwTVmq9733zd3XfcXrZ3+LblHAgEt5G
TfNxEKJ8soPLyWmwDH6HWCnjZ/aIQRBTIQ05uVeEoYxSh6wOai7ss/KveoSNBbYz
gbdzoqI2Y8cgH2nbfgp3DSasaLZEdCSsIsK1u05CinE7k2qZ7KgKAUIcT/cR/grk
C6VwsnDU0OUCideXcQ8WeHutqvgZH1JgKDbznoIzeQHJD238GEu+eKhRHcz8/jeG
94zkcgJOz3KbZGYMiTh277Fvj9zzvZsbMBCedV1BTg3TqgvdX4bdkhf5cH+7NtWO
lrFj6UwAsGukBTAOxC0l/dnSmZhJ7Z1KmEWilro/gOrjtOxqRQutlIqG22TaqoPG
fYVN+en3Zwbt97kcgZDwqbuykNt64oZWc4XKCa3mprEGC3IbJTBFqglXmZ7l9ywG
EEUJYOlb2XrSuPWml39beWdKM8kzr1OjnlOm6+lpTRCBfo0wa9F8YZRhHPAkwKkX
XDeOGpWRj4ohOx0d2GWkyV5xyN14p2tQOCdOODmz80yUTgRpPVQUtOEhXQARAQAB
tCFBV1MgQ0xJIFRlYW0gPGF3cy1jbGlAYW1hem9uLmNvbT6JAlQEEwEIAD4CGwMF
CwkIBwIGFQoJCAsCBBYCAwECHgECF4AWIQT7Xbd/1cEYuAURraimMQrMRnJHXAUC
aGveYQUJDMpiLAAKCRCmMQrMRnJHXKBYD/9Ab0qQdGiO5hObchG8xh8Rpb4Mjyf6
0JrVo6m8GNjNj6BHkSc8fuTQJ/FaEhaQxj3pjZ3GXPrXjIIVChmICLlFuRXYzrXc
Pw0lniybypsZEVai5kO0tCNBCCFuMN9RsmmRG8mf7lC4FSTbUDmxG/QlYK+0IV/l
uJkzxWa+rySkdpm0JdqumjegNRgObdXHAQDWlubWQHWyZyIQ2B4U7AxqSpcdJp6I
S4Zds4wVLd1WE5pquYQ8vS2cNlDm4QNg8wTj58e3lKN47hXHMIb6CHxRnb947oJa
pg189LLPR5koh+EorNkA1wu5mAJtJvy5YMsppy2y/kIjp3lyY6AmPT1posgGk70Z
CmToEZ5rbd7ARExtlh76A0cabMDFlEHDIK8RNUOSRr7L64+KxOUegKBfQHb9dADY
qqiKqpCbKgvtWlds909Ms74JBgr2KwZCSY1HaOxnIr4CY43QRqAq5YHOay/mU+6w
hhmdF18vpyK0vfkvvGresWtSXbag7Hkt3XjaEw76BzxQH21EBDqU8WJVjHgU6ru+
DJTs+SxgJbaT3hb/vyjlw0lK+hFfhWKRwgOXH8vqducF95NRSUxtS4fpqxWVaw3Q
V2OWSjbne99A5EPEySzryFTKbMGwaTlAwMCwYevt4YT6eb7NmFhTx0Fis4TalUs+
j+c7Kg92pDx2uQ==
=OBAt
-----END PGP PUBLIC KEY BLOCK-----
EOF
RUN set -eu \
    && gpg --import /tmp/aws-cli.pub \
    && curl -fsSL -o /tmp/awscliv2.zip "https://awscli.amazonaws.com/awscli-exe-linux-x86_64-${AWSCLI_VERSION}.zip" \
    && curl -fsSL -o /tmp/awscliv2.zip.sig "https://awscli.amazonaws.com/awscli-exe-linux-x86_64-${AWSCLI_VERSION}.zip.sig" \
    && gpg --verify /tmp/awscliv2.zip.sig /tmp/awscliv2.zip \
    && unzip -q /tmp/awscliv2.zip -d /tmp \
    && { if [ -d /usr/local/aws-cli ]; then /tmp/aws/install --update; else /tmp/aws/install; fi; } \
    && aws --version \
    && rm -rf /tmp/awscliv2.zip /tmp/awscliv2.zip.sig /tmp/aws /tmp/aws-cli.pub /root/.gnupg

# Copy CRIU from builder (needed by nsrestore running inside these namespaces)
COPY --from=criu-builder /criu-install/usr/local /usr/local
COPY --from=criu-builder /tmp/criu/COPYING /legal/CRIU/COPYING
RUN criu --version && echo "CRIU installed successfully"

# Copy CUDA checkpoint binaries
COPY --from=criu-builder /tmp/cuda-checkpoint/bin/x86_64_Linux/cuda-checkpoint /usr/local/sbin/cuda-checkpoint
COPY --from=criu-builder /tmp/cuda-checkpoint/LICENSE /legal/cuda-checkpoint/LICENSE
COPY --from=cuda-helper-builder /cuda-checkpoint-helper /usr/local/bin/cuda-checkpoint-helper
RUN chmod +x /usr/local/sbin/cuda-checkpoint /usr/local/bin/cuda-checkpoint-helper

# Copy nsrestore binary (invoked by DaemonSet via nsenter)
COPY --from=builder /nsrestore /usr/local/bin/nsrestore
RUN chmod +x /usr/local/bin/nsrestore

# Copy Pipeline C streamer (spawned by nsrestore when STREAM_MODE=c).
COPY --from=builder /criu-stream-fetch /usr/local/sbin/criu-stream-fetch
RUN chmod +x /usr/local/sbin/criu-stream-fetch

# Create directories
RUN mkdir -p /checkpoints /var/run/criu /var/criu-work
