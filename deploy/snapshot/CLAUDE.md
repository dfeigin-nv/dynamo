# Dynamo Snapshot Agent — Restore Architecture

This directory is the **Dynamo snapshot agent**: a Kubernetes DaemonSet that
checkpoints and restores GPU inference pods (vLLM) using **CRIU** (CPU state) +
**`cuda-checkpoint`** (GPU state). It checkpoints a running pod into a checkpoint
image (local PVC or S3) and later restores that image into a *placeholder* pod on
some node, bringing the model back to a live, warmed state without re-loading
weights from scratch.

This file explains the **restore data paths**, because there are several and they
coexist behind env/Helm flags. If you are reading restore code and wondering
"wait, which path is actually running?", start here.

> Branch note: `juicefs-restore-bench` is a **benchmark branch**. It deliberately
> keeps multiple restore strategies side-by-side so they can be A/B'd via
> `helm upgrade`. Do not assume one path is "the" path — check the flags below.

## The core bottleneck

CRIU restores private/shmem pages with **single-threaded `mmap` + sequential
reads**. Against S3 that caps restore throughput at **~2–3 Gbps** even with
prefetch, because effective read concurrency is ~5. Every restore strategy below
is an attempt to beat that ceiling. The model weight pages (vLLM `sleep()`-pinned
**CPU weight-shadow memfds**) dominate restore time for large models
(e.g. gpt-oss-120b), so that is what they optimize.

## Restore paths (S3-backed)

Selection happens in `internal/criu/restore.go` (top-level dispatch) and
`internal/criu/streams3/` (the S3 implementations). Helm wires the env in
`deploy/helm/charts/snapshot/templates/daemonset.yaml`.

| Path | How to select | What it does | Status |
| --- | --- | --- | --- |
| **A. S3 direct (s5cmd + tmpfs)** | `storage.s3.directMode=true` (`S3_DIRECT=1`), `restoreSource=""` | `ExecuteRestoreS3Direct` parallel-downloads `pages-*.img` into a tmpfs via **s5cmd**, then CRIU restores from local disk. Stage-then-restore. | **Default** |
| **B. JuiceFS warmup** | `storage.s3.restoreSource="juicefs:/mnt/jfs/checkpoints/<id>"` | Skips tmpfs+s5cmd; points CRIU at a **JuiceFS FUSE mount** and runs `juicefs warmup` (50 parallel workers) into the local cache *before* CRIU restore, so CRIU's serial mmap reads hit warm cache (~10–12 Gbps) instead of FUSE-serial S3 (~2–3 Gbps). | Active bench (the branch's namesake) |
| **C. Pipeline C (NIXL OBJ → memfd)** | `STREAM_MODE=c` (streaming path, `S3_DIRECT=0`) | `criu-stream-fetch-cpp` + a **NIXL OBJ** bytes-mover stream `pages-*.img` from S3 *directly into memfd*, overlapped with the PIE restorer via an ack/pipe-per-memfd handover. The most invasive (and most parallel) approach. `CRIU_REF=streaming-restore-pipeline-c`. | Built, **intentionally OFF** — Helm sets `STREAM_MODE=""` as defense-in-depth |

Path A and Path B both ultimately read the same S3 objects; the difference is
**who parallelizes the GETs** (s5cmd vs `juicefs warmup`). Path C avoids the
local stage entirely by streaming straight into the target memfds.

### Key restore env knobs

- `S3_DIRECT` — `1` = direct (Path A/B via `ExecuteRestoreS3Direct`); `0` = streaming (Path C).
- `RESTORE_SOURCE` — empty = s5cmd+tmpfs (A); `juicefs:<path>` = FUSE mount (B). Value is the **full per-checkpoint path**, agent does not synthesize it from hash.
- `STREAM_MODE` — `c` engages Pipeline C; cleared to `""` by Helm by default.
- `RESTORE_NO_WARMUP=1` — disables the JuiceFS warmup (benchmark knob: measure cold cache).
- `S3_SHARD_LZ4` — LZ4 shard decode, only consulted on the streaming path.
- `CRIU_CACHE_BUST` — bump to force a rebuild that pulls a new CRIU fork patch.

PVC-backed restore (`storage.type` PVC) is the simple path: CRIU reads
`pages-*.img` from the mounted volume directly.

## Node-local memfd content cache (orthogonal, ships dark)

`internal/memfdcache/` is a **separate axis** from the S3 path choice. It holds
populated, **`F_SEAL_FUTURE_WRITE`-sealed** memfds open across restores so a
second restore of the same checkpoint *on the same node* borrows the
already-filled inode (one shared physical copy) over `SCM_RIGHTS` instead of
re-reading and re-copying `pages-<shmid>.img`. For N pods of the same model on a
node: one RAM copy + sub-second fd-recv+mmap per pod.

- **Off by default** (`memfdCache.enabled` / `MEMFD_CACHE_ENABLED=1`).
- The server lives for the DaemonSet lifetime (`cmd/agent/main.go`), so it
  outlives any single `nsrestore` process.
- Correctness: only seal-verified inodes are shared; a HIT fd is kernel-state
  identical to what a MISS would produce after fill+seal, so downstream CRIU
  steps are unchanged. `uid`/`gid` are part of the cache key (one owner per
  inode).
- The CRIU client half lives in the **`criu-upstream`** and **`go-criu`** repos
  (`memfd-cache.c`, `SetMemfdCacheSock`, `CRIU_MEMFD_CACHE_SOCK`); this repo only
  has the **agent-side server + wiring**. The `go-criu` fork pin in `go.mod`
  carries the matching RPC fields.
- Full design + verification plan: **`internal/memfdcache/DESIGN.md`**.

## File map

| Area | Files |
| --- | --- |
| Restore dispatch | `internal/criu/restore.go` (`ExecuteRestore`, `ApplyMemfdCache`) |
| S3 restore impls | `internal/criu/streams3/{direct,stream,pipes,manifest}.go` |
| Checkpoint | `internal/executor/checkpoint.go`, `internal/criu/dump.go` |
| Restore orchestration | `internal/executor/restore.go`, `internal/executor/nsrestore.go`, `cmd/nsrestore/main.go` |
| Agent / controller | `cmd/agent/main.go`, `internal/controller/controller.go` |
| Container runtime | `internal/runtime/oci_containerd.go` (`ResolveContainer` by ID — avoids stale-PID restores) |
| CUDA | `internal/cuda/cuda.go` (lock/checkpoint/restore via `cuda-checkpoint`) |
| memfd cache | `internal/memfdcache/` (+ `DESIGN.md`), `cmd/memfdcache-testserve/` |
| NIXL (Pipeline C) | `third_party/nixl/` (vendored headers; `.so` is gitignored) |
| Helm | `deploy/helm/charts/snapshot/{values.yaml,templates/daemonset.yaml}` |

## Build / verify

```bash
cd deploy/snapshot
go build ./...
go vet ./...
go test ./internal/memfdcache/...   # 8 unit tests (protocol, LRU, TTL, seal gate)
```

The end-to-end checkpoint/restore + S3-path benchmarks live in the sibling
`snapshot-testing` harness (`~/Work/checkpoints/snapshot-testing`), not here.
