# Node-local memfd content cache

Holds **populated, sealed memfds open across restores** so a second restore of
the same checkpoint on the same node borrows the already-filled inode (one
shared physical copy) instead of re-reading `pages-<shmid>.img` and copying it
into a fresh memfd. For N pods of the same model on a node: one RAM copy + a
sub-second fd-recv+mmap per pod, instead of N full read+copy fills.

Targets the vLLM `sleep()` pinned **CPU weight-shadow** memfds, which dominate
the CRIU restore stage for large models (e.g. gpt-oss-120b). VRAM is a separate
cuda-checkpoint path and out of scope.

Ships **dark**: off unless `memfdCache.enabled` (config) / `MEMFD_CACHE_ENABLED=1`.

## Correctness gate (the whole safety argument)

Only inodes sealed `F_SEAL_FUTURE_WRITE` are cached/shared. The kernel then
forbids any writable mapping of the inode, so the one shared copy cannot be
corrupted by any borrower. The server **independently verifies** the seal on the
donated fd (`F_GET_SEALS`); it never trusts the request's declared seals.

A **HIT fd is kernel-state-identical** to what a MISS would produce after
fill+seal (a sealed, sized, correctly-owned memfd in the fdstore), so every
downstream CRIU step (open / reopen / mapping / seal) is unchanged. HIT only
skips the fill, the re-seal (already sealed), and the chown (already owned).

`uid`/`gid` are part of the cache key because a memfd inode has exactly one
owner: a different owner is a clean MISS that fills its own copy ("a copy per
owner"); same-owner pods (the common case) share one copy. This is also why
owner-in-key is load-bearing for userns restores — see
`criu-upstream/CLAUDE.md` "Memfd Restore Notes".

Hugetlb memfds are excluded from caching in v1.

## Wire protocol

One `AF_UNIX SOCK_SEQPACKET` socketpair brackets one restore. The agent holds
one end and services it (`Server.serve`); the other is inherited by CRIU via
`CRIU_MEMFD_CACHE_SOCK` (mirrors the Pipeline-C streamer sockets). SCM_RIGHTS
over the inherited socketpair is namespace-agnostic, so the fd crosses the
placeholder mount namespace with no bind-mount. Closing the criu end (restore
done) releases every borrow taken on it.

Fixed little-endian structs, **byte-identical** between C
(`criu-upstream/criu/include/memfd-cache.h`) and Go (`protocol.go`) — verified by
an `offsetof`/`sizeof` probe (key=144, req=160, resp=8):

- GET: client → `req{op=GET,size,key}`; server → `resp{HIT|MISS}`; on HIT then
  fd via SCM_RIGHTS. (MISS carries no fd — `recv_fds` cannot represent zero fds.)
- DONATE: client → `req{op=DONATE,seals,size,key}` then fd via SCM_RIGHTS;
  server → `resp{OK|DECLINE}`. A DECLINE (over budget) is not an error — the
  caller already has its own filled copy.

## Components

| Layer | Files |
| --- | --- |
| Proto | `criu-upstream/images/rpc.proto`, `go-criu/rpc/rpc.proto` — `memfd_cache=73`, `memfd_cache_id=74` |
| CRIU client | `criu-upstream/criu/memfd-cache.c` + `include/memfd-cache.h` |
| CRIU hooks | `memfd.c` `memfd_content_prepare` (HIT/MISS) + `memfd_content_donate` + `memfd_content_prime`; `cr-restore.c` donate-after-seal + prime early-exit; `cr-service.c` opts; `config.c` CLI opts |
| go-criu | `main.go` `SetMemfdCacheSock` + `CRIU_MEMFD_CACHE_SOCK` |
| Cache server | `internal/memfdcache/{server,protocol,scm}.go` |
| Agent wiring | `cmd/agent/main.go` (start), `controller.go` (thread), `executor/restore.go` (socketpair+`--memfd-cache-fd`), `cmd/nsrestore/main.go` (flags), `internal/criu/restore.go` `ApplyMemfdCache` (PVC + S3-direct), `streams3/stream.go` (S3-stream) |

Restore data path: agent `execNSRestore` opens a session (socketpair) → criu end
rides `nsenter` ExtraFiles (fd 3) → `nsrestore --memfd-cache-fd 3 --memfd-cache-id <ckpt>`
→ go-criu `SetMemfdCacheSock` → criu swrk reads `CRIU_MEMFD_CACHE_SOCK`.

## Lifetime / eviction

- Server lives for the DaemonSet lifetime (`cmd/agent/main.go`), so it outlives
  any single restore (today memfds die with the `nsrestore` process).
- Hard RAM budget (`memfdCache.maxBytes`): on donate, LRU-evict `refcount==0`
  entries to fit; if still over, DECLINE (correctness unaffected).
- `refcount` tracks in-flight borrows (GET++ / connection-close--); never evict
  while borrowed.
- Idle TTL (`memfdCache.idleTTLSeconds`) evicts cold entries.
- A new `version` (different `memfd_cache_id`) supersedes a checkpoint's older
  inodes; `Server.Invalidate(checkpointID)` for controller-driven deletes.
- Agent restart = cold start (acceptable).

## Eager primer

`criu restore --memfd-cache-prime --memfd-cache-id <id> -D <imgdir>` (with
`CRIU_MEMFD_CACHE_SOCK` set) fills+seals+donates every eligible inode, then stops
before forking a task tree — so even the **first** real restore is a HIT. It runs
inside `crtools_prepare_shared()`, reusing the same validated image/page-read
init and `memfd_shmem_fill_content` as a normal restore.

## Verification

**Done locally (this change):**
- `criu-upstream`: `make criu/criu` builds clean (client, donate, primer, opts).
- `go-criu`: `go build ./...` clean.
- agent: `go build ./...` clean; `go vet` clean.
- `internal/memfdcache`: 8 unit tests pass — protocol round-trip over a real
  socketpair (HIT/MISS/donate), LRU eviction, refcount-pins-budget, idle-TTL
  sweep, version invalidation, unsealed-decline, wrong-size/owner MISS.
- C↔Go wire layout probe: `LAYOUT MATCH` (144/160/8, field offsets).
- CLI option parse: criu accepts `--memfd-cache-prime` / `--memfd-cache-id`.
- Cache-off no-op: gated off by default, so existing `zdtm memfd00..06`,
  `shmemfd*` exercise the unchanged path.

**Deferred to a GPU node (needs a real checkpoint image / cluster):**
- **Win:** gpt-oss-120b, same node — pod #1 (donates) vs pod #2 (HITs) `criu_s` /
  `CRIURestoreDuration` (`executor/nsrestore.go`). Expect the memfd-fill
  component to collapse to fd-recv + mmap (sub-second). Add a `memfd-cache
  cold/warm` axis to the snapshot-testing restore benchmark alongside the
  existing page-cache cold/warm.
- **Shared, not copied:** after two pods restore, `/proc/<pid>/smaps_rollup`
  `Shared_*` on the weight VMAs and node `MemAvailable` → prove one physical copy.
- **Correctness:** userns donate→HIT cycle accepts the pre-owned fd; a borrowed
  memfd is still `F_SEAL_FUTURE_WRITE` in pod #2 and a writable-mapping attempt
  fails; multi-pod same-model restore — pod #2 logs HITs, both pass
  `ValidateProcessState`, inference output correct.
