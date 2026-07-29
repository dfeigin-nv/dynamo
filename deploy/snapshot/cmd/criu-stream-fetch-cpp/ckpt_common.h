// ckpt_common.h — shared CUDA custom-storage + NIXL OBJ plumbing for the
// Phase-0 split checkpoint/restore binaries (ckpt_stream_ckpt.cpp /
// ckpt_stream_restore.cpp). Carved out of the fused PoC ckpt_stream.cpp so the
// checkpoint half and the restore half can run in SEPARATE processes with a
// CRIU dump/restore in between.
//
// CUDA is resolved via dlopen(libcuda.so.1) at runtime (driver-injected), so
// this builds with no CUDA toolkit — only NIXL headers/libs.

#pragma once

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <algorithm>
#include <dlfcn.h>

#include "nixl.h"
#include "nixl_descriptors.h"

// ---- CUDA driver + checkpoint API (hand-declared, matches r615 cuda.h) ----
typedef int CUresult;
typedef int CUdevice;
typedef unsigned long long CUdeviceptr;
typedef struct CUctx_st *CUcontext;
typedef struct CUstream_st *CUstream;

typedef struct { CUdeviceptr devPtr; size_t size; CUstream stream; } PerDev;
typedef struct CUIcheckpointOperation_st *OpHandle;
typedef struct { OpHandle handle; PerDev *perDeviceData; unsigned deviceCount; } CsInfo;
typedef struct { unsigned timeoutMs; unsigned r0; unsigned long long r1[7]; } LockArgs;
typedef struct { CsInfo **out; char reserved[64 - sizeof(CsInfo *)]; } CkptArgs;
typedef struct CUuuid_st { char b[16]; } CUuuid;
typedef struct { CUuuid o, n; } GpuPair;
typedef struct { GpuPair *pairs; unsigned pairsCount; unsigned pad0; CsInfo **out;
                 char reserved[64 - sizeof(GpuPair *) - 2 * sizeof(unsigned) - sizeof(CsInfo **)]; } RestoreArgs;
typedef struct { unsigned long long reserved[8]; } UnlockArgs;

#define CU_MEMHOSTALLOC_PORTABLE 0x01

static CUresult (*cuInit)(unsigned);
static CUresult (*cuDeviceGetCount)(int *);
static CUresult (*cuDeviceGet)(CUdevice *, int);
static CUresult (*cuDevicePrimaryCtxRetain)(CUcontext *, CUdevice);
static CUresult (*cuCtxSetCurrent)(CUcontext);
static CUresult (*cuStreamGetCtx)(CUstream, CUcontext *);
static CUresult (*cuCheckpointProcessGetState)(int, int *);
static CUresult (*cuCheckpointProcessLock)(int, LockArgs *);
static CUresult (*cuCheckpointProcessCheckpoint)(int, CkptArgs *);
static CUresult (*cuCheckpointProcessRestore)(int, RestoreArgs *);
static CUresult (*cuCheckpointProcessUnlock)(int, UnlockArgs *);
static CUresult (*cuCheckpointOperationComplete)(OpHandle);
static CUresult (*cuMemHostAlloc)(void **, size_t, unsigned);
static CUresult (*cuMemFreeHost)(void *);
static CUresult (*cuMemcpyDtoHAsync_v2)(void *, CUdeviceptr, size_t, CUstream);
static CUresult (*cuMemcpyHtoDAsync_v2)(CUdeviceptr, const void *, size_t, CUstream);
static CUresult (*cuMemsetD8_v2)(CUdeviceptr, unsigned char, size_t);
static CUresult (*cuStreamSynchronize)(CUstream);

#define CK(call) do { CUresult _r = (call); if (_r != 0) { \
    std::fprintf(stderr, "FATAL %s:%d %s -> CUresult %d\n", __FILE__, __LINE__, #call, _r); \
    std::exit(1); } } while (0)

static void load_cuda() {
    void *h = dlopen("libcuda.so.1", RTLD_NOW | RTLD_GLOBAL);
    if (!h) h = dlopen("libcuda.so", RTLD_NOW | RTLD_GLOBAL);
    if (!h) { std::fprintf(stderr, "FATAL: dlopen libcuda: %s\n", dlerror()); std::exit(1); }
#define SYM(f) do { *(void **)(&f) = dlsym(h, #f); if (!f) { std::fprintf(stderr, "FATAL: dlsym %s\n", #f); std::exit(1); } } while (0)
    SYM(cuInit); SYM(cuDeviceGetCount); SYM(cuDeviceGet); SYM(cuDevicePrimaryCtxRetain);
    SYM(cuCtxSetCurrent); SYM(cuStreamGetCtx); SYM(cuCheckpointProcessGetState);
    SYM(cuCheckpointProcessLock); SYM(cuCheckpointProcessCheckpoint); SYM(cuCheckpointProcessRestore);
    SYM(cuCheckpointProcessUnlock); SYM(cuCheckpointOperationComplete); SYM(cuMemHostAlloc);
    SYM(cuMemFreeHost); SYM(cuMemcpyDtoHAsync_v2); SYM(cuMemcpyHtoDAsync_v2);
    SYM(cuMemsetD8_v2); SYM(cuStreamSynchronize);
#undef SYM
}

static double now_s() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

// Retain the primary context on every device so cuCtxSetCurrent(dctx) works.
static void retain_all_primary_ctx() {
    int ndev = 0; CK(cuDeviceGetCount(&ndev));
    for (int i = 0; i < ndev; i++) { CUdevice d; CUcontext c; CK(cuDeviceGet(&d, i)); CK(cuDevicePrimaryCtxRetain(&c, d)); }
    std::fprintf(stderr, "[ckpt] retained primary ctx on %d device(s)\n", ndev);
}

// ---- NIXL OBJ agent setup (mirrors criu-stream-fetch setupNixlObjAgents) ----
static std::string agentName() { return "CkptStream_0"; }

static bool setupNixl(const std::string &bucket, const std::string &throughput,
                      std::unique_ptr<nixlAgent> &agent_out, nixlBackendH *&backend_out) {
    const char *crt_part = std::getenv("CRT_PART_SIZE");
    nixl_b_params_t params{{"bucket", bucket}, {"crtThroughputGbps", throughput}, {"crtMinLimit", "1"}};
    if (crt_part && *crt_part) params["crtPartSize"] = crt_part;
    const char *region = std::getenv("AWS_REGION");
    if (!region || !*region) region = std::getenv("AWS_DEFAULT_REGION");
    if (region && *region) params["region"] = region;

    nixlAgentConfig cfg(/*use_prog_thread=*/true);
    auto agent = std::make_unique<nixlAgent>(agentName(), cfg);
    std::fprintf(stderr, "[ckpt] NIXL OBJ backend: bucket=%s crtThroughputGbps=%s region=%s\n",
                 bucket.c_str(), throughput.c_str(), region ? region : "(unset)");
    nixlBackendH *backend = nullptr;
    if (agent->createBackend("OBJ", params, backend) != NIXL_SUCCESS || !backend) {
        std::fprintf(stderr, "FATAL: createBackend OBJ failed\n"); return false;
    }
    agent_out = std::move(agent);
    backend_out = backend;
    return true;
}

// Parse a comma-separated pid list ("123" or "123,456").
static std::vector<int> parsePids(const char *s) {
    std::vector<int> pids;
    std::string cur;
    for (const char *p = s; ; ++p) {
        if (*p == ',' || *p == '\0') { if (!cur.empty()) pids.push_back(std::atoi(cur.c_str())); cur.clear(); if (!*p) break; }
        else cur.push_back(*p);
    }
    return pids;
}

static std::string throughputEnv() {
    const char *tp = std::getenv("S3_CRT_THROUGHPUT_GBPS");
    return (tp && *tp) ? tp : "100";
}

// One whole-object NIXL WRITE (host buffer -> s3://bucket/key). Honors the
// OBJ-backend offset-0 constraint (one WRITE == one whole object).
static bool nixlWriteWhole(nixlAgent *agent, nixl_opt_args_t *hint, void *host, size_t len,
                           unsigned devId, const std::string &key) {
    nixl_reg_dlist_t dram(DRAM_SEG), obj(OBJ_SEG);
    nixlBlobDesc dd{}; dd.addr = (uintptr_t)host; dd.len = len; dd.devId = devId; dram.addDesc(dd);
    nixlBlobDesc oo{}; oo.addr = 0; oo.len = len; oo.devId = devId; oo.metaInfo = key; obj.addDesc(oo);
    if (agent->registerMem(dram, hint) != NIXL_SUCCESS || agent->registerMem(obj, hint) != NIXL_SUCCESS) {
        std::fprintf(stderr, "FATAL: registerMem dev%u\n", devId); return false;
    }
    nixl_xfer_dlist_t dx = dram.trim(), ox = obj.trim();
    nixlXferReqH *req = nullptr;
    nixl_status_t s = agent->createXferReq(NIXL_WRITE, dx, ox, agentName(), req, hint);
    if (s != NIXL_SUCCESS || !req) { std::fprintf(stderr, "FATAL: createXferReq dev%u (%d)\n", devId, (int)s); return false; }
    s = agent->postXferReq(req, hint);
    while (s != NIXL_SUCCESS) { if (s < 0) { std::fprintf(stderr, "FATAL: write dev%u (%d)\n", devId, (int)s); return false; } s = agent->getXferStatus(req); }
    agent->releaseXferReq(req);
    return true;
}

// One whole-object NIXL READ (s3://bucket/key -> host buffer).
static bool nixlReadWhole(nixlAgent *agent, nixl_opt_args_t *hint, void *host, size_t len,
                          unsigned devId, const std::string &key) {
    nixl_reg_dlist_t dram(DRAM_SEG), obj(OBJ_SEG);
    nixlBlobDesc dd{}; dd.addr = (uintptr_t)host; dd.len = len; dd.devId = devId; dram.addDesc(dd);
    nixlBlobDesc oo{}; oo.addr = 0; oo.len = len; oo.devId = devId; oo.metaInfo = key; obj.addDesc(oo);
    if (agent->registerMem(dram, hint) != NIXL_SUCCESS || agent->registerMem(obj, hint) != NIXL_SUCCESS) {
        std::fprintf(stderr, "FATAL: read registerMem dev%u\n", devId); return false;
    }
    nixl_xfer_dlist_t dx = dram.trim(), ox = obj.trim();
    nixlXferReqH *req = nullptr;
    nixl_status_t s = agent->createXferReq(NIXL_READ, dx, ox, agentName(), req, hint);
    if (s != NIXL_SUCCESS || !req) { std::fprintf(stderr, "FATAL: read createXferReq dev%u (%d)\n", devId, (int)s); return false; }
    s = agent->postXferReq(req, hint);
    while (s != NIXL_SUCCESS) { if (s < 0) { std::fprintf(stderr, "FATAL: read dev%u (%d)\n", devId, (int)s); return false; } s = agent->getXferStatus(req); }
    agent->releaseXferReq(req);
    return true;
}

// S3 key for a device blob under a keyprefix. Phase 0 keeps the PoC layout
// (<keyprefix>/dev<i>.bin); Phase 1 moves it under <hash>/gpu/.
static std::string devKey(const std::string &keyprefix, unsigned i) {
    return keyprefix + "/dev" + std::to_string(i) + ".bin";
}

// ---- Zero-skip (Phase E): pack only non-zero extents + a fixed-size index ----
//
// vLLM pre-allocates the whole --gpu-memory-utilization VRAM pool; at checkpoint
// most of it is empty KV cache (zeros). Scanning the DtoH'd host buffer and
// writing only non-zero extents shrinks dev<i>.bin from the full pool (~38 GiB)
// to ~weights+live-KV. Gated by GPU_STREAM_ZERO_SKIP=1 (both binaries); off ->
// whole-blob path is byte-unchanged.
//
// dev<i>.idx = fixed ZS_IDX_BYTES so restore reads it with a compile-time size
// (no object-size query). dev<i>.bin = the packed non-zero bytes (packedSize,
// from the idx header). Overflow past ZS_MAX_EXTENTS -> one whole-blob extent.

static const uint32_t ZS_MAGIC = 0x585047; // "GPX"
static const uint32_t ZS_VERSION = 1;
static const size_t   ZS_MAX_EXTENTS = 4096;
static const size_t   ZS_PAGE = 4096;              // extent alignment
static const size_t   ZS_MIN_GAP = 2u << 20;       // coalesce zero gaps < 2 MiB

typedef struct { uint32_t magic, version, extentCount, pad; uint64_t logicalSize, packedSize; } ZsHeader;
typedef struct { uint64_t logicalOff, packedOff, length; } ZsExtent;

static const size_t ZS_IDX_BYTES = sizeof(ZsHeader) + ZS_MAX_EXTENTS * sizeof(ZsExtent);

static bool zeroSkipEnabled() {
    const char *v = std::getenv("GPU_STREAM_ZERO_SKIP");
    return v && (v[0] == '1' || v[0] == 't' || v[0] == 'T');
}

static std::string idxKey(const std::string &keyprefix, unsigned i) {
    return keyprefix + "/dev" + std::to_string(i) + ".idx";
}

// True if the page-sized chunk [p, p+n) is entirely zero.
static bool chunkIsZero(const uint8_t *p, size_t n) {
    size_t w = n / 8;
    const uint64_t *q = reinterpret_cast<const uint64_t *>(p);
    for (size_t i = 0; i < w; i++) if (q[i]) return false;
    for (size_t i = w * 8; i < n; i++) if (p[i]) return false;
    return true;
}

// Scan `host[0..len)` for non-zero extents (ZS_PAGE granularity), coalescing
// runs separated by < ZS_MIN_GAP zeros. Fills logicalOff/length (packedOff set
// by the caller). Returns false if it would exceed ZS_MAX_EXTENTS (caller falls
// back to a single whole-blob extent).
static bool scanExtents(const void *host, size_t len, std::vector<ZsExtent> &out) {
    out.clear();
    const uint8_t *base = static_cast<const uint8_t *>(host);
    size_t off = 0;
    while (off < len) {
        size_t chunk = std::min(ZS_PAGE, len - off);
        if (chunkIsZero(base + off, chunk)) { off += chunk; continue; }
        size_t start = off;
        size_t gap = 0, lastNonZeroEnd = off + chunk;
        off += chunk;
        while (off < len) {                          // extend, tolerating small gaps
            size_t c = std::min(ZS_PAGE, len - off);
            if (chunkIsZero(base + off, c)) {
                gap += c;
                if (gap >= ZS_MIN_GAP) break;
            } else { gap = 0; lastNonZeroEnd = off + c; }
            off += c;
        }
        ZsExtent e{}; e.logicalOff = start; e.length = lastNonZeroEnd - start;
        out.push_back(e);
        if (out.size() > ZS_MAX_EXTENTS) return false;
        off = lastNonZeroEnd;
    }
    return true;
}
