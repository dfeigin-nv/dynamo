// ckpt-stream — fused GPU checkpoint -> S3, NO local file, NO fsync.
//
//   ckpt-stream <pid> <bucket> <keyprefix>
//
// Checkpoints a CUDA process via the r615 custom-storage API, copies each
// device's checkpoint memory into a PINNED host buffer (cuMemHostAlloc), and
// NIXL_WRITEs that buffer straight to S3 (OBJ backend, CRT engine). The pinned
// buffers are kept and used for the in-RAM restore round-trip, so no re-read.
//
// vs the file path (ckpt2storage.c + criu-stream-fetch --upload): eliminates
// the write()+fsync() of tens of GiB to disk, which dominated staging
// (0.08 GiB/s / 404s for 34 GiB). Staging here ~= DtoH + NIXL_WRITE.
//
// CUDA is resolved via dlopen(libcuda.so.1) at runtime (driver-injected), so
// this builds with no CUDA toolkit — only NIXL headers/libs.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
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
    SYM(cuMemFreeHost); SYM(cuMemcpyDtoHAsync_v2); SYM(cuMemcpyHtoDAsync_v2); SYM(cuStreamSynchronize);
#undef SYM
}

static double now_s() {
    return std::chrono::duration<double>(std::chrono::steady_clock::now().time_since_epoch()).count();
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
    std::fprintf(stderr, "[ckpt-stream] NIXL OBJ backend: bucket=%s crtThroughputGbps=%s region=%s\n",
                 bucket.c_str(), throughput.c_str(), region ? region : "(unset)");
    nixlBackendH *backend = nullptr;
    if (agent->createBackend("OBJ", params, backend) != NIXL_SUCCESS || !backend) {
        std::fprintf(stderr, "FATAL: createBackend OBJ failed\n"); return false;
    }
    agent_out = std::move(agent);
    backend_out = backend;
    return true;
}

int main(int argc, char **argv) {
    if (argc != 4) { std::fprintf(stderr, "usage: %s <pid> <bucket> <keyprefix>\n", argv[0]); return 2; }
    int pid = std::atoi(argv[1]);
    std::string bucket = argv[2], keyprefix = argv[3];
    const char *tp = std::getenv("S3_CRT_THROUGHPUT_GBPS");
    std::string throughput = (tp && *tp) ? tp : "100";

    load_cuda();
    CK(cuInit(0));
    int ndev = 0; CK(cuDeviceGetCount(&ndev));
    for (int i = 0; i < ndev; i++) { CUdevice d; CUcontext c; CK(cuDeviceGet(&d, i)); CK(cuDevicePrimaryCtxRetain(&c, d)); }
    std::fprintf(stderr, "[ckpt-stream] retained primary ctx on %d device(s)\n", ndev);

    std::unique_ptr<nixlAgent> agent;
    nixlBackendH *backend = nullptr;
    if (!setupNixl(bucket, throughput, agent, backend)) return 1;
    nixl_opt_args_t hint; hint.backends.push_back(backend);

    int st = 0; CK(cuCheckpointProcessGetState(pid, &st));
    std::fprintf(stderr, "[ckpt-stream] pid %d state=%d (expect 0)\n", pid, st);
    LockArgs lock{}; lock.timeoutMs = 30000;
    CK(cuCheckpointProcessLock(pid, &lock));
    std::fprintf(stderr, "[ckpt-stream] locked\n");

    CsInfo *csi = nullptr;
    CkptArgs cargs{}; cargs.out = &csi;
    double t0 = now_s();
    CK(cuCheckpointProcessCheckpoint(pid, &cargs));
    std::fprintf(stderr, "[ckpt-stream] checkpoint(custom) %.3fs deviceCount=%u\n", now_s() - t0, csi->deviceCount);

    unsigned dc = csi->deviceCount;
    std::vector<void *> hostbuf(dc, nullptr);
    std::vector<size_t> sizes(dc, 0);
    uint64_t total = 0;
    double dtoh_s = 0, up_s = 0;

    for (unsigned i = 0; i < dc; i++) {
        PerDev *d = &csi->perDeviceData[i];
        CUcontext dctx; CK(cuStreamGetCtx(d->stream, &dctx)); CK(cuCtxSetCurrent(dctx));
        void *host = nullptr;
        CK(cuMemHostAlloc(&host, d->size, CU_MEMHOSTALLOC_PORTABLE));
        hostbuf[i] = host; sizes[i] = d->size;

        double a = now_s();
        CK(cuMemcpyDtoHAsync_v2(host, d->devPtr, d->size, d->stream));
        CK(cuStreamSynchronize(d->stream));
        double b = now_s(); dtoh_s += b - a;

        std::string key = keyprefix + "/dev" + std::to_string(i) + ".bin";
        nixl_reg_dlist_t dram(DRAM_SEG), obj(OBJ_SEG);
        nixlBlobDesc dd{}; dd.addr = (uintptr_t)host; dd.len = d->size; dd.devId = i; dram.addDesc(dd);
        nixlBlobDesc oo{}; oo.addr = 0; oo.len = d->size; oo.devId = i; oo.metaInfo = key; obj.addDesc(oo);
        if (agent->registerMem(dram, &hint) != NIXL_SUCCESS || agent->registerMem(obj, &hint) != NIXL_SUCCESS) {
            std::fprintf(stderr, "FATAL: registerMem dev%u\n", i); return 1;
        }
        nixl_xfer_dlist_t dx = dram.trim(), ox = obj.trim();
        nixlXferReqH *req = nullptr;
        nixl_status_t s = agent->createXferReq(NIXL_WRITE, dx, ox, agentName(), req, &hint);
        if (s != NIXL_SUCCESS || !req) { std::fprintf(stderr, "FATAL: createXferReq dev%u (%d)\n", i, (int)s); return 1; }
        s = agent->postXferReq(req, &hint);
        while (s != NIXL_SUCCESS) { if (s < 0) { std::fprintf(stderr, "FATAL: xfer dev%u (%d)\n", i, (int)s); return 1; } s = agent->getXferStatus(req); }
        agent->releaseXferReq(req);
        double c = now_s(); up_s += c - b;
        total += d->size;
        std::fprintf(stderr, "[ckpt-stream] dev%u %.0f MiB: DtoH %.2fs, WRITE->s3://%s/%s %.2fs\n",
                     i, d->size / 1048576.0, b - a, bucket.c_str(), key.c_str(), c - b);
    }
    std::fprintf(stderr, "[ckpt-stream] STAGED %.0f MiB  DtoH=%.2fs (%.2f GiB/s)  upload=%.2fs (%.2f GiB/s)  no-file\n",
                 total / 1048576.0, dtoh_s, total / 1073741824.0 / (dtoh_s > 0 ? dtoh_s : 1),
                 up_s, total / 1073741824.0 / (up_s > 0 ? up_s : 1));

    CK(cuCheckpointOperationComplete(csi->handle));
    std::fprintf(stderr, "[ckpt-stream] checkpoint OperationComplete (VRAM freed)\n");

    // RESTORE_FROM_S3=1: discard the staging RAM and re-source every byte from
    // S3 (NIXL_READ) — proves S3 is the source of truth, not the kept buffers.
    // Default: fill from the retained pinned buffers (fast same-process path).
    const char *from_s3 = std::getenv("RESTORE_FROM_S3");
    bool s3_restore = from_s3 && *from_s3 == '1';
    if (s3_restore) {
        for (unsigned i = 0; i < dc; i++) if (hostbuf[i]) { cuMemFreeHost(hostbuf[i]); hostbuf[i] = nullptr; }
        std::fprintf(stderr, "[ckpt-stream] freed staging RAM; restore will NIXL_READ from S3\n");
    }

    // ---- restore round-trip ----
    CsInfo *rsi = nullptr;
    RestoreArgs rargs{}; rargs.out = &rsi;
    CK(cuCheckpointProcessRestore(pid, &rargs));
    double dl_s = 0, htod_s = 0;
    for (unsigned i = 0; i < rsi->deviceCount; i++) {
        PerDev *d = &rsi->perDeviceData[i];
        CUcontext dctx; CK(cuStreamGetCtx(d->stream, &dctx)); CK(cuCtxSetCurrent(dctx));
        if (d->size != sizes[i]) { std::fprintf(stderr, "FATAL: dev%u size mismatch\n", i); return 1; }
        if (s3_restore) {
            void *host = nullptr; CK(cuMemHostAlloc(&host, d->size, CU_MEMHOSTALLOC_PORTABLE));
            hostbuf[i] = host;
            std::string key = keyprefix + "/dev" + std::to_string(i) + ".bin";
            nixl_reg_dlist_t dram(DRAM_SEG), obj(OBJ_SEG);
            nixlBlobDesc dd{}; dd.addr = (uintptr_t)host; dd.len = d->size; dd.devId = i; dram.addDesc(dd);
            nixlBlobDesc oo{}; oo.addr = 0; oo.len = d->size; oo.devId = i; oo.metaInfo = key; obj.addDesc(oo);
            if (agent->registerMem(dram, &hint) != NIXL_SUCCESS || agent->registerMem(obj, &hint) != NIXL_SUCCESS) {
                std::fprintf(stderr, "FATAL: restore registerMem dev%u\n", i); return 1;
            }
            nixl_xfer_dlist_t dx = dram.trim(), ox = obj.trim();
            nixlXferReqH *req = nullptr;
            double a = now_s();
            nixl_status_t s = agent->createXferReq(NIXL_READ, dx, ox, agentName(), req, &hint);
            if (s != NIXL_SUCCESS || !req) { std::fprintf(stderr, "FATAL: restore createXferReq dev%u (%d)\n", i, (int)s); return 1; }
            s = agent->postXferReq(req, &hint);
            while (s != NIXL_SUCCESS) { if (s < 0) { std::fprintf(stderr, "FATAL: read dev%u (%d)\n", i, (int)s); return 1; } s = agent->getXferStatus(req); }
            agent->releaseXferReq(req);
            double b = now_s(); dl_s += b - a;
            std::fprintf(stderr, "[ckpt-stream] dev%u READ<-s3 %.0f MiB %.2fs (%.2f GiB/s)\n",
                         i, d->size / 1048576.0, b - a, d->size / 1073741824.0 / (b - a > 0 ? b - a : 1));
        }
        double c = now_s();
        CK(cuMemcpyHtoDAsync_v2(d->devPtr, hostbuf[i], d->size, d->stream));
        CK(cuStreamSynchronize(d->stream));
        htod_s += now_s() - c;
    }
    if (s3_restore)
        std::fprintf(stderr, "[ckpt-stream] RESTORE-FROM-S3: download=%.2fs (%.2f GiB/s) HtoD=%.2fs\n",
                     dl_s, total / 1073741824.0 / (dl_s > 0 ? dl_s : 1), htod_s);
    CK(cuCheckpointOperationComplete(rsi->handle));
    UnlockArgs ul{};
    CK(cuCheckpointProcessUnlock(pid, &ul));
    CK(cuCheckpointProcessGetState(pid, &st));
    std::fprintf(stderr, "[ckpt-stream] restored + unlocked, state=%d (expect 0). round-trip OK.\n", st);

    for (unsigned i = 0; i < dc; i++) if (hostbuf[i]) cuMemFreeHost(hostbuf[i]);
    return 0;
}
