// ckpt-stream-ckpt — Phase-0 checkpoint half.
//
//   ckpt-stream-ckpt <pids> <bucket> <keyprefix>
//
// <pids> = comma-separated CUDA host PIDs (one per rank). Two-phase across the
// tree: LOCK all pids, then CHECKPOINT all pids (preserves the TP>1 invariant),
// then per-pid DtoH -> NIXL_WRITE -> cuCheckpointOperationComplete.
//
// EXITS WITHOUT UNLOCK. The target is left locked + CHECKPOINTED with VRAM
// freed — exactly the state `cuda-checkpoint-helper --action lock|checkpoint`
// leaves today, so CRIU can then dump a VRAM-free process and (in a different
// process, on a possibly different node) ckpt-stream-restore refills VRAM.
//
// S3 layout: <keyprefix>/p<pidIdx>/dev<i>.bin

#include "ckpt_common.h"

int main(int argc, char **argv) {
    if (argc != 4) { std::fprintf(stderr, "usage: %s <pids> <bucket> <keyprefix>\n", argv[0]); return 2; }
    std::vector<int> pids = parsePids(argv[1]);
    std::string bucket = argv[2], keyprefix = argv[3];
    if (pids.empty()) { std::fprintf(stderr, "FATAL: no pids\n"); return 2; }
    std::string throughput = throughputEnv();

    load_cuda();
    CK(cuInit(0));
    retain_all_primary_ctx();

    std::unique_ptr<nixlAgent> agent;
    nixlBackendH *backend = nullptr;
    if (!setupNixl(bucket, throughput, agent, backend)) return 1;
    nixl_opt_args_t hint; hint.backends.push_back(backend);

    // Phase 1: lock every pid.
    for (int pid : pids) {
        int st = 0; CK(cuCheckpointProcessGetState(pid, &st));
        std::fprintf(stderr, "[ckpt-ckpt] pid %d state=%d (expect 0)\n", pid, st);
        LockArgs lock{}; lock.timeoutMs = 30000;
        CK(cuCheckpointProcessLock(pid, &lock));
        std::fprintf(stderr, "[ckpt-ckpt] pid %d locked\n", pid);
    }

    // Phase 2: checkpoint every pid.
    std::vector<CsInfo *> csis(pids.size(), nullptr);
    for (size_t pi = 0; pi < pids.size(); pi++) {
        CkptArgs cargs{}; cargs.out = &csis[pi];
        double t0 = now_s();
        CK(cuCheckpointProcessCheckpoint(pids[pi], &cargs));
        std::fprintf(stderr, "[ckpt-ckpt] pid %d checkpoint(custom) %.3fs deviceCount=%u\n",
                     pids[pi], now_s() - t0, csis[pi]->deviceCount);
    }

    // Phase 3: per-pid DtoH -> NIXL_WRITE -> OperationComplete.
    uint64_t total = 0;
    double dtoh_s = 0, up_s = 0;
    for (size_t pi = 0; pi < pids.size(); pi++) {
        CsInfo *csi = csis[pi];
        std::string pidPrefix = keyprefix + "/p" + std::to_string(pi);
        for (unsigned i = 0; i < csi->deviceCount; i++) {
            PerDev *d = &csi->perDeviceData[i];
            CUcontext dctx; CK(cuStreamGetCtx(d->stream, &dctx)); CK(cuCtxSetCurrent(dctx));
            void *host = nullptr;
            CK(cuMemHostAlloc(&host, d->size, CU_MEMHOSTALLOC_PORTABLE));

            double a = now_s();
            CK(cuMemcpyDtoHAsync_v2(host, d->devPtr, d->size, d->stream));
            CK(cuStreamSynchronize(d->stream));
            double b = now_s(); dtoh_s += b - a;

            std::string key = devKey(pidPrefix, i);
            if (!nixlWriteWhole(agent.get(), &hint, host, d->size, i, key)) return 1;
            double c = now_s(); up_s += c - b;
            total += d->size;
            std::fprintf(stderr, "[ckpt-ckpt] pid %d dev%u %.0f MiB: DtoH %.2fs, WRITE->s3://%s/%s %.2fs\n",
                         pids[pi], i, d->size / 1048576.0, b - a, bucket.c_str(), key.c_str(), c - b);
            cuMemFreeHost(host);
        }
        CK(cuCheckpointOperationComplete(csi->handle));
        std::fprintf(stderr, "[ckpt-ckpt] pid %d OperationComplete (VRAM freed, still LOCKED)\n", pids[pi]);
    }

    std::fprintf(stderr, "[ckpt-ckpt] STAGED %.0f MiB  DtoH=%.2fs (%.2f GiB/s)  upload=%.2fs (%.2f GiB/s)  no-file; EXIT WITHOUT UNLOCK\n",
                 total / 1048576.0, dtoh_s, total / 1073741824.0 / (dtoh_s > 0 ? dtoh_s : 1),
                 up_s, total / 1073741824.0 / (up_s > 0 ? up_s : 1));
    return 0;
}
