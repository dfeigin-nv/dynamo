// ckpt-stream-restore — Phase-0 restore half.
//
//   ckpt-stream-restore <pids> <bucket> <keyprefix>
//
// <pids> = comma-separated CUDA host PIDs of the CRIU-RESTORED process tree
// (fresh pids; the GPU is still in CHECKPOINTED+locked state, tracked by the
// driver per-pid, NOT by the tool that checkpointed it). For each pid:
// cuCheckpointProcessRestore -> NIXL_READ each dev blob from S3 -> HtoD ->
// cuCheckpointOperationComplete. Then UNLOCK every pid.
//
// Diverges from the fused PoC: S3 is the ONLY source (no retained pinned
// buffers in a fresh process); per-device sizes come from the restore API
// (rsi->perDeviceData[i].size), never a captured array; RestoreArgs.pairs is
// left empty -> restore must land on the SAME GPU UUID / same node (Phase 0
// constraint; Phase 1+ can pass the old->new UUID pairs).
//
// S3 layout must match ckpt-stream-ckpt: <keyprefix>/p<pidIdx>/dev<i>.bin

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

    uint64_t total = 0;
    double dl_s = 0, htod_s = 0;

    // Phase 1: restore + refill + OperationComplete each pid.
    for (size_t pi = 0; pi < pids.size(); pi++) {
        int pid = pids[pi];
        int st = 0; CK(cuCheckpointProcessGetState(pid, &st));
        std::fprintf(stderr, "[ckpt-restore] pid %d state=%d (expect checkpointed)\n", pid, st);

        CsInfo *rsi = nullptr;
        RestoreArgs rargs{}; rargs.out = &rsi;   // pairs left empty -> same-GPU restore
        CK(cuCheckpointProcessRestore(pid, &rargs));
        std::fprintf(stderr, "[ckpt-restore] pid %d restore deviceCount=%u\n", pid, rsi->deviceCount);

        std::string pidPrefix = keyprefix + "/p" + std::to_string(pi);
        for (unsigned i = 0; i < rsi->deviceCount; i++) {
            PerDev *d = &rsi->perDeviceData[i];
            CUcontext dctx; CK(cuStreamGetCtx(d->stream, &dctx)); CK(cuCtxSetCurrent(dctx));
            void *host = nullptr;
            CK(cuMemHostAlloc(&host, d->size, CU_MEMHOSTALLOC_PORTABLE));

            std::string key = devKey(pidPrefix, i);
            double a = now_s();
            if (!nixlReadWhole(agent.get(), &hint, host, d->size, i, key)) return 1;
            double b = now_s(); dl_s += b - a;

            CK(cuMemcpyHtoDAsync_v2(d->devPtr, host, d->size, d->stream));
            CK(cuStreamSynchronize(d->stream));
            double c = now_s(); htod_s += c - b;
            total += d->size;
            std::fprintf(stderr, "[ckpt-restore] pid %d dev%u %.0f MiB: READ<-s3://%s/%s %.2fs (%.2f GiB/s), HtoD %.2fs\n",
                         pid, i, d->size / 1048576.0, bucket.c_str(), key.c_str(), b - a,
                         d->size / 1073741824.0 / (b - a > 0 ? b - a : 1), c - b);
            cuMemFreeHost(host);
        }
        CK(cuCheckpointOperationComplete(rsi->handle));
        std::fprintf(stderr, "[ckpt-restore] pid %d OperationComplete (VRAM refilled)\n", pid);
    }

    // Phase 2: unlock every pid (resume execution).
    for (int pid : pids) {
        UnlockArgs ul{};
        CK(cuCheckpointProcessUnlock(pid, &ul));
        int st = 0; CK(cuCheckpointProcessGetState(pid, &st));
        std::fprintf(stderr, "[ckpt-restore] pid %d unlocked, state=%d (expect 0/running)\n", pid, st);
    }

    std::fprintf(stderr, "[ckpt-restore] RESTORED %.0f MiB  download=%.2fs (%.2f GiB/s)  HtoD=%.2fs  round-trip OK\n",
                 total / 1048576.0, dl_s, total / 1073741824.0 / (dl_s > 0 ? dl_s : 1), htod_s);
    return 0;
}
