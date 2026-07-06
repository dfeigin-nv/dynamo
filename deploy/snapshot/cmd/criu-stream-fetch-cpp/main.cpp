// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// criu-stream-fetch (C++)
//
// Stage 2 Pipeline C smart-streaming CRIU restore streamer. Replaces the Go
// reference at sibling cmd/criu-stream-fetch/. Per docs/streaming-restore-design.md:
//
//   1. Read pipeline-c-manifest.json. Each private_range carries
//      {id, size, source="s3://bucket/key", bucket, key}.
//   2. memfd_create + ftruncate per range; mmap each memfd into local DRAM
//      so NIXL OBJ's AWS CRT writes bytes directly into the memfd's tmpfs
//      pages (zero-copy via PreallocatedStreamBuf inside the patched
//      libplugin_OBJ.so).
//   3. nixlAgent + OBJ backend; registerMem(DRAM_SEG) for memfd mmaps and
//      registerMem(OBJ_SEG) for the S3 objects (metaInfo = object key).
//   4. createXferReq(NIXL_READ, dram_list, obj_list) per memfd, postXferReq
//      to kick off all transfers in parallel.
//   5. Main thread polls getXferStatus per req; on NIXL_SUCCESS for range i,
//      writes 1 to that range's eventfd. Stage 2c daemon's epoll loop reacts
//      with UFFDIO_CONTINUE(addr, len) to install PTEs proactively.
//   6. Worker thread services per-task CRIU_STREAMER_PRIVATE_SOCK requests
//      (same 4-byte-id / SCM_RIGHTS(memfd) / 1-byte-ack wire protocol as
//      the Go streamer). Each request blocks on the corresponding state's
//      done condvar before sending the ack so PIE only resumes after the
//      memfd is fully filled.

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <fcntl.h>
#include <getopt.h>
#include <linux/memfd.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/wait.h>
#include <unistd.h>

#include "nixl.h"
#include "nixl_descriptors.h"

namespace {

// Per-agent name. With STREAMER_NUM_AGENTS=N the streamer constructs N
// independent nixlAgent instances, each with its own OBJ backend (and
// therefore its own Aws::S3Crt::S3CrtClient / TCP pool) to lift the
// single-client throughput cap. Names must be unique within the process
// because the OBJ backend's createXferReq local_agent string matches the
// agent's own name.
inline std::string streamerAgentName(size_t idx) {
    return std::string("CriuStreamFetch_") + std::to_string(idx);
}

// Monotonic stream-start anchor used to prefix every "[stream]" log line
// with millisecond elapsed-since-streamer-start so PIE AIO completion times
// (recorded with the agent's wall clock) can be aligned against streamer
// per-id NIXL_SUCCESS timestamps.
static const auto kStreamStart = std::chrono::steady_clock::now();
static inline long stream_ms_now() {
    return (long)std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - kStreamStart).count();
}

struct RangeEntry {
    uint32_t id;
    uint64_t size;
    std::string source;  // s3://bucket/key when sourced from S3, else local path
    std::string bucket;  // populated for s3:// sources
    std::string key;     // S3 object key under bucket
};

// Stage 2c: one entry from a pagemap-shmem-<shmid>.img file. The streamer
// posts one NIXL OBJ READ per entry: bytes [pages_img_offset, pages_img_
// offset+len) of the shared pages-<pages_id>.img land at the memfd's
// [vaddr, vaddr+len) slot, which the daemon later UFFDIO_CONTINUEs into
// the restored process's shmem VMA at that vaddr.
struct ShmemPagemapEntry {
    uint64_t vaddr;
    uint64_t len;
    uint64_t pages_img_offset;
};

struct ShmemRange {
    uint64_t shmid;
    uint32_t pages_id;
    uint64_t size;       // memfd byte size; round_up(max(vaddr+len), PAGE_SIZE)
    std::string source;  // s3://bucket/key or local path
    std::string bucket;
    std::string key;
    std::vector<ShmemPagemapEntry> entries;
};

struct Manifest {
    std::vector<ShmemRange> shmem_ranges;
    std::vector<RangeEntry> private_ranges;
};

// Per-range runtime state owned by the NIXL fetch path. memfd + mmap stay
// alive until either CRIU claims the memfd or the streamer exits.
struct PrivateRangeState {
    uint32_t id = 0;
    uint64_t size = 0;
    int memfd = -1;
    void *mmap_addr = nullptr;
    int eventfd_fd = -1;
    // Range is split into N parallel S3 chunks of CHUNK_BYTESIZE each (see
    // parseChunkBytesize). Chunks of one range fan out across all agents
    // (chunk_owners[k] = k % N) so a single large range (e.g. id=12 at
    // 33 GiB) splits across N independent S3CrtClient pools instead of
    // serializing on one client's ~8 Gbps ceiling. xfer_reqs[k] /
    // xfer_done[k] / chunk_lens[k] / chunk_owners[k] all track chunk k;
    // chunks_remaining counts down to fire the per-range eventfd exactly
    // once when every chunk has filled its memfd slice.
    std::vector<nixlXferReqH *> xfer_reqs;
    std::vector<bool> xfer_done;
    std::vector<uint32_t> chunk_lens;     // per-chunk byte count
    std::vector<uint32_t> chunk_owners;   // per-chunk agent index in [0, N)
    std::atomic<unsigned> chunks_remaining{0};
    long post_t_ms = 0;
    std::mutex done_mu;
    std::condition_variable done_cv;
    bool done = false;
    nixl_status_t done_status = NIXL_SUCCESS;
    // Plan v4 async overlap. servePrivateSocket creates a pipe, hands the
    // read end to CRIU alongside the memfd, and stores the write end here.
    // pollPrivateXfers (and the postXfer inline-success / inline-failure
    // paths) call signalPrivateReady on chunk-completion to either write a
    // single byte (memfd filled → PIE's sys_read returns 1) or close the
    // pipe without writing (abort/failure → PIE sys_read returns 0 / EOF).
    // -1 means async mode was off for this state.
    int ready_pipe_wfd = -1;
};

// Per-shmem-range runtime state. The streamer posts one NIXL OBJ READ per
// pagemap entry; entries_remaining counts down to zero, and the eventfd
// fires exactly once when the entire shmem range is filled. The daemon
// gates UFFDIO_CONTINUE on that signal.
struct ShmemRangeState {
    uint64_t shmid = 0;
    uint64_t size = 0;
    int memfd = -1;
    void *mmap_addr = nullptr;
    int eventfd_fd = -1;
    // Each pagemap entry is split into chunks of CHUNK_BYTESIZE. Chunks fan
    // out round-robin across all agents (chunk_owners[k] = k % N, counter
    // cumulative across all entries of this range) so a single large entry
    // doesn't serialize on one CRT client. xfer_reqs[k] / xfer_done[k] /
    // chunk_lens[k] / chunk_owners[k] all track chunk k; entries_remaining
    // counts chunks remaining (legacy name, semantically chunks_remaining).
    std::vector<nixlXferReqH *> xfer_reqs;
    std::vector<bool> xfer_done;
    std::vector<uint32_t> chunk_lens;
    std::vector<uint32_t> chunk_owners;
    std::atomic<unsigned> entries_remaining{0};
    long post_t_ms = 0;
    // Synchronization: serveShmemSocket waits on done_cv until the entire
    // range has been NIXL-filled. CRIU's open_shmem (and memfd_open) then
    // mmap the memfd as the shmem VMA inode; bytes are already in the
    // shmem page cache so any later access finds them without faulting.
    std::mutex done_mu;
    std::condition_variable done_cv;
    bool done = false;
    bool failed = false;
};

// JSON parse helpers. The schema is fixed by writePipelineCManifest in
// dynamo/deploy/snapshot/internal/criu/restore.go, so a hand-rolled parser
// is enough. Stage 2 may swap to nlohmann/json once we need richer schemas.
bool extractStringField(std::string_view obj, std::string_view key, std::string &out) {
    std::string pat = "\"";
    pat.append(key);
    pat.append("\":");
    auto pos = obj.find(pat);
    if (pos == std::string_view::npos) return false;
    pos += pat.size();
    while (pos < obj.size() && obj[pos] != '"') {
        if (obj[pos] == ',' || obj[pos] == '}') return false;
        ++pos;
    }
    if (pos >= obj.size()) return false;
    ++pos;
    auto end = obj.find('"', pos);
    if (end == std::string_view::npos) return false;
    out.assign(obj.substr(pos, end - pos));
    return true;
}

bool extractU64Field(std::string_view obj, std::string_view key, uint64_t &out) {
    std::string pat = "\"";
    pat.append(key);
    pat.append("\":");
    auto pos = obj.find(pat);
    if (pos == std::string_view::npos) return false;
    pos += pat.size();
    while (pos < obj.size() && (obj[pos] == ' ' || obj[pos] == '\t')) ++pos;
    uint64_t v = 0;
    bool has = false;
    while (pos < obj.size() && obj[pos] >= '0' && obj[pos] <= '9') {
        v = v * 10 + (uint64_t)(obj[pos] - '0');
        ++pos;
        has = true;
    }
    if (!has) return false;
    out = v;
    return true;
}

bool extractArrayBody(std::string_view obj, std::string_view key, std::string_view &out) {
    std::string pat = "\"";
    pat.append(key);
    pat.append("\":");
    auto pos = obj.find(pat);
    if (pos == std::string_view::npos) return false;
    pos = obj.find('[', pos);
    if (pos == std::string_view::npos) return false;
    int depth = 1;
    size_t lo = pos + 1;
    size_t i = lo;
    while (i < obj.size() && depth > 0) {
        if (obj[i] == '[') ++depth;
        else if (obj[i] == ']') --depth;
        if (depth == 0) break;
        ++i;
    }
    if (depth != 0) return false;
    out = obj.substr(lo, i - lo);
    return true;
}

bool parseRanges(std::string_view arrayBody, std::vector<RangeEntry> &out) {
    size_t i = 0;
    while (i < arrayBody.size()) {
        while (i < arrayBody.size() && arrayBody[i] != '{') ++i;
        if (i >= arrayBody.size()) break;
        size_t lo = i + 1;
        int depth = 1;
        while (i + 1 < arrayBody.size() && depth > 0) {
            ++i;
            if (arrayBody[i] == '{') ++depth;
            else if (arrayBody[i] == '}') --depth;
        }
        if (depth != 0) return false;
        std::string_view item = arrayBody.substr(lo, i - lo);
        RangeEntry r{};
        uint64_t id_u64 = 0;
        if (!extractU64Field(item, "id", id_u64)) return false;
        r.id = (uint32_t)id_u64;
        if (!extractU64Field(item, "size", r.size)) return false;
        if (!extractStringField(item, "source", r.source)) return false;
        (void)extractStringField(item, "bucket", r.bucket);
        (void)extractStringField(item, "key", r.key);
        out.push_back(std::move(r));
        ++i;
    }
    return true;
}

// parseShmemRanges parses the shmem_ranges JSON array. Each element is one
// pagemap-shmem-<shmid>.img output: shmid + pages_id + total memfd size +
// pages-<pages_id>.img source + a nested entries[] array.
bool parseShmemRanges(std::string_view arrayBody, std::vector<ShmemRange> &out) {
    size_t i = 0;
    while (i < arrayBody.size()) {
        while (i < arrayBody.size() && arrayBody[i] != '{') ++i;
        if (i >= arrayBody.size()) break;
        size_t lo = i + 1;
        int depth = 1;
        while (i + 1 < arrayBody.size() && depth > 0) {
            ++i;
            if (arrayBody[i] == '{') ++depth;
            else if (arrayBody[i] == '}') --depth;
        }
        if (depth != 0) return false;
        std::string_view item = arrayBody.substr(lo, i - lo);
        ShmemRange r{};
        if (!extractU64Field(item, "shmid", r.shmid)) return false;
        uint64_t pages_u64 = 0;
        if (!extractU64Field(item, "pages_id", pages_u64)) return false;
        r.pages_id = (uint32_t)pages_u64;
        if (!extractU64Field(item, "size", r.size)) return false;
        if (!extractStringField(item, "source", r.source)) return false;
        (void)extractStringField(item, "bucket", r.bucket);
        (void)extractStringField(item, "key", r.key);

        std::string_view entriesBody;
        if (!extractArrayBody(item, "entries", entriesBody)) return false;
        size_t j = 0;
        while (j < entriesBody.size()) {
            while (j < entriesBody.size() && entriesBody[j] != '{') ++j;
            if (j >= entriesBody.size()) break;
            size_t elo = j + 1;
            int edepth = 1;
            while (j + 1 < entriesBody.size() && edepth > 0) {
                ++j;
                if (entriesBody[j] == '{') ++edepth;
                else if (entriesBody[j] == '}') --edepth;
            }
            if (edepth != 0) return false;
            std::string_view eitem = entriesBody.substr(elo, j - elo);
            ShmemPagemapEntry e{};
            if (!extractU64Field(eitem, "vaddr", e.vaddr)) return false;
            if (!extractU64Field(eitem, "len", e.len)) return false;
            if (!extractU64Field(eitem, "pages_img_offset", e.pages_img_offset))
                return false;
            r.entries.push_back(e);
            ++j;
        }
        out.push_back(std::move(r));
        ++i;
    }
    return true;
}

bool loadManifest(const std::string &path, Manifest &m) {
    std::ifstream f(path);
    if (!f.is_open()) {
        std::fprintf(stderr, "criu-stream-fetch: open manifest %s: %s\n",
                     path.c_str(), std::strerror(errno));
        return false;
    }
    std::stringstream buf;
    buf << f.rdbuf();
    std::string body = buf.str();
    std::string_view sv(body);
    std::string_view shmem, priv;
    if (!extractArrayBody(sv, "shmem_ranges", shmem) ||
        !extractArrayBody(sv, "private_ranges", priv)) {
        std::fprintf(stderr, "criu-stream-fetch: manifest missing required arrays\n");
        return false;
    }
    if (!parseShmemRanges(shmem, m.shmem_ranges) ||
        !parseRanges(priv, m.private_ranges)) {
        std::fprintf(stderr, "criu-stream-fetch: manifest range parse failed\n");
        return false;
    }
    return true;
}

int memfdCreate(const std::string &name, uint64_t size) {
    int fd = (int)::syscall(SYS_memfd_create, name.c_str(), (unsigned)MFD_CLOEXEC);
    if (fd < 0) {
        std::fprintf(stderr, "criu-stream-fetch: memfd_create(%s): %s\n",
                     name.c_str(), std::strerror(errno));
        return -1;
    }
    if (size > 0 && ::ftruncate(fd, (off_t)size) < 0) {
        std::fprintf(stderr, "criu-stream-fetch: ftruncate(%s, %lu): %s\n",
                     name.c_str(), (unsigned long)size, std::strerror(errno));
        ::close(fd);
        return -1;
    }
    return fd;
}

int eventfdCreate() {
    int fd = ::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (fd < 0) {
        std::fprintf(stderr, "criu-stream-fetch: eventfd: %s\n", std::strerror(errno));
        return -1;
    }
    return fd;
}

bool eventfdSignal(int fd) {
    uint64_t v = 1;
    return ::write(fd, &v, sizeof(v)) == (ssize_t)sizeof(v);
}

void poisonAbort(int abort_fd) {
    if (abort_fd < 0) return;
    uint64_t v = 1;
    ssize_t n = ::write(abort_fd, &v, sizeof(v));
    (void)n;
}

// Plan v4 async overlap: STREAM_RESTORE_ASYNC env (default on). Streamer
// must match CRIU's --stream-restore-async setting (the snapshot agent
// passes both consistently). When off, servePrivateSocket falls back to
// the legacy one-fd handover + ack-after-fill protocol.
bool g_async_overlap = []() {
    const char *e = std::getenv("STREAM_RESTORE_ASYNC");
    return (e == nullptr) || std::atoi(e) != 0;
}();

// signalPrivateReady_locked fires the async-overlap ready signal on a
// PrivateRange after its memfd is fully filled (success=true) or the fill
// aborted (success=false). PIE's blocking sys_read on the pipe wakes up:
// success → 1 byte payload → AIO proceeds; failure → close → EOF →
// goto aio_error. Idempotent across the multiple completion sites
// (pollPrivateXfers normal, inline-success, inline-failure, abort).
//
// PRECONDITION: caller holds st->done_mu. The mutex serializes against
// servePrivateSocket's late pipe registration so we never miss a wakeup
// or double-close the fd. If the serve thread hasn't registered a pipe
// yet, st->ready_pipe_wfd is -1 and this is a no-op — the serve thread
// will observe st->done set under the same lock and signal directly.
void signalPrivateReady_locked(PrivateRangeState *st, bool success) {
    if (!st) return;
    int fd = st->ready_pipe_wfd;
    if (fd < 0) return;
    st->ready_pipe_wfd = -1;
    if (success) {
        char b = '1';
        ssize_t n = ::write(fd, &b, 1);
        (void)n;
    }
    ::close(fd);
}

int fdFromEnv(const char *name, bool required) {
    const char *v = std::getenv(name);
    if (v == nullptr || *v == '\0') {
        if (required) std::fprintf(stderr, "criu-stream-fetch: env %s is unset\n", name);
        return -1;
    }
    char *endp = nullptr;
    long fd = std::strtol(v, &endp, 10);
    if (endp == v || (endp && *endp != '\0') || fd < 0 || fd > INT32_MAX) {
        std::fprintf(stderr, "criu-stream-fetch: env %s=%s is not a valid fd\n", name, v);
        return -1;
    }
    return (int)fd;
}

// SCM_RIGHTS payload sender. Used for both the daemon-fd handshake (header =
// uint32 n_evfd, fds = [abort_fd, ev_0..ev_{n-1}]) and the per-task private-
// memfd reply (header = 1 byte dummy, fds = [memfd]).
bool sendFds(int sock, const std::vector<int> &fds, const void *header, size_t header_len) {
    char dummy = 0;
    iovec iov{};
    iov.iov_base = (header_len > 0) ? const_cast<void *>(header) : (void *)&dummy;
    iov.iov_len = (header_len > 0) ? header_len : 1;

    msghdr msg{};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;

    std::vector<char> cmsg_buf(CMSG_SPACE(fds.size() * sizeof(int)));
    msg.msg_control = cmsg_buf.data();
    msg.msg_controllen = cmsg_buf.size();

    cmsghdr *cmsg = CMSG_FIRSTHDR(&msg);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    cmsg->cmsg_len = CMSG_LEN(fds.size() * sizeof(int));
    std::memcpy(CMSG_DATA(cmsg), fds.data(), fds.size() * sizeof(int));

    if (::sendmsg(sock, &msg, 0) < 0) {
        std::fprintf(stderr, "criu-stream-fetch: sendmsg fds: %s\n", std::strerror(errno));
        return false;
    }
    return true;
}

// readExact loops over read() to drain n bytes from sock. Returns true on
// success, false on EOF or error. Used to read the 4-byte pages_img_id sent
// by CRIU's recv_streamer_private_fd.
bool readExact(int sock, void *buf, size_t n) {
    size_t got = 0;
    auto *p = static_cast<char *>(buf);
    while (got < n) {
        ssize_t r = ::read(sock, p + got, n - got);
        if (r > 0) {
            got += (size_t)r;
            continue;
        }
        if (r == 0) return false;            // EOF
        if (errno == EINTR) continue;
        std::fprintf(stderr, "criu-stream-fetch: read(%zu/%zu): %s\n",
                     got, n, std::strerror(errno));
        return false;
    }
    return true;
}

bool writeExact(int sock, const void *buf, size_t n) {
    size_t put = 0;
    auto *p = static_cast<const char *>(buf);
    while (put < n) {
        ssize_t w = ::write(sock, p + put, n - put);
        if (w > 0) {
            put += (size_t)w;
            continue;
        }
        if (w < 0 && errno == EINTR) continue;
        std::fprintf(stderr, "criu-stream-fetch: write(%zu/%zu): %s\n",
                     put, n, std::strerror(errno));
        return false;
    }
    return true;
}

// setupNixlObjAgents constructs N independent nixlAgent instances, each with
// its own OBJ backend (and therefore its own Aws::S3Crt::S3CrtClient + TCP
// pool). nixlAgent::createBackend rejects a second OBJ backend on the same
// agent (nixl_agent.cpp:291-294), so multi-client scaling requires multiple
// agents — same lever runai-model-streamer's ClientMgr uses to scale past the
// single-CRT-client ceiling (~8 Gbps observed on us-east-2 A100). Aws::InitAPI
// is wrapped in std::call_once (aws_sdk_init.h:37-49) so concurrent agent
// constructions are safe. throughput is the cluster's S3_CRT_THROUGHPUT_GBPS
// (per design doc table, 100 is the 28.98 Gbps sweet spot on us-east-2 A100
// boxes).
bool setupNixlObjAgents(size_t n,
                        const std::string &bucket,
                        const std::string &throughput,
                        std::vector<std::unique_ptr<nixlAgent>> &agents_out,
                        std::vector<nixlBackendH *> &backends_out) {
    // crtMinLimit must stay >0 to select S3CrtObjEngineImpl (obj_backend.cpp:50).
    // Raising it to gate small objects through the standard S3 client breaks
    // this workload (the dual-client path returns NIXL_ERR_BACKEND for the
    // shmem + small private xfers), so we keep it pinned at 1 and instead use
    // the new crtPartSize override to choose multipart part size independently.
    // CRT_PART_SIZE env (bytes; e.g. 16777216 for 16 MiB) lets a single restore
    // pick a larger partSize without recompiling. Unset = fall back to the
    // partSize-equals-crtMinLimit behavior (=> SDK clamps to its 5 MiB floor).
    const char *crt_part_env = std::getenv("CRT_PART_SIZE");
    std::string crt_part = (crt_part_env && *crt_part_env) ? crt_part_env : "";
    std::fprintf(stderr, "[stream] CRT_PART_SIZE = '%s'\n", crt_part.c_str());
    nixl_b_params_t params{
        {"bucket", bucket},
        {"crtThroughputGbps", throughput},
        {"crtMinLimit", "1"},
    };
    if (!crt_part.empty()) {
        params["crtPartSize"] = crt_part;
    }
    // NIXL OBJ ClientConfiguration does NOT auto-load AWS_REGION env. It
    // defaults to us-east-1 which yields a 301 PermanentRedirect → silent
    // NIXL_ERR_BACKEND on getXferStatus for buckets in other regions. Read
    // the region from env (AWS_REGION preferred, then AWS_DEFAULT_REGION)
    // and pass it explicitly. Empty value falls through to plugin default.
    const char *region_env = std::getenv("AWS_REGION");
    if (!region_env || *region_env == '\0') region_env = std::getenv("AWS_DEFAULT_REGION");
    if (region_env && *region_env != '\0') {
        params["region"] = region_env;
    }

    agents_out.reserve(n);
    backends_out.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        nixlAgentConfig cfg(/*use_prog_thread=*/true);
        auto name = streamerAgentName(i);
        auto agent = std::make_unique<nixlAgent>(name, cfg);

        std::fprintf(stderr,
                     "[stream] NIXL OBJ backend config: bucket=%s crtThroughputGbps=%s crtMinLimit=%s region=%s agent=%zu/%zu name=%s\n",
                     bucket.c_str(),
                     throughput.c_str(),
                     params.count("crtMinLimit") ? params["crtMinLimit"].c_str() : "(unset)",
                     params.count("region") ? params["region"].c_str() : "(unset)",
                     i, n, name.c_str());

        nixlBackendH *backend = nullptr;
        nixl_status_t ret = agent->createBackend("OBJ", params, backend);
        if (ret != NIXL_SUCCESS || backend == nullptr) {
            std::fprintf(stderr,
                         "criu-stream-fetch: createBackend(\"OBJ\", bucket=%s, agent=%zu) failed (status=%d)\n",
                         bucket.c_str(), i, (int)ret);
            return false;
        }
        agents_out.push_back(std::move(agent));
        backends_out.push_back(backend);
    }
    return true;
}

// parseNumAgents returns the number of independent nixlAgent instances the
// streamer should spawn. Reads STREAMER_NUM_AGENTS (decimal); defaults to 4.
// Clamps to [1, 16]: floor preserves single-agent debug; cap bounds CRT
// connection-pool memory and DNS/ENA pressure. Cached on first call so
// multiple prep/teardown paths see the same value without re-logging.
size_t parseNumAgents() {
    static size_t cached = 0;
    static bool initialized = false;
    if (initialized) return cached;

    constexpr size_t kDefault = 4;
    constexpr size_t kMin = 1;
    constexpr size_t kMax = 16;
    size_t n = kDefault;
    const char *env = std::getenv("STREAMER_NUM_AGENTS");
    if (env && *env) {
        char *endp = nullptr;
        unsigned long long v = std::strtoull(env, &endp, 10);
        if (endp != env && v > 0) n = (size_t)v;
    }
    if (n < kMin) {
        std::fprintf(stderr,
                     "[stream] STREAMER_NUM_AGENTS=%zu below %zu floor; clamping\n",
                     n, kMin);
        n = kMin;
    }
    if (n > kMax) {
        std::fprintf(stderr,
                     "[stream] STREAMER_NUM_AGENTS=%zu above %zu cap; clamping\n",
                     n, kMax);
        n = kMax;
    }
    std::fprintf(stderr, "[stream] num agents = %zu\n", n);
    cached = n;
    initialized = true;
    return cached;
}

// parseChunkBytesize returns the per-chunk byte size used to split large
// transfers (both private ranges and shmem pagemap entries) into multiple
// parallel GetObjectAsync requests. Reads STREAMER_CHUNK_BYTESIZE (decimal
// bytes); defaults to 16 MiB. Clamps to [5 MiB, 256 MiB]: floor matches
// S3's multipart minimum; cap bounds dlist size per range. Parses once on
// first call and caches; safe to invoke from multiple prep functions in
// the main thread without re-logging.
size_t parseChunkBytesize() {
    static size_t cached = 0;
    static bool initialized = false;
    if (initialized) return cached;

    constexpr size_t kDefault = 16 * 1024 * 1024;
    constexpr size_t kMin = 5 * 1024 * 1024;
    constexpr size_t kMax = 256 * 1024 * 1024;
    size_t chunk = kDefault;
    const char *env = std::getenv("STREAMER_CHUNK_BYTESIZE");
    if (env && *env) {
        char *endp = nullptr;
        unsigned long long v = std::strtoull(env, &endp, 10);
        if (endp != env && v > 0) chunk = (size_t)v;
    }
    if (chunk < kMin) {
        std::fprintf(stderr,
                     "[stream] STREAMER_CHUNK_BYTESIZE=%zu below %zu floor; clamping\n",
                     chunk, kMin);
        chunk = kMin;
    }
    if (chunk > kMax) {
        std::fprintf(stderr,
                     "[stream] STREAMER_CHUNK_BYTESIZE=%zu above %zu cap; clamping\n",
                     chunk, kMax);
        chunk = kMax;
    }
    std::fprintf(stderr, "[stream] chunk bytesize = %zu MiB\n",
                 chunk / (1024 * 1024));
    cached = chunk;
    initialized = true;
    return chunk;
}

// preparePrivateRanges allocates per-range memfd + mmap + eventfd, registers
// EVERY range's full DRAM + OBJ descs on EVERY agent (so any agent's
// devIdToObjKey_ can resolve any devId), and creates per-chunk transfer
// requests via the per-chunk owner (chunk k of range i → agent k % N). This
// fans a single large range across all N CRT clients in parallel, lifting
// the per-client ~8 Gbps ceiling on big files like the 14B id=12 (33 GiB).
// Returns false on any failure; on success the caller can postXferReq each
// state's reqs in parallel.
//
// Cross-agent safety: each nixlAgent owns its own engine_impl with its own
// devIdToObjKey_ map (see nixl/src/plugins/obj/s3/engine_impl.h:58). Same
// (devId → key) entry on N agents is N independent map writes, no
// collision. DRAM_SEG registration stores no metadata in the OBJ engine
// (engine_impl.cpp:141-143), so overlapping registrations of the same
// memfd region across agents are harmless — each chunk's xfer desc targets
// a non-overlapping sub-slice of that memfd.
bool preparePrivateRanges(std::vector<std::unique_ptr<nixlAgent>> &agents,
                          std::vector<nixlBackendH *> &backends,
                          const std::vector<RangeEntry> &ranges,
                          std::vector<std::unique_ptr<PrivateRangeState>> &states) {
    if (ranges.empty()) return true;
    const size_t N = agents.size();

    // Single full-range register dlist shared across agents — each agent's
    // registerMem call receives the same list, but writes into its OWN
    // devIdToObjKey_ map. Build it once instead of N times.
    nixl_reg_dlist_t dram_all(DRAM_SEG);
    nixl_reg_dlist_t obj_all(OBJ_SEG);

    states.reserve(ranges.size());
    for (size_t i = 0; i < ranges.size(); ++i) {
        const auto &r = ranges[i];
        if (r.key.empty()) {
            std::fprintf(stderr, "criu-stream-fetch: range id=%u missing S3 key\n", r.id);
            return false;
        }
        auto st = std::make_unique<PrivateRangeState>();
        st->id = r.id;
        st->size = r.size;

        char name[64];
        std::snprintf(name, sizeof(name), "criu-private-%u", r.id);
        st->memfd = memfdCreate(name, r.size);
        if (st->memfd < 0) return false;

        if (r.size > 0) {
            st->mmap_addr = ::mmap(nullptr, r.size, PROT_READ | PROT_WRITE,
                                   MAP_SHARED, st->memfd, 0);
            if (st->mmap_addr == MAP_FAILED) {
                std::fprintf(stderr, "criu-stream-fetch: mmap id=%u size=%lu: %s\n",
                             r.id, (unsigned long)r.size, std::strerror(errno));
                st->mmap_addr = nullptr;
                return false;
            }
        }
        st->eventfd_fd = eventfdCreate();
        if (st->eventfd_fd < 0) return false;

        nixlBlobDesc dram{};
        dram.addr = (uintptr_t)st->mmap_addr;
        dram.len = r.size;
        dram.devId = (uint32_t)i;
        dram_all.addDesc(dram);

        nixlBlobDesc object{};
        object.addr = 0;
        object.len = r.size;
        object.devId = (uint32_t)i;
        object.metaInfo = r.key;
        obj_all.addDesc(object);

        states.push_back(std::move(st));
    }

    // registerMem on every agent so any agent can resolve any devId.
    for (size_t a = 0; a < N; ++a) {
        nixl_opt_args_t backend_hint;
        backend_hint.backends.push_back(backends[a]);
        if (agents[a]->registerMem(dram_all, &backend_hint) != NIXL_SUCCESS) {
            std::fprintf(stderr, "criu-stream-fetch: registerMem(DRAM) agent=%zu failed\n", a);
            return false;
        }
        if (agents[a]->registerMem(obj_all, &backend_hint) != NIXL_SUCCESS) {
            std::fprintf(stderr, "criu-stream-fetch: registerMem(OBJ) agent=%zu failed\n", a);
            return false;
        }
    }

    // Split each range into CHUNK-sized sub-ranges; chunk k of range i is
    // routed to agents[k % N]. Both small ranges (1-2 chunks) and big
    // ranges (id=12 ≈ 1981 chunks) split evenly across agents.
    const size_t CHUNK = parseChunkBytesize();
    for (size_t i = 0; i < states.size(); ++i) {
        auto &st = *states[i];
        const uint64_t total = st.size;
        uint64_t off = 0;
        size_t k_idx = 0;
        if (total == 0) {
            st.chunks_remaining = 0;
            continue;
        }
        while (off < total) {
            const uint64_t chunk = std::min<uint64_t>(CHUNK, total - off);
            const size_t a = k_idx % N;

            nixl_reg_dlist_t dram_one(DRAM_SEG);
            nixl_reg_dlist_t obj_one(OBJ_SEG);

            nixlBlobDesc dram{};
            dram.addr = (uintptr_t)st.mmap_addr + off;
            dram.len = chunk;
            dram.devId = (uint32_t)i;
            dram_one.addDesc(dram);

            nixlBlobDesc object{};
            object.addr = off;
            object.len = chunk;
            object.devId = (uint32_t)i;
            object.metaInfo = ranges[i].key;
            obj_one.addDesc(object);

            nixl_xfer_dlist_t dram_xfer = dram_one.trim();
            nixl_xfer_dlist_t obj_xfer = obj_one.trim();

            nixlXferReqH *req = nullptr;
            nixl_opt_args_t xfer_hint;
            xfer_hint.backends.push_back(backends[a]);
            nixl_status_t ret = agents[a]->createXferReq(NIXL_READ, dram_xfer, obj_xfer,
                                                          streamerAgentName(a), req, &xfer_hint);
            if (ret != NIXL_SUCCESS || req == nullptr) {
                std::fprintf(stderr,
                             "criu-stream-fetch: createXferReq id=%u agent=%zu off=%lu len=%lu failed (status=%d)\n",
                             st.id, a, (unsigned long)off, (unsigned long)chunk, (int)ret);
                return false;
            }
            st.xfer_reqs.push_back(req);
            st.chunk_lens.push_back((uint32_t)chunk);
            st.chunk_owners.push_back((uint32_t)a);
            off += chunk;
            ++k_idx;
        }
        st.xfer_done.assign(st.xfer_reqs.size(), false);
        st.chunks_remaining = (unsigned)st.xfer_reqs.size();
    }
    return true;
}

// prepareShmemRanges allocates per-shmem-range memfd + mmap + eventfd,
// registers EVERY range/entry's descs on EVERY agent (so any agent can
// resolve any devId), and creates per-chunk transfer requests via the
// per-chunk owner. Chunks of one range are distributed across all N agents
// using a cumulative counter (across entries) modulo N, so even small entries
// participate in the fan-out.
bool prepareShmemRanges(std::vector<std::unique_ptr<nixlAgent>> &agents,
                        std::vector<nixlBackendH *> &backends,
                        const std::vector<ShmemRange> &ranges,
                        std::vector<std::unique_ptr<ShmemRangeState>> &states) {
    if (ranges.empty()) return true;
    const size_t N = agents.size();

    nixl_reg_dlist_t dram_all(DRAM_SEG);
    nixl_reg_dlist_t obj_all(OBJ_SEG);

    states.reserve(ranges.size());
    for (size_t i = 0; i < ranges.size(); ++i) {
        const auto &r = ranges[i];
        if (r.key.empty()) {
            std::fprintf(stderr,
                         "criu-stream-fetch: shmem shmid=0x%lx missing S3 key\n",
                         (unsigned long)r.shmid);
            return false;
        }
        auto st = std::make_unique<ShmemRangeState>();
        st->shmid = r.shmid;
        st->size = r.size;

        char name[64];
        std::snprintf(name, sizeof(name), "criu-shmem-%lx", (unsigned long)r.shmid);
        st->memfd = memfdCreate(name, r.size);
        if (st->memfd < 0) return false;

        if (r.size > 0) {
            st->mmap_addr = ::mmap(nullptr, r.size, PROT_READ | PROT_WRITE,
                                   MAP_SHARED, st->memfd, 0);
            if (st->mmap_addr == MAP_FAILED) {
                std::fprintf(stderr,
                             "criu-stream-fetch: mmap shmid=0x%lx size=%lu: %s\n",
                             (unsigned long)r.shmid, (unsigned long)r.size,
                             std::strerror(errno));
                st->mmap_addr = nullptr;
                return false;
            }
        }
        st->eventfd_fd = eventfdCreate();
        if (st->eventfd_fd < 0) return false;

        // A shmem range with zero PE_PRESENT entries has nothing to fill;
        // signal eventfd + done immediately so serveShmemSocket doesn't
        // block forever. entries_remaining stays at its atomic-default 0
        // and xfer_done stays empty; pollShmemXfers iterates nothing.
        if (r.entries.empty()) {
            eventfdSignal(st->eventfd_fd);
            {
                std::lock_guard<std::mutex> g(st->done_mu);
                st->done = true;
            }
            st->done_cv.notify_all();
        }

        // One register descriptor per entry on each side. devId encodes
        // (range_index, entry_index) so per-entry createXferReq descriptors
        // match a unique registered descriptor 1:1.
        for (size_t k = 0; k < r.entries.size(); ++k) {
            const auto &e = r.entries[k];
            // NIXL OBJ backend keys devIdToObjKey_ map on devId only (see
            // nixl/src/plugins/obj/s3/engine_impl.cpp). Reserve devId>=1<<31
            // so shmem entries cannot collide with priv devIds 0..N-1 inside
            // any single agent's map.
            uint32_t dev = (uint32_t)((1u << 31) | (i << 16) | (k & 0xFFFF));

            nixlBlobDesc dram{};
            dram.addr = (uintptr_t)st->mmap_addr + e.vaddr;
            dram.len = e.len;
            dram.devId = 0;
            dram_all.addDesc(dram);

            nixlBlobDesc object{};
            object.addr = e.pages_img_offset;
            object.len = e.len;
            object.devId = dev;
            object.metaInfo = r.key;
            obj_all.addDesc(object);
        }

        states.push_back(std::move(st));
    }

    // registerMem on every agent so any agent can resolve any (range, entry)
    // devId.
    for (size_t a = 0; a < N; ++a) {
        nixl_opt_args_t backend_hint;
        backend_hint.backends.push_back(backends[a]);
        if (agents[a]->registerMem(dram_all, &backend_hint) != NIXL_SUCCESS) {
            std::fprintf(stderr,
                         "criu-stream-fetch: registerMem(shmem DRAM) agent=%zu failed\n", a);
            return false;
        }
        if (agents[a]->registerMem(obj_all, &backend_hint) != NIXL_SUCCESS) {
            std::fprintf(stderr,
                         "criu-stream-fetch: registerMem(shmem OBJ) agent=%zu failed\n", a);
            return false;
        }
    }

    // Per-entry chunking with chunks distributed across all agents using a
    // cumulative counter across the range's entries. Single-chunk entries
    // still rotate through agents instead of all landing on agent 0.
    const size_t CHUNK = parseChunkBytesize();
    for (size_t i = 0; i < states.size(); ++i) {
        auto &st = *states[i];
        const auto &r = ranges[i];
        size_t cumulative_k = 0;
        for (size_t k = 0; k < r.entries.size(); ++k) {
            const auto &e = r.entries[k];
            uint32_t dev = (uint32_t)((1u << 31) | (i << 16) | (k & 0xFFFF));
            uint64_t off = 0;
            while (off < e.len) {
                const uint64_t chunk = std::min<uint64_t>(CHUNK, e.len - off);
                const size_t a = cumulative_k % N;

                nixl_reg_dlist_t dram_one(DRAM_SEG);
                nixl_reg_dlist_t obj_one(OBJ_SEG);

                nixlBlobDesc dram{};
                dram.addr = (uintptr_t)st.mmap_addr + e.vaddr + off;
                dram.len = chunk;
                dram.devId = 0;
                dram_one.addDesc(dram);

                nixlBlobDesc object{};
                object.addr = e.pages_img_offset + off;
                object.len = chunk;
                object.devId = dev;
                object.metaInfo = r.key;
                obj_one.addDesc(object);

                nixl_xfer_dlist_t dram_xfer = dram_one.trim();
                nixl_xfer_dlist_t obj_xfer = obj_one.trim();

                nixlXferReqH *req = nullptr;
                nixl_opt_args_t xfer_hint;
                xfer_hint.backends.push_back(backends[a]);
                nixl_status_t ret = agents[a]->createXferReq(NIXL_READ, dram_xfer, obj_xfer,
                                                              streamerAgentName(a), req, &xfer_hint);
                if (ret != NIXL_SUCCESS || req == nullptr) {
                    std::fprintf(stderr,
                                 "criu-stream-fetch: shmem createXferReq shmid=0x%lx agent=%zu "
                                 "entry=%zu off=%lu len=%lu failed (status=%d)\n",
                                 (unsigned long)r.shmid, a, k,
                                 (unsigned long)off, (unsigned long)chunk, (int)ret);
                    return false;
                }
                st.xfer_reqs.push_back(req);
                st.chunk_lens.push_back((uint32_t)chunk);
                st.chunk_owners.push_back((uint32_t)a);
                off += chunk;
                ++cumulative_k;
            }
        }
        st.xfer_done.assign(st.xfer_reqs.size(), false);
        st.entries_remaining = (unsigned)st.xfer_reqs.size();
    }
    return true;
}

// pollShmemXfers spins until every per-chunk xfer in every shmem range
// reaches a terminal state. On per-chunk NIXL_SUCCESS the
// entries_remaining counter for that range drops; when it hits zero the
// range's eventfd fires once (the daemon UFFD_CONTINUE side consumes it).
// Per-chunk Gbps and per-range aggregate Gbps are logged for sweep work.
void pollShmemXfers(std::vector<std::unique_ptr<nixlAgent>> &agents,
                    std::vector<std::unique_ptr<ShmemRangeState>> &states,
                    int abort_fd) {
    if (states.empty()) return;
    size_t total = 0;
    for (auto &st : states) total += st->xfer_reqs.size();
    std::fprintf(stderr,
                 "[stream] pollShmemXfers entered ranges=%zu chunks=%zu agents=%zu\n",
                 states.size(), total, agents.size());

    size_t completed = 0;
    auto last_tick = std::chrono::steady_clock::now();
    while (completed < total) {
        size_t in_prog = 0;
        size_t failed = 0;
        for (auto &st : states) {
            for (size_t k = 0; k < st->xfer_reqs.size(); ++k) {
                if (st->xfer_done[k]) continue;
                auto &owner = agents[st->chunk_owners[k]];
                nixl_status_t s = owner->getXferStatus(st->xfer_reqs[k]);
                if (s == NIXL_SUCCESS) {
                    st->xfer_done[k] = true;
                    ++completed;
                    long done_t = stream_ms_now();
                    long dur_ms = done_t - st->post_t_ms;
                    double mb = (double)st->chunk_lens[k] / (1024.0 * 1024.0);
                    double mb_per_s = (dur_ms > 0) ? (mb * 1000.0 / dur_ms) : 0.0;
                    double gbps = mb_per_s * 8.0 / 1024.0;
                    std::fprintf(stderr,
                                 "[stream t=%ldms] shmem shmid=0x%lx chunk=%zu/%zu NIXL_SUCCESS"
                                 " size=%.1fMB dur=%ldms throughput=%.2fGbps\n",
                                 done_t, (unsigned long)st->shmid,
                                 k + 1, st->xfer_reqs.size(), mb, dur_ms, gbps);
                    unsigned left = --st->entries_remaining;
                    if (left == 0) {
                        double total_mb = (double)st->size / (1024.0 * 1024.0);
                        long wall_ms = done_t - st->post_t_ms;
                        double agg = (wall_ms > 0)
                            ? (total_mb * 8.0 * 1000.0 / wall_ms / 1024.0)
                            : 0.0;
                        std::fprintf(stderr,
                                     "[stream t=%ldms] shmem shmid=0x%lx COMPLETE total=%.1fMB"
                                     " chunks=%zu wall=%ldms aggregate=%.2fGbps\n",
                                     done_t, (unsigned long)st->shmid,
                                     total_mb, st->xfer_reqs.size(), wall_ms, agg);
                        if (!eventfdSignal(st->eventfd_fd))
                            std::fprintf(stderr,
                                         "criu-stream-fetch: shmem eventfd signal shmid=0x%lx failed\n",
                                         (unsigned long)st->shmid);
                        {
                            std::lock_guard<std::mutex> g(st->done_mu);
                            st->done = true;
                        }
                        st->done_cv.notify_all();
                    }
                } else if (s != NIXL_IN_PROG) {
                    std::fprintf(stderr,
                                 "criu-stream-fetch: shmem xfer shmid=0x%lx chunk=%zu/%zu failed (status=%d)\n",
                                 (unsigned long)st->shmid, k + 1,
                                 st->xfer_reqs.size(), (int)s);
                    st->xfer_done[k] = true;
                    {
                        std::lock_guard<std::mutex> g(st->done_mu);
                        st->failed = true;
                        st->done = true;
                    }
                    st->done_cv.notify_all();
                    poisonAbort(abort_fd);
                    ++completed;
                    ++failed;
                } else {
                    ++in_prog;
                }
            }
        }
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(now - last_tick).count() >= 200) {
            std::fprintf(stderr, "[stream] shmem poll tick done=%zu in_prog=%zu failed=%zu total=%zu\n",
                         completed - failed, in_prog, failed, total);
            last_tick = now;
        }
        if (completed < total)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::fprintf(stderr, "[stream] pollShmemXfers done\n");
}

// serveShmemSocket reads 8-byte shmid LE from CRIU and replies with
// SCM_RIGHTS(memfd). No ack: the streamer fills asynchronously and the
// daemon side gates user-visible reads via UFFDIO_CONTINUE + eventfd.
void serveShmemSocket(int sock,
                      std::vector<std::unique_ptr<ShmemRangeState>> &states,
                      int abort_fd) {
    std::unordered_map<uint64_t, ShmemRangeState *> by_shmid;
    by_shmid.reserve(states.size());
    for (auto &st : states) by_shmid[st->shmid] = st.get();

    while (true) {
        uint64_t shmid = 0;
        if (!readExact(sock, &shmid, sizeof(shmid))) break;
        auto it = by_shmid.find(shmid);
        if (it == by_shmid.end()) {
            std::fprintf(stderr,
                         "criu-stream-fetch: CRIU asked for unknown shmid=0x%lx\n",
                         (unsigned long)shmid);
            poisonAbort(abort_fd);
            return;
        }
        ShmemRangeState *st = it->second;
        // Synchronously wait for the streamer to finish NIXL-filling the
        // entire range before handing the memfd to CRIU. This guarantees
        // the shmem page cache is populated when CRIU's open_shmem /
        // memfd_open mmaps the memfd, so the restored process sees real
        // bytes (not zeros) on first access. Trade: serializes CRIU
        // restore behind the slowest shmem fill. Async overlap via the
        // lazy-pages daemon + UFFDIO_CONTINUE would lift this; see
        // Stage 2c followups.
        {
            std::unique_lock<std::mutex> lk(st->done_mu);
            st->done_cv.wait(lk, [&]() { return st->done; });
            if (st->failed) {
                std::fprintf(stderr,
                             "criu-stream-fetch: shmem fill failed for shmid=0x%lx; refusing memfd\n",
                             (unsigned long)st->shmid);
                poisonAbort(abort_fd);
                return;
            }
        }
        if (!sendFds(sock, {st->memfd}, nullptr, 0)) {
            poisonAbort(abort_fd);
            return;
        }
    }
}

// pollPrivateXfers spins on getXferStatus for every in-flight chunk across
// every state. On per-chunk NIXL_SUCCESS, logs chunk throughput. When a
// state's chunks_remaining hits 0, fires the per-range eventfd once,
// notifies done_cv, and emits a per-state aggregate throughput line.
// Returns when every state is terminal (all chunks done or any failed).
void pollPrivateXfers(std::vector<std::unique_ptr<nixlAgent>> &agents,
                      std::vector<std::unique_ptr<PrivateRangeState>> &states,
                      int abort_fd) {
    if (states.empty()) return;
    size_t total_chunks = 0;
    for (auto &st : states) total_chunks += st->xfer_reqs.size();
    std::fprintf(stderr,
                 "[stream] pollPrivateXfers entered states=%zu chunks=%zu agents=%zu\n",
                 states.size(), total_chunks, agents.size());
    auto last_tick = std::chrono::steady_clock::now();
    size_t completed_chunks = 0;
    size_t completed_states = 0;
    // States may already be terminal before poll runs: zero-size range or
    // inline NIXL_SUCCESS during the post loop both flip st->done = true.
    // Count them up front so the while-guard can exit.
    for (auto &st : states) {
        std::lock_guard<std::mutex> g(st->done_mu);
        if (st->done) ++completed_states;
    }
    while (completed_states < states.size()) {
        size_t in_prog = 0;
        size_t failed = 0;
        for (auto &st : states) {
            bool state_already_done = false;
            {
                std::lock_guard<std::mutex> g(st->done_mu);
                state_already_done = st->done;
            }
            if (state_already_done) continue;
            for (size_t k = 0; k < st->xfer_reqs.size(); ++k) {
                if (st->xfer_done[k]) continue;
                auto &owner = agents[st->chunk_owners[k]];
                nixl_status_t s = owner->getXferStatus(st->xfer_reqs[k]);
                if (s == NIXL_SUCCESS) {
                    st->xfer_done[k] = true;
                    ++completed_chunks;
                    long done_t = stream_ms_now();
                    long dur_ms = done_t - st->post_t_ms;
                    double mb = (double)st->chunk_lens[k] / (1024.0 * 1024.0);
                    double mb_per_s = (dur_ms > 0) ? (mb * 1000.0 / dur_ms) : 0.0;
                    double gbps = mb_per_s * 8.0 / 1024.0;
                    std::fprintf(stderr,
                                 "[stream t=%ldms] xfer id=%u chunk=%zu/%zu NIXL_SUCCESS"
                                 " size=%.1fMB dur=%ldms throughput=%.2fGbps\n",
                                 done_t, st->id, k + 1, st->xfer_reqs.size(),
                                 mb, dur_ms, gbps);
                    unsigned left = --st->chunks_remaining;
                    if (left == 0) {
                        double total_mb = (double)st->size / (1024.0 * 1024.0);
                        long wall_ms = done_t - st->post_t_ms;
                        double agg = (wall_ms > 0)
                            ? (total_mb * 8.0 * 1000.0 / wall_ms / 1024.0)
                            : 0.0;
                        std::fprintf(stderr,
                                     "[stream t=%ldms] xfer id=%u COMPLETE total=%.1fMB"
                                     " chunks=%zu wall=%ldms aggregate=%.2fGbps\n",
                                     done_t, st->id, total_mb,
                                     st->xfer_reqs.size(), wall_ms, agg);
                        if (!eventfdSignal(st->eventfd_fd))
                            std::fprintf(stderr,
                                         "criu-stream-fetch: eventfd signal id=%u failed\n",
                                         st->id);
                        {
                            std::lock_guard<std::mutex> g(st->done_mu);
                            st->done = true;
                            st->done_status = NIXL_SUCCESS;
                            signalPrivateReady_locked(st.get(), true);
                        }
                        st->done_cv.notify_all();
                        ++completed_states;
                    }
                } else if (s != NIXL_IN_PROG) {
                    std::fprintf(stderr,
                                 "criu-stream-fetch: xfer id=%u chunk=%zu/%zu failed (status=%d)\n",
                                 st->id, k + 1, st->xfer_reqs.size(), (int)s);
                    st->xfer_done[k] = true;
                    ++completed_chunks;
                    {
                        std::lock_guard<std::mutex> g(st->done_mu);
                        st->done = true;
                        st->done_status = s;
                        signalPrivateReady_locked(st.get(), false);
                    }
                    st->done_cv.notify_all();
                    poisonAbort(abort_fd);
                    ++completed_states;
                    ++failed;
                    break;
                } else {
                    ++in_prog;
                }
            }
        }
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(now - last_tick).count() >= 200) {
            std::fprintf(stderr,
                         "[stream] poll tick chunks_done=%zu/%zu in_prog=%zu failed=%zu states_done=%zu/%zu\n",
                         completed_chunks, total_chunks, in_prog, failed,
                         completed_states, states.size());
            last_tick = now;
        }
        if (completed_states < states.size())
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    long t_end = stream_ms_now();
    long t_start = states[0]->post_t_ms;
    for (auto &st : states) {
        if (st->post_t_ms < t_start) t_start = st->post_t_ms;
    }
    uint64_t total_bytes = 0;
    for (auto &st : states) total_bytes += st->size;
    long wall_ms = t_end - t_start;
    double total_mb = (double)total_bytes / (1024.0 * 1024.0);
    double agg_gbps = (wall_ms > 0) ? (total_mb * 8.0 * 1000.0 / wall_ms / 1024.0) : 0.0;
    std::fprintf(stderr,
                 "[stream] pollPrivateXfers done: total=%.1fMB wall=%ldms aggregate=%.2fGbps (states=%zu chunks=%zu)\n",
                 total_mb, wall_ms, agg_gbps, states.size(), total_chunks);
}

// servePrivateSocket reads 4-byte pages_img_id values from CRIU, looks up
// the matching PrivateRangeState, replies with SCM_RIGHTS(memfd), and ack
// bytes.
//
// Two modes (g_async_overlap):
//
//  - SYNC (legacy): send memfd, wait done_cv until fill complete, send 'A'.
//    PIE is unblocked only after the memfd is fully populated.
//
//  - ASYNC (Plan v4, default): create a pipe, send [memfd, pipe_rfd] via
//    SCM_RIGHTS, send 'A' immediately. CRIU returns from
//    recv_streamer_private_fd at handover; the rest of CRIU's restore prep
//    and PIE setup run concurrent with the S3 download. PIE blocks on
//    sys_read(pipe_rfd) at AIO entry. The fill-completion sites
//    (pollPrivateXfers + inline-success in run_main) call
//    signalPrivateReady_locked under st->done_mu, which writes '1' to wfd
//    on success or closes wfd on failure (PIE sees EOF → aio_error).
//
// Race-safe handoff in ASYNC: register wfd under the same st->done_mu the
// completion sites use. If st->done is already true (poll thread or
// inline-success raced past us between sendFds and the lock), we signal
// directly inline; otherwise the next completion site signals.
void servePrivateSocket(int sock,
                        std::vector<std::unique_ptr<PrivateRangeState>> &states,
                        int abort_fd) {
    std::unordered_map<uint32_t, PrivateRangeState *> by_id;
    by_id.reserve(states.size());
    for (auto &st : states) by_id[st->id] = st.get();

    while (true) {
        uint32_t id = 0;
        if (!readExact(sock, &id, sizeof(id))) break;
        long t_req_ms = stream_ms_now();
        auto it = by_id.find(id);
        if (it == by_id.end()) {
            std::fprintf(stderr,
                         "criu-stream-fetch: CRIU asked for unknown pages_img_id=%u\n", id);
            poisonAbort(abort_fd);
            return;
        }
        PrivateRangeState *st = it->second;
        std::fprintf(stderr,
                     "[stream t=%ldms] CRIU asked for id=%u; sending memfd (async=%d)\n",
                     t_req_ms, id, g_async_overlap ? 1 : 0);

        if (!g_async_overlap) {
            // Legacy sync path: send memfd, wait fill, send 'A'.
            if (!sendFds(sock, {st->memfd}, nullptr, 0)) {
                poisonAbort(abort_fd);
                return;
            }
            {
                std::unique_lock<std::mutex> lk(st->done_mu);
                st->done_cv.wait(lk, [&]() { return st->done; });
                if (st->done_status != NIXL_SUCCESS) {
                    std::fprintf(stderr,
                                 "criu-stream-fetch: fill failed for id=%u; skipping ack\n",
                                 st->id);
                    poisonAbort(abort_fd);
                    return;
                }
            }
            std::fprintf(stderr,
                         "[stream t=%ldms] ack id=%u sync (wait=%ldms)\n",
                         stream_ms_now(), id, stream_ms_now() - t_req_ms);
            const char ack = 'A';
            if (!writeExact(sock, &ack, 1)) {
                poisonAbort(abort_fd);
                return;
            }
            by_id.erase(it);
            continue;
        }

        // ASYNC path: pipe + 2-fd send + immediate ack.
        int p[2] = { -1, -1 };
        if (::pipe2(p, O_CLOEXEC) != 0) {
            std::fprintf(stderr,
                         "criu-stream-fetch: pipe2 for id=%u: %s\n",
                         id, std::strerror(errno));
            poisonAbort(abort_fd);
            return;
        }
        if (!sendFds(sock, {st->memfd, p[0]}, nullptr, 0)) {
            ::close(p[0]);
            ::close(p[1]);
            poisonAbort(abort_fd);
            return;
        }
        ::close(p[0]);  // CRIU owns the recv'd dup now.

        const char ack = 'A';
        if (!writeExact(sock, &ack, 1)) {
            ::close(p[1]);
            poisonAbort(abort_fd);
            return;
        }

        // Register wfd race-free against the completion sites. If the fill
        // already completed between sendFds and now, signal inline (the
        // poll/inline-success path saw ready_pipe_wfd == -1 when it took
        // the lock and could not signal us).
        bool signal_inline = false;
        bool signal_success = false;
        {
            std::lock_guard<std::mutex> g(st->done_mu);
            if (st->done) {
                signal_inline = true;
                signal_success = (st->done_status == NIXL_SUCCESS);
            } else {
                st->ready_pipe_wfd = p[1];
            }
        }
        if (signal_inline) {
            if (signal_success) {
                char b = '1';
                ssize_t n = ::write(p[1], &b, 1);
                (void)n;
            }
            ::close(p[1]);
        }
        std::fprintf(stderr,
                     "[stream t=%ldms] async ack id=%u (handover; inline_done=%d success=%d)\n",
                     stream_ms_now(), id, signal_inline ? 1 : 0,
                     signal_success ? 1 : 0);
        by_id.erase(it);
    }
}

// ============================================================================
// Multi-process download support (DOWNLOAD_PROCS env, default 1)
//
// aws-c-s3 / S3CrtClient throughput on us-east-2 p4d and eu-north L40S hosts
// plateaus around 12-15 Gbps per process, even with multiple nixlAgent
// instances in one process (STREAMER_NUM_AGENTS). The same workload driven by
// N independent processes (each running its own s5cmd or its own
// criu-stream-fetch-cpp) aggregates to ~50 Gbps on eu-north L40S, confirming
// the bottleneck is per-process. Suspected causes include single-process ENA
// queue affinity, single-process kernel scheduler bias, and shared
// aws-c-s3 event-loop-group internals. Multi-process bypasses all of them.
//
// Architecture when DOWNLOAD_PROCS > 1:
//   - Parent allocates memfds + eventfds + chunk-distribution metadata + a
//     MAP_SHARED|MAP_ANONYMOUS region with per-range atomic counters.
//   - Parent does the daemon handshake (sends shmem eventfds + abort_fd via
//     SCM_RIGHTS to the co-located lazy-pages daemon) BEFORE forking.
//   - Parent forks N download workers and a child→parent notification pipe.
//   - Each worker creates its OWN nixlAgent set (so each has its own
//     S3CrtClient + aws-c-s3 EventLoopGroup), registers memory, creates xfer
//     reqs only for chunks where chunk_owners[k] is in its agent-index
//     window, posts + polls those chunks, and exits.
//   - On each chunk completion the worker atomically decrements the shared
//     per-range counter; the worker that decrements to zero writes a 5-byte
//     "range done" message to the parent pipe.
//   - Parent's dispatcher thread reads the pipe and flips st->done +
//     signals st->done_cv + st->eventfd_fd for that range, so the unchanged
//     servePrivateSocket / serveShmemSocket loops can ack CRIU as soon as a
//     single range completes (preserves range-level pipelining).

struct SharedDoneState {
    // Per-private-range remaining-chunks counter. Initialized in parent
    // before fork; decremented atomically by workers as chunks finish.
    std::atomic<uint32_t> *private_remaining;
    size_t n_private;
    std::atomic<uint32_t> *shmem_remaining;
    size_t n_shmem;
    // Set to 1 by any worker that observed a fatal NIXL error.
    std::atomic<uint32_t> *failed;
};

// 5-byte child→parent notification:
//   uint8_t  type;   // 0 = private done, 1 = shmem done, 0xEE = abort
//   uint32_t idx;    // index into priv_states or shmem_states (LE)
struct DoneMsg {
    uint8_t  type;
    uint32_t idx;
} __attribute__((packed));

constexpr uint8_t kDoneMsgPriv  = 0x00;
constexpr uint8_t kDoneMsgShmem = 0x01;
constexpr uint8_t kDoneMsgAbort = 0xEE;

// parseDownloadProcs reads DOWNLOAD_PROCS env. Default 1 = single-process
// path unchanged. Clamp to [1, 16].
size_t parseDownloadProcs() {
    static size_t cached = 0;
    static bool initialized = false;
    if (initialized) return cached;
    constexpr size_t kDefault = 1;
    constexpr size_t kMin = 1;
    constexpr size_t kMax = 16;
    size_t n = kDefault;
    const char *env = std::getenv("DOWNLOAD_PROCS");
    if (env && *env) {
        char *endp = nullptr;
        unsigned long long v = std::strtoull(env, &endp, 10);
        if (endp != env && v > 0) n = (size_t)v;
    }
    if (n < kMin) n = kMin;
    if (n > kMax) {
        std::fprintf(stderr, "[stream] DOWNLOAD_PROCS=%zu above %zu cap; clamping\n", n, kMax);
        n = kMax;
    }
    std::fprintf(stderr, "[stream] download procs = %zu\n", n);
    cached = n;
    initialized = true;
    return cached;
}

// allocPrivateMemfds fills static fields (memfd, mmap_addr, eventfd_fd, size)
// and chunk-distribution metadata (chunk_lens, chunk_owners) per range. No
// NIXL calls. `total_agents` is the global agent index space; each worker P
// with agents_per_proc=M handles chunks where chunk_owners[k] in
// [P*M, (P+1)*M).
bool allocPrivateMemfds(const std::vector<RangeEntry> &ranges,
                        size_t total_agents,
                        std::vector<std::unique_ptr<PrivateRangeState>> &states) {
    if (ranges.empty()) return true;
    states.reserve(ranges.size());
    const size_t CHUNK = parseChunkBytesize();
    for (size_t i = 0; i < ranges.size(); ++i) {
        const auto &r = ranges[i];
        if (r.key.empty()) {
            std::fprintf(stderr, "criu-stream-fetch: range id=%u missing S3 key\n", r.id);
            return false;
        }
        auto st = std::make_unique<PrivateRangeState>();
        st->id = r.id;
        st->size = r.size;
        char name[64];
        std::snprintf(name, sizeof(name), "criu-private-%u", r.id);
        st->memfd = memfdCreate(name, r.size);
        if (st->memfd < 0) return false;
        if (r.size > 0) {
            st->mmap_addr = ::mmap(nullptr, r.size, PROT_READ | PROT_WRITE,
                                   MAP_SHARED, st->memfd, 0);
            if (st->mmap_addr == MAP_FAILED) {
                std::fprintf(stderr, "criu-stream-fetch: mmap id=%u size=%lu: %s\n",
                             r.id, (unsigned long)r.size, std::strerror(errno));
                st->mmap_addr = nullptr;
                return false;
            }
        }
        st->eventfd_fd = eventfdCreate();
        if (st->eventfd_fd < 0) return false;
        uint64_t off = 0;
        size_t k_idx = 0;
        while (off < r.size) {
            const uint64_t chunk = std::min<uint64_t>(CHUNK, r.size - off);
            const size_t a = k_idx % total_agents;
            st->chunk_lens.push_back((uint32_t)chunk);
            st->chunk_owners.push_back((uint32_t)a);
            off += chunk;
            ++k_idx;
        }
        st->xfer_reqs.assign(k_idx, nullptr);
        st->xfer_done.assign(k_idx, false);
        st->chunks_remaining = (unsigned)k_idx;
        states.push_back(std::move(st));
    }
    return true;
}

bool allocShmemMemfds(const std::vector<ShmemRange> &ranges,
                      size_t total_agents,
                      std::vector<std::unique_ptr<ShmemRangeState>> &states) {
    if (ranges.empty()) return true;
    states.reserve(ranges.size());
    const size_t CHUNK = parseChunkBytesize();
    for (size_t i = 0; i < ranges.size(); ++i) {
        const auto &r = ranges[i];
        if (r.key.empty()) {
            std::fprintf(stderr,
                         "criu-stream-fetch: shmem shmid=0x%lx missing S3 key\n",
                         (unsigned long)r.shmid);
            return false;
        }
        auto st = std::make_unique<ShmemRangeState>();
        st->shmid = r.shmid;
        st->size = r.size;
        char name[64];
        std::snprintf(name, sizeof(name), "criu-shmem-%lx", (unsigned long)r.shmid);
        st->memfd = memfdCreate(name, r.size);
        if (st->memfd < 0) return false;
        if (r.size > 0) {
            st->mmap_addr = ::mmap(nullptr, r.size, PROT_READ | PROT_WRITE,
                                   MAP_SHARED, st->memfd, 0);
            if (st->mmap_addr == MAP_FAILED) {
                std::fprintf(stderr,
                             "criu-stream-fetch: mmap shmid=0x%lx size=%lu: %s\n",
                             (unsigned long)r.shmid, (unsigned long)r.size,
                             std::strerror(errno));
                st->mmap_addr = nullptr;
                return false;
            }
        }
        st->eventfd_fd = eventfdCreate();
        if (st->eventfd_fd < 0) return false;
        size_t cumulative_k = 0;
        for (size_t k = 0; k < r.entries.size(); ++k) {
            const auto &e = r.entries[k];
            uint64_t off = 0;
            while (off < e.len) {
                const uint64_t chunk = std::min<uint64_t>(CHUNK, e.len - off);
                const size_t a = cumulative_k % total_agents;
                st->chunk_lens.push_back((uint32_t)chunk);
                st->chunk_owners.push_back((uint32_t)a);
                off += chunk;
                ++cumulative_k;
            }
        }
        st->xfer_reqs.assign(cumulative_k, nullptr);
        st->xfer_done.assign(cumulative_k, false);
        st->entries_remaining = (unsigned)cumulative_k;
        states.push_back(std::move(st));
    }
    return true;
}

// attachNixlPrivateOwned runs in each forked worker. Registers full DRAM +
// OBJ desc lists on every local agent so any local agent can resolve any
// devId, then creates xfer reqs only for chunks where chunk_owners[k] is in
// the worker's agent window. Non-owned chunks keep xfer_reqs[k] == nullptr.
bool attachNixlPrivateOwned(std::vector<std::unique_ptr<nixlAgent>> &agents,
                            std::vector<nixlBackendH *> &backends,
                            const std::vector<RangeEntry> &ranges,
                            std::vector<std::unique_ptr<PrivateRangeState>> &states,
                            uint32_t agent_idx_start,
                            uint32_t agent_idx_end) {
    if (states.empty()) return true;
    const size_t M = agents.size();
    nixl_reg_dlist_t dram_all(DRAM_SEG);
    nixl_reg_dlist_t obj_all(OBJ_SEG);
    for (size_t i = 0; i < states.size(); ++i) {
        auto &st = *states[i];
        const auto &r = ranges[i];
        nixlBlobDesc dram{};
        dram.addr = (uintptr_t)st.mmap_addr;
        dram.len = r.size;
        dram.devId = (uint32_t)i;
        dram_all.addDesc(dram);
        nixlBlobDesc object{};
        object.addr = 0;
        object.len = r.size;
        object.devId = (uint32_t)i;
        object.metaInfo = r.key;
        obj_all.addDesc(object);
    }
    for (size_t a = 0; a < M; ++a) {
        nixl_opt_args_t backend_hint;
        backend_hint.backends.push_back(backends[a]);
        if (agents[a]->registerMem(dram_all, &backend_hint) != NIXL_SUCCESS) {
            std::fprintf(stderr, "criu-stream-fetch: registerMem(DRAM) agent=%zu failed\n", a);
            return false;
        }
        if (agents[a]->registerMem(obj_all, &backend_hint) != NIXL_SUCCESS) {
            std::fprintf(stderr, "criu-stream-fetch: registerMem(OBJ) agent=%zu failed\n", a);
            return false;
        }
    }
    for (size_t i = 0; i < states.size(); ++i) {
        auto &st = *states[i];
        const auto &r = ranges[i];
        if (st.size == 0) continue;
        uint64_t off = 0;
        for (size_t k = 0; k < st.chunk_lens.size(); ++k) {
            const uint64_t chunk = st.chunk_lens[k];
            const uint32_t global_owner = st.chunk_owners[k];
            if (global_owner < agent_idx_start || global_owner >= agent_idx_end) {
                off += chunk;
                continue;
            }
            const size_t a = (size_t)(global_owner - agent_idx_start);
            nixl_reg_dlist_t dram_one(DRAM_SEG);
            nixl_reg_dlist_t obj_one(OBJ_SEG);
            nixlBlobDesc dram{};
            dram.addr = (uintptr_t)st.mmap_addr + off;
            dram.len = chunk;
            dram.devId = (uint32_t)i;
            dram_one.addDesc(dram);
            nixlBlobDesc object{};
            object.addr = off;
            object.len = chunk;
            object.devId = (uint32_t)i;
            object.metaInfo = r.key;
            obj_one.addDesc(object);
            nixl_xfer_dlist_t dram_xfer = dram_one.trim();
            nixl_xfer_dlist_t obj_xfer = obj_one.trim();
            nixlXferReqH *req = nullptr;
            nixl_opt_args_t xfer_hint;
            xfer_hint.backends.push_back(backends[a]);
            nixl_status_t ret = agents[a]->createXferReq(NIXL_READ, dram_xfer, obj_xfer,
                                                          streamerAgentName(a), req, &xfer_hint);
            if (ret != NIXL_SUCCESS || req == nullptr) {
                std::fprintf(stderr,
                             "criu-stream-fetch: createXferReq id=%u agent=%zu off=%lu len=%lu failed (status=%d)\n",
                             st.id, a, (unsigned long)off, (unsigned long)chunk, (int)ret);
                return false;
            }
            st.xfer_reqs[k] = req;
            off += chunk;
        }
    }
    return true;
}

bool attachNixlShmemOwned(std::vector<std::unique_ptr<nixlAgent>> &agents,
                          std::vector<nixlBackendH *> &backends,
                          const std::vector<ShmemRange> &ranges,
                          std::vector<std::unique_ptr<ShmemRangeState>> &states,
                          uint32_t agent_idx_start,
                          uint32_t agent_idx_end) {
    if (states.empty()) return true;
    const size_t M = agents.size();
    nixl_reg_dlist_t dram_all(DRAM_SEG);
    nixl_reg_dlist_t obj_all(OBJ_SEG);
    for (size_t i = 0; i < states.size(); ++i) {
        auto &st = *states[i];
        const auto &r = ranges[i];
        for (size_t k = 0; k < r.entries.size(); ++k) {
            const auto &e = r.entries[k];
            uint32_t dev = (uint32_t)((1u << 31) | (i << 16) | (k & 0xFFFF));
            nixlBlobDesc dram{};
            dram.addr = (uintptr_t)st.mmap_addr + e.vaddr;
            dram.len = e.len;
            dram.devId = 0;
            dram_all.addDesc(dram);
            nixlBlobDesc object{};
            object.addr = e.pages_img_offset;
            object.len = e.len;
            object.devId = dev;
            object.metaInfo = r.key;
            obj_all.addDesc(object);
        }
    }
    for (size_t a = 0; a < M; ++a) {
        nixl_opt_args_t backend_hint;
        backend_hint.backends.push_back(backends[a]);
        if (agents[a]->registerMem(dram_all, &backend_hint) != NIXL_SUCCESS) {
            std::fprintf(stderr,
                         "criu-stream-fetch: registerMem(shmem DRAM) agent=%zu failed\n", a);
            return false;
        }
        if (agents[a]->registerMem(obj_all, &backend_hint) != NIXL_SUCCESS) {
            std::fprintf(stderr,
                         "criu-stream-fetch: registerMem(shmem OBJ) agent=%zu failed\n", a);
            return false;
        }
    }
    for (size_t i = 0; i < states.size(); ++i) {
        auto &st = *states[i];
        const auto &r = ranges[i];
        size_t cumulative_k = 0;
        for (size_t k = 0; k < r.entries.size(); ++k) {
            const auto &e = r.entries[k];
            uint32_t dev = (uint32_t)((1u << 31) | (i << 16) | (k & 0xFFFF));
            uint64_t off = 0;
            while (off < e.len) {
                const uint64_t this_chunk = st.chunk_lens[cumulative_k];
                const uint32_t global_owner = st.chunk_owners[cumulative_k];
                if (global_owner >= agent_idx_start && global_owner < agent_idx_end) {
                    const size_t a = (size_t)(global_owner - agent_idx_start);
                    nixl_reg_dlist_t dram_one(DRAM_SEG);
                    nixl_reg_dlist_t obj_one(OBJ_SEG);
                    nixlBlobDesc dram{};
                    dram.addr = (uintptr_t)st.mmap_addr + e.vaddr + off;
                    dram.len = this_chunk;
                    dram.devId = 0;
                    dram_one.addDesc(dram);
                    nixlBlobDesc object{};
                    object.addr = e.pages_img_offset + off;
                    object.len = this_chunk;
                    object.devId = dev;
                    object.metaInfo = r.key;
                    obj_one.addDesc(object);
                    nixl_xfer_dlist_t dram_xfer = dram_one.trim();
                    nixl_xfer_dlist_t obj_xfer = obj_one.trim();
                    nixlXferReqH *req = nullptr;
                    nixl_opt_args_t xfer_hint;
                    xfer_hint.backends.push_back(backends[a]);
                    nixl_status_t ret = agents[a]->createXferReq(NIXL_READ, dram_xfer, obj_xfer,
                                                                  streamerAgentName(a), req, &xfer_hint);
                    if (ret != NIXL_SUCCESS || req == nullptr) {
                        std::fprintf(stderr,
                                     "criu-stream-fetch: shmem createXferReq shmid=0x%lx agent=%zu entry=%zu off=%lu len=%lu failed (status=%d)\n",
                                     (unsigned long)r.shmid, a, k,
                                     (unsigned long)off, (unsigned long)this_chunk, (int)ret);
                        return false;
                    }
                    st.xfer_reqs[cumulative_k] = req;
                }
                off += this_chunk;
                ++cumulative_k;
            }
        }
    }
    return true;
}

// downloadWorker is the body of each forked child. Posts owned chunks then
// polls until they are all terminal. On chunk completion, decrements the
// shared per-range counter; the worker that decrements to zero writes a
// DoneMsg into the parent pipe.
int downloadWorker(size_t my_idx,
                   size_t agents_per_proc,
                   const std::string &bucket,
                   const std::string &throughput,
                   const std::vector<RangeEntry> &priv_ranges,
                   const std::vector<ShmemRange> &shmem_ranges,
                   std::vector<std::unique_ptr<PrivateRangeState>> &priv_states,
                   std::vector<std::unique_ptr<ShmemRangeState>> &shmem_states,
                   SharedDoneState *shared,
                   int parent_pipe_write_fd,
                   int abort_fd) {
    const uint32_t agent_idx_start = (uint32_t)(my_idx * agents_per_proc);
    const uint32_t agent_idx_end = (uint32_t)(agent_idx_start + agents_per_proc);
    auto chunk_owned = [&](uint32_t global_owner) {
        return global_owner >= agent_idx_start && global_owner < agent_idx_end;
    };
    auto write_done_msg = [&](uint8_t type, uint32_t idx) {
        DoneMsg msg{type, idx};
        ssize_t w = ::write(parent_pipe_write_fd, &msg, sizeof(msg));
        (void)w;
    };

    std::vector<std::unique_ptr<nixlAgent>> agents;
    std::vector<nixlBackendH *> backends;
    if (!setupNixlObjAgents(agents_per_proc, bucket, throughput, agents, backends)) {
        shared->failed->store(1);
        write_done_msg(kDoneMsgAbort, 0);
        poisonAbort(abort_fd);
        return 1;
    }
    if (!attachNixlPrivateOwned(agents, backends, priv_ranges, priv_states,
                                agent_idx_start, agent_idx_end)) {
        shared->failed->store(1);
        write_done_msg(kDoneMsgAbort, 0);
        poisonAbort(abort_fd);
        return 1;
    }
    if (!attachNixlShmemOwned(agents, backends, shmem_ranges, shmem_states,
                              agent_idx_start, agent_idx_end)) {
        shared->failed->store(1);
        write_done_msg(kDoneMsgAbort, 0);
        poisonAbort(abort_fd);
        return 1;
    }

    std::vector<nixl_opt_args_t> post_hints(agents_per_proc);
    for (size_t a = 0; a < agents_per_proc; ++a)
        post_hints[a].backends.push_back(backends[a]);

    // Post in size-ascending order so small ranges complete early on this
    // worker, mirroring single-process behavior.
    std::vector<size_t> priv_order(priv_states.size());
    for (size_t i = 0; i < priv_states.size(); ++i) priv_order[i] = i;
    std::sort(priv_order.begin(), priv_order.end(),
              [&](size_t a, size_t b) {
                  return priv_states[a]->size < priv_states[b]->size;
              });

    auto handle_inline_success_priv = [&](size_t i, size_t k) {
        priv_states[i]->xfer_done[k] = true;
        uint32_t prev = shared->private_remaining[i].fetch_sub(1);
        if (prev == 1) write_done_msg(kDoneMsgPriv, (uint32_t)i);
    };
    auto handle_inline_success_shmem = [&](size_t i, size_t k) {
        shmem_states[i]->xfer_done[k] = true;
        uint32_t prev = shared->shmem_remaining[i].fetch_sub(1);
        if (prev == 1) write_done_msg(kDoneMsgShmem, (uint32_t)i);
    };

    for (size_t pi : priv_order) {
        auto &st = *priv_states[pi];
        st.post_t_ms = stream_ms_now();
        const size_t nchunks = st.xfer_reqs.size();
        if (nchunks == 0) continue;   // already marked done by parent
        for (size_t k = 0; k < nchunks; ++k) {
            if (!chunk_owned(st.chunk_owners[k])) continue;
            const size_t a = (size_t)(st.chunk_owners[k] - agent_idx_start);
            nixl_status_t s = agents[a]->postXferReq(st.xfer_reqs[k], &post_hints[a]);
            if (s < 0) {
                std::fprintf(stderr,
                             "criu-stream-fetch[worker=%zu]: postXferReq id=%u agent=%zu chunk=%zu/%zu failed (status=%d)\n",
                             my_idx, st.id, a, k, nchunks, (int)s);
                shared->failed->store(1);
                write_done_msg(kDoneMsgAbort, 0);
                poisonAbort(abort_fd);
                return 1;
            }
            if (s == NIXL_SUCCESS) handle_inline_success_priv(pi, k);
        }
    }
    for (size_t i = 0; i < shmem_states.size(); ++i) {
        auto &st = *shmem_states[i];
        st.post_t_ms = stream_ms_now();
        const size_t nchunks = st.xfer_reqs.size();
        for (size_t k = 0; k < nchunks; ++k) {
            if (!chunk_owned(st.chunk_owners[k])) continue;
            const size_t a = (size_t)(st.chunk_owners[k] - agent_idx_start);
            nixl_status_t s = agents[a]->postXferReq(st.xfer_reqs[k], &post_hints[a]);
            if (s < 0) {
                std::fprintf(stderr,
                             "criu-stream-fetch[worker=%zu]: shmem postXferReq shmid=0x%lx agent=%zu chunk=%zu/%zu failed (status=%d)\n",
                             my_idx, (unsigned long)st.shmid, a, k, nchunks, (int)s);
                shared->failed->store(1);
                write_done_msg(kDoneMsgAbort, 0);
                poisonAbort(abort_fd);
                return 1;
            }
            if (s == NIXL_SUCCESS) handle_inline_success_shmem(i, k);
        }
    }

    // Count remaining owned chunks for this worker.
    size_t local_remaining = 0;
    for (size_t i = 0; i < priv_states.size(); ++i) {
        auto &st = *priv_states[i];
        for (size_t k = 0; k < st.xfer_reqs.size(); ++k) {
            if (chunk_owned(st.chunk_owners[k]) && !st.xfer_done[k] && st.xfer_reqs[k])
                ++local_remaining;
        }
    }
    for (size_t i = 0; i < shmem_states.size(); ++i) {
        auto &st = *shmem_states[i];
        for (size_t k = 0; k < st.xfer_reqs.size(); ++k) {
            if (chunk_owned(st.chunk_owners[k]) && !st.xfer_done[k] && st.xfer_reqs[k])
                ++local_remaining;
        }
    }
    std::fprintf(stderr,
                 "[stream worker=%zu] post done; entering poll, local_remaining=%zu\n",
                 my_idx, local_remaining);

    auto last_tick = std::chrono::steady_clock::now();
    while (local_remaining > 0) {
        if (shared->failed->load() != 0) {
            std::fprintf(stderr, "[stream worker=%zu] peer reported failure; aborting\n",
                         my_idx);
            return 1;
        }
        size_t in_prog = 0;
        for (size_t i = 0; i < priv_states.size(); ++i) {
            auto &st = *priv_states[i];
            for (size_t k = 0; k < st.xfer_reqs.size(); ++k) {
                if (!chunk_owned(st.chunk_owners[k])) continue;
                if (st.xfer_done[k]) continue;
                if (st.xfer_reqs[k] == nullptr) continue;
                const size_t a = (size_t)(st.chunk_owners[k] - agent_idx_start);
                nixl_status_t s = agents[a]->getXferStatus(st.xfer_reqs[k]);
                if (s == NIXL_SUCCESS) {
                    st.xfer_done[k] = true;
                    --local_remaining;
                    long done_t = stream_ms_now();
                    long dur_ms = done_t - st.post_t_ms;
                    double mb = (double)st.chunk_lens[k] / (1024.0 * 1024.0);
                    double mb_per_s = (dur_ms > 0) ? (mb * 1000.0 / dur_ms) : 0.0;
                    double gbps = mb_per_s * 8.0 / 1024.0;
                    std::fprintf(stderr,
                                 "[stream t=%ldms worker=%zu] xfer id=%u chunk=%zu/%zu NIXL_SUCCESS size=%.1fMB dur=%ldms throughput=%.2fGbps\n",
                                 done_t, my_idx, st.id, k + 1, st.xfer_reqs.size(),
                                 mb, dur_ms, gbps);
                    uint32_t prev = shared->private_remaining[i].fetch_sub(1);
                    if (prev == 1) write_done_msg(kDoneMsgPriv, (uint32_t)i);
                } else if (s != NIXL_IN_PROG) {
                    std::fprintf(stderr,
                                 "criu-stream-fetch[worker=%zu]: xfer id=%u chunk=%zu/%zu failed (status=%d)\n",
                                 my_idx, st.id, k + 1, st.xfer_reqs.size(), (int)s);
                    shared->failed->store(1);
                    write_done_msg(kDoneMsgAbort, 0);
                    poisonAbort(abort_fd);
                    return 1;
                } else {
                    ++in_prog;
                }
            }
        }
        for (size_t i = 0; i < shmem_states.size(); ++i) {
            auto &st = *shmem_states[i];
            for (size_t k = 0; k < st.xfer_reqs.size(); ++k) {
                if (!chunk_owned(st.chunk_owners[k])) continue;
                if (st.xfer_done[k]) continue;
                if (st.xfer_reqs[k] == nullptr) continue;
                const size_t a = (size_t)(st.chunk_owners[k] - agent_idx_start);
                nixl_status_t s = agents[a]->getXferStatus(st.xfer_reqs[k]);
                if (s == NIXL_SUCCESS) {
                    st.xfer_done[k] = true;
                    --local_remaining;
                    uint32_t prev = shared->shmem_remaining[i].fetch_sub(1);
                    if (prev == 1) write_done_msg(kDoneMsgShmem, (uint32_t)i);
                } else if (s != NIXL_IN_PROG) {
                    std::fprintf(stderr,
                                 "criu-stream-fetch[worker=%zu]: shmem xfer shmid=0x%lx chunk=%zu/%zu failed (status=%d)\n",
                                 my_idx, (unsigned long)st.shmid,
                                 k + 1, st.xfer_reqs.size(), (int)s);
                    shared->failed->store(1);
                    write_done_msg(kDoneMsgAbort, 0);
                    poisonAbort(abort_fd);
                    return 1;
                } else {
                    ++in_prog;
                }
            }
        }
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::milliseconds>(now - last_tick).count() >= 500) {
            std::fprintf(stderr,
                         "[stream worker=%zu] poll local_remaining=%zu in_prog=%zu\n",
                         my_idx, local_remaining, in_prog);
            last_tick = now;
        }
        if (local_remaining > 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::fprintf(stderr, "[stream worker=%zu] all owned chunks done; exiting\n", my_idx);
    for (size_t i = 0; i < priv_states.size(); ++i) {
        auto &st = *priv_states[i];
        for (size_t k = 0; k < st.xfer_reqs.size(); ++k) {
            if (chunk_owned(st.chunk_owners[k]) && st.xfer_reqs[k]) {
                const size_t a = (size_t)(st.chunk_owners[k] - agent_idx_start);
                agents[a]->releaseXferReq(st.xfer_reqs[k]);
            }
        }
    }
    for (size_t i = 0; i < shmem_states.size(); ++i) {
        auto &st = *shmem_states[i];
        for (size_t k = 0; k < st.xfer_reqs.size(); ++k) {
            if (chunk_owned(st.chunk_owners[k]) && st.xfer_reqs[k]) {
                const size_t a = (size_t)(st.chunk_owners[k] - agent_idx_start);
                agents[a]->releaseXferReq(st.xfer_reqs[k]);
            }
        }
    }
    return 0;
}

// parentDispatcher reads DoneMsg records from the worker pipe and flips the
// corresponding state's done flag + signals its eventfd + cv. Exits when the
// pipe is closed.
void parentDispatcher(int pipe_read_fd,
                      std::vector<std::unique_ptr<PrivateRangeState>> &priv_states,
                      std::vector<std::unique_ptr<ShmemRangeState>> &shmem_states,
                      int abort_fd) {
    while (true) {
        DoneMsg msg{};
        ssize_t r = ::read(pipe_read_fd, &msg, sizeof(msg));
        if (r == 0) return;
        if (r < 0) {
            if (errno == EINTR) continue;
            std::fprintf(stderr, "criu-stream-fetch: dispatcher read: %s\n",
                         std::strerror(errno));
            return;
        }
        if (r != (ssize_t)sizeof(msg)) {
            std::fprintf(stderr,
                         "criu-stream-fetch: dispatcher short read %zd\n", r);
            return;
        }
        if (msg.type == kDoneMsgPriv) {
            if (msg.idx >= priv_states.size()) continue;
            auto &st = *priv_states[msg.idx];
            eventfdSignal(st.eventfd_fd);
            {
                std::lock_guard<std::mutex> g(st.done_mu);
                st.done = true;
                st.done_status = NIXL_SUCCESS;
            }
            st.done_cv.notify_all();
            std::fprintf(stderr, "[stream] dispatcher: private id=%u done\n", st.id);
        } else if (msg.type == kDoneMsgShmem) {
            if (msg.idx >= shmem_states.size()) continue;
            auto &st = *shmem_states[msg.idx];
            eventfdSignal(st.eventfd_fd);
            {
                std::lock_guard<std::mutex> g(st.done_mu);
                st.done = true;
            }
            st.done_cv.notify_all();
            std::fprintf(stderr,
                         "[stream] dispatcher: shmem shmid=0x%lx done\n",
                         (unsigned long)st.shmid);
        } else if (msg.type == kDoneMsgAbort) {
            std::fprintf(stderr, "[stream] dispatcher: ABORT from worker\n");
            poisonAbort(abort_fd);
            for (auto &up : priv_states) {
                std::lock_guard<std::mutex> g(up->done_mu);
                up->done = true;
                up->done_status = NIXL_ERR_BACKEND;
                up->done_cv.notify_all();
            }
            for (auto &up : shmem_states) {
                std::lock_guard<std::mutex> g(up->done_mu);
                up->done = true;
                up->failed = true;
                up->done_cv.notify_all();
            }
        }
    }
}

// runMultiProcessFlow is the top-level orchestrator for DOWNLOAD_PROCS > 1.
int runMultiProcessFlow(const Manifest &m,
                        const std::string &bucket,
                        const std::string &throughput,
                        int daemon_sock,
                        int shmem_sock,
                        int private_sock,
                        int abort_fd,
                        size_t n_procs,
                        size_t agents_per_proc) {
    const size_t total_agents = n_procs * agents_per_proc;
    std::vector<std::unique_ptr<PrivateRangeState>> priv_states;
    std::vector<std::unique_ptr<ShmemRangeState>> shmem_states;
    if (!allocPrivateMemfds(m.private_ranges, total_agents, priv_states)) {
        poisonAbort(abort_fd);
        return 1;
    }
    if (!allocShmemMemfds(m.shmem_ranges, total_agents, shmem_states)) {
        poisonAbort(abort_fd);
        return 1;
    }
    if (daemon_sock >= 0 && !shmem_states.empty()) {
        uint32_t n_evfd = (uint32_t)shmem_states.size();
        if (!writeExact(daemon_sock, &n_evfd, sizeof(n_evfd))) {
            poisonAbort(abort_fd);
            return 1;
        }
        std::vector<uint64_t> shmids;
        shmids.reserve(shmem_states.size());
        for (auto &st : shmem_states) shmids.push_back(st->shmid);
        if (!writeExact(daemon_sock, shmids.data(),
                        shmids.size() * sizeof(uint64_t))) {
            poisonAbort(abort_fd);
            return 1;
        }
        std::vector<int> fds;
        fds.reserve(1 + shmem_states.size());
        fds.push_back(abort_fd);
        for (auto &st : shmem_states) fds.push_back(st->eventfd_fd);
        if (!sendFds(daemon_sock, fds, nullptr, 0)) {
            poisonAbort(abort_fd);
            return 1;
        }
        std::fprintf(stderr,
                     "criu-stream-fetch: daemon handshake sent %u shmem range(s)\n",
                     n_evfd);
    }

    const size_t shared_bytes =
        sizeof(SharedDoneState)
        + sizeof(std::atomic<uint32_t>) * (priv_states.size() + shmem_states.size() + 1);
    void *shared_mem = ::mmap(nullptr, shared_bytes, PROT_READ | PROT_WRITE,
                              MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (shared_mem == MAP_FAILED) {
        std::fprintf(stderr, "criu-stream-fetch: shared mmap: %s\n",
                     std::strerror(errno));
        poisonAbort(abort_fd);
        return 1;
    }
    SharedDoneState *shared = (SharedDoneState *)shared_mem;
    shared->n_private = priv_states.size();
    shared->n_shmem = shmem_states.size();
    auto *cnt_ptr = (std::atomic<uint32_t> *)((char *)shared_mem
                                              + sizeof(SharedDoneState));
    shared->private_remaining = cnt_ptr;
    shared->shmem_remaining = cnt_ptr + priv_states.size();
    shared->failed = cnt_ptr + priv_states.size() + shmem_states.size();
    for (size_t i = 0; i < priv_states.size(); ++i) {
        new (&shared->private_remaining[i])
            std::atomic<uint32_t>((uint32_t)priv_states[i]->xfer_reqs.size());
    }
    for (size_t i = 0; i < shmem_states.size(); ++i) {
        new (&shared->shmem_remaining[i])
            std::atomic<uint32_t>((uint32_t)shmem_states[i]->xfer_reqs.size());
    }
    new (shared->failed) std::atomic<uint32_t>(0);

    // Empty / zero-size ranges: nothing to download. Mark done immediately so
    // the serve threads don't block CRIU on a range no worker will ever
    // signal.
    for (size_t i = 0; i < priv_states.size(); ++i) {
        if (priv_states[i]->xfer_reqs.empty()) {
            eventfdSignal(priv_states[i]->eventfd_fd);
            std::lock_guard<std::mutex> g(priv_states[i]->done_mu);
            priv_states[i]->done = true;
            priv_states[i]->done_status = NIXL_SUCCESS;
            priv_states[i]->done_cv.notify_all();
        }
    }
    for (size_t i = 0; i < shmem_states.size(); ++i) {
        if (shmem_states[i]->xfer_reqs.empty()) {
            eventfdSignal(shmem_states[i]->eventfd_fd);
            std::lock_guard<std::mutex> g(shmem_states[i]->done_mu);
            shmem_states[i]->done = true;
            shmem_states[i]->done_cv.notify_all();
        }
    }

    int pipefds[2];
    if (::pipe(pipefds) < 0) {
        std::fprintf(stderr, "criu-stream-fetch: pipe: %s\n", std::strerror(errno));
        poisonAbort(abort_fd);
        return 1;
    }

    std::fprintf(stderr,
                 "[stream] multi-process flow: n_procs=%zu agents_per_proc=%zu total_agents=%zu private=%zu shmem=%zu\n",
                 n_procs, agents_per_proc, total_agents,
                 priv_states.size(), shmem_states.size());

    std::vector<pid_t> children;
    children.reserve(n_procs);
    for (size_t p = 0; p < n_procs; ++p) {
        pid_t pid = ::fork();
        if (pid < 0) {
            std::fprintf(stderr, "criu-stream-fetch: fork: %s\n",
                         std::strerror(errno));
            poisonAbort(abort_fd);
            return 1;
        }
        if (pid == 0) {
            ::close(pipefds[0]);
            int rc = downloadWorker(p, agents_per_proc, bucket, throughput,
                                    m.private_ranges, m.shmem_ranges,
                                    priv_states, shmem_states, shared,
                                    pipefds[1], abort_fd);
            _exit(rc);
        }
        children.push_back(pid);
    }
    ::close(pipefds[1]);

    std::thread dispatcher_thr(parentDispatcher, pipefds[0],
                               std::ref(priv_states), std::ref(shmem_states),
                               abort_fd);
    std::thread priv_thread(servePrivateSocket, private_sock,
                            std::ref(priv_states), abort_fd);
    std::thread shmem_thread;
    if (shmem_sock >= 0 && !shmem_states.empty()) {
        shmem_thread = std::thread(serveShmemSocket, shmem_sock,
                                   std::ref(shmem_states), abort_fd);
    }

    bool any_failed = false;
    for (pid_t pid : children) {
        int status = 0;
        if (::waitpid(pid, &status, 0) < 0) {
            std::fprintf(stderr, "criu-stream-fetch: waitpid: %s\n",
                         std::strerror(errno));
            any_failed = true;
            continue;
        }
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            std::fprintf(stderr,
                         "criu-stream-fetch: worker pid=%d exited %s rc=%d\n",
                         pid,
                         WIFEXITED(status) ? "normally" : "abnormally",
                         WIFEXITED(status) ? WEXITSTATUS(status) : 0);
            any_failed = true;
        }
    }
    ::close(pipefds[0]);
    dispatcher_thr.join();
    priv_thread.join();
    if (shmem_thread.joinable()) shmem_thread.join();

    return any_failed ? 1 : 0;
}

}  // namespace

static int run_main(int argc, char **argv);

int main(int argc, char **argv) {
    try {
        return run_main(argc, argv);
    } catch (const std::exception &e) {
        std::fprintf(stderr, "criu-stream-fetch: uncaught exception: %s\n", e.what());
        return 99;
    } catch (...) {
        std::fprintf(stderr, "criu-stream-fetch: uncaught non-std exception\n");
        return 99;
    }
}

static int run_main(int argc, char **argv) {
    const char *manifest_path = nullptr;

    static option longopts[] = {
        {"manifest", required_argument, nullptr, 'm'},
        {nullptr, 0, nullptr, 0},
    };
    int opt;
    while ((opt = getopt_long(argc, argv, "m:", longopts, nullptr)) != -1) {
        switch (opt) {
        case 'm':
            manifest_path = optarg;
            break;
        default:
            std::fprintf(stderr, "Usage: %s --manifest <path>\n", argv[0]);
            return 2;
        }
    }
    if (manifest_path == nullptr) manifest_path = std::getenv("CRIU_STREAMER_MANIFEST");
    if (manifest_path == nullptr) {
        std::fprintf(stderr, "criu-stream-fetch: --manifest or CRIU_STREAMER_MANIFEST required\n");
        return 2;
    }

    Manifest m;
    if (!loadManifest(manifest_path, m)) return 1;
    std::fprintf(stderr,
                 "criu-stream-fetch: loaded manifest with %zu shmem + %zu private ranges\n",
                 m.shmem_ranges.size(), m.private_ranges.size());

    if (m.private_ranges.empty() && m.shmem_ranges.empty()) {
        std::fprintf(stderr,
                     "criu-stream-fetch: manifest is empty (no private or shmem ranges)\n");
        return 1;
    }

    // Resolve the S3 bucket. NIXL OBJ backend wants a single bucket; the
    // Go-side manifest writer already routes all pages-*.img through one
    // bucket, but enforce it here.
    std::string bucket;
    auto check_bucket = [&](const std::string &b, const char *what) -> bool {
        if (b.empty()) {
            std::fprintf(stderr,
                         "criu-stream-fetch: %s missing bucket; manifest needs s3:// source\n",
                         what);
            return false;
        }
        if (bucket.empty()) {
            bucket = b;
            return true;
        }
        if (bucket != b) {
            std::fprintf(stderr,
                         "criu-stream-fetch: mixed buckets (%s vs %s) not supported\n",
                         bucket.c_str(), b.c_str());
            return false;
        }
        return true;
    };
    for (const auto &r : m.private_ranges) {
        if (!check_bucket(r.bucket, "private range")) return 1;
    }
    for (const auto &r : m.shmem_ranges) {
        if (!check_bucket(r.bucket, "shmem range")) return 1;
    }

    int daemon_sock = fdFromEnv("CRIU_STREAMER_DAEMON_SOCK", false);
    int shmem_sock = fdFromEnv("CRIU_STREAMER_SHMEM_SOCK", false);
    int private_sock = fdFromEnv("CRIU_STREAMER_PRIVATE_SOCK", true);
    if (private_sock < 0) return 2;

    int abort_fd = eventfdCreate();
    if (abort_fd < 0) return 1;

    const char *throughput_env = std::getenv("S3_CRT_THROUGHPUT_GBPS");
    std::string throughput = (throughput_env && *throughput_env) ? throughput_env : "100";

    // Multi-process branch: DOWNLOAD_PROCS > 1 forks N download workers, each
    // with its own NIXL CRT client. Bypasses the per-process S3 throughput
    // plateau (~12-15 Gbps observed on us-east-2 p4d and eu-north L40S).
    const size_t n_procs = parseDownloadProcs();
    if (n_procs > 1) {
        const size_t agents_per_proc = parseNumAgents();
        return runMultiProcessFlow(m, bucket, throughput,
                                   daemon_sock, shmem_sock, private_sock,
                                   abort_fd, n_procs, agents_per_proc);
    }

    const size_t num_agents = parseNumAgents();
    std::vector<std::unique_ptr<nixlAgent>> agents;
    std::vector<nixlBackendH *> backends;
    if (!setupNixlObjAgents(num_agents, bucket, throughput, agents, backends)) {
        poisonAbort(abort_fd);
        return 1;
    }

    // Pre-build per-agent post hints once; passed into the post loops below
    // so we don't reconstruct the {backends[a]} vector per xfer.
    std::vector<nixl_opt_args_t> post_hints(num_agents);
    for (size_t a = 0; a < num_agents; ++a) {
        post_hints[a].backends.push_back(backends[a]);
    }

    std::vector<std::unique_ptr<PrivateRangeState>> states;
    if (!preparePrivateRanges(agents, backends, m.private_ranges, states)) {
        poisonAbort(abort_fd);
        return 1;
    }

    std::vector<std::unique_ptr<ShmemRangeState>> shmem_states;
    if (!prepareShmemRanges(agents, backends, m.shmem_ranges, shmem_states)) {
        poisonAbort(abort_fd);
        return 1;
    }

    // Daemon handshake (only when CRIU_STREAMER_DAEMON_SOCK is set, i.e.
    // when the agent spawned a lazy-pages daemon for a shmem-bearing
    // restore). Wire format:
    //     uint32 n_evfd
    //     uint64 shmid_0..shmid_{n-1}
    //     SCM_RIGHTS([abort_fd, ev_0..ev_{n-1}])
    if (daemon_sock >= 0 && !shmem_states.empty()) {
        uint32_t n_evfd = (uint32_t)shmem_states.size();
        if (!writeExact(daemon_sock, &n_evfd, sizeof(n_evfd))) {
            poisonAbort(abort_fd);
            return 1;
        }
        std::vector<uint64_t> shmids;
        shmids.reserve(shmem_states.size());
        for (auto &st : shmem_states) shmids.push_back(st->shmid);
        if (!writeExact(daemon_sock, shmids.data(), shmids.size() * sizeof(uint64_t))) {
            poisonAbort(abort_fd);
            return 1;
        }
        std::vector<int> fds;
        fds.reserve(1 + shmem_states.size());
        fds.push_back(abort_fd);
        for (auto &st : shmem_states) fds.push_back(st->eventfd_fd);
        if (!sendFds(daemon_sock, fds, nullptr, 0)) {
            poisonAbort(abort_fd);
            return 1;
        }
        std::fprintf(stderr,
                     "criu-stream-fetch: daemon handshake sent %u shmem range(s)\n",
                     n_evfd);
    }

    std::fprintf(stderr,
                 "criu-stream-fetch: posting %zu private + %zu shmem NIXL OBJ transfers (bucket=%s)\n",
                 states.size(), shmem_states.size(), bucket.c_str());

    // Submit ranges in size-ascending order so small ranges queue ahead of
    // any 30GB private mapping. AWS S3 CRT processes requests roughly FIFO;
    // without this, a 5MB range posted after a 30GB range with ~2000 chunks
    // can sit 18 s behind that backlog and block CRIU's per-id memfd request
    // on the streamer's done_cv. Iteration order is post-only; pollers and
    // serve loops index by id, so reordering here is safe.
    // [stream-order] log line emitted below to make the ordering observable.
    std::vector<PrivateRangeState *> post_order;
    post_order.reserve(states.size());
    for (auto &st : states) post_order.push_back(st.get());
    std::sort(post_order.begin(), post_order.end(),
              [](const PrivateRangeState *a, const PrivateRangeState *b) {
                  return a->size < b->size;
              });
    {
        std::string order_summary;
        for (auto *st : post_order) {
            char buf[64];
            std::snprintf(buf, sizeof(buf), " id=%u(%lu)", st->id,
                          (unsigned long)st->size);
            order_summary += buf;
        }
        std::fprintf(stderr,
                     "[stream-order] private post order (asc size):%s\n",
                     order_summary.c_str());
    }

    for (auto *st : post_order) {
        st->post_t_ms = stream_ms_now();
        const size_t nchunks = st->xfer_reqs.size();
        // Zero-size range: no chunks to post; mark done so servePrivateSocket
        // doesn't hang on done_cv (CRIU still asks for the memfd).
        if (nchunks == 0) {
            eventfdSignal(st->eventfd_fd);
            std::lock_guard<std::mutex> g(st->done_mu);
            st->done = true;
            st->done_status = NIXL_SUCCESS;
            signalPrivateReady_locked(st, true);
            continue;
        }
        for (size_t k = 0; k < nchunks; ++k) {
            const size_t a = st->chunk_owners[k];
            nixl_status_t s = agents[a]->postXferReq(st->xfer_reqs[k], &post_hints[a]);
            if (s < 0) {
                std::fprintf(stderr,
                             "criu-stream-fetch: postXferReq id=%u agent=%zu chunk=%zu/%zu failed (status=%d)\n",
                             st->id, a, k, nchunks, (int)s);
                poisonAbort(abort_fd);
                return 1;
            }
            if (s == NIXL_SUCCESS) {
                st->xfer_done[k] = true;
                unsigned left = --st->chunks_remaining;
                if (left == 0) {
                    eventfdSignal(st->eventfd_fd);
                    std::lock_guard<std::mutex> g(st->done_mu);
                    st->done = true;
                    st->done_status = NIXL_SUCCESS;
                    signalPrivateReady_locked(st, true);
                    st->done_cv.notify_all();
                }
            }
        }
        std::fprintf(stderr,
                     "[stream t=%ldms] postXfer id=%u size=%lu chunks=%zu (split across %zu agents)\n",
                     st->post_t_ms, st->id, (unsigned long)st->size, nchunks, num_agents);
    }
    // Shmem: post every per-chunk xfer (each pagemap entry is now split
    // into CHUNK_BYTESIZE chunks). Inline NIXL_SUCCESS is unusual at these
    // sizes but handle it the same way pollShmemXfers does: drop
    // entries_remaining and fire eventfd when the range completes.
    for (auto &st : shmem_states) {
        st->post_t_ms = stream_ms_now();
        const size_t nchunks = st->xfer_reqs.size();
        for (size_t k = 0; k < nchunks; ++k) {
            const size_t a = st->chunk_owners[k];
            nixl_status_t s = agents[a]->postXferReq(st->xfer_reqs[k], &post_hints[a]);
            if (s < 0) {
                std::fprintf(stderr,
                             "criu-stream-fetch: shmem postXferReq shmid=0x%lx agent=%zu chunk=%zu/%zu failed (status=%d)\n",
                             (unsigned long)st->shmid, a, k, nchunks, (int)s);
                poisonAbort(abort_fd);
                return 1;
            }
            if (s == NIXL_SUCCESS) {
                st->xfer_done[k] = true;
                unsigned left = --st->entries_remaining;
                if (left == 0) {
                    eventfdSignal(st->eventfd_fd);
                    {
                        std::lock_guard<std::mutex> g(st->done_mu);
                        st->done = true;
                    }
                    st->done_cv.notify_all();
                }
            }
        }
        std::fprintf(stderr,
                     "[stream t=%ldms] shmem postXfer shmid=0x%lx size=%lu chunks=%zu (split across %zu agents)\n",
                     st->post_t_ms, (unsigned long)st->shmid,
                     (unsigned long)st->size, nchunks, num_agents);
    }

    std::thread priv_thread(servePrivateSocket, private_sock,
                            std::ref(states), abort_fd);
    std::thread shmem_serve_thread;
    if (shmem_sock >= 0 && !shmem_states.empty()) {
        shmem_serve_thread = std::thread(serveShmemSocket, shmem_sock,
                                         std::ref(shmem_states), abort_fd);
    }
    std::thread shmem_poll_thread;
    if (!shmem_states.empty()) {
        shmem_poll_thread = std::thread(pollShmemXfers, std::ref(agents),
                                        std::ref(shmem_states), abort_fd);
    }
    pollPrivateXfers(agents, states, abort_fd);
    if (shmem_poll_thread.joinable()) shmem_poll_thread.join();
    priv_thread.join();
    if (shmem_serve_thread.joinable()) shmem_serve_thread.join();

    // Teardown. Each chunk's xferReq is owned by agents[chunk_owners[k]]; the
    // full-range register dlists were replicated across every agent in
    // preparePrivateRanges / prepareShmemRanges, so deregisterMem mirrors
    // that — same dlist passed to every agent.
    for (auto &st : states) {
        for (size_t k = 0; k < st->xfer_reqs.size(); ++k) {
            if (st->xfer_reqs[k])
                agents[st->chunk_owners[k]]->releaseXferReq(st->xfer_reqs[k]);
        }
    }
    for (auto &st : shmem_states) {
        for (size_t k = 0; k < st->xfer_reqs.size(); ++k) {
            if (st->xfer_reqs[k])
                agents[st->chunk_owners[k]]->releaseXferReq(st->xfer_reqs[k]);
        }
    }
    if (!m.private_ranges.empty()) {
        nixl_reg_dlist_t dram_all(DRAM_SEG);
        nixl_reg_dlist_t obj_all(OBJ_SEG);
        for (size_t i = 0; i < states.size(); ++i) {
            nixlBlobDesc d{};
            d.addr = (uintptr_t)states[i]->mmap_addr;
            d.len = states[i]->size;
            d.devId = (uint32_t)i;
            dram_all.addDesc(d);

            nixlBlobDesc o{};
            o.addr = 0;
            o.len = states[i]->size;
            o.devId = (uint32_t)i;
            o.metaInfo = m.private_ranges[i].key;
            obj_all.addDesc(o);
        }
        for (size_t a = 0; a < num_agents; ++a) {
            nixl_opt_args_t hint;
            hint.backends.push_back(backends[a]);
            agents[a]->deregisterMem(dram_all, &hint);
            agents[a]->deregisterMem(obj_all, &hint);
        }
    }
    if (!m.shmem_ranges.empty()) {
        nixl_reg_dlist_t dram_all(DRAM_SEG);
        nixl_reg_dlist_t obj_all(OBJ_SEG);
        for (size_t i = 0; i < shmem_states.size(); ++i) {
            const auto &r = m.shmem_ranges[i];
            for (size_t k = 0; k < r.entries.size(); ++k) {
                const auto &e = r.entries[k];
                uint32_t dev = (uint32_t)((1u << 31) | (i << 16) | (k & 0xFFFF));

                nixlBlobDesc d{};
                d.addr = (uintptr_t)shmem_states[i]->mmap_addr + e.vaddr;
                d.len = e.len;
                d.devId = 0;
                dram_all.addDesc(d);

                nixlBlobDesc o{};
                o.addr = e.pages_img_offset;
                o.len = e.len;
                o.devId = dev;
                o.metaInfo = r.key;
                obj_all.addDesc(o);
            }
        }
        for (size_t a = 0; a < num_agents; ++a) {
            nixl_opt_args_t hint;
            hint.backends.push_back(backends[a]);
            agents[a]->deregisterMem(dram_all, &hint);
            agents[a]->deregisterMem(obj_all, &hint);
        }
    }
    for (auto &st : states) {
        if (st->mmap_addr && st->size) ::munmap(st->mmap_addr, st->size);
        if (st->memfd >= 0) ::close(st->memfd);
        if (st->eventfd_fd >= 0) ::close(st->eventfd_fd);
    }
    for (auto &st : shmem_states) {
        if (st->mmap_addr && st->size) ::munmap(st->mmap_addr, st->size);
        if (st->memfd >= 0) ::close(st->memfd);
        if (st->eventfd_fd >= 0) ::close(st->eventfd_fd);
    }
    if (abort_fd >= 0) ::close(abort_fd);

    return 0;
}
