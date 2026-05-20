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
#include <unistd.h>

#include "nixl.h"
#include "nixl_descriptors.h"

namespace {

constexpr const char *kStreamerAgentName = "CriuStreamFetch";

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
    // Range may be split into N parallel S3 chunks of CHUNK_BYTESIZE each
    // (see parseChunkBytesize). xfer_reqs[k] / xfer_done[k] track chunk k;
    // chunks_remaining counts down to fire the per-range eventfd exactly
    // once when every chunk has filled its memfd slice.
    std::vector<nixlXferReqH *> xfer_reqs;
    std::vector<bool> xfer_done;
    std::vector<uint32_t> chunk_lens;   // per-chunk byte count, parallel to xfer_reqs
    std::atomic<unsigned> chunks_remaining{0};
    long post_t_ms = 0;
    std::mutex done_mu;
    std::condition_variable done_cv;
    bool done = false;
    nixl_status_t done_status = NIXL_SUCCESS;
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
    // Each pagemap entry may be split into multiple chunks of CHUNK_BYTESIZE.
    // xfer_reqs[k] / xfer_done[k] / chunk_lens[k] track chunk k (across all
    // entries of this range); entries_remaining counts chunks remaining.
    std::vector<nixlXferReqH *> xfer_reqs;
    std::vector<bool> xfer_done;
    std::vector<uint32_t> chunk_lens;
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

// setupNixlObjAgent constructs the streamer's NIXL agent, instantiates the
// OBJ backend for `bucket`, and returns both via out parameters. throughput
// is the cluster's S3_CRT_THROUGHPUT_GBPS (per design doc table, 100 is the
// 28.98 Gbps sweet spot on us-east-2 A100 boxes).
bool setupNixlObjAgent(const std::string &bucket,
                       const std::string &throughput,
                       std::unique_ptr<nixlAgent> &agent_out,
                       nixlBackendH *&backend_out) {
    nixlAgentConfig cfg;
    cfg.useProgThread = true;
    auto agent = std::make_unique<nixlAgent>(kStreamerAgentName, cfg);

    nixl_b_params_t params{
        {"bucket", bucket},
        {"crtThroughputGbps", throughput},
        // crtMinLimit gates which engine impl gets used in obj_backend.cpp:50.
        // Without it, createObjEngineImpl picks DefaultObjEngineImpl → Standard
        // S3 client only, which ignores crtThroughputGbps and fails the
        // 13-conn parallel get with NIXL_ERR_BACKEND. crtMinLimit=1 selects
        // S3CrtObjEngineImpl in CRT-only mode (drops the standard client).
        {"crtMinLimit", "1"},
    };
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

    std::fprintf(stderr,
                 "[stream] NIXL OBJ backend config: bucket=%s crtThroughputGbps=%s crtMinLimit=%s region=%s\n",
                 bucket.c_str(),
                 throughput.c_str(),
                 params.count("crtMinLimit") ? params["crtMinLimit"].c_str() : "(unset)",
                 params.count("region") ? params["region"].c_str() : "(unset)");

    nixlBackendH *backend = nullptr;
    nixl_status_t ret = agent->createBackend("OBJ", params, backend);
    if (ret != NIXL_SUCCESS || backend == nullptr) {
        std::fprintf(stderr,
                     "criu-stream-fetch: createBackend(\"OBJ\", bucket=%s) failed (status=%d)\n",
                     bucket.c_str(), (int)ret);
        return false;
    }
    agent_out = std::move(agent);
    backend_out = backend;
    return true;
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
// the local DRAM and S3 object descriptors with NIXL, and creates one
// transfer request per range. Returns false on any failure; on success the
// caller can postXferReq each state's req in parallel.
bool preparePrivateRanges(nixlAgent &agent,
                          nixlBackendH *backend,
                          const std::vector<RangeEntry> &ranges,
                          std::vector<std::unique_ptr<PrivateRangeState>> &states) {
    if (ranges.empty()) return true;

    nixl_opt_args_t backend_hint;
    backend_hint.backends.push_back(backend);

    nixl_reg_dlist_t dram_for_obj(DRAM_SEG);
    nixl_reg_dlist_t obj_for_obj(OBJ_SEG);

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
        dram_for_obj.addDesc(dram);

        nixlBlobDesc object{};
        object.addr = 0;
        object.len = r.size;
        object.devId = (uint32_t)i;
        object.metaInfo = r.key;
        obj_for_obj.addDesc(object);

        states.push_back(std::move(st));
    }

    if (agent.registerMem(dram_for_obj, &backend_hint) != NIXL_SUCCESS) {
        std::fprintf(stderr, "criu-stream-fetch: registerMem(DRAM) failed\n");
        return false;
    }
    if (agent.registerMem(obj_for_obj, &backend_hint) != NIXL_SUCCESS) {
        std::fprintf(stderr, "criu-stream-fetch: registerMem(OBJ) failed\n");
        return false;
    }

    // Split each range into chunks of CHUNK bytes for parallel S3
    // GetObjectAsync. NIXL OBJ devIdToObjKey_ keys on devId only, so every
    // chunk for range i reuses the same devId=i → same key lookup. Chunks
    // address sub-ranges of the same registered DRAM/OBJ descriptors via
    // addr/len in the per-chunk createXferReq descs.
    const size_t CHUNK = parseChunkBytesize();
    for (size_t i = 0; i < states.size(); ++i) {
        auto &st = *states[i];
        const uint64_t total = st.size;
        uint64_t off = 0;
        if (total == 0) {
            st.chunks_remaining = 0;
            continue;
        }
        while (off < total) {
            const uint64_t chunk = std::min<uint64_t>(CHUNK, total - off);
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
            xfer_hint.backends.push_back(backend);
            nixl_status_t ret = agent.createXferReq(NIXL_READ, dram_xfer, obj_xfer,
                                                    kStreamerAgentName, req, &xfer_hint);
            if (ret != NIXL_SUCCESS || req == nullptr) {
                std::fprintf(stderr,
                             "criu-stream-fetch: createXferReq id=%u off=%lu len=%lu failed (status=%d)\n",
                             st.id, (unsigned long)off, (unsigned long)chunk, (int)ret);
                return false;
            }
            st.xfer_reqs.push_back(req);
            st.chunk_lens.push_back((uint32_t)chunk);
            off += chunk;
        }
        st.xfer_done.assign(st.xfer_reqs.size(), false);
        st.chunks_remaining = (unsigned)st.xfer_reqs.size();
    }
    return true;
}

// prepareShmemRanges allocates per-shmem-range memfd + mmap + eventfd, and
// builds one NIXL OBJ READ per pagemap entry. registerMem is called with
// a wide descriptor-list collected across every entry, so the (addr, len,
// devId) tuple of each createXferReq matches a registered descriptor 1:1
// (mirroring the private path's pattern, which we know NIXL OBJ accepts).
bool prepareShmemRanges(nixlAgent &agent,
                        nixlBackendH *backend,
                        const std::vector<ShmemRange> &ranges,
                        std::vector<std::unique_ptr<ShmemRangeState>> &states) {
    if (ranges.empty()) return true;

    nixl_opt_args_t backend_hint;
    backend_hint.backends.push_back(backend);

    nixl_reg_dlist_t dram_for_obj(DRAM_SEG);
    nixl_reg_dlist_t obj_for_obj(OBJ_SEG);

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
            // so shmem entries cannot collide with priv devIds 0..N-1.
            uint32_t dev = (uint32_t)((1u << 31) | (i << 16) | (k & 0xFFFF));

            nixlBlobDesc dram{};
            dram.addr = (uintptr_t)st->mmap_addr + e.vaddr;
            dram.len = e.len;
            dram.devId = 0;
            dram_for_obj.addDesc(dram);

            nixlBlobDesc object{};
            object.addr = e.pages_img_offset;
            object.len = e.len;
            object.devId = dev;
            object.metaInfo = r.key;
            obj_for_obj.addDesc(object);
        }

        states.push_back(std::move(st));
    }

    if (agent.registerMem(dram_for_obj, &backend_hint) != NIXL_SUCCESS) {
        std::fprintf(stderr, "criu-stream-fetch: registerMem(shmem DRAM) failed\n");
        return false;
    }
    if (agent.registerMem(obj_for_obj, &backend_hint) != NIXL_SUCCESS) {
        std::fprintf(stderr, "criu-stream-fetch: registerMem(shmem OBJ) failed\n");
        return false;
    }

    // Per-entry chunking: each pagemap entry (potentially hundreds of MB on
    // 14B+ models) is split into CHUNK-sized sub-ranges so a single large
    // entry doesn't serialize on the per-stream CRT ~9 Gbps ceiling. Every
    // chunk of entry k reuses the same per-entry devId, so the Phase-1
    // devIdToObjKey_ mapping covers them all via NIXL `covers()`.
    const size_t CHUNK = parseChunkBytesize();
    for (size_t i = 0; i < states.size(); ++i) {
        auto &st = *states[i];
        const auto &r = ranges[i];
        for (size_t k = 0; k < r.entries.size(); ++k) {
            const auto &e = r.entries[k];
            uint32_t dev = (uint32_t)((1u << 31) | (i << 16) | (k & 0xFFFF));
            uint64_t off = 0;
            while (off < e.len) {
                const uint64_t chunk = std::min<uint64_t>(CHUNK, e.len - off);
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
                xfer_hint.backends.push_back(backend);
                nixl_status_t ret = agent.createXferReq(NIXL_READ, dram_xfer, obj_xfer,
                                                        kStreamerAgentName, req, &xfer_hint);
                if (ret != NIXL_SUCCESS || req == nullptr) {
                    std::fprintf(stderr,
                                 "criu-stream-fetch: shmem createXferReq shmid=0x%lx "
                                 "entry=%zu off=%lu len=%lu failed (status=%d)\n",
                                 (unsigned long)r.shmid, k,
                                 (unsigned long)off, (unsigned long)chunk, (int)ret);
                    return false;
                }
                st.xfer_reqs.push_back(req);
                st.chunk_lens.push_back((uint32_t)chunk);
                off += chunk;
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
void pollShmemXfers(nixlAgent &agent,
                    std::vector<std::unique_ptr<ShmemRangeState>> &states,
                    int abort_fd) {
    if (states.empty()) return;
    size_t total = 0;
    for (auto &st : states) total += st->xfer_reqs.size();
    std::fprintf(stderr,
                 "[stream] pollShmemXfers entered ranges=%zu chunks=%zu\n",
                 states.size(), total);

    size_t completed = 0;
    auto last_tick = std::chrono::steady_clock::now();
    while (completed < total) {
        size_t in_prog = 0;
        size_t failed = 0;
        for (auto &st : states) {
            for (size_t k = 0; k < st->xfer_reqs.size(); ++k) {
                if (st->xfer_done[k]) continue;
                nixl_status_t s = agent.getXferStatus(st->xfer_reqs[k]);
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
void pollPrivateXfers(nixlAgent &agent,
                      std::vector<std::unique_ptr<PrivateRangeState>> &states,
                      int abort_fd) {
    if (states.empty()) return;
    size_t total_chunks = 0;
    for (auto &st : states) total_chunks += st->xfer_reqs.size();
    std::fprintf(stderr,
                 "[stream] pollPrivateXfers entered states=%zu chunks=%zu\n",
                 states.size(), total_chunks);
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
                nixl_status_t s = agent.getXferStatus(st->xfer_reqs[k]);
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
// the matching PrivateRangeState, replies with SCM_RIGHTS(memfd), waits for
// the state's NIXL fill to complete, then writes a 1-byte 'A' ack so PIE's
// io_submit only runs against a fully populated memfd. The loop exits when
// CRIU closes the socket.
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
                     "[stream t=%ldms] CRIU asked for id=%u; sending memfd\n",
                     t_req_ms, id);

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
                     "[stream t=%ldms] ack id=%u (wait=%ldms after req)\n",
                     stream_ms_now(), id, stream_ms_now() - t_req_ms);

        // DEBUG: dump first 16 bytes of memfd content to verify NIXL filled it.
        if (st->mmap_addr && st->size >= 16) {
            const unsigned char *p = (const unsigned char *)st->mmap_addr;
            uint64_t tail_off = (st->size >= 16) ? (st->size - 16) : 0;
            const unsigned char *t = (const unsigned char *)st->mmap_addr + tail_off;
            std::fprintf(stderr,
                         "[stream-dbg] priv id=%u size=%lu head=%02x%02x%02x%02x%02x%02x%02x%02x"
                         " tail@%lu=%02x%02x%02x%02x%02x%02x%02x%02x\n",
                         st->id, (unsigned long)st->size,
                         p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7],
                         (unsigned long)tail_off,
                         t[0], t[1], t[2], t[3], t[4], t[5], t[6], t[7]);
        }

        const char ack = 'A';
        if (!writeExact(sock, &ack, 1)) {
            poisonAbort(abort_fd);
            return;
        }
        by_id.erase(it);
    }
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

    std::unique_ptr<nixlAgent> agent;
    nixlBackendH *backend = nullptr;
    if (!setupNixlObjAgent(bucket, throughput, agent, backend)) {
        poisonAbort(abort_fd);
        return 1;
    }

    std::vector<std::unique_ptr<PrivateRangeState>> states;
    if (!preparePrivateRanges(*agent, backend, m.private_ranges, states)) {
        poisonAbort(abort_fd);
        return 1;
    }

    std::vector<std::unique_ptr<ShmemRangeState>> shmem_states;
    if (!prepareShmemRanges(*agent, backend, m.shmem_ranges, shmem_states)) {
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

    nixl_opt_args_t post_hint;
    post_hint.backends.push_back(backend);
    for (auto &st : states) {
        st->post_t_ms = stream_ms_now();
        const size_t nchunks = st->xfer_reqs.size();
        // Zero-size range: no chunks to post; mark done so servePrivateSocket
        // doesn't hang on done_cv (CRIU still asks for the memfd).
        if (nchunks == 0) {
            eventfdSignal(st->eventfd_fd);
            std::lock_guard<std::mutex> g(st->done_mu);
            st->done = true;
            st->done_status = NIXL_SUCCESS;
            continue;
        }
        for (size_t k = 0; k < nchunks; ++k) {
            nixl_status_t s = agent->postXferReq(st->xfer_reqs[k], &post_hint);
            if (s < 0) {
                std::fprintf(stderr,
                             "criu-stream-fetch: postXferReq id=%u chunk=%zu/%zu failed (status=%d)\n",
                             st->id, k, nchunks, (int)s);
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
                    st->done_cv.notify_all();
                }
            }
        }
        std::fprintf(stderr,
                     "[stream t=%ldms] postXfer id=%u size=%lu chunks=%zu\n",
                     st->post_t_ms, st->id, (unsigned long)st->size, nchunks);
    }
    // Shmem: post every per-chunk xfer (each pagemap entry is now split
    // into CHUNK_BYTESIZE chunks). Inline NIXL_SUCCESS is unusual at these
    // sizes but handle it the same way pollShmemXfers does: drop
    // entries_remaining and fire eventfd when the range completes.
    for (auto &st : shmem_states) {
        st->post_t_ms = stream_ms_now();
        const size_t nchunks = st->xfer_reqs.size();
        for (size_t k = 0; k < nchunks; ++k) {
            nixl_status_t s = agent->postXferReq(st->xfer_reqs[k], &post_hint);
            if (s < 0) {
                std::fprintf(stderr,
                             "criu-stream-fetch: shmem postXferReq shmid=0x%lx chunk=%zu/%zu failed (status=%d)\n",
                             (unsigned long)st->shmid, k, nchunks, (int)s);
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
                     "[stream t=%ldms] shmem postXfer shmid=0x%lx size=%lu chunks=%zu\n",
                     st->post_t_ms, (unsigned long)st->shmid,
                     (unsigned long)st->size, nchunks);
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
        shmem_poll_thread = std::thread(pollShmemXfers, std::ref(*agent),
                                        std::ref(shmem_states), abort_fd);
    }
    pollPrivateXfers(*agent, states, abort_fd);
    if (shmem_poll_thread.joinable()) shmem_poll_thread.join();
    priv_thread.join();
    if (shmem_serve_thread.joinable()) shmem_serve_thread.join();

    // Teardown.
    for (auto &st : states) {
        for (auto *r : st->xfer_reqs)
            if (r) agent->releaseXferReq(r);
    }
    for (auto &st : shmem_states) {
        for (auto *r : st->xfer_reqs)
            if (r) agent->releaseXferReq(r);
    }
    if (!m.private_ranges.empty()) {
        nixl_reg_dlist_t dram(DRAM_SEG);
        nixl_reg_dlist_t obj(OBJ_SEG);
        for (size_t i = 0; i < states.size(); ++i) {
            nixlBlobDesc d{};
            d.addr = (uintptr_t)states[i]->mmap_addr;
            d.len = states[i]->size;
            d.devId = 0;
            dram.addDesc(d);

            nixlBlobDesc o{};
            o.addr = 0;
            o.len = states[i]->size;
            o.devId = (uint32_t)i;
            o.metaInfo = m.private_ranges[i].key;
            obj.addDesc(o);
        }
        nixl_opt_args_t hint;
        hint.backends.push_back(backend);
        agent->deregisterMem(dram, &hint);
        agent->deregisterMem(obj, &hint);
    }
    if (!m.shmem_ranges.empty()) {
        nixl_reg_dlist_t dram(DRAM_SEG);
        nixl_reg_dlist_t obj(OBJ_SEG);
        for (size_t i = 0; i < shmem_states.size(); ++i) {
            const auto &r = m.shmem_ranges[i];
            for (size_t k = 0; k < r.entries.size(); ++k) {
                const auto &e = r.entries[k];
                // NIXL OBJ backend keys devIdToObjKey_ map on devId only (see
            // nixl/src/plugins/obj/s3/engine_impl.cpp). Reserve devId>=1<<31
            // so shmem entries cannot collide with priv devIds 0..N-1.
            uint32_t dev = (uint32_t)((1u << 31) | (i << 16) | (k & 0xFFFF));

                nixlBlobDesc d{};
                d.addr = (uintptr_t)shmem_states[i]->mmap_addr + e.vaddr;
                d.len = e.len;
                d.devId = 0;
                dram.addDesc(d);

                nixlBlobDesc o{};
                o.addr = e.pages_img_offset;
                o.len = e.len;
                o.devId = dev;
                o.metaInfo = r.key;
                obj.addDesc(o);
            }
        }
        nixl_opt_args_t hint;
        hint.backends.push_back(backend);
        agent->deregisterMem(dram, &hint);
        agent->deregisterMem(obj, &hint);
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
