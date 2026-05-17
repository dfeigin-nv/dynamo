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

struct RangeEntry {
    uint32_t id;
    uint64_t size;
    std::string source;  // s3://bucket/key when sourced from S3, else local path
    std::string bucket;  // populated for s3:// sources
    std::string key;     // S3 object key under bucket
};

struct Manifest {
    std::vector<RangeEntry> shmem_ranges;
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
    nixlXferReqH *xfer_req = nullptr;
    std::mutex done_mu;
    std::condition_variable done_cv;
    bool done = false;
    nixl_status_t done_status = NIXL_SUCCESS;
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
    if (!parseRanges(shmem, m.shmem_ranges) || !parseRanges(priv, m.private_ranges)) {
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
        {"use_virtual_addressing", "false"},
    };

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
        dram.devId = 0;
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

    // One transfer request per memfd so getXferStatus gives per-range
    // completion granularity for the eventfd signal.
    for (size_t i = 0; i < states.size(); ++i) {
        nixl_reg_dlist_t dram_one(DRAM_SEG);
        nixl_reg_dlist_t obj_one(OBJ_SEG);

        nixlBlobDesc dram{};
        dram.addr = (uintptr_t)states[i]->mmap_addr;
        dram.len = states[i]->size;
        dram.devId = 0;
        dram_one.addDesc(dram);

        nixlBlobDesc object{};
        object.addr = 0;
        object.len = states[i]->size;
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
                         "criu-stream-fetch: createXferReq id=%u failed (status=%d)\n",
                         states[i]->id, (int)ret);
            return false;
        }
        states[i]->xfer_req = req;
    }
    return true;
}

// pollPrivateXfers spins on getXferStatus for every state with an in-flight
// req. When NIXL_SUCCESS for state i, signal eventfd[i] and wake any waiter
// on done_cv. Returns when all states are complete (success or error).
void pollPrivateXfers(nixlAgent &agent,
                      std::vector<std::unique_ptr<PrivateRangeState>> &states,
                      int abort_fd) {
    size_t completed = 0;
    while (completed < states.size()) {
        completed = 0;
        for (auto &st : states) {
            {
                std::lock_guard<std::mutex> g(st->done_mu);
                if (st->done) {
                    ++completed;
                    continue;
                }
            }
            nixl_status_t s = agent.getXferStatus(st->xfer_req);
            if (s == NIXL_SUCCESS) {
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
                ++completed;
            } else if (s != NIXL_IN_PROG) {
                std::fprintf(stderr,
                             "criu-stream-fetch: xfer id=%u failed (status=%d)\n",
                             st->id, (int)s);
                {
                    std::lock_guard<std::mutex> g(st->done_mu);
                    st->done = true;
                    st->done_status = s;
                }
                st->done_cv.notify_all();
                poisonAbort(abort_fd);
                ++completed;
            }
        }
        if (completed < states.size())
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
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
        auto it = by_id.find(id);
        if (it == by_id.end()) {
            std::fprintf(stderr,
                         "criu-stream-fetch: CRIU asked for unknown pages_img_id=%u\n", id);
            poisonAbort(abort_fd);
            return;
        }
        PrivateRangeState *st = it->second;

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

        const char ack = 'A';
        if (!writeExact(sock, &ack, 1)) {
            poisonAbort(abort_fd);
            return;
        }
        by_id.erase(it);
    }
}

}  // namespace

int main(int argc, char **argv) {
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

    if (m.private_ranges.empty()) {
        std::fprintf(stderr,
                     "criu-stream-fetch: no private ranges; shmem-only path not implemented yet\n");
        return 1;
    }
    const std::string bucket = m.private_ranges[0].bucket;
    if (bucket.empty()) {
        std::fprintf(stderr,
                     "criu-stream-fetch: private_ranges[0].bucket is empty; "
                     "manifest must carry an s3:// source\n");
        return 1;
    }
    for (const auto &r : m.private_ranges) {
        if (r.bucket != bucket) {
            std::fprintf(stderr,
                         "criu-stream-fetch: mixed buckets in private_ranges "
                         "(%s vs %s) not supported\n",
                         bucket.c_str(), r.bucket.c_str());
            return 1;
        }
    }

    int daemon_sock = fdFromEnv("CRIU_STREAMER_DAEMON_SOCK", false);
    int private_sock = fdFromEnv("CRIU_STREAMER_PRIVATE_SOCK", true);
    if (private_sock < 0) return 2;

    int abort_fd = eventfdCreate();
    if (abort_fd < 0) return 1;

    // Per-shmem-range eventfds: the daemon's epoll loop reacts to them with
    // UFFDIO_CONTINUE per design doc. Stage 2c's daemon is the consumer; we
    // hand them over here even if the NIXL shmem fill lands in a later stage.
    std::vector<int> shmem_memfds(m.shmem_ranges.size(), -1);
    std::vector<int> shmem_evfds(m.shmem_ranges.size(), -1);
    for (size_t i = 0; i < m.shmem_ranges.size(); ++i) {
        const auto &r = m.shmem_ranges[i];
        char name[64];
        std::snprintf(name, sizeof(name), "criu-shmem-%u", r.id);
        int mfd = memfdCreate(name, r.size);
        if (mfd < 0) { poisonAbort(abort_fd); return 1; }
        int evfd = eventfdCreate();
        if (evfd < 0) { ::close(mfd); poisonAbort(abort_fd); return 1; }
        shmem_memfds[i] = mfd;
        shmem_evfds[i] = evfd;
    }
    if (daemon_sock >= 0 && !shmem_evfds.empty()) {
        uint32_t n_evfd = (uint32_t)shmem_evfds.size();
        std::vector<int> fds;
        fds.reserve(1 + shmem_evfds.size());
        fds.push_back(abort_fd);
        for (int f : shmem_evfds) fds.push_back(f);
        if (!sendFds(daemon_sock, fds, &n_evfd, sizeof(n_evfd))) {
            poisonAbort(abort_fd);
            return 1;
        }
    }

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

    std::fprintf(stderr,
                 "criu-stream-fetch: posting %zu NIXL OBJ transfers (bucket=%s)\n",
                 states.size(), bucket.c_str());
    nixl_opt_args_t post_hint;
    post_hint.backends.push_back(backend);
    for (auto &st : states) {
        const auto start = std::chrono::steady_clock::now();
        nixl_status_t s = agent->postXferReq(st->xfer_req, &post_hint);
        if (s < 0) {
            std::fprintf(stderr,
                         "criu-stream-fetch: postXferReq id=%u failed (status=%d)\n",
                         st->id, (int)s);
            poisonAbort(abort_fd);
            return 1;
        }
        const auto post_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - start)
                                 .count();
        std::fprintf(stderr,
                     "[stream] postXfer id=%u size=%lu post_ms=%ld in_prog=%d\n",
                     st->id, (unsigned long)st->size, (long)post_ms,
                     s == NIXL_IN_PROG ? 1 : 0);
        // Small transfers may complete inline; handle that path here so we
        // don't poll a request that's already done.
        if (s == NIXL_SUCCESS) {
            eventfdSignal(st->eventfd_fd);
            std::lock_guard<std::mutex> g(st->done_mu);
            st->done = true;
            st->done_status = NIXL_SUCCESS;
        }
    }

    std::thread socket_thread(servePrivateSocket, private_sock,
                              std::ref(states), abort_fd);
    pollPrivateXfers(*agent, states, abort_fd);
    socket_thread.join();

    // Teardown order: cancel/release each xfer req, deregister memory,
    // unmap, close memfds + eventfds. nixlAgent's destructor tears down
    // the OBJ backend.
    for (auto &st : states) {
        if (st->xfer_req) agent->releaseXferReq(st->xfer_req);
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
    for (auto &st : states) {
        if (st->mmap_addr && st->size) ::munmap(st->mmap_addr, st->size);
        if (st->memfd >= 0) ::close(st->memfd);
        if (st->eventfd_fd >= 0) ::close(st->eventfd_fd);
    }
    for (int f : shmem_memfds) if (f >= 0) ::close(f);
    for (int f : shmem_evfds) if (f >= 0) ::close(f);
    if (abort_fd >= 0) ::close(abort_fd);

    return 0;
}
