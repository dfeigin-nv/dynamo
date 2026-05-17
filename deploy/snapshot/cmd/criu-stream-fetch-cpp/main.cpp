// SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// criu-stream-fetch (C++)
//
// Stage 2 Pipeline C smart-streaming CRIU restore streamer. Replaces the Go
// skeleton at sibling cmd/criu-stream-fetch/. Per docs/streaming-restore-design.md:
//
//   1. Receive per-pages_img_id memfds from CRIU via SCM_RIGHTS.
//   2. mmap each memfd into local DRAM.
//   3. Set up nixlAgent with the OBJ backend (S3 via AWS CRT inside the patched
//      libplugin_OBJ.so). Register each memfd's mmap as a NIXL local buffer.
//   4. postXfer one descriptor pair per memfd: BufferDesc(local DRAM) <-
//      ObjDesc(S3 key + offset + len). AWS CRT writes the bytes directly into
//      the memfd's tmpfs pages (PreallocatedStreamBuf, zero-copy).
//   5. Fire eventfd[i] when memfd i is fully filled. The UFFD daemon's epoll
//      loop reacts with UFFDIO_CONTINUE(addr, len) to install PTEs proactively.
//   6. Reactive fallback: if the user-process touches a page before the
//      streamer eventfd fires, UFFD MISSING handler in the daemon performs an
//      on-demand UFFDIO_CONTINUE off the same memfd page.
//
// This file is the Stage 2a scaffold: structure, manifest parsing, SCM_RIGHTS
// memfd receive, eventfd creation, and a nixlAgent placeholder. NIXL OBJ
// backend wiring (createBackend("OBJ"), registerMem, postXfer, getXferStatus)
// lives in Stage 2b. The UFFD daemon process is a sibling binary in Stage 2c.

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
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

// NIXL public API. Stage 2a uses only the agent constructor placeholder; the
// full OBJ-backend setup lands in Stage 2b.
#include "nixl.h"

namespace {

struct RangeEntry {
    uint32_t id;
    uint64_t size;
    std::string source;  // s3://bucket/key when sourced from S3, else local path
    std::string bucket;  // populated for s3:// sources by writePipelineCManifest
    std::string key;     // S3 object key under bucket
};

struct Manifest {
    std::vector<RangeEntry> shmem_ranges;
    std::vector<RangeEntry> private_ranges;
};

// Minimal hand-rolled parser sufficient for the dynamo writePipelineCManifest
// schema:
//   {"shmem_ranges":[],"private_ranges":[{"id":N,"size":M,"source":"...",
//                                         "bucket":"...","key":"..."}]}
// Real JSON parsing arrives with nlohmann/json (single header) in Stage 2b.
bool extractStringField(std::string_view obj, std::string_view key, std::string &out) {
    std::string pat = "\"";
    pat.append(key);
    pat.append("\":");
    auto pos = obj.find(pat);
    if (pos == std::string_view::npos)
        return false;
    pos += pat.size();
    while (pos < obj.size() && obj[pos] != '"') {
        if (obj[pos] == ',' || obj[pos] == '}')
            return false;
        ++pos;
    }
    if (pos >= obj.size())
        return false;
    ++pos;
    auto end = obj.find('"', pos);
    if (end == std::string_view::npos)
        return false;
    out.assign(obj.substr(pos, end - pos));
    return true;
}

bool extractU64Field(std::string_view obj, std::string_view key, uint64_t &out) {
    std::string pat = "\"";
    pat.append(key);
    pat.append("\":");
    auto pos = obj.find(pat);
    if (pos == std::string_view::npos)
        return false;
    pos += pat.size();
    while (pos < obj.size() && (obj[pos] == ' ' || obj[pos] == '\t'))
        ++pos;
    uint64_t v = 0;
    bool has = false;
    while (pos < obj.size() && obj[pos] >= '0' && obj[pos] <= '9') {
        v = v * 10 + (uint64_t)(obj[pos] - '0');
        ++pos;
        has = true;
    }
    if (!has)
        return false;
    out = v;
    return true;
}

// Slice the JSON array body between matching '[' and ']' at the given key.
bool extractArrayBody(std::string_view obj, std::string_view key, std::string_view &out) {
    std::string pat = "\"";
    pat.append(key);
    pat.append("\":");
    auto pos = obj.find(pat);
    if (pos == std::string_view::npos)
        return false;
    pos = obj.find('[', pos);
    if (pos == std::string_view::npos)
        return false;
    int depth = 1;
    size_t lo = pos + 1;
    size_t i = lo;
    while (i < obj.size() && depth > 0) {
        if (obj[i] == '[') ++depth;
        else if (obj[i] == ']') --depth;
        if (depth == 0) break;
        ++i;
    }
    if (depth != 0)
        return false;
    out = obj.substr(lo, i - lo);
    return true;
}

bool parseRanges(std::string_view arrayBody, std::vector<RangeEntry> &out) {
    size_t i = 0;
    while (i < arrayBody.size()) {
        while (i < arrayBody.size() && arrayBody[i] != '{')
            ++i;
        if (i >= arrayBody.size())
            break;
        size_t lo = i + 1;
        int depth = 1;
        while (i + 1 < arrayBody.size() && depth > 0) {
            ++i;
            if (arrayBody[i] == '{') ++depth;
            else if (arrayBody[i] == '}') --depth;
        }
        if (depth != 0)
            return false;
        std::string_view item = arrayBody.substr(lo, i - lo);
        RangeEntry r{};
        uint64_t id_u64 = 0;
        if (!extractU64Field(item, "id", id_u64)) return false;
        r.id = (uint32_t)id_u64;
        if (!extractU64Field(item, "size", r.size)) return false;
        if (!extractStringField(item, "source", r.source)) return false;
        (void)extractStringField(item, "bucket", r.bucket);  // optional
        (void)extractStringField(item, "key", r.key);
        out.push_back(std::move(r));
        ++i;  // step past '}'
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

// SCM_RIGHTS payload sender shared by the daemon-fd handshake and the per-task
// private-pages reply. iov is a single dummy byte to satisfy the kernel's
// requirement that recvmsg always have a real iov to receive.
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

int fdFromEnv(const char *name, bool required) {
    const char *v = std::getenv(name);
    if (v == nullptr || *v == '\0') {
        if (required)
            std::fprintf(stderr, "criu-stream-fetch: env %s is unset\n", name);
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

// Stage 2a placeholder. Stage 2b replaces this with a real OBJ-backend setup:
//   nixlAgentConfig cfg; cfg.useProgThread = true;
//   nixlAgent agent("criu-stream-fetch", cfg);
//   nixl_b_params_t params = {{"crtThroughputGbps","100"}, ...};
//   nixlBackendH *bknd = nullptr;
//   agent.createBackend("OBJ", params, bknd);
// and per-memfd registerMem + createXferReq + postXferReq + getXferStatus.
int setupNixlAgentPlaceholder() {
    // Just sanity-check that the NIXL headers compile and the agent type is
    // visible in this translation unit. The actual agent construction in
    // Stage 2a is deferred to avoid linking libnixl.so before the image
    // build env can ship it.
    static_assert(sizeof(nixlAgentConfig) > 0, "nixl.h not picking up agent config");
    return 0;
}

void poisonAbort(int abort_fd) {
    if (abort_fd < 0)
        return;
    uint64_t v = 1;
    ssize_t n = ::write(abort_fd, &v, sizeof(v));
    (void)n;
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
    if (manifest_path == nullptr)
        manifest_path = std::getenv("CRIU_STREAMER_MANIFEST");
    if (manifest_path == nullptr) {
        std::fprintf(stderr, "criu-stream-fetch: --manifest or CRIU_STREAMER_MANIFEST required\n");
        return 2;
    }

    Manifest m;
    if (!loadManifest(manifest_path, m))
        return 1;
    std::fprintf(stderr,
                 "criu-stream-fetch: loaded manifest with %zu shmem + %zu private ranges\n",
                 m.shmem_ranges.size(), m.private_ranges.size());

    int daemon_sock = fdFromEnv("CRIU_STREAMER_DAEMON_SOCK", false);
    int private_sock = fdFromEnv("CRIU_STREAMER_PRIVATE_SOCK", true);
    if (private_sock < 0)
        return 2;

    int abort_fd = eventfdCreate();
    if (abort_fd < 0)
        return 1;

    // Per-range eventfds (Stage 2c daemon will consume these).
    std::vector<int> shmem_memfds(m.shmem_ranges.size(), -1);
    std::vector<int> shmem_evfds(m.shmem_ranges.size(), -1);
    for (size_t i = 0; i < m.shmem_ranges.size(); ++i) {
        const auto &r = m.shmem_ranges[i];
        char name[64];
        std::snprintf(name, sizeof(name), "criu-shmem-%u", r.id);
        int mfd = memfdCreate(name, r.size);
        if (mfd < 0) {
            poisonAbort(abort_fd);
            return 1;
        }
        int evfd = eventfdCreate();
        if (evfd < 0) {
            ::close(mfd);
            poisonAbort(abort_fd);
            return 1;
        }
        shmem_memfds[i] = mfd;
        shmem_evfds[i] = evfd;
    }

    // Send abort_fd + per-range eventfds to the daemon via SCM_RIGHTS.
    // Skip when private-only (no daemon socket inherited).
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

    if (setupNixlAgentPlaceholder() != 0) {
        poisonAbort(abort_fd);
        return 1;
    }

    // TODO Stage 2b: build nixlAgent + OBJ backend; registerMem each memfd's
    // mmap; postXfer per memfd; poll get_xfer_status; fire eventfd[i] on
    // completion. Also handles private_ranges via the same NIXL agent +
    // backend instance; CRIU's per-task recv_streamer_private_fd request
    // protocol stays unchanged from the Go streamer.

    // TODO Stage 2c: split the per-task private-pages SCM_RIGHTS reply loop
    // out from this binary if we end up with a separate UFFD daemon process.
    // For now this binary just exits cleanly so the build pipeline can verify
    // the C++ skeleton compiles.

    std::fprintf(stderr, "criu-stream-fetch: scaffold exit (stage 2a placeholder)\n");

    // Best-effort cleanup. Stage 2b extends this with NIXL agent teardown.
    for (int f : shmem_memfds) if (f >= 0) ::close(f);
    for (int f : shmem_evfds) if (f >= 0) ::close(f);
    if (abort_fd >= 0) ::close(abort_fd);

    return 0;
}
