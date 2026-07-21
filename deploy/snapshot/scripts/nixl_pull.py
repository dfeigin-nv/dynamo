#!/usr/bin/env python3
"""nixl-pull v3: parallel S3-to-DRAM via NIXL OBJ backend, ALL-CRT routing.

Key change from v2: crtMinLimit=1 (was 8 MiB) — a documented sentinel that
makes S3CrtObjEngineImpl drop the standard awsS3Client entirely and route
EVERY object through the AWS CRT client. The CRT client handles 435 sub-MiB
objects + 2 large ones uniformly, with its own internal connection-pool
sizing (driven by crtThroughputGbps) instead of the AWS SDK 1.11 default
maxConnections=25 of the standard client.

Why v2 failed (concrete, verified from source — not a guess):

  src/plugins/obj/obj_backend.cpp:50
      if (getCrtMinLimit(...) > 0) return S3CrtObjEngineImpl(...);
      // else DefaultObjEngineImpl (CRT not constructed at all)

  src/plugins/obj/s3_crt/engine_impl.cpp:31
      S3CrtObjEngineImpl(...) : DefaultObjEngineImpl(init_params) {
          if (crtMinLimit_ == 1) s3Client_.reset();   // drop standard client
          s3ClientCrt_ = make_shared<awsS3CrtClient>(...);
      }

  src/plugins/obj/s3_crt/engine_impl.cpp:54
      getClientForSize(size_t data_len) {
          if (!s3ClientCrt_) return s3Client_.get();
          if (!s3Client_ || data_len >= crtMinLimit_)
              return s3ClientCrt_.get();
          return s3Client_.get();
      }

With v2's crtMinLimit=8388608, getClientForSize routes 435 objects of
size < 8 MiB to the standard awsS3Client. That client uses AWS SDK 1.11
ClientConfiguration defaults: maxConnections=25 and DefaultRetryStrategy
(exponential backoff, ~25s before giving up). 435 in-flight GETs into a
25-slot HTTP connection pool — see aws-sdk-cpp issue #523 — combined with
retry exhaustion produces NIXL_ERR_BACKEND at ~26s wall, which is exactly
what v2 hit. s5cmd works because it uses its own concurrency model
(NumWorkers default ~256), not aws-sdk-cpp's S3Client.

The May-12 bench at 29 Gbps never exercised the standard client because
MIN_FILE_BYTES=33554432 filtered to objects all >= crtMinLimit (5 MiB),
so every object went CRT-only. That's why the bug only shows up now
with the 437-object mixed-size CRIU manifest.

Side note on the AWS S3 5 MiB part-size floor:
  crtMinLimit also sets partSize and multipartUploadThreshold. With
  crtMinLimit=1, partSize is clamped to 5 MiB by the AWS CRT SDK
  (silent clamp with a warning log) — sub-5-MiB objects upload as
  single-part MPU, which S3 allows. For READ (this script), part size
  only affects how CRT chunks the GET range, not whether the request
  succeeds, so the clamp is harmless on the READ path.

Manifest TSV: <s3_key>\\t<local_path>\\t<size_bytes>

Env:
  AWS_DEFAULT_BUCKET, AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
  NIXL_CRT_THROUGHPUT_GBPS (default 100)
  NIXL_CRT_MIN_LIMIT       (default "1" — all-CRT route)

Stdout: SUMMARY-NIXL duration_s=... write_s=... total_mb=... throughput_mbps=...
"""
import os
import sys
import time
import numpy as np

import nixl_cu12._api as nixl  # type: ignore[import-not-found]


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <manifest.tsv>", file=sys.stderr)
        return 2

    manifest_path = sys.argv[1]
    bucket = os.environ["AWS_DEFAULT_BUCKET"]
    region = os.environ["AWS_DEFAULT_REGION"]
    throughput = os.environ.get("NIXL_CRT_THROUGHPUT_GBPS", "100")
    # "1" is a sentinel: drops the standard S3 client and forces all objects
    # through the CRT client regardless of size.
    crt_min_limit = os.environ.get("NIXL_CRT_MIN_LIMIT", "1")

    jobs: list[tuple[str, str, int]] = []
    with open(manifest_path) as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            key, local, size = line.split("\t")
            size = int(size)
            if size <= 0:
                # AWS GetObject with bytes=0--1 is undefined; skip zero-size
                # entries up front rather than letting the SDK error out.
                continue
            jobs.append((key, local, size))
    if not jobs:
        print("manifest empty", file=sys.stderr)
        return 1

    agent = nixl.nixl_agent("snapshot-restore")
    agent.create_backend(
        "OBJ",
        {
            "bucket": bucket,
            "region": region,
            "crtThroughputGbps": throughput,
            "crtMinLimit": crt_min_limit,
        },
    )

    # Pre-allocate numpy buffers (stable addresses, RAM-resident).
    bufs: list[np.ndarray] = []
    local_reg_descs: list[tuple[int, int, int, str]] = []
    remote_reg_descs: list[tuple[int, int, int, str]] = []
    local_xfer_descs: list[tuple[int, int, int]] = []
    remote_xfer_descs: list[tuple[int, int, int]] = []

    for i, (key, _local, size) in enumerate(jobs):
        arr = np.zeros(size, dtype=np.uint8)
        bufs.append(arr)
        addr = arr.ctypes.data
        # Local DRAM: (addr, len, gpu=0, "")
        local_reg_descs.append((addr, size, 0, ""))
        local_xfer_descs.append((addr, size, 0))
        # Remote OBJ: (addr=0, len=size, devId=i, meta=key)
        # devId i is unique per object; metaInfo carries the S3 key.
        remote_reg_descs.append((0, size, i, key))
        remote_xfer_descs.append((0, size, i))

    # Bulk-register DRAM and OBJ sides. Both pin straight to the OBJ backend.
    agent.register_memory(local_reg_descs, mem_type="DRAM", backends=["OBJ"])
    agent.register_memory(remote_reg_descs, mem_type="OBJ", backends=["OBJ"])

    # One xfer per object — same shape as the May-12 bench that hit 29 Gbps.
    # Per-object xfers let CRT parallelize freely; a single 437-element
    # XferList would also work but is harder to bisect when something fails.
    xfers = []
    for i in range(len(jobs)):
        l_xfer = agent.get_xfer_descs(
            [(local_xfer_descs[i][0], local_xfer_descs[i][1], local_xfer_descs[i][2])],
            mem_type="DRAM",
        )
        r_xfer = agent.get_xfer_descs(
            [(remote_xfer_descs[i][0], remote_xfer_descs[i][1], remote_xfer_descs[i][2])],
            mem_type="OBJ",
        )
        h = agent.initialize_xfer(
            "READ", l_xfer, r_xfer,
            remote_agent="snapshot-restore", backends=["OBJ"],
        )
        xfers.append(h)

    start = time.monotonic()
    for h in xfers:
        if agent.transfer(h) == "ERR":
            print("transfer post failed: ERR", file=sys.stderr)
            return 1

    pending = list(range(len(xfers)))
    first_err_idx = None
    while pending:
        still = []
        for idx in pending:
            status = agent.check_xfer_state(xfers[idx])
            if status == "DONE":
                continue
            if status == "ERR":
                if first_err_idx is None:
                    first_err_idx = idx
                    print(
                        f"xfer {idx} (key={jobs[idx][0]} size={jobs[idx][2]}) "
                        f"failed: ERR",
                        file=sys.stderr,
                    )
                # Drain other pending xfers so we report the full count of failures.
                continue
            still.append(idx)
        pending = still
        if pending:
            time.sleep(0.01)
    if first_err_idx is not None:
        return 1

    duration = time.monotonic() - start

    total_bytes = sum(s for _, _, s in jobs)

    write_start = time.monotonic()
    for arr, (_, local, _) in zip(bufs, jobs):
        os.makedirs(os.path.dirname(local) or ".", exist_ok=True)
        arr.tofile(local)
    write_duration = time.monotonic() - write_start

    total_mb = total_bytes / (1024 * 1024)
    throughput_mbps = total_mb / duration
    print(
        f"SUMMARY-NIXL duration_s={duration:.3f} "
        f"write_s={write_duration:.3f} "
        f"total_mb={int(total_mb)} "
        f"throughput_mbps={throughput_mbps:.1f}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
