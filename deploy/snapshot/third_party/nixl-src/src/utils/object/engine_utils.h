/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBJ_PLUGIN_UTILS_OBJECT_ENGINE_UTILS_H
#define OBJ_PLUGIN_UTILS_OBJECT_ENGINE_UTILS_H

#include "common/nixl_log.h"
#include "nixl_types.h"
#include <algorithm>
#include <cctype>
#include <string>
#include <thread>
#include <vector>

inline std::size_t
getNumThreads(nixl_b_params_t *custom_params) {
    return custom_params && custom_params->count("num_threads") > 0 ?
        std::stoul(custom_params->at("num_threads")) :
        std::max(1u, std::thread::hardware_concurrency() / 2);
}

inline size_t
getCrtMinLimit(nixl_b_params_t *custom_params) {
    if (!custom_params) return 0;

    auto it = custom_params->find("crtMinLimit");
    if (it != custom_params->end()) {
        try {
            return std::stoull(it->second);
        }
        catch (const std::exception &e) {
            NIXL_WARN << "Invalid crtMinLimit value: " << it->second
                      << ", using default (CRT disabled)";
            return 0;
        }
    }
    return 0; // Disabled by default
}

// Target throughput passed to the AWS S3 CRT client. Higher values let the CRT
// SDK scale internal concurrency up to the underlying NIC; the SDK's own
// default (around 5 Gbps) leaves significant throughput on the table for
// modern instances. Default 100.0 Gbps is well above any plausible single-host
// NIC, so CRT uses it as an upper bound and scales to whatever the network
// actually delivers.
inline double
getCrtThroughputGbps(nixl_b_params_t *custom_params) {
    constexpr double kDefault = 100.0;
    if (!custom_params) return kDefault;

    auto it = custom_params->find("crtThroughputGbps");
    if (it != custom_params->end()) {
        try {
            return std::stod(it->second);
        }
        catch (const std::exception &e) {
            NIXL_WARN << "Invalid crtThroughputGbps value: " << it->second
                      << ", using default (" << kDefault << ")";
            return kDefault;
        }
    }
    return kDefault;
}

// Optional explicit S3 CRT partSize. When set (positive), overrides the
// previous behavior of tying partSize to crtMinLimit. Lets a caller select
// crtMinLimit purely for engine routing (>0 selects S3CrtObjEngineImpl) while
// independently choosing the multipart part size (e.g. 16777216 = 16 MiB so a
// 16 MB chunk becomes a single multipart GET part). Unit: bytes.
inline size_t
getCrtPartSize(nixl_b_params_t *custom_params) {
    if (!custom_params) return 0;
    auto it = custom_params->find("crtPartSize");
    if (it == custom_params->end() || it->second.empty()) return 0;
    try {
        return std::stoull(it->second);
    }
    catch (const std::exception &e) {
        NIXL_WARN << "Invalid crtPartSize value: " << it->second
                  << "; falling back to crtMinLimit-derived partSize";
        return 0;
    }
}

// Network interfaces the AWS S3 CRT client should bind connections to. When
// non-empty, the CRT SDK distributes connections across the listed interfaces,
// enabling multi-NIC aggregate throughput (e.g. p4d.24xlarge: 4x 100 Gbps EFA
// -> 400 Gbps aggregate). Empty default leaves the field unset and the SDK
// falls back to OS routing (single-NIC).
//
// Format: comma-separated interface names, e.g. "ens5,ens6,ens7,ens8".
// Whitespace around each name is trimmed; empty entries are ignored.
//
// NOTE: maps to AWS C++ SDK's S3CrtClientConfiguration::networkInterfaceNames
// (Aws::Vector<Aws::String>). The SDK marks this field
// "EXPERIMENTAL AND UNSTABLE"; the underlying aws-c-s3 support
// (network_interface_names_array) has been stable since 0.4.x.
inline std::vector<std::string>
getCrtNetworkInterfaceNames(nixl_b_params_t *custom_params) {
    std::vector<std::string> result;
    if (!custom_params) return result;

    auto it = custom_params->find("networkInterfaceNames");
    if (it == custom_params->end() || it->second.empty()) return result;

    const std::string &list = it->second;
    size_t start = 0;
    while (start < list.size()) {
        size_t comma = list.find(',', start);
        size_t end = (comma == std::string::npos) ? list.size() : comma;
        std::string name = list.substr(start, end - start);
        // Trim leading/trailing whitespace.
        while (!name.empty() &&
               std::isspace(static_cast<unsigned char>(name.front())))
            name.erase(name.begin());
        while (!name.empty() &&
               std::isspace(static_cast<unsigned char>(name.back())))
            name.pop_back();
        if (!name.empty()) result.push_back(std::move(name));
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    return result;
}

inline bool
isAcceleratedRequested(nixl_b_params_t *custom_params) {
    if (!custom_params) return false;
    auto accel_it = custom_params->find("accelerated");
    return accel_it != custom_params->end() && accel_it->second == "true";
}

inline bool
isDellOBSRequested(nixl_b_params_t *custom_params) {
    if (!isAcceleratedRequested(custom_params)) return false;
    auto type_it = custom_params->find("type");
    return type_it != custom_params->end() && type_it->second == "dell";
}


#endif // OBJ_PLUGIN_UTILS_OBJECT_ENGINE_UTILS_H
