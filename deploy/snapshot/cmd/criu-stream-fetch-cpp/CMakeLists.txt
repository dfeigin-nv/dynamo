# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# CMake build for the Stage 2 C++ criu-stream-fetch streamer.
#
# Configure with NIXL_ROOT set to the NIXL source/build tree so we pick up
# headers and the libnixl shared library:
#
#   cmake -S . -B build \
#       -DNIXL_INCLUDE_DIR=/opt/nixl/src/api/cpp \
#       -DNIXL_LIBRARY=/opt/nixl/build/src/libnixl.so
#
# Inside the agent Dockerfile (Stage 2 delta) the NIXL build is unpacked under
# /usr/local/{include,lib} so the defaults below pick it up.

cmake_minimum_required(VERSION 3.20)
project(criu-stream-fetch-cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(NIXL_INCLUDE_DIR "/usr/local/include/nixl" CACHE PATH "Path to NIXL public headers (nixl.h)")
set(NIXL_LIBRARY "/usr/local/lib/libnixl.so" CACHE FILEPATH "Path to libnixl.so")
# Newer NIXL splits nixlDescList::addDesc etc. into libnixl_build.so. Linked
# only when it exists (older NIXL keeps everything in libnixl.so).
set(NIXL_BUILD_LIBRARY "" CACHE FILEPATH "Optional path to libnixl_build.so (split NIXL)")

add_executable(criu-stream-fetch main.cpp)

target_include_directories(criu-stream-fetch
    PRIVATE
        ${NIXL_INCLUDE_DIR}
)

target_link_libraries(criu-stream-fetch
    PRIVATE
        ${NIXL_LIBRARY}
        pthread
        dl
        rt
)

if(NIXL_BUILD_LIBRARY AND EXISTS "${NIXL_BUILD_LIBRARY}")
    target_link_libraries(criu-stream-fetch PRIVATE ${NIXL_BUILD_LIBRARY})
endif()

target_compile_options(criu-stream-fetch
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

install(TARGETS criu-stream-fetch RUNTIME DESTINATION sbin)
