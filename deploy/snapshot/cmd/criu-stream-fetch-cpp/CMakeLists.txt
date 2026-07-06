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
# NIXL's symbols are split across sibling libs (descriptor lists, serdes,
# stream, common). Point NIXL_LIB_DIR at the directory holding them and link
# whichever siblings the build produced (glob — an OBJ-only build may omit
# some). --no-as-needed keeps them linked so runtime symbol resolution works.
get_filename_component(_nixl_lib_dir "${NIXL_LIBRARY}" DIRECTORY)
set(NIXL_LIB_DIR "${_nixl_lib_dir}" CACHE PATH "Directory with libnixl*.so siblings")
file(GLOB NIXL_SIBLINGS
    "${NIXL_LIB_DIR}/libnixl_common.so"
    "${NIXL_LIB_DIR}/libnixl_build.so"
    "${NIXL_LIB_DIR}/libserdes.so"
    "${NIXL_LIB_DIR}/libstream.so"
    "${NIXL_LIB_DIR}/libobj_utils.so"
)

add_executable(criu-stream-fetch main.cpp)

target_include_directories(criu-stream-fetch
    PRIVATE
        ${NIXL_INCLUDE_DIR}
)

target_link_libraries(criu-stream-fetch
    PRIVATE
        -Wl,--no-as-needed
        ${NIXL_LIBRARY}
        ${NIXL_SIBLINGS}
        -Wl,--as-needed
        pthread
        dl
        rt
)

target_compile_options(criu-stream-fetch
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

install(TARGETS criu-stream-fetch RUNTIME DESTINATION sbin)
