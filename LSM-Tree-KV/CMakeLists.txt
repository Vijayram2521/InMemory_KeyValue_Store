cmake_minimum_required(VERSION 3.15)
project(LSM_KV_Store)

# Set C++17 for std::optional and shared_mutex
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Default to an optimized build unless the caller asks for something else --
# matters most for kv_benchmark, whose numbers are meaningless unoptimized.
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

find_package(Threads REQUIRED)

# Include directories so headers can be found
# include/ contains engine headers, tests/ contains tests.h
include_directories(include)
include_directories(tests)

# 1. Create the engine library from the implementations in src/
add_library(
    kv_engine
    src/storage_engine.cpp
    src/wal.cpp
    src/manifest.cpp
    src/sstable.cpp
    src/bloom_filter.cpp
)
target_link_libraries(kv_engine Threads::Threads)

# 2. Define Test Sources
# Note: main.cpp is now correctly mapped to src/main.cpp
set(TEST_SOURCES
    src/main.cpp
    tests/test_wal.cpp
    tests/test_sstable.cpp
    tests/test_sstable_index.cpp
    tests/test_bloom_filter.cpp
    tests/test_tombstone.cpp
    tests/test_manifest.cpp
    tests/test_storage_engine.cpp
)

# 3. Create the Test Executable
add_executable(kv_tests ${TEST_SOURCES})

# 4. Link the executable to the library
target_link_libraries(kv_tests kv_engine)

# 5. Support for Filesystem in older GCC/MinGW versions.
# GCC >= 9 folds std::filesystem into libstdc++ itself; only pre-9 toolchains
# need the separate -lstdc++fs, and modern distros (e.g. Debian's) don't
# necessarily ship that stub anymore, so gate this on the compiler version.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0)
    target_link_libraries(kv_tests stdc++fs)
endif()

# 6. Register with CTest so `ctest` (or `ctest --output-on-failure`) can drive
# the suite and report pass/fail, not just a human reading stdout.
enable_testing()
add_test(NAME kv_tests COMMAND kv_tests WORKING_DIRECTORY ${CMAKE_BINARY_DIR})

# 7. Throughput benchmark (writes/sec, reads/sec) -- see src/benchmark.cpp.
add_executable(kv_benchmark src/benchmark.cpp)
target_link_libraries(kv_benchmark kv_engine Threads::Threads)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9.0)
    target_link_libraries(kv_benchmark stdc++fs)
endif()

# 8. Distributed leader/compute-node layer -- POSIX sockets. The project has
# moved to targeting Unix exclusively (perf testing and deployment are both
# Linux-only already), so this is unconditional rather than platform-gated.
add_library(
    kv_cluster
    src/cluster/hash_ring.cpp
    src/cluster/wire_protocol.cpp
    src/cluster/tcp_socket.cpp
)
target_link_libraries(kv_cluster kv_engine Threads::Threads)

add_executable(kv_cluster_tests src/cluster_test_main.cpp tests/test_hash_ring.cpp tests/test_wire_protocol.cpp)
target_link_libraries(kv_cluster_tests kv_cluster kv_engine)
add_test(NAME kv_cluster_tests COMMAND kv_cluster_tests WORKING_DIRECTORY ${CMAKE_BINARY_DIR})