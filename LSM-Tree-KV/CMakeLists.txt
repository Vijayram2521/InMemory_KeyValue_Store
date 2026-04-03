cmake_minimum_required(VERSION 3.15)
project(LSM_KV_Store)

# Set C++17 for std::optional and shared_mutex
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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
)

# 2. Define Test Sources
# Note: main.cpp is now correctly mapped to src/main.cpp
set(TEST_SOURCES
    src/main.cpp
    tests/test_wal.cpp
    tests/test_sstable.cpp
    tests/test_tombstone.cpp
)

# 3. Create the Test Executable
add_executable(kv_tests ${TEST_SOURCES})

# 4. Link the executable to the library
target_link_libraries(kv_tests kv_engine)

# 5. Support for Filesystem in older GCC/MinGW versions
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_link_libraries(kv_tests stdc++fs)
endif()