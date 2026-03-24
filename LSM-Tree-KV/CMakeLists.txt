cmake_minimum_required(VERSION 3.15)
project(LSM_KV_Store)

# Set C++17 for std::optional and shared_mutex
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Tell CMake where to find your .h files
include_directories(include)

# Create the library from your implementation
add_library(kv_engine src/storage_engine.cpp src/wal.cpp)

# Create the executable and link it to the library
add_executable(lsm_cli src/main.cpp)
target_link_libraries(lsm_cli kv_engine)