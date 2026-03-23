cmake_minimum_required(VERSION 3.15)
project(LSM_KV_Store VERSION 1.0 LANGUAGES CXX)

# Use C++17 for shared_mutex and modern filesystem support
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(include)

# Define our library (the engine)
file(GLOB_RECURSE SOURCES "src/*.cpp")
add_library(kv_engine ${SOURCES})

# Define the main executable
add_executable(lsm_cli src/main.cpp)
target_link_libraries(lsm_cli kv_engine)