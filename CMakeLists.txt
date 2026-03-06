cmake_minimum_required(VERSION 3.14)
project(YDB VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-Wall -Wextra -Wpedantic -O2 -g)
endif()

include_directories(include)

# --- Library sources (all .cpp except main) ---
set(YDB_SOURCES
    src/storage/disk_manager.cpp
    src/storage/table_page.cpp
    src/storage/table_heap.cpp
    src/buffer/lru_replacer.cpp
    src/buffer/buffer_pool_manager.cpp
    src/index/bplus_tree.cpp
    src/concurrency/lock_manager.cpp
    src/concurrency/transaction.cpp
    src/recovery/log_manager.cpp
    src/recovery/recovery_manager.cpp
)

add_library(ydb_lib STATIC ${YDB_SOURCES})

add_executable(ydb src/main.cpp)
target_link_libraries(ydb PRIVATE ydb_lib)

# Helpful build info
message(STATUS "Build type : ${CMAKE_BUILD_TYPE}")
message(STATUS "Compiler   : ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
