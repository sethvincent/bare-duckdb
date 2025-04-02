cmake_minimum_required(VERSION 3.31)

find_package(cmake-bare REQUIRED PATHS node_modules/cmake-bare)
find_package(cmake-fetch REQUIRED PATHS node_modules/cmake-fetch)

project(bare_duckdb C CXX)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_MACOSX_BUNDLE OFF)

fetch_package("github:duckdb/duckdb@1.2.1" SOURCE_DIR duckdb_src)

add_bare_module(bare_duckdb)

target_sources(
  ${bare_duckdb}
  PRIVATE
    binding.c
)

target_include_directories(
  ${bare_duckdb}
  PRIVATE
    ${duckdb_src}/src/include
    ${duckdb_src}/third_party/fsst
    ${duckdb_src}/third_party/fmt/include
    ${duckdb_src}/third_party/hyperloglog
    ${duckdb_src}/third_party/fastpforlib
    ${duckdb_src}/third_party/skiplist
    ${duckdb_src}/third_party/fast_float
    ${duckdb_src}/third_party/re2
    ${duckdb_src}/third_party/miniz
    ${duckdb_src}/third_party/utf8proc/include
    ${duckdb_src}/third_party/concurrentqueue
    ${duckdb_src}/third_party/pcg
    ${duckdb_src}/third_party/tdigest
    ${duckdb_src}/third_party/mbedtls/include
    ${duckdb_src}/third_party/jaro_winkler
    ${duckdb_src}/third_party/yyjson/include
    ${duckdb_src}/third_party/zstd/include
    # For GitHub Actions environment
    $<$<CXX_COMPILER_ID:GNU>:/usr/include/c++/${CMAKE_CXX_COMPILER_VERSION}>
    $<$<CXX_COMPILER_ID:GNU>:/usr/include/x86_64-linux-gnu/c++/${CMAKE_CXX_COMPILER_VERSION}>
    $<$<CXX_COMPILER_ID:Clang>:/usr/include/c++/v1>
)

target_link_libraries(
  ${bare_duckdb}
  PUBLIC
    duckdb_static
)
