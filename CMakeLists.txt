cmake_minimum_required(VERSION 3.31)

find_package(cmake-bare REQUIRED PATHS node_modules/cmake-bare)
find_package(cmake-fetch REQUIRED PATHS node_modules/cmake-fetch)

project(bare_duckdb C CXX)

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
)

target_link_libraries(
  ${bare_duckdb}
  PUBLIC
    duckdb_static
)
