cmake_minimum_required(VERSION 3.31)

find_package(cmake-bare REQUIRED PATHS node_modules/cmake-bare)
find_package(cmake-fetch REQUIRED PATHS node_modules/cmake-fetch)

project(bare_duckdb C CXX)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_MACOSX_BUNDLE OFF)
set(ENABLE_SANITIZER FALSE CACHE BOOL "Enable address sanitizer" FORCE)
set(ENABLE_UBSAN FALSE CACHE BOOL "Enable undefined behavior sanitizer" FORCE)

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

if(WIN32)
  target_compile_definitions(
    ${bare_duckdb}
    PRIVATE
      DUCKDB_STATIC_BUILD
  )
endif()

target_link_libraries(
  ${bare_duckdb}
  PUBLIC
    duckdb_static
)
