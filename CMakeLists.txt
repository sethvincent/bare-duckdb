cmake_minimum_required(VERSION 3.31)

find_package(cmake-bare REQUIRED PATHS node_modules/cmake-bare)
find_package(cmake-fetch REQUIRED PATHS node_modules/cmake-fetch)

project(bare_duckdb C CXX)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_CXX_STANDARD 17)
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
)

if (target MATCHES "win32")
    add_definitions(-DDUCKDB_STATIC_BUILD)
    target_compile_definitions(
        ${bare_duckdb}
        PRIVATE
        DUCKDB_STATIC_BUILD
    )
    get_target_property(DUCKDB_COMPILE_DEFS duckdb_static COMPILE_DEFINITIONS)

    if(DUCKDB_COMPILE_DEFS)
        list(APPEND DUCKDB_COMPILE_DEFS DUCKDB_STATIC_BUILD)
    else()
        set(DUCKDB_COMPILE_DEFS DUCKDB_STATIC_BUILD)
    endif()

    set_target_properties(duckdb_static PROPERTIES COMPILE_DEFINITIONS "${DUCKDB_COMPILE_DEFS}")
endif()

target_link_libraries(
  ${bare_duckdb}
  PUBLIC
    duckdb_static
)
