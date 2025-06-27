cmake_minimum_required(VERSION 3.16)

project(RDB LANGUAGES CXX)

include(ExternalProject)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ON)

# set(ASAN_FLAGS "-fsanitize=address -fno-omit-frame-pointer -g")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${ASAN_FLAGS}")
# set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${ASAN_FLAGS}")

add_subdirectory(External/Snappy)
add_subdirectory(External/AbseilCpp)
add_subdirectory(External/LibART)
add_executable(RDB main.cpp
    # Core

    rdb_version.hpp
    rdb_root_config.hpp
    rdb_containers.hpp
    rdb_containers.cpp
    rdb_mount.hpp
    rdb_mount.cpp

    # Query

    Query/rdb_qop.hpp
    Query/rdb_dsl.hpp

    # Memory

    Memory/rdb_memory.hpp
    Memory/rdb_memory.cpp
    Memory/rdb_log.hpp
    Memory/rdb_log.cpp
    Memory/rdb_writetype.hpp

    # Schema

    Schema/rdb_schema.hpp
    Schema/rdb_schema.cpp
    Schema/rdb_reflect.hpp
    Schema/rdb_reflect.cpp
    Schema/rdb_keytype.hpp
    Schema/rdb_types.hpp

    # Types

    Schema/Types/rdb_scalar.hpp
    Schema/Types/rdb_tuple.hpp
    Schema/Types/rdb_buffer.hpp
    Schema/Types/rdb_time.hpp
    Schema/Types/rdb_bitset.hpp
    Schema/Types/rdb_array.hpp
    Schema/Types/rdb_nullable.hpp
    Schema/Types/rdb_array_iterator.hpp
    Schema/Types/rdb_trivial_helper.hpp

    # Network

    Network/rdb_network.hpp
    Network/rdb_oracle.hpp

    # Utilities

    Utils/rdb_utils.hpp
    Utils/rdb_utils.cpp
    Utils/rdb_shared_buffer.hpp
    Utils/rdb_shared_buffer.cpp
    Utils/rdb_mapper.hpp
    Utils/rdb_mapper.cpp
    Utils/rdb_locale.hpp
    Utils/rdb_memunits.hpp
    Utils/rdb_dbg.hpp
)

# Jemalloc

set(JEMALLOC_SOURCE_DIR ${CMAKE_SOURCE_DIR}/External/jemalloc)

ExternalProject_Add(JemallocProject
    SOURCE_DIR        ${JEMALLOC_SOURCE_DIR}
    CONFIGURE_COMMAND ${JEMALLOC_SOURCE_DIR}/autogen.sh &&
                      ${JEMALLOC_SOURCE_DIR}/configure
                        --disable-debug
                        --with-jemalloc-prefix=
                        --with-malloc-conf=percpu_arena:percpu
    BUILD_COMMAND make -j
    INSTALL_COMMAND ""
    BUILD_ALWAYS 0
    BUILD_IN_SOURCE 1
)

add_library(jemalloc STATIC IMPORTED GLOBAL)
set_target_properties(jemalloc PROPERTIES
    IMPORTED_LOCATION ${JEMALLOC_SOURCE_DIR}/lib/libjemalloc.a
)
add_dependencies(jemalloc JemallocProject)

# Dependencies

target_link_libraries(RDB PRIVATE
    jemalloc
    absl::base
    absl::flat_hash_map
    libart
    snappy
)

target_include_directories(RDB PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/External
    ${CMAKE_SOURCE_DIR}/Memory
    ${CMAKE_SOURCE_DIR}/Query
    ${CMAKE_SOURCE_DIR}/Schema
    ${CMAKE_SOURCE_DIR}/Network
    ${CMAKE_SOURCE_DIR}/Utils
)

include(GNUInstallDirs)
install(TARGETS RDB
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
