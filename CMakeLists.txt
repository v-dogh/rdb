cmake_minimum_required(VERSION 3.16)

project(RDB LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# For debbergering
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g -fno-omit-frame-pointer")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fvisibility=default -flto")
# set(ENV{ASAN_OPTIONS} "${ASAN_OPTIONS}:second_deadlock_stack=1")
# set(ENV{ASAN_OPTIONS} "${ASAN_OPTIONS}:symbolize=1")
# add_compile_options(-fsanitize=address,undefined,leak,bounds)
# set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address,undefined,leak,bounds")
# Concurrency options
# add_compile_options(-fsanitize=thread,bounds)
# set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread,bounds")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -fPIC -fno-plt -flto -funroll-loops -finline-functions")

set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
add_subdirectory(External/Snappy)
add_executable(RDB main.cpp
    # Core

    rdb_version.hpp
    rdb_root_config.hpp
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
    Schema/Types/rdb_scalar.hpp
    Schema/Types/rdb_tuple.hpp
    Schema/Types/rdb_buffer.hpp

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
    Schema/Types/rdb_time.hpp
)
target_link_libraries(RDB PRIVATE snappy)

target_include_directories(RDB
    PRIVATE
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
