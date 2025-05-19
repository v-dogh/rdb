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
    rdb_root_config.hpp
    rdb_keytype.hpp
    rdb_writetype.hpp
    rdb_qop.hpp
    rdb_dsl.hpp
    rdb_utils.hpp
    rdb_shared_buffer.hpp
    rdb_memunits.hpp
    rdb_reflect.hpp
    rdb_dbg.hpp

    rdb_mount.hpp
    rdb_schema.hpp
    rdb_locale.hpp
    rdb_memory.hpp
    rdb_types.hpp
    rdb_log.hpp
    rdb_mapper.hpp

    rdb_mount.cpp
    rdb_schema.cpp
    rdb_memory.cpp
    rdb_log.cpp
    rdb_mapper.cpp
    rdb_network.hpp
    rdb_utils.cpp
    rdb_shared_buffer.cpp
    rdb_reflect.cpp
    rdb_version.hpp
)
target_link_libraries(RDB PRIVATE snappy)

include(GNUInstallDirs)
install(TARGETS RDB
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
