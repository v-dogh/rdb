cmake_minimum_required(VERSION 3.21)
project(RDB LANGUAGES CXX VERSION 0.1.0)

include(ExternalProject)
include(GNUInstallDirs)

###########
# OPTIONS #
###########

option(D2_SANITIZE_MEMORY "Enable memory sanitizer" OFF)
option(D2_SANITIZE_THREAD "Enable thread sanitizer" OFF)

set(D2_SANITIZER_FLAGS "")
if(D2_SANITIZE_MEMORY AND D2_SANITIZE_THREAD)
    message(FATAL_ERROR "ASAN and TSAN cannot be enabled together")
endif()
if(D2_SANITIZE_MEMORY)
    list(APPEND D2_SANITIZER_FLAGS -fsanitize=memory -fsanitize-recover=memory)
endif()
if(D2_SANITIZE_THREAD)
    list(APPEND D2_SANITIZER_FLAGS -fsanitize=thread)
endif()

#########################
# SUBLIBRARY PROPERTIES #
#########################

function(rdb_setup_target target)
    target_compile_features   (${target} PUBLIC cxx_std_20)
    target_include_directories(${target} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/Utils>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/Memory>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/Schema>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/Query>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/Network>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/External>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/External/jemalloc/include>
        $<INSTALL_INTERFACE:include>
    )
    if (D2_SANITIZE_MEMORY OR D2_SANITIZE_THREAD)
        target_compile_options (${target} PUBLIC -fno-omit-frame-pointer ${D2_SANITIZER_FLAGS})
        target_link_options    (${target} PUBLIC                         ${D2_SANITIZER_FLAGS})
    endif()
    if(CMAKE_INTERPROCEDURAL_OPTIMIZATION_SUPPORTED)
        set_target_properties  (${target} PROPERTIES INTERPROCEDURAL_OPTIMIZATION ON)
    endif()
endfunction()

#############
# LIBRARIES #
#############

# Core library; includes memory and disk management, the mount point and network
add_compile_definitions(RDB_RUNTIME_LOGS)
add_subdirectory(External/LibART)
add_subdirectory(External/Snappy)
add_library(RDBCore STATIC
    # Core

    rdb_version.hpp
    rdb_root_config.hpp
    rdb_containers.hpp
    rdb_containers.cpp
    rdb_mount.hpp
    rdb_mount.cpp
    rdb_root_config.cpp

    # Query

    Query/rdb_qop.hpp
    Query/rdb_dsl.hpp

    # Memory

    Memory/rdb_memory.hpp
    Memory/rdb_memory.cpp
    Memory/rdb_log.hpp
    Memory/rdb_log.cpp
    Memory/rdb_writetype.hpp
    Memory/rdb_disk_cache.hpp
    Memory/rdb_disk_cache.cpp

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
    Network/rdb_ctl.hpp
    Network/rdb_ctl.cpp

    # Utilities

    Utils/rdb_utils.hpp
    Utils/rdb_utils.cpp
    Utils/rdb_task_ring.hpp
    Utils/rdb_shared_buffer.hpp
    Utils/rdb_shared_buffer.cpp
    Utils/rdb_mapper.hpp
    Utils/rdb_mapper.cpp
    Utils/rdb_locale.hpp
    Utils/rdb_locale.cpp
    Utils/rdb_memunits.hpp
    Utils/rdb_runtime_logs.hpp
    Utils/rdb_runtime_logs.cpp
    Utils/rdb_dbg.hpp
    Utils/rdb_meta.hpp
)

set(JEMALLOC_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/External/jemalloc)

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

find_package(absl REQUIRED)

target_link_libraries(RDBCore PRIVATE
    absl::base
    absl::flat_hash_map
    libart
    snappy
    jemalloc
)
rdb_setup_target(RDBCore)

######################
# EXPORTED INTERFACE #
######################

add_library(RDB       INTERFACE)
add_library(RDB::Core ALIAS RDB)

target_link_libraries(RDB INTERFACE RDBCore)

set(_install_targets RDBCore)
install(TARGETS ${_install_targets} RDB libart
    EXPORT  RDBTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
install(
    DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/RDB
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
install(
    EXPORT      RDBTargets
    FILE        RDBTargets.cmake
    NAMESPACE   RDB::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/RDB
)
