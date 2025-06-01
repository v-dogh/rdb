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
    Schema/rdb_types.hpp
    Schema/Types/rdb_scalar.hpp
    Schema/Types/rdb_tuple.hpp
    Schema/Types/rdb_buffer.hpp
    Schema/Types/rdb_time.hpp
    Schema/Types/rdb_bitset.hpp
    Schema/Types/rdb_array.hpp
    Schema/Types/rdb_array_iterator.hpp
    Schema/Types/rdb_nullable.hpp

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
target_link_libraries(RDB PRIVATE snappy)

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

# cmake_minimum_required(VERSION 3.21)
# project(RDB LANGUAGES CXX VERSION 0.1.0)

# ###########
# # OPTIONS #
# ###########

# option(D2_SANITIZE_MEMORY           "Enable memory sanitizer"      OFF)
# option(D2_SANITIZE_THREAD           "Enable thread sanitizer"      OFF)

# set(D2_SANITIZER_FLAGS "")
# if(D2_SANITIZE_MEMORY AND D2_SANITIZE_THREAD)
#     message(FATAL_ERROR "ASAN and TSAN cannot be enabled together")
# endif()
# if(D2_SANITIZE_MEMORY)
#     list(APPEND D2_SANITIZER_FLAGS -fsanitize=memory -fsanitize-recover=memory)
# endif()
# if(D2_SANITIZE_THREAD)
#     list(APPEND D2_SANITIZER_FLAGS -fsanitize=thread)
# endif()

# #########################
# # SUBLIBRARY PROPERTIES #
# #########################

# function(rdb_setup_target target)
#     target_compile_features   (${target} PUBLIC cxx_std_20)
#     target_include_directories(${target} PUBLIC
#         $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
#         $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/Utils>
#         $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/Memory>
#         $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/Schema>
#         $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/Query>
#         $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/External>
#         $<INSTALL_INTERFACE:include>
#     )
#     if (D2_SANITIZE_MEMORY OR D2_SANITIZE_THREAD)
#         target_compile_options (${target} PUBLIC -fno-omit-frame-pointer ${D2_SANITIZER_FLAGS})
#         target_link_options    (${target} PUBLIC                         ${D2_SANITIZER_FLAGS})
#     endif()
#     if(CMAKE_INTERPROCEDURAL_OPTIMIZATION_SUPPORTED)
#         set_target_properties  (${target} PROPERTIES INTERPROCEDURAL_OPTIMIZATION ON)
#     endif()
# endfunction()

# #############
# # LIBRARIES #
# #############

# # Various utilities
# add_library(RDBUtilities STATIC
#     # Utilities
#     Utils/rdb_utils.hpp
#     Utils/rdb_utils.cpp
#     Utils/rdb_shared_buffer.hpp
#     Utils/rdb_shared_buffer.cpp
#     Utils/rdb_mapper.hpp
#     Utils/rdb_mapper.cpp
#     Utils/rdb_locale.hpp
#     Utils/rdb_memunits.hpp
#     Utils/rdb_dbg.hpp
# )
# target_include_directories(RDBUtilities PUBLIC
#     $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
#     $<INSTALL_INTERFACE:include>
# )
# rdb_setup_target(RDBUtilities)

# # Core library; includes memory and disk management, the mount point and network
# add_library(RDBCore STATIC
#     # Memory
#     rdb_version.hpp
#     rdb_root_config.hpp
#     rdb_mount.hpp
#     rdb_mount.cpp
#     Memory/rdb_memory.hpp
#     Memory/rdb_memory.cpp
#     Memory/rdb_log.hpp
#     Memory/rdb_log.cpp
#     Memory/rdb_writetype.hpp
# )
# target_link_libraries     (RDBCore PRIVATE snappy)
# target_link_libraries     (RDBCore PUBLIC  RDBUtilities)
# rdb_setup_target(RDBCore)

# # Schema and query related stuff
# add_library(RDBMeta STATIC
#     # Query
#     Query/rdb_qop.hpp
#     Query/rdb_dsl.hpp
#     # Schema
#     Schema/rdb_schema.hpp
#     Schema/rdb_schema.cpp
#     Schema/rdb_reflect.hpp
#     Schema/rdb_reflect.cpp
#     Schema/rdb_keytype.hpp
#     Schema/rdb_types.hpp
#     Schema/Types/rdb_scalar.hpp
#     Schema/Types/rdb_tuple.hpp
#     Schema/Types/rdb_buffer.hpp
#     Schema/Types/rdb_time.hpp
#     Schema/Types/rdb_bitset.hpp
#     Schema/Types/rdb_array.hpp
#     Schema/Types/rdb_array_iterator.hpp
# )
# target_link_libraries     (RDBMeta     PUBLIC RDBUtilities)
# rdb_setup_target(RDBMeta)

# ######################
# # EXPORTED INTERFACE #
# ######################

# add_library(RDB       INTERFACE)
# add_library(RDB::Core ALIAS RDB)

# target_link_libraries(RDB INTERFACE
#     RDBCore
#     $<$<TARGET_EXISTS:RDBMeta>:RDBMeta>
#     $<$<TARGET_EXISTS:RDBUtilities>:RDBUtilities>
# )

# include(GNUInstallDirs)

# set(_install_targets RDBCore)
# if(TARGET RDBMeta)
#     list(APPEND _install_targets RDBMeta)
# endif()
# if(TARGET RDBUtilities)
#     list(APPEND _install_targets RDBUtilities)
# endif()

# install(TARGETS ${_install_targets} RDB
#     EXPORT  RDBTargets
#     ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
#     LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
#     INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
# )
# install(
#     DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/RDB
#     DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
# )
# install(
#     EXPORT      RDBTargets
#     FILE        RDBTargets.cmake
#     NAMESPACE   RDB::
#     DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/RDB
# )
