cmake_minimum_required(VERSION 3.16)
project(libart LANGUAGES C)

add_library(libart STATIC src/art.c)

target_include_directories(libart
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_compile_options(libart PRIVATE
    $<$<C_COMPILER_ID:GNU,Clang>:-Wall -Wextra -Wpedantic>
)
set_target_properties(libart PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)
