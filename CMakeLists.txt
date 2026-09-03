cmake_minimum_required(VERSION 3.20)

project(AtlasFS VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(atlasfs_protocol
    src/protocol.cpp
)
add_library(AtlasFS::protocol ALIAS atlasfs_protocol)

target_include_directories(atlasfs_protocol
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

if(MSVC)
    target_compile_options(atlasfs_protocol PRIVATE /W4 /permissive-)
else()
    target_compile_options(atlasfs_protocol PRIVATE -Wall -Wextra -Wpedantic)
endif()
include(CTest)
if(BUILD_TESTING)
    include(FetchContent)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.15.2
        GIT_SHALLOW TRUE
    )
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    add_executable(atlasfs_protocol_tests tests/protocol_test.cpp)
    target_link_libraries(atlasfs_protocol_tests PRIVATE AtlasFS::protocol GTest::gtest_main)
    if(MSVC)
        target_compile_options(atlasfs_protocol_tests PRIVATE /W4 /permissive-)
    else()
        target_compile_options(atlasfs_protocol_tests PRIVATE -Wall -Wextra -Wpedantic)
    endif()

    include(GoogleTest)
    gtest_discover_tests(atlasfs_protocol_tests)
endif()
