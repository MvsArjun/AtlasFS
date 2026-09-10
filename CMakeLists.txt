cmake_minimum_required(VERSION 3.20)

project(AtlasFS VERSION 0.1.0 LANGUAGES CXX)

if(NOT UNIX)
    message(FATAL_ERROR "AtlasFS networking currently requires a POSIX platform")
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(atlasfs_protocol
    src/protocol.cpp
)
add_library(AtlasFS::protocol ALIAS atlasfs_protocol)

add_library(atlasfs_storage
    src/checksum.cpp
    src/manifest.cpp
    src/object_protocol.cpp
    src/object_store.cpp
    src/storage_handler.cpp
)
add_library(AtlasFS::storage ALIAS atlasfs_storage)

add_library(atlasfs_metadata
    src/coordinator.cpp
    src/node_registry.cpp
)
add_library(AtlasFS::metadata ALIAS atlasfs_metadata)

add_library(atlasfs_replication
    src/coordinator_operation.cpp
    src/recovery.cpp
    src/replication.cpp
    src/remote_storage_node.cpp
    src/storage_node_protocol.cpp
)
add_library(AtlasFS::replication ALIAS atlasfs_replication)

add_library(atlasfs_net
    src/socket.cpp
    src/tcp_client.cpp
    src/tcp_server.cpp
)
add_library(AtlasFS::net ALIAS atlasfs_net)

target_include_directories(atlasfs_protocol
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)

target_include_directories(atlasfs_net
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
target_link_libraries(atlasfs_net PUBLIC AtlasFS::protocol)

target_include_directories(atlasfs_storage
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
target_link_libraries(atlasfs_storage PUBLIC AtlasFS::protocol)

target_include_directories(atlasfs_metadata
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
target_link_libraries(atlasfs_metadata PUBLIC AtlasFS::storage)

target_include_directories(atlasfs_replication
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
)
target_link_libraries(atlasfs_replication
    PUBLIC AtlasFS::metadata AtlasFS::storage AtlasFS::net)

if(MSVC)
    target_compile_options(atlasfs_protocol PRIVATE /W4 /permissive-)
else()
    target_compile_options(atlasfs_protocol PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(MSVC)
    target_compile_options(atlasfs_replication PRIVATE /W4 /permissive-)
else()
    target_compile_options(atlasfs_replication PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(MSVC)
    target_compile_options(atlasfs_net PRIVATE /W4 /permissive-)
else()
    target_compile_options(atlasfs_net PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(MSVC)
    target_compile_options(atlasfs_storage PRIVATE /W4 /permissive-)
else()
    target_compile_options(atlasfs_storage PRIVATE -Wall -Wextra -Wpedantic)
endif()

if(MSVC)
    target_compile_options(atlasfs_metadata PRIVATE /W4 /permissive-)
else()
    target_compile_options(atlasfs_metadata PRIVATE -Wall -Wextra -Wpedantic)
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

    find_package(Threads REQUIRED)
    add_executable(atlasfs_tcp_transport_tests tests/tcp_transport_test.cpp)
    target_link_libraries(atlasfs_tcp_transport_tests
        PRIVATE AtlasFS::net GTest::gtest_main Threads::Threads)

    add_executable(atlasfs_storage_tests
        tests/checksum_test.cpp
        tests/manifest_test.cpp
        tests/object_protocol_test.cpp
        tests/object_store_test.cpp
    )
    target_link_libraries(atlasfs_storage_tests PRIVATE AtlasFS::storage GTest::gtest_main)

    add_executable(atlasfs_storage_integration_tests tests/storage_integration_test.cpp)
    target_link_libraries(atlasfs_storage_integration_tests
        PRIVATE AtlasFS::net AtlasFS::storage GTest::gtest_main Threads::Threads)

    add_executable(atlasfs_metadata_tests
        tests/coordinator_test.cpp
        tests/node_registry_test.cpp
    )
    target_link_libraries(atlasfs_metadata_tests PRIVATE AtlasFS::metadata GTest::gtest_main)
    add_executable(atlasfs_replication_tests
        tests/coordinator_operation_test.cpp
        tests/replication_test.cpp
        tests/recovery_test.cpp
        tests/storage_node_protocol_test.cpp
    )
    target_link_libraries(atlasfs_replication_tests
        PRIVATE AtlasFS::replication GTest::gtest_main Threads::Threads)
    if(MSVC)
        target_compile_options(atlasfs_protocol_tests PRIVATE /W4 /permissive-)
    else()
        target_compile_options(atlasfs_protocol_tests PRIVATE -Wall -Wextra -Wpedantic)
        target_compile_options(atlasfs_tcp_transport_tests PRIVATE -Wall -Wextra -Wpedantic)
        target_compile_options(atlasfs_storage_tests PRIVATE -Wall -Wextra -Wpedantic)
        target_compile_options(atlasfs_storage_integration_tests
            PRIVATE -Wall -Wextra -Wpedantic)
        target_compile_options(atlasfs_metadata_tests PRIVATE -Wall -Wextra -Wpedantic)
        target_compile_options(atlasfs_replication_tests PRIVATE -Wall -Wextra -Wpedantic)
    endif()

    include(GoogleTest)
    gtest_discover_tests(atlasfs_protocol_tests)
    gtest_discover_tests(atlasfs_tcp_transport_tests)
    gtest_discover_tests(atlasfs_storage_tests)
    gtest_discover_tests(atlasfs_storage_integration_tests)
    gtest_discover_tests(atlasfs_metadata_tests)
    gtest_discover_tests(atlasfs_replication_tests)
endif()
