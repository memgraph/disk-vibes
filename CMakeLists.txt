cmake_minimum_required(VERSION 3.28)
project(arrow_cpp_experiment VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
enable_language(CXX)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
enable_testing()

# Find deps: Arrow, RocksDB, GTest, fmt, spdlog
find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h /opt/homebrew/include)
find_library(ROCKSDB_LIBRARY NAMES rocksdb PATHS /opt/homebrew/lib)
if(NOT ROCKSDB_INCLUDE_DIR OR NOT ROCKSDB_LIBRARY)
    message(FATAL_ERROR "RocksDB not found. Please install it first.")
endif()

find_path(ARROW_INCLUDE_DIR arrow/api.h /opt/homebrew/include)
find_library(ARROW_LIBRARY NAMES arrow PATHS /opt/homebrew/lib)
find_library(ARROWFLIGHT_LIBRARY NAMES arrow_flight PATHS /opt/homebrew/lib)
find_library(PARQUET_LIBRARY NAMES parquet PATHS /opt/homebrew/lib)
if(NOT ARROW_INCLUDE_DIR OR NOT ARROW_LIBRARY)
    message(FATAL_ERROR "Arrow not found. Please install it first.")
endif()
if(NOT PARQUET_LIBRARY)
    message(FATAL_ERROR "Parquet not found. Please install it first.")
endif()

find_package(GTest REQUIRED)

# Find fmt
find_path(FMT_INCLUDE_DIR fmt/core.h /opt/homebrew/include)
find_library(FMT_LIBRARY NAMES fmt PATHS /opt/homebrew/lib)
if(NOT FMT_INCLUDE_DIR OR NOT FMT_LIBRARY)
    message(FATAL_ERROR "fmt not found. Please install it first.")
endif()

# Find spdlog
find_path(SPDLOG_INCLUDE_DIR spdlog/spdlog.h /opt/homebrew/include)
find_library(SPDLOG_LIBRARY NAMES spdlog PATHS /opt/homebrew/lib)
if(NOT SPDLOG_INCLUDE_DIR OR NOT SPDLOG_LIBRARY)
    message(FATAL_ERROR "spdlog not found. Please install it first.")
endif()

# Find Folly
find_path(FOLLY_INCLUDE_DIR folly/ConcurrentSkipList.h /opt/homebrew/include)
find_library(FOLLY_LIBRARY NAMES folly PATHS /opt/homebrew/lib)
if(NOT FOLLY_INCLUDE_DIR OR NOT FOLLY_LIBRARY)
    message(FATAL_ERROR "Folly not found. Please install it first: brew install folly")
endif()

# Find glog (required by Folly)
find_path(GLOG_INCLUDE_DIR glog/logging.h /opt/homebrew/include)
find_library(GLOG_LIBRARY NAMES glog PATHS /opt/homebrew/lib)
if(NOT GLOG_INCLUDE_DIR OR NOT GLOG_LIBRARY)
    message(FATAL_ERROR "glog not found. Please install it first: brew install glog")
endif()

add_executable(example_graph
  src/main_example_graph.cpp
)
add_executable(test_graph
  src/storage/graph_test.cpp
)
add_executable(test_transactional_graph
  src/storage/graph_transaction_test.cpp
)
add_executable(benchmark
  src/test/benchmark.cpp
)
add_executable(example_rocks
  src/main_example_rocks.cpp
)
add_executable(test_hermitage
  src/test/hermitage.cpp
)

include_directories(${CMAKE_SOURCE_DIR}/src)
target_include_directories(example_graph
    PRIVATE
    ${ARROW_INCLUDE_DIR}
    ${ARROWFLIGHT_INCLUDE_DIR}
    ${ROCKSDB_INCLUDE_DIR}
    ${SPDLOG_INCLUDE_DIR}
    ${FMT_INCLUDE_DIR}
    ${FOLLY_INCLUDE_DIR}
    ${GLOG_INCLUDE_DIR}
)
target_include_directories(test_graph
    PRIVATE
    ${ARROW_INCLUDE_DIR}
    ${ARROWFLIGHT_INCLUDE_DIR}
    ${ROCKSDB_INCLUDE_DIR}
    ${GTEST_INCLUDE_DIRS}
    ${SPDLOG_INCLUDE_DIR}
    ${FMT_INCLUDE_DIR}
    ${FOLLY_INCLUDE_DIR}
    ${GLOG_INCLUDE_DIR}
)
target_include_directories(test_transactional_graph
    PRIVATE
    ${ARROW_INCLUDE_DIR}
    ${ARROWFLIGHT_INCLUDE_DIR}
    ${ROCKSDB_INCLUDE_DIR}
    ${GTEST_INCLUDE_DIRS}
    ${SPDLOG_INCLUDE_DIR}
    ${FMT_INCLUDE_DIR}
    ${FOLLY_INCLUDE_DIR}
    ${GLOG_INCLUDE_DIR}
)
target_include_directories(benchmark
    PRIVATE
    ${ARROW_INCLUDE_DIR}
    ${ARROWFLIGHT_INCLUDE_DIR}
    ${ROCKSDB_INCLUDE_DIR}
    ${SPDLOG_INCLUDE_DIR}
    ${FMT_INCLUDE_DIR}
    ${FOLLY_INCLUDE_DIR}
    ${GLOG_INCLUDE_DIR}
)
target_include_directories(example_rocks
    PRIVATE
    ${ROCKSDB_INCLUDE_DIR}
    ${SPDLOG_INCLUDE_DIR}
    ${FMT_INCLUDE_DIR}
    ${FOLLY_INCLUDE_DIR}
    ${GLOG_INCLUDE_DIR}
)

target_link_libraries(example_graph
    PRIVATE
    ${ARROWFLIGHT_LIBRARY}
    ${ROCKSDB_LIBRARY}
    ${ARROW_LIBRARY}
    ${PARQUET_LIBRARY}
    ${SPDLOG_LIBRARY}
    ${FMT_LIBRARY}
    ${FOLLY_LIBRARY}
    ${GLOG_LIBRARY}
)
target_link_libraries(test_graph
    PRIVATE
    ${ARROWFLIGHT_LIBRARY}
    ${ROCKSDB_LIBRARY}
    ${ARROW_LIBRARY}
    ${PARQUET_LIBRARY}
    GTest::GTest
    GTest::Main
    ${SPDLOG_LIBRARY}
    ${FMT_LIBRARY}
    ${FOLLY_LIBRARY}
    ${GLOG_LIBRARY}
)
target_link_libraries(test_transactional_graph
    PRIVATE
    ${ARROWFLIGHT_LIBRARY}
    ${ROCKSDB_LIBRARY}
    ${ARROW_LIBRARY}
    ${PARQUET_LIBRARY}
    GTest::GTest
    GTest::Main
    ${SPDLOG_LIBRARY}
    ${FMT_LIBRARY}
    ${FOLLY_LIBRARY}
    ${GLOG_LIBRARY}
)
target_link_libraries(benchmark
    PRIVATE
    ${ARROWFLIGHT_LIBRARY}
    ${ROCKSDB_LIBRARY}
    ${ARROW_LIBRARY}
    ${PARQUET_LIBRARY}
    ${SPDLOG_LIBRARY}
    ${FMT_LIBRARY}
    ${FOLLY_LIBRARY}
    ${GLOG_LIBRARY}
)
target_link_libraries(example_rocks
    PRIVATE
    ${ROCKSDB_LIBRARY}
    ${SPDLOG_LIBRARY}
    ${FMT_LIBRARY}
    ${FOLLY_LIBRARY}
    ${GLOG_LIBRARY}
)
target_link_libraries(test_hermitage
    PRIVATE
    ${ARROWFLIGHT_LIBRARY}
    ${ROCKSDB_LIBRARY}
    ${ARROW_LIBRARY}
    ${PARQUET_LIBRARY}
    GTest::GTest
    GTest::Main
    ${SPDLOG_LIBRARY}
    ${FMT_LIBRARY}
    ${FOLLY_LIBRARY}
    ${GLOG_LIBRARY}
)

# Add compile definitions for Arrow
target_compile_definitions(example_graph PRIVATE ARROW_STATIC)
target_compile_definitions(test_graph PRIVATE ARROW_STATIC)
target_compile_definitions(test_transactional_graph PRIVATE ARROW_STATIC)
target_compile_definitions(benchmark PRIVATE ARROW_STATIC)

# Add RocksDB compile definitions
target_compile_definitions(example_graph PRIVATE ROCKSDB_PLATFORM_POSIX)
target_compile_definitions(test_graph PRIVATE ROCKSDB_PLATFORM_POSIX)
target_compile_definitions(test_transactional_graph PRIVATE ROCKSDB_PLATFORM_POSIX)
target_compile_definitions(benchmark PRIVATE ROCKSDB_PLATFORM_POSIX)
target_compile_definitions(example_rocks PRIVATE ROCKSDB_PLATFORM_POSIX)

# Enable testing
add_test(NAME test_graph COMMAND test_graph)
add_test(NAME test_transactional_graph COMMAND test_transactional_graph)
add_test(NAME test_hermitage COMMAND test_hermitage)