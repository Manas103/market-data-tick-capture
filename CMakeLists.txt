cmake_minimum_required(VERSION 3.15)
project(market_data_tick_capture CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-O2 -Wall -Wextra)

include_directories(include)

find_package(Threads REQUIRED)

# The multicast layer uses POSIX sockets (IP_ADD_MEMBERSHIP) and the capture
# reader uses pread(), so this builds on Linux/macOS rather than everywhere.
# That is a deliberate choice, not an oversight -- see the README.
if(WIN32)
  message(FATAL_ERROR "This project targets POSIX (built and tested on Linux). "
                      "See README.md for what would need abstracting for Windows.")
endif()

add_executable(live_capture     src/live_capture.cpp)
add_executable(replay_verify    src/replay_verify.cpp)
add_executable(capture_bench    src/capture_bench.cpp)
add_executable(capture_dump     src/capture_dump.cpp)
add_executable(correctness_test tests/correctness_test.cpp)

target_link_libraries(live_capture     Threads::Threads)
target_link_libraries(replay_verify    Threads::Threads)
target_link_libraries(capture_bench    Threads::Threads)
target_link_libraries(capture_dump     Threads::Threads)
target_link_libraries(correctness_test Threads::Threads)

enable_testing()
add_test(NAME correctness COMMAND correctness_test)
