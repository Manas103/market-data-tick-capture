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

# Gateway extension: A/B line arbitration + TCP gap recovery, OUCH-style
# order entry with sequenced resend on reconnect, and a pinned-core replay
# throughput benchmark (see README, "Extended: Equity Exchange Connectivity
# Gateway").
add_executable(live_capture_ab  src/live_capture_ab.cpp)
add_executable(replay_bench     src/replay_bench.cpp)
add_executable(ouch_session_test tests/ouch_session_test.cpp)

target_link_libraries(live_capture_ab   Threads::Threads)
target_link_libraries(replay_bench      Threads::Threads)
target_link_libraries(ouch_session_test Threads::Threads)

# Typed predicate query path extension (see README, "Extended: Typed
# Predicate Query Path"): exports a capture to pcap for the tshark-plus-grep
# baseline, and the typed columnar query path it is measured against.
add_executable(tcap_to_pcap src/tcap_to_pcap.cpp)
add_executable(tcap_query   src/tcap_query.cpp)
target_link_libraries(tcap_to_pcap Threads::Threads)
target_link_libraries(tcap_query   Threads::Threads)

# Multi-venue order router extension (see README, "Extended: Multi-Venue
# Order Router with Pre-Trade Risk"): best-price routing across 3 simulated
# venues, inline pre-trade risk checks, and the demo/measurement binary
# that drives both.
add_executable(order_router_bench src/order_router_bench.cpp)
add_executable(order_router_test  tests/order_router_test.cpp)
target_link_libraries(order_router_bench Threads::Threads)
target_link_libraries(order_router_test  Threads::Threads)

enable_testing()
add_test(NAME correctness COMMAND correctness_test)
add_test(NAME ouch_session COMMAND ouch_session_test)
add_test(NAME order_router COMMAND order_router_test)
