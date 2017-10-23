PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS kvstore.proto)

include_directories(${ZLOG_INCLUDE_DIRS})

add_library(libkvstore SHARED
  db_impl.cc
  transaction_impl.cc
  node_cache.cc
  iterator_impl.cc
  node.cc
  ${PROTO_SRCS})
target_include_directories(libkvstore
  PUBLIC ${PROTOBUF_INCLUDE_DIR})
target_link_libraries(libkvstore
  ${ZLOG_LIBRARIES}
  ${PROTOBUF_LIBRARIES})
set_target_properties(libkvstore PROPERTIES
  OUTPUT_NAME cruzdb
  VERSION 1.0.0
  SOVERSION 1)
install(TARGETS libkvstore LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

add_executable(thrash thrash.cc)
target_link_libraries(thrash libkvstore)

add_executable(rbt rbt.cc)
target_link_libraries(rbt libkvstore)

add_executable(bench bench.cc)
target_link_libraries(bench libkvstore)
if (CMAKE_BUILD_TYPE STREQUAL "Coverage")
  setup_target_for_coverage(bench-cov bench coverage "/tmp/zlog-db;10000")
endif()

add_executable(zlog_test_kvstore test_kvstore.cc)
target_link_libraries(zlog_test_kvstore
  libkvstore
  gtest)
install(TARGETS zlog_test_kvstore DESTINATION bin)

if (CMAKE_BUILD_TYPE STREQUAL "Coverage")
  setup_target_for_coverage(zlog_test_kvstore_coverage
    zlog_test_kvstore coverage)
endif()
