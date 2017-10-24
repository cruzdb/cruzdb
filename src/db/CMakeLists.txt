PROTOBUF_GENERATE_CPP(PROTO_SRCS PROTO_HDRS cruzdb.proto)

include_directories(${ZLOG_INCLUDE_DIRS})

add_library(cruzdb SHARED
  db_impl.cc
  transaction_impl.cc
  node_cache.cc
  iterator_impl.cc
  node.cc
  ${PROTO_SRCS})
target_include_directories(cruzdb
  PUBLIC ${PROTOBUF_INCLUDE_DIR})
target_link_libraries(cruzdb
  ${ZLOG_LIBRARIES}
  ${PROTOBUF_LIBRARIES})
set_target_properties(cruzdb PROPERTIES
  OUTPUT_NAME cruzdb
  VERSION 1.0.0
  SOVERSION 1)
install(TARGETS cruzdb LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

add_executable(thrash thrash.cc)
target_link_libraries(thrash cruzdb)

add_executable(rbt rbt.cc)
target_link_libraries(rbt cruzdb)

add_executable(bench bench.cc)
target_link_libraries(bench cruzdb)
if (CMAKE_BUILD_TYPE STREQUAL "Coverage")
  setup_target_for_coverage(bench-cov bench coverage "/tmp/cruzdb-db;10000")
endif()

add_executable(cruzdb_test_db test_db.cc)
target_link_libraries(cruzdb_test_db
  cruzdb
  gtest)
install(TARGETS cruzdb_test_db DESTINATION bin)

if (CMAKE_BUILD_TYPE STREQUAL "Coverage")
  setup_target_for_coverage(cruzdb_test_db_coverage
    cruzdb_test_db coverage)
endif()
