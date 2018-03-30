include_directories(${ZLOG_INCLUDE_DIRS})

PROTOBUF_GENERATE_CPP(DB_PROTO_SRCS DB_PROTO_HDRS cruzdb.proto)
add_library(cruzdb_pb OBJECT ${DB_PROTO_SRCS})
set_property(TARGET cruzdb_pb
  PROPERTY POSITION_INDEPENDENT_CODE ON)

add_executable(thrash thrash.cc)
target_link_libraries(thrash cruzdb)

add_executable(bench bench.cc)
target_link_libraries(bench cruzdb stack_trace)
if (CMAKE_BUILD_TYPE STREQUAL "Coverage")
  setup_target_for_coverage(bench-cov bench coverage "/tmp/cruzdb-db;10000")
endif()

add_executable(cruzdb_test_db test_db.cc)
target_link_libraries(cruzdb_test_db
  cruzdb
  stack_trace
  gtest)
install(TARGETS cruzdb_test_db DESTINATION bin)

if (CMAKE_BUILD_TYPE STREQUAL "Coverage")
  setup_target_for_coverage(cruzdb_test_db_coverage
    cruzdb_test_db coverage)
endif()

add_executable(cruzdb_test_ptree ptree_test.cc)
target_link_libraries(cruzdb_test_ptree
  gtest_main)
install(TARGETS cruzdb_test_ptree DESTINATION bin)

add_executable(tree_bench tree_bench.cc)
target_link_libraries(tree_bench benchmark)

add_executable(ptree2 ptree_test2.cc)
target_link_libraries(ptree2)
install(TARGETS ptree2 DESTINATION bin)
