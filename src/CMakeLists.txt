include(GNUInstallDirs)

# fixup embedded library paths
SET(CMAKE_SKIP_BUILD_RPATH FALSE)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
LIST(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES
  "${CMAKE_INSTALL_FULL_LIBDIR}" isSystemDir)
IF("${isSystemDir}" STREQUAL "-1")
  SET(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_FULL_LIBDIR}")
ENDIF("${isSystemDir}" STREQUAL "-1")

################################################################################

include_directories(${CMAKE_BINARY_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR}/src)
include_directories(${CMAKE_SOURCE_DIR}/src/include)
include_directories(${CMAKE_SOURCE_DIR}/src/rapidjson/include)
include_directories(${CMAKE_SOURCE_DIR}/src/spdlog/include)

################################################################################

set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL
  "Disable Google benchmark tests")
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL
  "Disable testing of Google benchmark")
set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL
  "Disable installing of Google benchmark")
add_subdirectory(benchmark)
add_subdirectory(googletest/googlemock)

add_subdirectory(include)
add_subdirectory(port)
add_subdirectory(db)
add_subdirectory(tools)

add_library(cruzdb SHARED
  db/db_impl.cc
  db/transaction_impl.cc
  db/node_cache.cc
  db/iterator_impl.cc
  db/node.cc
  db/persistent_tree.cc
  db/db.cc
  db/entry_service.cc
  $<TARGET_OBJECTS:cruzdb_pb>
  port/port_posix.cc
  util/random.cc
  util/thread_local.cc
  monitoring/statistics.cc
  monitoring/histogram.cc
  civetweb/src/civetweb.c
  civetweb/src/CivetServer.cpp)
target_include_directories(cruzdb
  PUBLIC ${PROTOBUF_INCLUDE_DIR}
  PUBLIC ${ZLOG_INCLUDE_DIRS}
  PUBLIC "${PROJECT_SOURCE_DIR}/src/civetweb/include")
target_link_libraries(cruzdb
  ${ZLOG_LIBRARIES}
  ${PROTOBUF_LIBRARIES})
set_target_properties(cruzdb PROPERTIES
  OUTPUT_NAME cruzdb
  VERSION 1.0.0
  SOVERSION 1)
install(TARGETS cruzdb LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

option(WITH_JNI "build with JNI" OFF)
if(WITH_JNI)
  message(STATUS "JNI library is enabled")
  add_subdirectory(java)
else()
  message(STATUS "JNI library is disabled")
endif(WITH_JNI)
