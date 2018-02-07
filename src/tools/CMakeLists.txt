include_directories(${ZLOG_INCLUDE_DIRS})
add_executable(kvs-export kvs-export.cc)
target_link_libraries(kvs-export cruzdb)

include_directories(${ZLOG_INCLUDE_DIRS})
add_executable(cruzdb_thrash thrash.cc)
target_link_libraries(cruzdb_thrash
  cruzdb
  ${Boost_PROGRAM_OPTIONS_LIBRARY}
  ${Boost_SYSTEM_LIBRARY})
install(TARGETS cruzdb_thrash DESTINATION bin)
