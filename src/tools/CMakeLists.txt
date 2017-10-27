include_directories(${ZLOG_INCLUDE_DIRS})
add_executable(kvs-export kvs-export.cc)
target_link_libraries(kvs-export cruzdb)
