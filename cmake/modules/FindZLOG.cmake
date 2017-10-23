find_path(ZLOG_INCLUDE_DIR NAMES  zlog/log.h PATHS "$ENV{ZLOG_DIR}/include")
find_library(ZLOG_LIBRARY NAMES zlog PATHS "$ENV{ZLOG_DIR}/lib" )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ZLOG DEFAULT_MSG ZLOG_INCLUDE_DIR ZLOG_LIBRARY)

if(ZLOG_FOUND)
  set(ZLOG_LIBRARIES ${ZLOG_LIBRARY})
  set(ZLOG_INCLUDE_DIRS ${ZLOG_INCLUDE_DIR})
endif()

mark_as_advanced(ZLOG_INCLUDE_DIR ZLOG_LIBRARY)
