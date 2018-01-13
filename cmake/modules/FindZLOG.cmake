include(UseJava)

find_path(ZLOG_INCLUDE_DIR NAMES  zlog/log.h PATHS "${ZLOG_INSTALL_DIR}/include")
find_library(ZLOG_LIBRARY NAMES zlog PATHS "${ZLOG_INSTALL_DIR}/lib")
find_jar(ZLOG_JAVA zlog PATHS "${ZLOG_INSTALL_DIR}/share/java")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ZLOG DEFAULT_MSG ZLOG_INCLUDE_DIR ZLOG_LIBRARY)

if(ZLOG_FOUND)
  set(ZLOG_LIBRARIES ${ZLOG_LIBRARY})
  set(ZLOG_INCLUDE_DIRS ${ZLOG_INCLUDE_DIR})
endif()

mark_as_advanced(ZLOG_INCLUDE_DIR ZLOG_LIBRARY)
