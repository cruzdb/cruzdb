include_directories(${ZLOG_INCLUDE_DIRS})

add_library(cruzdbjni SHARED
  cruzdbjni.cc
  iterator.cc
  transaction.cc
  portal.h)
set_target_properties(cruzdbjni PROPERTIES
  VERSION 1.0.0
  SOVERSION 1)

set(add_libs cruzdb ${JNI_LIBRARIES})

add_dependencies(cruzdbjni jni_headers)
include_directories(${JNI_INCLUDE_DIRS} ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(cruzdbjni LINK_PRIVATE ${add_libs})
install(TARGETS cruzdbjni DESTINATION ${CMAKE_INSTALL_LIBDIR})
