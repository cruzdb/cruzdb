add_library(stack_trace STATIC stack_trace.cc)
target_link_libraries(stack_trace unwind)
