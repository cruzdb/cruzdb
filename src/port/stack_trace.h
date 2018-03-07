#pragma once
namespace cruzdb {

void InstallStackTraceHandler();
void PrintStack(int first_frames_to_skip = 0);

}
