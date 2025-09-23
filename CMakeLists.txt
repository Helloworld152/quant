cmake_minimum_required(VERSION 3.10)
project(LockFreeRingBuffer)

# 设置C++标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 包含头文件目录
include_directories(include)

# 创建测试可执行文件
add_executable(test_ringbuffer src/test_ringbuffer.cpp)
add_executable(test_shared_ptr src/test_shared_ptr.cpp)

# 设置编译选项
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    target_compile_options(test_ringbuffer PRIVATE -Wall -Wextra -O2)
    target_compile_options(test_shared_ptr PRIVATE -Wall -Wextra -O2)
endif()

# 链接线程库
find_package(Threads REQUIRED)
target_link_libraries(test_ringbuffer Threads::Threads)
target_link_libraries(test_shared_ptr Threads::Threads)
