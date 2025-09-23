# MyLib - C++工具库

一个高性能的C++工具库，包含无锁环形缓冲区和智能指针实现。

## 组件

### 1. 无锁RingBuffer
一个高性能的无锁环形缓冲区实现，使用C++17的原子操作保证线程安全。

### 2. 共享指针(SharedPtr)
一个线程安全的智能指针实现，提供自动内存管理和引用计数功能。

## 特性

### RingBuffer特性
- **无锁设计**: 使用原子操作，避免互斥锁开销
- **线程安全**: 支持多生产者多消费者场景
- **高性能**: 基于自旋等待，适合高频率操作
- **模板化**: 支持任意类型的数据
- **非阻塞操作**: 提供tryPush/tryPop方法

### SharedPtr特性
- **自动内存管理**: 自动释放内存，防止内存泄漏
- **引用计数**: 线程安全的引用计数机制
- **移动语义**: 支持高效的移动操作
- **标准兼容**: 接口设计类似std::shared_ptr

## 使用方法

### RingBuffer使用

```cpp
#include "include/ringbuffer.hpp"

// 创建容量为100的int类型ringbuffer
LockFreeRingBuffer<int> buffer(100);

// 阻塞操作
buffer.push(42);
int value = buffer.pop();

// 非阻塞操作
if (buffer.tryPush(42)) {
    // 成功推入
}

int value;
if (buffer.tryPop(value)) {
    // 成功弹出
}

// 状态查询
bool empty = buffer.isEmpty();
bool full = buffer.isFull();
size_t size = buffer.size();
size_t capacity = buffer.capacity();
```

### SharedPtr使用

```cpp
#include "include/shared_ptr.hpp"

// 创建共享指针
SharedPtr<int> ptr1 = makeShared<int>(42);

// 拷贝共享指针
SharedPtr<int> ptr2 = ptr1;

// 获取引用计数
long count = ptr1.useCount(); // 返回2

// 访问对象
int value = *ptr1;
*ptr2 = 100;

// 重置指针
ptr1.reset();

// 检查是否为空
if (ptr2) {
    // 指针不为空
}

// 比较指针
if (ptr1 == ptr2) {
    // 指向同一个对象
}
```

## 编译和运行

```bash
mkdir build
cd build
cmake ..
make

# 测试RingBuffer
./test_ringbuffer

# 测试SharedPtr
./test_shared_ptr

# 运行示例
g++ -std=c++17 -I. example_shared_ptr.cpp -o example && ./example
```

## 注意事项

### RingBuffer注意事项
1. 这是一个单生产者单消费者(SPSC)的无锁实现
2. 对于多生产者多消费者场景，可能需要额外的同步机制
3. 自旋等待可能消耗CPU，适合高频率但短时间的等待场景
4. 容量必须是2的幂次方以获得最佳性能（当前实现支持任意容量）

### SharedPtr注意事项
1. 引用计数使用原子操作，保证线程安全
2. 循环引用会导致内存泄漏，需要使用weak_ptr来打破循环
3. 相比原始指针有一定的性能开销
4. 适合需要共享所有权的场景

## 性能特点

- 无锁设计避免了上下文切换开销
- 使用内存序约束保证数据一致性
- 适合高并发、低延迟的应用场景
- SharedPtr提供RAII自动内存管理
