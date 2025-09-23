#include "../include/ringbuffer.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>

void producer(LockFreeRingBuffer<int>& buffer, int id, int count) {
    for (int i = 0; i < count; ++i) {
        int value = id * 1000 + i;
        auto now = std::chrono::high_resolution_clock::now();
        auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
        std::cout << "生产者" << id << " 在 " << timestamp << " 推入 " << value << std::endl;
        buffer.push(value);
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
}

void consumer(LockFreeRingBuffer<int>& buffer, int& consumed_count) {
    int value;
    while (buffer.tryPop(value)) {
        ++consumed_count;
        auto now = std::chrono::high_resolution_clock::now();
        auto timestamp = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
        std::cout << "消费者 在 " << timestamp << " 消费 " << value << std::endl;
        std::this_thread::sleep_for(std::chrono::microseconds(2));
    }
}

int main() {
    const size_t capacity = 10;
    LockFreeRingBuffer<int> buffer(capacity);
    
    std::cout << "无锁RingBuffer测试" << std::endl;
    std::cout << "容量: " << buffer.capacity() << std::endl;
    
    // 基本功能测试
    std::cout << "\n=== 基本功能测试 ===" << std::endl;
    
    // 测试空状态
    std::cout << "初始状态 - 空: " << buffer.isEmpty() << ", 满: " << buffer.isFull() 
              << ", 大小: " << buffer.size() << std::endl;
    
    // 测试push
    for (int i = 1; i <= 5; ++i) {
        buffer.push(i);
        std::cout << "Push " << i << " - 大小: " << buffer.size() 
                  << ", 空: " << buffer.isEmpty() << ", 满: " << buffer.isFull() << std::endl;
    }
    
    // 测试pop
    std::cout << "\n开始pop操作:" << std::endl;
    for (int i = 0; i < 3; ++i) {
        int value = buffer.pop();
        std::cout << "Pop: " << value << " - 大小: " << buffer.size() << std::endl;
    }
    
    // 测试tryPush和tryPop
    std::cout << "\n=== 非阻塞操作测试 ===" << std::endl;
    
    // 填满缓冲区
    int pushed = 0;
    while (buffer.tryPush(++pushed)) {
        std::cout << "TryPush成功: " << pushed << std::endl;
    }
    std::cout << "缓冲区已满，TryPush失败" << std::endl;
    
    // 清空缓冲区
    int value;
    while (buffer.tryPop(value)) {
        std::cout << "TryPop成功: " << value << std::endl;
    }
    std::cout << "缓冲区已空，TryPop失败" << std::endl;
    
    // 多线程测试
    std::cout << "\n=== 多线程测试 ===" << std::endl;
    
    const int num_producers = 2;
    const int items_per_producer = 5;
    const int total_items = num_producers * items_per_producer;
    
    std::vector<std::thread> producers;
    std::vector<std::thread> consumers;
    
    int consumed_count = 0;
    
    // 启动生产者线程
    for (int i = 0; i < num_producers; ++i) {
        producers.emplace_back(producer, std::ref(buffer), i, items_per_producer);
    }
    
    // 启动消费者线程
    for (int i = 0; i < 2; ++i) {
        consumers.emplace_back(consumer, std::ref(buffer), std::ref(consumed_count));
    }
    
    // 等待生产者完成
    for (auto& t : producers) {
        t.join();
    }
    
    // 等待消费者完成
    for (auto& t : consumers) {
        t.join();
    }
    
    std::cout << "多线程测试完成，总共消费了 " << consumed_count << " 个项目" << std::endl;
    
    return 0;
}
