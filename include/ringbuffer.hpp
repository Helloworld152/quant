#ifndef RINGBUFFER_HPP
#define RINGBUFFER_HPP

#include <vector>
#include <atomic>
#include <thread>

template <typename T>
class LockFreeRingBuffer {
public:
    explicit LockFreeRingBuffer(size_t capacity);
    ~LockFreeRingBuffer() = default;
    
    // 禁用拷贝构造和赋值
    LockFreeRingBuffer(const LockFreeRingBuffer&) = delete;
    LockFreeRingBuffer& operator=(const LockFreeRingBuffer&) = delete;
    
    // 非阻塞操作
    bool tryPush(const T& value);
    bool tryPop(T& value);
    
    // 阻塞操作（自旋等待）
    void push(const T& value);
    T pop();
    
    // 状态查询
    bool isEmpty() const;
    bool isFull() const;
    size_t size() const;
    size_t capacity() const;

private:
    std::vector<T> buffer_;
    const size_t capacity_;
    std::atomic<size_t> head_{0};
    std::atomic<size_t> tail_{0};
    
    // 辅助函数
    size_t nextIndex(size_t current) const;
};

// 模板实现
template <typename T>
LockFreeRingBuffer<T>::LockFreeRingBuffer(size_t capacity) 
    : capacity_(capacity) {
    buffer_.resize(capacity_);
}

template <typename T>
bool LockFreeRingBuffer<T>::tryPush(const T& value) {
    size_t currentTail = tail_.load(std::memory_order_relaxed);
    size_t nextTail = nextIndex(currentTail);
    
    // 检查是否已满
    if (nextTail == head_.load(std::memory_order_acquire)) {
        return false;
    }
    
    buffer_[currentTail] = value;
    tail_.store(nextTail, std::memory_order_release);
    return true;
}

template <typename T>
bool LockFreeRingBuffer<T>::tryPop(T& value) {
    size_t currentHead = head_.load(std::memory_order_relaxed);
    
    // 检查是否为空
    if (currentHead == tail_.load(std::memory_order_acquire)) {
        return false;
    }
    
    value = buffer_[currentHead];
    head_.store(nextIndex(currentHead), std::memory_order_release);
    return true;
}

template <typename T>
void LockFreeRingBuffer<T>::push(const T& value) {
    while (!tryPush(value)) {
        // 自旋等待
        std::this_thread::yield();
    }
}

template <typename T>
T LockFreeRingBuffer<T>::pop() {
    T value;
    while (!tryPop(value)) {
        // 自旋等待
        std::this_thread::yield();
    }
    return value;
}

template <typename T>
bool LockFreeRingBuffer<T>::isEmpty() const {
    return head_.load(std::memory_order_acquire) == 
           tail_.load(std::memory_order_acquire);
}

template <typename T>
bool LockFreeRingBuffer<T>::isFull() const {
    size_t currentTail = tail_.load(std::memory_order_acquire);
    return nextIndex(currentTail) == head_.load(std::memory_order_acquire);
}

template <typename T>
size_t LockFreeRingBuffer<T>::size() const {
    size_t currentTail = tail_.load(std::memory_order_acquire);
    size_t currentHead = head_.load(std::memory_order_acquire);
    
    if (currentTail >= currentHead) {
        return currentTail - currentHead;
    } else {
        return capacity_ - currentHead + currentTail;
    }
}

template <typename T>
size_t LockFreeRingBuffer<T>::capacity() const {
    return capacity_;
}

template <typename T>
size_t LockFreeRingBuffer<T>::nextIndex(size_t current) const {
    return (current + 1) % capacity_;
}

#endif