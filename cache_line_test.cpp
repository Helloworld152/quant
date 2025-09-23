// 缓存行性能测试：比较同一缓存行和不同缓存行的原子变量访问性能
// 这个测试展示了 False Sharing 对性能的影响

#include <atomic>
#include <thread>
#include <chrono>
#include <iostream>
#include <memory>


// 同一缓存行的两个原子变量（会导致 False Sharing）
struct alignas(64) SameCacheLine {
    std::atomic<size_t> counter1{0};
    std::atomic<size_t> counter2{0};
    // 填充到 128 字节，确保两个变量在同一缓存行
    char padding[128 - 16]; // 两个 atomic<size_t> 各占 8 字节
};

// 不同缓存行的两个原子变量（避免 False Sharing）
struct alignas(64) DifferentCacheLine {
    alignas(64) std::atomic<size_t> counter1{0};
    alignas(64) std::atomic<size_t> counter2{0};
};

// 测试同一缓存行的性能
void test_same_cache_line() {
    auto data = std::make_unique<SameCacheLine>();
    auto* counter1 = &data->counter1;
    auto* counter2 = &data->counter2;
    
    auto start = std::chrono::high_resolution_clock::now();
    auto end_time = start + std::chrono::seconds(3);
    
    // 启动两个线程，分别递增不同的原子变量
    std::thread thread1([counter1, end_time]() {
        while (std::chrono::high_resolution_clock::now() < end_time) {
            counter1->fetch_add(1, std::memory_order_relaxed);
        }
    });
    
    std::thread thread2([counter2, end_time]() {
        while (std::chrono::high_resolution_clock::now() < end_time) {
            counter2->fetch_add(1, std::memory_order_relaxed);
        }
    });
    
    thread1.join();
    thread2.join();
    
    auto actual_end = std::chrono::high_resolution_clock::now();
    auto elapsed = actual_end - start;
    
    // 获取实际的操作数
    size_t count1 = counter1->load(std::memory_order_relaxed);
    size_t count2 = counter2->load(std::memory_order_relaxed);
    size_t total_count = count1 + count2;
    
    double elapsed_seconds = std::chrono::duration<double>(elapsed).count();
    
    std::cout << "同一缓存行 (False Sharing) 测试结果:" << std::endl;
    std::cout << "  总操作数: " << total_count << std::endl;
    std::cout << "  线程1操作数: " << count1 << std::endl;
    std::cout << "  线程2操作数: " << count2 << std::endl;
    std::cout << "  总耗时: " << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() << "ms" << std::endl;
    std::cout << "  每秒操作数: " << static_cast<size_t>(total_count / elapsed_seconds) << std::endl;
    std::cout << "  平均每次操作耗时: " << (elapsed_seconds * 1e9 / total_count) << "ns" << std::endl;
    std::cout << std::endl;
}

// 测试不同缓存行的性能
void test_different_cache_line() {
    auto data = std::make_unique<DifferentCacheLine>();
    auto* counter1 = &data->counter1;
    auto* counter2 = &data->counter2;
    
    auto start = std::chrono::high_resolution_clock::now();
    auto end_time = start + std::chrono::seconds(3);
    
    // 启动两个线程，分别递增不同的原子变量
    std::thread thread1([counter1, end_time]() {
        while (std::chrono::high_resolution_clock::now() < end_time) {
            counter1->fetch_add(1, std::memory_order_relaxed);
        }
    });
    
    std::thread thread2([counter2, end_time]() {
        while (std::chrono::high_resolution_clock::now() < end_time) {
            counter2->fetch_add(1, std::memory_order_relaxed);
        }
    });
    
    thread1.join();
    thread2.join();
    
    auto actual_end = std::chrono::high_resolution_clock::now();
    auto elapsed = actual_end - start;
    
    // 获取实际的操作数
    size_t count1 = counter1->load(std::memory_order_relaxed);
    size_t count2 = counter2->load(std::memory_order_relaxed);
    size_t total_count = count1 + count2;
    
    double elapsed_seconds = std::chrono::duration<double>(elapsed).count();
    
    std::cout << "不同缓存行 (避免 False Sharing) 测试结果:" << std::endl;
    std::cout << "  总操作数: " << total_count << std::endl;
    std::cout << "  线程1操作数: " << count1 << std::endl;
    std::cout << "  线程2操作数: " << count2 << std::endl;
    std::cout << "  总耗时: " << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() << "ms" << std::endl;
    std::cout << "  每秒操作数: " << static_cast<size_t>(total_count / elapsed_seconds) << std::endl;
    std::cout << "  平均每次操作耗时: " << (elapsed_seconds * 1e9 / total_count) << "ns" << std::endl;
    std::cout << std::endl;
}

int main() {
    std::cout << "缓存行性能测试" << std::endl;
    std::cout << "================" << std::endl;
    std::cout << "测试两个线程分别递增不同的原子变量" << std::endl;
    std::cout << "比较同一缓存行和不同缓存行的性能差异" << std::endl;
    std::cout << std::endl;
    
    // 测试同一缓存行（False Sharing）
    test_same_cache_line();
    
    // 测试不同缓存行（避免 False Sharing）
    std::cout << "开始测试不同缓存行..." << std::endl;
    std::cout.flush();
    test_different_cache_line();
    std::cout << "不同缓存行测试完成" << std::endl;
    
    std::cout << "测试说明:" << std::endl;
    std::cout << "- 同一缓存行测试中，两个原子变量位于同一缓存行" << std::endl;
    std::cout << "- 当一个线程修改一个变量时，会使得另一个线程的缓存行失效" << std::endl;
    std::cout << "- 这导致频繁的缓存同步，降低性能" << std::endl;
    std::cout << "- 不同缓存行测试中，两个原子变量位于不同的缓存行" << std::endl;
    std::cout << "- 避免了 False Sharing，性能应该明显更好" << std::endl;
    
    return 0;
}