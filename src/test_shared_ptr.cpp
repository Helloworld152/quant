#include "shared_ptr.hpp"
#include <iostream>
#include <vector>
#include <thread>
#include <cassert>

// 测试用的简单类
class TestObject {
public:
    TestObject(int value) : value_(value) {
        std::cout << "TestObject(" << value_ << ") 构造\n";
    }
    
    ~TestObject() {
        std::cout << "TestObject(" << value_ << ") 析构\n";
    }
    
    int getValue() const { return value_; }
    void setValue(int value) { value_ = value; }

private:
    int value_;
};

void testBasicFunctionality() {
    std::cout << "\n=== 基本功能测试 ===\n";
    
    // 创建共享指针
    SharedPtr<TestObject> ptr1 = makeShared<TestObject>(42);
    std::cout << "ptr1 引用计数: " << ptr1.useCount() << std::endl;
    assert(ptr1.useCount() == 1);
    assert(ptr1->getValue() == 42);
    
    // 拷贝构造
    SharedPtr<TestObject> ptr2(ptr1);
    std::cout << "拷贝后 ptr1 引用计数: " << ptr1.useCount() << std::endl;
    std::cout << "拷贝后 ptr2 引用计数: " << ptr2.useCount() << std::endl;
    assert(ptr1.useCount() == 2);
    assert(ptr2.useCount() == 2);
    
    // 赋值操作
    SharedPtr<TestObject> ptr3;
    ptr3 = ptr1;
    std::cout << "赋值后引用计数: " << ptr1.useCount() << std::endl;
    assert(ptr1.useCount() == 3);
    
    // 测试解引用和成员访问
    (*ptr1).setValue(100);
    assert(ptr2->getValue() == 100);
    assert(ptr3->getValue() == 100);
    
    // 测试比较操作
    assert(ptr1 == ptr2);
    assert(ptr1 == ptr3);
    
    SharedPtr<TestObject> ptr4;
    assert(ptr4 == nullptr);
    assert(ptr1 != nullptr);
}

void testMoveSemantics() {
    std::cout << "\n=== 移动语义测试 ===\n";
    
    SharedPtr<TestObject> ptr1 = makeShared<TestObject>(200);
    std::cout << "移动前 ptr1 引用计数: " << ptr1.useCount() << std::endl;
    
    SharedPtr<TestObject> ptr2 = std::move(ptr1);
    std::cout << "移动后 ptr1 引用计数: " << ptr1.useCount() << std::endl;
    std::cout << "移动后 ptr2 引用计数: " << ptr2.useCount() << std::endl;
    
    assert(ptr1.get() == nullptr);
    assert(ptr2.useCount() == 1);
    assert(ptr2->getValue() == 200);
}

void testReset() {
    std::cout << "\n=== Reset 功能测试 ===\n";
    
    SharedPtr<TestObject> ptr1 = makeShared<TestObject>(300);
    SharedPtr<TestObject> ptr2 = ptr1;
    
    std::cout << "reset前引用计数: " << ptr1.useCount() << std::endl;
    ptr1.reset();
    std::cout << "ptr1 reset后 ptr2 引用计数: " << ptr2.useCount() << std::endl;
    
    assert(ptr1.get() == nullptr);
    assert(ptr2.useCount() == 1);
    assert(ptr2->getValue() == 300);
    
    // 重置为新对象
    ptr1.reset(new TestObject(400));
    assert(ptr1.useCount() == 1);
    assert(ptr1->getValue() == 400);
}

void testThreadSafety() {
    std::cout << "\n=== 线程安全测试 ===\n";
    
    SharedPtr<TestObject> sharedPtr = makeShared<TestObject>(500);
    const int numThreads = 10;
    const int numOperations = 1000;
    
    std::vector<std::thread> threads;
    
    // 启动多个线程进行拷贝和赋值操作
    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back([&sharedPtr, numOperations]() {
            for (int j = 0; j < numOperations; ++j) {
                SharedPtr<TestObject> localPtr = sharedPtr;
                // 进行一些操作
                if (localPtr) {
                    int value = localPtr->getValue();
                    (void)value; // 避免未使用变量警告
                }
            }
        });
    }
    
    // 等待所有线程完成
    for (auto& thread : threads) {
        thread.join();
    }
    
    std::cout << "线程安全测试完成，最终引用计数: " << sharedPtr.useCount() << std::endl;
    assert(sharedPtr.useCount() == 1);
}

void testSwap() {
    std::cout << "\n=== Swap 功能测试 ===\n";
    
    SharedPtr<TestObject> ptr1 = makeShared<TestObject>(600);
    SharedPtr<TestObject> ptr2 = makeShared<TestObject>(700);
    
    int value1 = ptr1->getValue();
    int value2 = ptr2->getValue();
    
    ptr1.swap(ptr2);
    
    assert(ptr1->getValue() == value2);
    assert(ptr2->getValue() == value1);
    std::cout << "Swap 测试通过\n";
}

int main() {
    try {
        testBasicFunctionality();
        testMoveSemantics();
        testReset();
        testThreadSafety();
        testSwap();
        
        std::cout << "\n=== 所有测试通过! ===\n";
    } catch (const std::exception& e) {
        std::cerr << "测试失败: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
