#ifndef SHARED_PTR_HPP
#define SHARED_PTR_HPP

#include <atomic>
#include <utility>

template <typename T>
class SharedPtr {
public:
    // 默认构造函数
    SharedPtr() noexcept : ptr_(nullptr), control_block_(nullptr) {}
    
    // 从原始指针构造
    explicit SharedPtr(T* ptr) : ptr_(ptr) {
        if (ptr_) {
            control_block_ = new ControlBlock(ptr_);
        } else {
            control_block_ = nullptr;
        }
    }
    
    // 拷贝构造函数
    SharedPtr(const SharedPtr& other) noexcept : ptr_(other.ptr_), control_block_(other.control_block_) {
        if (control_block_) {
            control_block_->addRef();
        }
    }
    
    // 移动构造函数
    SharedPtr(SharedPtr&& other) noexcept : ptr_(other.ptr_), control_block_(other.control_block_) {
        other.ptr_ = nullptr;
        other.control_block_ = nullptr;
    }
    
    // 析构函数
    ~SharedPtr() {
        reset();
    }
    
    // 拷贝赋值操作符
    SharedPtr& operator=(const SharedPtr& other) noexcept {
        if (this != &other) {
            reset();
            ptr_ = other.ptr_;
            control_block_ = other.control_block_;
            if (control_block_) {
                control_block_->addRef();
            }
        }
        return *this;
    }
    
    // 移动赋值操作符
    SharedPtr& operator=(SharedPtr&& other) noexcept {
        if (this != &other) {
            reset();
            ptr_ = other.ptr_;
            control_block_ = other.control_block_;
            other.ptr_ = nullptr;
            other.control_block_ = nullptr;
        }
        return *this;
    }
    
    // 解引用操作符
    T& operator*() const noexcept {
        return *ptr_;
    }
    
    // 成员访问操作符
    T* operator->() const noexcept {
        return ptr_;
    }
    
    // 获取原始指针
    T* get() const noexcept {
        return ptr_;
    }
    
    // 检查是否为空
    explicit operator bool() const noexcept {
        return ptr_ != nullptr;
    }
    
    // 获取引用计数
    long useCount() const noexcept {
        return control_block_ ? control_block_->getRefCount() : 0;
    }
    
    // 检查是否唯一
    bool unique() const noexcept {
        return useCount() == 1;
    }
    
    // 重置指针
    void reset() noexcept {
        if (control_block_) {
            control_block_->release();
            if (control_block_->getRefCount() == 0) {
                delete control_block_;
            }
        }
        ptr_ = nullptr;
        control_block_ = nullptr;
    }
    
    // 重置为新指针
    void reset(T* ptr) {
        SharedPtr temp(ptr);
        *this = std::move(temp);
    }
    
    // 交换
    void swap(SharedPtr& other) noexcept {
        std::swap(ptr_, other.ptr_);
        std::swap(control_block_, other.control_block_);
    }

private:
    // 控制块，管理引用计数和对象生命周期
    struct ControlBlock {
        std::atomic<long> ref_count_;
        T* ptr_;
        
        explicit ControlBlock(T* ptr) : ref_count_(1), ptr_(ptr) {}
        
        ~ControlBlock() {
            delete ptr_;
        }
        
        void addRef() {
            ref_count_.fetch_add(1, std::memory_order_relaxed);
        }
        
        void release() {
            ref_count_.fetch_sub(1, std::memory_order_acq_rel);
        }
        
        long getRefCount() const {
            return ref_count_.load(std::memory_order_acquire);
        }
    };
    
    T* ptr_;
    ControlBlock* control_block_;
};

// 比较操作符
template <typename T, typename U>
bool operator==(const SharedPtr<T>& lhs, const SharedPtr<U>& rhs) noexcept {
    return lhs.get() == rhs.get();
}

template <typename T, typename U>
bool operator!=(const SharedPtr<T>& lhs, const SharedPtr<U>& rhs) noexcept {
    return !(lhs == rhs);
}

template <typename T>
bool operator==(const SharedPtr<T>& lhs, std::nullptr_t) noexcept {
    return !lhs;
}

template <typename T>
bool operator==(std::nullptr_t, const SharedPtr<T>& rhs) noexcept {
    return !rhs;
}

template <typename T>
bool operator!=(const SharedPtr<T>& lhs, std::nullptr_t) noexcept {
    return static_cast<bool>(lhs);
}

template <typename T>
bool operator!=(std::nullptr_t, const SharedPtr<T>& rhs) noexcept {
    return static_cast<bool>(rhs);
}

// make_shared 工厂函数
template <typename T, typename... Args>
SharedPtr<T> makeShared(Args&&... args) {
    return SharedPtr<T>(new T(std::forward<Args>(args)...));
}

// swap 函数
template <typename T>
void swap(SharedPtr<T>& lhs, SharedPtr<T>& rhs) noexcept {
    lhs.swap(rhs);
}

#endif
