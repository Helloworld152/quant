// 教学版 Rust 原型：边学边理解
// UDP 接收 -> SPSC 无锁环形缓冲 -> 消费者示例 -> 原始抓包文件
// 注释特别标明了 Rust 的所有权、借用、线程和原子操作对应的概念

use std::net::UdpSocket;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::fs::OpenOptions;
use std::io::Write;
use std::env;
use std::mem::MaybeUninit;

const PACKET_MAX: usize = 2048; // 单包最大字节
const RING_CAP: usize = 1 << 16; // 环形缓冲容量（必须为 2 的幂）

// SPSC 无锁环形缓冲教学版
struct SpscRing {
    buf: Vec<MaybeUninit<Vec<u8>>>, // Vec 存放未初始化空间
    cap_mask: usize,
    head: AtomicUsize, // 写索引，生产者线程独占
    tail: AtomicUsize, // 读索引，消费者线程独占
}

impl SpscRing {
    fn new(cap: usize) -> Self {
        assert!(cap.is_power_of_two(), "capacity must be power of two");
        let mut v = Vec::with_capacity(cap);
        for _ in 0..cap { v.push(MaybeUninit::uninit()); }
        SpscRing {
            buf: v,
            cap_mask: cap - 1,
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }

    // 尝试写入数据，如果满返回 Err
    fn try_push(&self, payload: Vec<u8>) -> Result<(), Vec<u8>> {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire); // 保证读取 tail 时的最新值
        if head.wrapping_sub(tail) == self.buf.len() {
            return Err(payload); // 缓冲区满
        }
        let idx = head & self.cap_mask;
        unsafe { self.buf.get_unchecked(idx).as_ptr().write(payload); } // 写入 slot
        self.head.store(head.wrapping_add(1), Ordering::Release); // 更新 head
        Ok(())
    }

    // 尝试读取数据，如果空返回 None
    fn try_pop(&self) -> Option<Vec<u8>> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);
        if head == tail { return None; } // 空
        let idx = tail & self.cap_mask;
        let payload = unsafe { self.buf.get_unchecked(idx).as_ptr().read() };
        self.tail.store(tail.wrapping_add(1), Ordering::Release);
        Some(payload)
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: {} <bind_addr> <raw_log_path>", args[0]);
        return;
    }
    let bind = &args[1];
    let log_path = &args[2];

    // UDP socket，非阻塞
    let sock = UdpSocket::bind(bind).expect("bind failed");
    sock.set_nonblocking(true).expect("cannot set nonblocking");
    println!("Listening on {}", bind);

    // 环形缓冲使用 Arc 共享给线程
    let ring = Arc::new(SpscRing::new(RING_CAP));
    let ring_producer = ring.clone();
    let ring_consumer = ring.clone();

    // 打开原始抓包文件
    let mut raw_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .expect("open log file failed");
    println!("Raw log path: {}", log_path);

    // 网络线程：接收 UDP 包 -> 写环形缓冲 -> 写抓包
    let net_thread = thread::spawn(move || {
        let mut buf = [0u8; PACKET_MAX];
        loop {
            match sock.recv_from(&mut buf) {
                Ok((n, _src)) => {
                    let data = Vec::from(&buf[..n]); // 所有权转移到 data
                    if let Err(e) = raw_file.write_all(&data) {
                        eprintln!("raw write error: {}", e);
                    }
                    if let Err(_payload) = ring_producer.try_push(data) {
                        // 如果满了，数据丢弃（可加报警）
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_micros(50));
                }
                Err(e) => {
                    eprintln!("recv_from error: {}", e);
                    thread::sleep(Duration::from_millis(10));
                }
            }
        }
    });

    // 消费线程：从环形缓冲取数据并处理
    let consumer_thread = thread::spawn(move || {
        let mut cnt: usize = 0;
        let mut last = Instant::now();
        loop {
            let mut local_batch = Vec::with_capacity(1024);
            while let Some(pkt) = ring_consumer.try_pop() {
                // 这里可以解析成业务消息
                local_batch.push(pkt);
                if local_batch.len() >= 1024 { break; }
            }

            if !local_batch.is_empty() {
                cnt += local_batch.len();
            } else {
                thread::sleep(Duration::from_micros(100));
            }

            if last.elapsed() >= Duration::from_secs(1) {
                println!("recv/s ≈ {}", cnt);
                cnt = 0;
                last = Instant::now();
            }
        }
    });

    let _ = net_thread.join();
    let _ = consumer_thread.join();
}

// 学习要点注释：
// - 所有权: Vec<u8> 被网络线程生成，然后 try_push 接管所有权
// - 借用: raw_file 由网络线程独占，所以不用 &mut borrow 跨线程
// - Arc: 环形缓冲共享给多个线程
// - AtomicUsize + Ordering: 确保无锁同步可见性
// - spawn + move: 线程拥有必要资源，避免悬空引用
