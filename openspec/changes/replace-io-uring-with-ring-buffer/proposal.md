## Why
The io_uring-based message passing approach has proven fundamentally flawed. Despite extensive testing with multiple configurations (NOP, READV, WRITEV operations, various setup flags), io_uring operations are submitted successfully but generate zero completions. This prevents end-to-end IPC communication and blocks achieving the target 1.4 GB/s throughput (35 MB payloads at 40 Hz).

## What Changes
- **REPLACE** io_uring-based message queue with Shared Memory Ring Buffer
- **IMPLEMENT** atomic operations for lock-free synchronization
- **DESIGN** 1:N pub/sub with single producer, multiple consumers
- **ACHIEVE** zero-copy message passing for 35 MB payloads at 40 Hz
- **SUPPORT** 1 publisher + multiple subscribers (at least 6) with minimal contention

## Impact
- **Breaking change**: Complete architecture replacement
- **Affected code**: Core Rust implementation in src/lib.rs
- **Expected performance**: 1.4 GB/s sustained throughput (35 MB × 40 Hz)
- **Benefits**: Zero-copy, no syscalls after setup, scalable to multiple subscribers