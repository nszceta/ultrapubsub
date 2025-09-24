## Why
The current ultrapubsub implementation is fundamentally flawed - it uses `IORING_OP_WRITE` to stdout instead of real inter-process communication (IPC). Performance measurements (~0.003ms per message) are meaningless stdout write speeds, not actual IPC. The vendor/io-uring-ipc implementation demonstrates correct patterns achieving ~270ns real IPC latency.

## What Changes
- **BREAKING**: Complete rewrite of core Rust implementation to use proper io_uring IPC patterns
- Replace `MessageQueueHeader` with `SharedMemoryPool` using bitmap-based block allocation
- Implement `HringAddr` (64-bit) for memory references instead of direct data copying
- Use `IORING_OP_NOP` operations to send shared memory references (not data)
- Add proper multi-process architecture with fork/exec and ring sharing
- Implement shared memory management with `/dev/shm/` naming conventions
- Add bitmap-based memory pool allocation system like vendor implementation
- Implement process coordination using `pidfd_getfd()` for cross-process ring sharing

## Impact
- **Affected specs**: Core IPC capability (new spec needed)
- **Affected code**: Complete rewrite of `src/lib.rs`, removal of flawed single-process implementation
- **Performance**: Target real IPC performance of ~270ns latency (vs current meaningless stdout metrics)
- **Architecture**: Shift from single-process to true multi-process communication
- **Compatibility**: Breaking change - Python APIs will need updates after core is fixed