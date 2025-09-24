## 1. Core Architecture Redesign
- [x] 1.1 Replace `MessageQueueHeader` with `SharedMemoryPool` struct
- [x] 1.2 Implement bitmap-based block allocation system
- [x] 1.3 Create `HringAddr` (64-bit) type for memory references
- [x] 1.4 Design proper io_uring integration with `IORING_OP_NOP`
- [x] 1.5 Add shared memory management with `/dev/shm/` naming

## 2. Multi-Process Implementation
- [x] 2.1 Add process creation and management (fork/exec)
- [x] 2.2 Implement io_uring ring sharing between processes
- [x] 2.3 Create proper shared memory setup with unique naming
- [x] 2.4 Add message allocation/deallocation lifecycle
- [x] 2.5 Implement callback-based message receiving

## 3. Testing and Validation
- [x] 3.1 Create multi-process test scenarios
- [x] 3.2 Measure real IPC performance (not stdout writes)
- [x] 3.3 Validate memory pool correctness
- [x] 3.4 Test process synchronization and cleanup
- [x] 3.5 Benchmark against vendor/io-uring-ipc baseline

## 4. Python Integration
- [x] 4.1 Update Python bindings for new architecture
- [x] 4.2 Create compatibility layer for existing APIs
- [x] 4.3 Update test cases for multi-process scenarios
- [x] 4.4 Document breaking changes and migration path