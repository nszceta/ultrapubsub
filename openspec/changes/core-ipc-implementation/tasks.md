## 1. Core Architecture Redesign
- [ ] 1.1 Replace `MessageQueueHeader` with `SharedMemoryPool` struct
- [ ] 1.2 Implement bitmap-based block allocation system
- [ ] 1.3 Create `HringAddr` (64-bit) type for memory references
- [ ] 1.4 Design proper io_uring integration with `IORING_OP_NOP`
- [ ] 1.5 Add shared memory management with `/dev/shm/` naming

## 2. Multi-Process Implementation
- [ ] 2.1 Add process creation and management (fork/exec)
- [ ] 2.2 Implement io_uring ring sharing between processes
- [ ] 2.3 Create proper shared memory setup with unique naming
- [ ] 2.4 Add message allocation/deallocation lifecycle
- [ ] 2.5 Implement callback-based message receiving

## 3. Testing and Validation
- [ ] 3.1 Create multi-process test scenarios
- [ ] 3.2 Measure real IPC performance (not stdout writes)
- [ ] 3.3 Validate memory pool correctness
- [ ] 3.4 Test process synchronization and cleanup
- [ ] 3.5 Benchmark against vendor/io-uring-ipc baseline

## 4. Python Integration
- [ ] 4.1 Update Python bindings for new architecture
- [ ] 4.2 Create compatibility layer for existing APIs
- [ ] 4.3 Update test cases for multi-process scenarios
- [ ] 4.4 Document breaking changes and migration path