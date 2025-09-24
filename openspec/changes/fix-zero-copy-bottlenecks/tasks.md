## 1. Fix Memory Pool Architecture
- [ ] 1.1 Replace 32MB fixed blocks with variable-sized allocations
- [ ] 1.2 Implement bitmap allocation for minimum block units (64 bytes)
- [ ] 1.3 Add block size classes for efficient memory usage
- [ ] 1.4 Fix contiguous block allocation algorithm
- [ ] 1.5 Update Hring initialization for new memory pool structure

## 2. Implement True Zero-Copy
- [ ] 2.1 Remove `ptr::copy_nonoverlapping` from publisher
- [ ] 2.2 Ensure data is placed directly in shared memory without copying
- [ ] 2.3 Preserve subscriber zero-copy access (already working)
- [ ] 2.4 Implement proper memory reuse after consumption

## 3. Fix io_uring Integration
- [ ] 3.1 Ensure io_uring NOP operations only carry memory addresses
- [ ] 3.2 Remove any unnecessary data copying in reference path
- [ ] 3.3 Validate that `user_data` field properly encodes memory references

## 4. Optimize Process Architecture
- [ ] 4.1 Fix shared io_uring ring management across processes
- [ ] 4.2 Reduce kernel overhead from multiple io_uring instances
- [ ] 4.3 Ensure proper synchronization between publisher and subscribers

## 5. Update OpenSpec Materials
- [ ] 5.1 Update core-ipc spec to reflect current architectural reality
- [ ] 5.2 Document the identified bottlenecks and their fixes
- [ ] 5.3 Validate that specs match the corrected implementation

## 6. Performance Validation
- [ ] 6.1 Test memory pool efficiency improvements
- [ ] 6.2 Measure zero-copy performance gains
- [ ] 6.3 Validate target throughput of 800MB/s is achieved
- [ ] 6.4 Ensure stability under sustained load