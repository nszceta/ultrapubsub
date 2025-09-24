# Core Rust Implementation Redesign

## Overview
This document tracks the complete redesign of ultrapubsub's core Rust implementation to properly use io_uring for inter-process communication (IPC). The previous implementation was fundamentally flawed - using stdout writes instead of real IPC.

## Critical Issues Discovered

### 1. Wrong io_uring Usage
- **Current**: Using `IORING_OP_WRITE` to stdout (fd=1)
- **Problem**: This is just console output, not IPC
- **Correct**: Use `IORING_OP_NOP` to send shared memory references

### 2. No Real Shared Memory Pool
- **Current**: Simple linear buffer with manual atomic counters
- **Problem**: No sophisticated memory management
- **Correct**: Implement bitmap-based memory pool like `hring` system

### 3. Single-Process Only
- **Current**: All communication within same process
- **Problem**: Not actually inter-process communication
- **Correct**: Multi-process architecture with proper ring sharing

### 4. Missing Process Coordination
- **Current**: No process creation or management
- **Problem**: Can't demonstrate real IPC
- **Correct**: Use fork/exec with `pidfd_getfd()` for ring sharing

## Implementation Plan

### Phase 1: Study and Understand vendor/io-uring-ipc/
- [ ] Thoroughly analyze `hring.h` memory pool implementation
- [ ] Understand bitmap-based block allocation system
- [ ] Learn proper `IORING_OP_NOP` usage for IPC
- [ ] Master `pidfd_getfd()` for cross-process ring sharing
- [ ] Study shared memory naming and discovery patterns

### Phase 2: Redesign Core Architecture
- [ ] Replace `MessageQueueHeader` with `SharedMemoryPool`
- [ ] Implement bitmap-based block allocation system
- [ ] Create `hring_addr_t` (64-bit) for memory references
- [ ] Design proper io_uring integration with `IORING_OP_NOP`
- [ ] Add shared memory management with `/dev/shm/` naming

### Phase 3: Implement True IPC
- [ ] Add process creation and management (fork/exec)
- [ ] Implement io_uring ring sharing between processes
- [ ] Create proper shared memory setup with unique naming
- [ ] Add message allocation/deallocation lifecycle
- [ ] Implement callback-based message receiving

### Phase 4: Testing and Validation
- [ ] Create multi-process test scenarios
- [ ] Measure real IPC performance (not stdout writes)
- [ ] Validate memory pool correctness
- [ ] Test process synchronization and cleanup
- [ ] Benchmark against vendor/io-uring-ipc baseline

## Technical Architecture Redesign

### Current (Flawed) Architecture
```rust
pub struct MessageQueueHeader {
    write_pos: AtomicUsize,
    read_pos: AtomicUsize,
    message_count: AtomicUsize,
}

// Wrong: Using IORING_OP_WRITE to stdout
let write_e = opcode::Write::new(types::Fd(1), ptr, size)
```

### Target (Correct) Architecture
```rust
pub struct SharedMemoryPool {
    blocks: u32,
    bitmap: Vec<u64>,
    map: *mut u8,  // Actual shared memory region
}

pub type HringAddr = u64;  // 64-bit memory reference

// Correct: Using IORING_OP_NOP to send memory references
let nop_e = opcode::Nop::new()
    .build()
    .user_data(addr);  // Send shared memory reference
```

## Key Components to Implement

### 1. Memory Pool Management
- Bitmap-based block allocation
- Shared memory mapping with `mmap`
- Block size management (e.g., 4KB blocks)
- Memory pool initialization and cleanup

### 2. io_uring IPC Integration
- `IORING_OP_NOP` operations for sending references
- Proper `user_data` field usage for addresses
- Submission and completion queue management
- Cross-process ring buffer sharing

### 3. Process Management
- Fork/exec for creating separate processes
- `pidfd_getfd()` for accessing io_uring rings across processes
- Process lifecycle management
- Cleanup and resource reclamation

### 4. Message Lifecycle
- Message allocation from shared memory pool
- Message queuing via io_uring NOP operations
- Message receiving via completion queue callbacks
- Message deallocation and pool cleanup

## Success Criteria

### Functional Requirements
- [ ] True inter-process communication (not same-process)
- [ ] Proper io_uring IPC using `IORING_OP_NOP`
- [ ] Shared memory pool with bitmap allocation
- [ ] Multi-process message passing with correct synchronization
- [ ] Process cleanup and resource management

### Performance Requirements
- [ ] Real IPC performance measurement (not stdout writes)
- [ ] Latency in nanosecond range (targeting ~40ns like vendor/io-uring-ipc)
- [ ] Proper memory pool efficiency
- [ ] No memory leaks in multi-process scenarios

### Correctness Requirements
- [ ] Message integrity across process boundaries
- [ ] Proper memory pool allocation/deallocation
- [ ] No race conditions in concurrent access
- [ ] Graceful handling of edge cases

## Current Status

**Phase**: 0 - Discovery and Planning
**Next Step**: Phase 1 - Study vendor/io-uring-ipc/ implementation
**Blocking**: None - ready to begin analysis

## Notes

- **Performance Reality Check**: Previous "0.003ms per message" was meaningless stdout write speed
- **Target Performance**: Real io_uring IPC achieves ~39.76 nanoseconds latency
- **Complexity Increase**: Proper implementation is significantly more complex than current PoC
- **Focus**: Rust core only - Python bindings will be added after core is correct