## Context

The ultrapubsub project requires a complete redesign of its core Rust implementation to properly use io_uring for inter-process communication (IPC). The current implementation is fundamentally flawed - using stdout writes instead of real IPC.

### Current State Analysis

**Critical Issues Discovered:**
1. **Wrong io_uring Usage**: Using `IORING_OP_WRITE` to stdout (fd=1) instead of `IORING_OP_NOP` for IPC
2. **No Real Shared Memory Pool**: Simple linear buffer with manual atomic counters instead of bitmap-based memory pool
3. **Single-Process Only**: All communication within same process, not actual inter-process communication
4. **Missing Process Coordination**: No process creation or management for true IPC

**Performance Reality Check:**
- Current "0.003ms per message" was meaningless stdout write speed
- Real io_uring IPC achieves ~39.76 nanoseconds latency (vendor/io-uring-ipc results)
- Current implementation is not doing IPC at all

### Vendor Implementation Validation

**Multi-process test**: 102,400,000 messages in 36.11s (352.66ns latency)
**One-way benchmark**: 10,240,000 messages in 2.76s (270.67ns latency)
**Real IPC**: Confirmed true inter-process communication via fork/exec

## Goals / Non-Goals

### Goals
- [ ] Implement true inter-process communication using io_uring
- [ ] Achieve real IPC performance of ~270ns latency
- [ ] Support large data packets (20MB+) at 40Hz with 6 subscribers
- [ ] Maintain zero-copy semantics throughout
- [ ] Proper memory management with no leaks in multi-process scenarios

### Non-Goals
- [ ] Maintain backward compatibility with current flawed implementation
- [ ] Support Windows or non-Linux systems
- [ ] Implement complex message routing or filtering
- [ ] Add network-based communication capabilities

## Decisions

### Decision: Replace MessageQueueHeader with SharedMemoryPool
**What**: Replace the current simple linear buffer with a sophisticated bitmap-based memory pool system
**Why**: Current implementation lacks proper memory management and cannot handle the allocation patterns needed for real IPC
**Alternatives considered**: 
- Keep current simple buffer (rejected - insufficient for real IPC)
- Use existing memory pool libraries (rejected - need custom integration with io_uring)

### Decision: Use IORING_OP_NOP for IPC
**What**: Use `IORING_OP_NOP` operations to send shared memory references between processes
**Why**: This is the correct pattern for io_uring-based IPC, allowing zero-copy message passing
**Alternatives considered**:
- Continue using `IORING_OP_WRITE` (rejected - this is just stdout, not IPC)
- Use `IORING_OP_SENDMSG` (rejected - more complex than needed for shared memory)

### Decision: Implement Multi-Process Architecture
**What**: Add proper process creation and management using fork/exec with `pidfd_getfd()` for ring sharing
**Why**: Current single-process implementation is not actually doing inter-process communication
**Alternatives considered**:
- Keep single-process (rejected - defeats the purpose of IPC)
- Use threads instead of processes (rejected - doesn't demonstrate true IPC)

### Decision: Use Bitmap-Based Memory Allocation
**What**: Implement sophisticated memory pool with bitmap-based block allocation like `hring` system
**Why**: Simple atomic counters are insufficient for real io_uring IPC and proper memory management
**Alternatives considered**:
- Linear allocation (rejected - fragmentation issues)
- Slab allocation (rejected - overkill for current use case)

## Risks / Trade-offs

### Risks
- **Implementation Complexity**: Proper io_uring IPC is significantly more complex than current PoC
- **Performance Regression**: Initial implementation may be slower than vendor baseline
- **Memory Leaks**: Multi-process scenarios increase risk of resource leaks
- **Synchronization Issues**: Race conditions in concurrent access across processes

### Trade-offs
- **Development Time**: Significant rewrite required vs. incremental improvements
- **Code Size**: More complex implementation vs. simple current code
- **Dependencies**: Additional system dependencies (nix, fcntl) vs. minimal current deps
- **Learning Curve**: Steeper learning curve for io_uring IPC vs. simple shared memory

## Migration Plan

### Phase 1: Core Architecture Redesign (High Priority)
1. Replace `MessageQueueHeader` with `SharedMemoryPool` struct
2. Implement bitmap-based block allocation system
3. Create `HringAddr` (64-bit) type for memory references
4. Design proper io_uring integration with `IORING_OP_NOP`
5. Add shared memory management with `/dev/shm/` naming

### Phase 2: Multi-Process Implementation (Medium Priority)
1. Add process creation and management (fork/exec)
2. Implement io_uring ring sharing between processes
3. Create proper shared memory setup with unique naming
4. Add message allocation/deallocation lifecycle
5. Implement callback-based message receiving

### Phase 3: Testing and Validation (Low Priority)
1. Create multi-process test scenarios
2. Measure real IPC performance (not stdout writes)
3. Validate memory pool correctness
4. Test process synchronization and cleanup
5. Benchmark against vendor/io-uring-ipc baseline

### Phase 4: Python Integration Updates
1. Update Python bindings to work with new architecture
2. Add multi-process support to Python API
3. Update performance benchmarks
4. Add proper error handling and cleanup

## Open Questions

- **Memory Block Size**: What block size should be used for the bitmap allocation? (vendor uses 4KB)
- **Process Scaling**: How many processes can realistically share a single io_uring ring?
- **Error Recovery**: What happens when a process crashes while holding allocated memory blocks?
- **Memory Limits**: What are the practical limits on shared memory pool size?
- **Performance Tuning**: How to optimize the bitmap allocation for high-frequency message passing?