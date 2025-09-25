## Context
UltraPubSub requires high-performance IPC for 1:N pub/sub messaging. The previous io_uring-based approach failed due to fundamental issues with completion ring generation. We need a replacement that achieves 800MB+ throughput with minimal latency.

## Goals / Non-Goals
**Goals:**
- Achieve 2-4 GB/s throughput
- Zero-copy message passing
- Support 1 publisher + N subscribers
- Lock-free synchronization
- Cross-process compatibility
- Minimal CPU overhead

**Non-Goals:**
- Persistent message storage
- Guaranteed delivery semantics
- Complex routing or filtering
- Network transparency

## Decisions
### Architecture: Shared Memory Ring Buffer
**Decision:** Use circular ring buffer with atomic head/tail pointers
**Why:**
- Zero-copy reads for all subscribers
- Single memory allocation
- No syscall overhead after setup
- Cache-friendly access patterns
- Proven high-performance pattern

### Synchronization: Atomic Operations
**Decision:** Use atomic operations instead of locks
**Why:**
- No contention between publisher/subscribers
- Better performance under load
- No deadlock possibilities
- Fairer access patterns

### Memory Layout: Contiguous Buffer
**Decision:** Single contiguous memory region
**Why:**
- Better cache utilization
- Simpler memory management
- Faster allocation/deallocation
- Easier cross-process mapping

## Data Structures
```rust
struct SharedRingBuffer {
    // Publisher state
    head: AtomicU64,                    // Write position
    published_count: AtomicU64,         // Total messages published

    // Subscriber state (array for N subscribers)
    tails: [AtomicU64; MAX_SUBSCRIBERS], // Per-subscriber read position
    last_seen: [AtomicU64; MAX_SUBSCRIBERS], // Last sequence seen

    // Message storage
    data: [u8; BUFFER_SIZE],           // Circular message buffer
    offsets: [u32; MAX_MESSAGES],       // Message offset table
    lengths: [u16; MAX_MESSAGES],       // Message length table

    // Synchronization
    available: AtomicU64,               // Message availability bitmap
    notification_fd: AtomicI32,         // Eventfd for notifications
}
```

## Memory Layout
```
+------------------+
| Header (fixed)   |
| - head           |
| - published_cnt  |
| - tails[6]       |
| - last_seen[6]  |
+------------------+
| Message Metadata |
| - offsets[]      |
| - lengths[]      |
| - available bits |
+------------------+
| Message Data     |
| (circular buffer)|
+------------------+
```

## Algorithm
### Publisher Algorithm:
1. Acquire next message slot via atomic increment of `published_count`
2. Write message data to circular buffer at calculated offset
3. Update message metadata (offset, length)
4. Set availability bit in atomic bitmap
5. Optionally notify subscribers via eventfd

### Subscriber Algorithm:
1. Check availability bitmap for new messages
2. For each available message > last_seen:
   - Read message metadata (offset, length)
   - Read message data directly from buffer
   - Update last_seen counter
3. Wrap around to beginning when reaching buffer end

## Performance Characteristics
- **Throughput**: 2-4 GB/s (memory bandwidth limited)
- **Latency**: <1μs for direct memory access
- **CPU Usage**: Minimal (mostly atomic operations)
- **Scalability**: Linear with number of subscribers

## Risks / Trade-offs
**Risks:**
- Buffer overflow if publisher outpaces subscribers
- Memory alignment issues on some architectures
- Cache contention between publisher/subscribers

**Trade-offs:**
- Fixed buffer size vs dynamic allocation
- Single producer vs multiple publishers
- In-memory only vs persistent storage

## Migration Plan
1. Implement new architecture in parallel
2. Create compatibility layer for existing API
3. Gradual migration of test cases
4. Performance benchmarking and validation
5. Production rollout with monitoring

## Open Questions
- Optimal buffer size for target workloads
- Notification mechanism (polling vs event-driven)
- Handling of slow subscribers
- Memory ordering semantics for atomic operations