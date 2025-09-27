# Core IPC Design Document

## Context

The Core IPC system implements high-performance inter-process communication using Shared Memory Broadcast Buffer with process-shared mutex synchronization. This design document provides technical details on the architecture, patterns, and implementation decisions for the synchronous broadcast system.

## Architecture Overview

### System Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Publisher     │    │   Subscriber 1  │    │   Subscriber 2  │
│   Process       │    │   Process       │    │   Process       │
│                 │    │                 │    │                 │
│  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │
│  │   Shared │  │    │  │   Shared │  │    │  │   Shared │  │
│  │  Memory  │  │    │  │  Memory  │  │    │  │  Memory  │  │
│  │ Broadcast│  │    │  │ Broadcast│  │    │  │ Broadcast│  │
│  │  Buffer  │  │    │  │  Buffer  │  │    │  │  Buffer  │  │
│  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │
│        │        │    │        │        │    │        │        │
│  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │
│  │Process-   │  │    │  │Process-   │  │    │  │Process-   │  │
│  │Shared     │  │    │  │Shared     │  │    │  │Shared     │  │
│  │Mutex      │  │    │  │Mutex      │  │    │  │Mutex      │  │
│  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │
│        │        │    │        │        │    │        │        │
│        └────────┼────┼────────┼────────┼────┼────────┘        │
│                 │    │        │        │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                        │        │        │
                        └────────┼────────┘
                                 │
                    ┌─────────────────┐
                    │   POSIX Shared  │
                    │   /dev/shm/     │
                    │   Memory Region │
                    └─────────────────┘
```

### Key Data Structures

#### SharedBroadcastBuffer
```rust
#[repr(C, align(64))]
pub struct SharedBroadcastBuffer {
    // Process-shared synchronization (cache-aligned)
    mutex: ProcessSharedMutex,

    // Synchronous broadcast state (packed for cache efficiency)
    subscriber_count: u32,        // Number of registered subscribers (0-32)
    broadcast_state: u32,         // Current broadcast state
    sequence_number: u64,         // Monotonically increasing sequence number
    broadcast_length: u32,        // Length of data in broadcast slot
    max_subscriber_id: u32,       // Highest subscriber ID for optimization

    // Dynamic subscriber tracking arrays
    subscriber_ack_array: [u32; MAX_DYNAMIC_SUBSCRIBERS],  // Acknowledgment status per subscriber
    subscriber_reg_array: [u32; MAX_DYNAMIC_SUBSCRIBERS],  // Registration status per subscriber

    // Single broadcast slot accessible to all subscribers
    broadcast_data: [u8; BROADCAST_SLOT_SIZE],  // 35MB single broadcast slot
}
```

#### ProcessSharedMutex
```rust
#[repr(C, align(64))]
pub struct ProcessSharedMutex {
    mutex: pthread_mutex_t,
}

impl ProcessSharedMutex {
    pub fn init(&mut self)  // Initialize for cross-process sharing
    pub fn lock(&self)      // Lock with atomic operations
    pub fn unlock(&self)    // Unlock with atomic operations
    pub fn destroy(&mut self) // Clean up resources
}
```

#### BroadcastPublisher
```rust
pub struct BroadcastPublisher {
    buffer: *mut SharedBroadcastBuffer,
}

impl BroadcastPublisher {
    pub fn new(buffer: *mut SharedBroadcastBuffer) -> Self
    pub fn broadcast(&mut self, data: &[u8]) -> Result<u64, Error>
    pub fn register_subscriber(&mut self) -> usize
    pub fn subscriber_count(&self) -> usize
    pub fn wait_for_subscribers(&self, expected_count: u32) -> Result<(), Error>
}
```

#### BroadcastSubscriber
```rust
pub struct BroadcastSubscriber {
    buffer: *mut SharedBroadcastBuffer,
    subscriber_id: usize,
    last_sequence: u64,
}

impl BroadcastSubscriber {
    pub fn new(buffer: *mut SharedBroadcastBuffer, subscriber_id: usize) -> Self
    pub fn register(&self) -> Result<(), Error>
    pub fn receive(&mut self) -> Result<Vec<u8>, Error>
    pub fn has_new_message(&self) -> bool
    pub fn last_processed(&self) -> u64
}
```

## Implementation Patterns

### Process-Shared Mutex Implementation

#### Constants and Configuration
- **Mutex Type**: pthread_mutex_t with PTHREAD_PROCESS_SHARED attribute
- **Alignment**: 64-byte cache-line alignment to prevent false sharing
- **Performance**: 10-30 nanoseconds for uncontended lock/unlock
- **Memory Ordering**: Sequential consistency for cross-process visibility

#### Memory Layout
```
┌─────────────────────────────────────────────────────────────┐
│                Shared Broadcast Buffer                      │
├─────────────────────────────────────────────────────────────┤
│ Process-Shared Mutex (64 bytes, cache-aligned)              │
├─────────────────────────────────────────────────────────────┤
│ Broadcast State (20 bytes)                                  │
│ - subscriber_count: u32                                     │
│ - broadcast_state: u32                                      │
│ - sequence_number: u64                                      │
│ - broadcast_length: u32                                     │
│ - max_subscriber_id: u32                                    │
├─────────────────────────────────────────────────────────────┤
│ Subscriber Tracking Arrays (256 bytes)                      │
│ - subscriber_ack_array[32]: u32                             │
│ - subscriber_reg_array[32]: u32                             │
├─────────────────────────────────────────────────────────────┤
│ Broadcast Data Slot (35MB)                                  │
│ - Direct memory access for all processes                    │
│ - Zero-copy transmission                                    │
└─────────────────────────────────────────────────────────────┘
```

#### Mutex Operations
- **Initialization**: Set PTHREAD_PROCESS_SHARED attribute for cross-process sharing
- **Locking**: Atomic operation with system call fallback for contention
- **Unlocking**: Atomic operation with memory barrier
- **Cleanup**: Proper destruction on shared memory cleanup

### Synchronous Broadcast Coordination

#### Broadcast States
- **STATE_PUBLISHING (0)**: Publisher is writing data to broadcast slot
- **STATE_WAITING (1)**: Publisher waiting for subscriber acknowledgments
- **STATE_COMPLETED (2)**: All subscribers acknowledged, ready for next broadcast

#### Broadcast Flow
1. **State Check**: Publisher ensures previous broadcast is completed
2. **Data Copy**: Publisher writes data directly to shared broadcast slot
3. **State Update**: Publisher sets state to PUBLISHING, then WAITING
4. **Acknowledgment Wait**: Publisher waits for all subscribers to acknowledge
5. **Completion**: Publisher sets state to COMPLETED after all acknowledgments
6. **Reset**: Publisher resets acknowledgment bits for next broadcast

#### Subscriber Flow
1. **Registration**: Subscriber registers and receives unique ID (0-31)
2. **Message Wait**: Subscriber waits for state to be WAITING with new sequence
3. **Data Read**: Subscriber reads directly from broadcast slot
4. **Acknowledgment**: Subscriber sets acknowledgment bit atomically
5. **Sequence Update**: Subscriber updates last processed sequence

### Multi-Process Architecture

#### Process Creation Flow
1. **Parent**: Creates shared memory region with unique identifier
2. **Parent**: Initializes broadcast buffer with process-shared mutex
3. **Parent**: Forks child processes using `fork()`
4. **Child**: Attaches to parent's shared memory using identifier
5. **Child**: Creates subscriber with unique ID and registers
6. **Child**: Begins receiving synchronous broadcasts

#### Shared Memory Naming
- **Pattern**: `/ultrapubsub_[identifier]` (POSIX shared memory)
- **Uniqueness**: Include process ID and timestamp for uniqueness
- **Cleanup**: Automatic unlink on last process exit or explicit cleanup

#### Shared Memory Operations
```rust
// Parent creates shared memory
let shm_name = format!("/ultrapubsub_{}", unique_id);
let fd = shm_open(&shm_name, O_CREAT | O_RDWR | O_EXCL, 0666);
ftruncate(fd, buffer_size);
let ptr = mmap(nullptr, buffer_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

// Child attaches to existing shared memory
let fd = shm_open(&shm_name, O_RDWR, 0666);
let ptr = mmap(nullptr, buffer_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
```

### Large Binary Data Handling

#### Single Slot Architecture
- **Slot Size**: 35MB contiguous memory block
- **Access**: Direct memory access for all processes
- **Synchronization**: Mutex-protected coordinated access
- **Performance**: Zero-copy transmission between processes

#### Data Transmission Process
1. **Publisher**: Writes data directly to 35MB broadcast slot
2. **Subscribers**: All subscribers read from identical memory location
3. **Acknowledgment**: Each subscriber acknowledges independently
4. **Completion**: Publisher waits for all acknowledgments before continuing

#### Memory Safety
- **Bounds Checking**: All accesses validated against 35MB limit
- **Synchronization**: Mutex prevents concurrent write access
- **Atomic Operations**: Acknowledgment bits updated atomically
- **Cleanup**: Proper shared memory cleanup on termination

### Error Handling Strategy

#### Memory Allocation Errors
- **Shared Memory Creation**: Return `Error::SharedMemoryCreationFailed`
- **Memory Mapping**: Return `Error::MemoryMappingFailed`
- **Size Validation**: Return `Error::InvalidSize` for oversized messages

#### Process Communication Errors
- **Attachment Failure**: Return `Error::AttachFailed`
- **Mutex Errors**: Return `Error::MutexOperationFailed`
- **Process Exit**: Handle cleanup automatically in Drop implementations

#### Synchronization Errors
- **Timeout**: Return `Error::Timeout` for unresponsive subscribers
- **Invalid State**: Return `Error::InvalidState` for state inconsistencies
- **Registration Errors**: Return `Error::RegistrationFailed` for duplicate IDs

## Performance Optimizations

### Cache-Line Optimization
- **64-byte Alignment**: All shared structures aligned to cache line size
- **False Sharing Prevention**: Separate arrays for different data types
- **Spatial Locality**: Related data grouped together
- **Memory Ordering**: Appropriate memory ordering for atomic operations

### Zero-Copy Architecture
- **Direct Memory Access**: Publisher and subscribers access same memory
- **No Data Copying**: Eliminates memory bandwidth overhead
- **Contiguous Memory**: Single 35MB block for efficient access
- **Memory Mapping**: Shared memory mapped into each process space

### Efficient Acknowledgment System
- **Bitmap Tracking**: Bit arrays for acknowledgment status
- **Atomic Operations**: Fast acknowledgment updates
- **Batched Checking**: Efficient checking of all acknowledgments
- **Adaptive Waiting**: Spin-wait for short periods, yield for longer

## Security Considerations

### Memory Safety
- **Bounds Checking**: All array accesses validated
- **Null Pointer Protection**: All pointers validated before use
- **Memory Mapping**: Proper mapping and unmapping
- **Synchronization**: All shared access properly synchronized

### Process Isolation
- **Shared Memory Security**: Proper permissions on shared memory
- **Independent Attachment**: Each process attaches independently
- **Resource Limits**: Prevent resource exhaustion
- **Cleanup**: Proper cleanup on process termination

### Data Integrity
- **Synchronous Delivery**: All subscribers receive identical data
- **Acknowledgment Verification**: Publisher verifies all acknowledgments
- **Sequence Tracking**: Monotonically increasing sequence numbers
- **State Validation**: All state transitions validated

## Testing Strategy

### Unit Tests
- **Mutex Operations**: Test process-shared mutex functionality
- **Broadcast Buffer**: Test buffer creation, attachment, cleanup
- **Publisher**: Test broadcast operations and acknowledgment waiting
- **Subscriber**: Test registration, message reception, acknowledgment
- **State Management**: Test state transitions and validation

### Integration Tests
- **Multi-Process**: Test fork/exec with shared memory attachment
- **Large Data**: Test 35MB message transmission
- **Performance**: Verify 40Hz frequency and throughput targets
- **Error Handling**: Test timeout and error recovery scenarios
- **Python Integration**: Test PyO3 bindings and Python APIs

### Stress Tests
- **High Frequency**: Test sustained 40Hz messaging with 35MB payloads
- **Maximum Subscribers**: Test with 32 concurrent subscribers
- **Memory Pressure**: Test behavior under continuous load
- **Timing Accuracy**: Test 25ms cycle time precision
- **Long Duration**: Test stability over extended operation

## Future Enhancements

### Performance Improvements
- **Adaptive Timing**: Dynamic timeout adjustment based on load
- **Memory Prefetching**: Optimize memory access patterns
- **Batched Operations**: Group multiple operations for efficiency
- **NUMA Awareness**: Optimize for NUMA architectures

### Feature Enhancements
- **Message Filtering**: Optional subscriber-side filtering
- **Priority Messaging**: Priority-based message handling
- **Statistics**: Performance metrics collection
- **Dynamic Scaling**: Adaptive subscriber limits

### Platform Support
- **Alternative Mutex**: Support different synchronization primitives
- **Cross-Platform**: Windows/macOS support if needed
- **Hardware Acceleration**: CPU-specific optimizations