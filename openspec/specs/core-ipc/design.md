# Core IPC Design Document

## Context

The Core IPC system implements high-performance inter-process communication using Shared Memory Ring Buffer with atomic operations. This design document provides technical details on the architecture, patterns, and implementation decisions.

## ⚠️ Important Warning: Do NOT Use io_uring

**CRITICAL**: The io_uring-based approach has been deprecated and abandoned due to fundamental architectural issues. All implementations MUST use the Shared Memory Ring Buffer approach described in this specification.

**Why io_uring Failed**:
- io_uring operations submitted successfully but generated zero completions
- Complex ring sharing between processes proved unreliable
- Kernel completion ring mechanism unsuitable for message passing
- Unpredictable behavior under high-frequency messaging scenarios

**Current Implementation**: Shared Memory Ring Buffer with atomic operations only

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
│  │   Pool   │  │    │  │   Pool   │  │    │  │   Pool   │  │
│  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │
│        │        │    │        │        │    │        │        │
│  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │
│  │Ring Buffer│  │    │  │Ring Buffer│  │    │  │Ring Buffer│  │
│  │   with   │  │    │  │   with   │  │    │  │   with   │  │
│  │ Atomics  │  │    │  │ Atomics  │  │    │  │ Atomics  │  │
│  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │
│        │        │    │        │        │    │        │        │
│        └────────┼────┼────────┼────────┼────┼────────┘        │
│                 │    │        │        │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                        │        │        │
                        └────────┼────────┘
                                 │
                    ┌─────────────────┐
                    │   Shared        │
                    │   /dev/shm/     │
                    │   Memory Region │
                    └─────────────────┘
```

### Key Data Structures

#### SharedRingBuffer
```rust
pub struct SharedRingBuffer {
    fd: i32,                    // File descriptor for shared memory
    ptr: *mut u8,              // Pointer to mapped memory
    size: usize,                // Total buffer size
    head: AtomicU64,           // Atomic head pointer for publisher
    tails: [AtomicU64; 16],     // Atomic tail pointers for subscribers
    offsets: [u32; MAX_MESSAGES], // Message offsets in buffer
    lengths: [u32; MAX_MESSAGES], // Message lengths
    available: [AtomicU64; 16], // Bitmap tracking message availability
    pool: [[u8; POOL_SLOT_SIZE]; POOL_SIZE],  // Pre-allocated 35MB slots
    pool_available: AtomicU64,                // Bitmap tracking pool slots
    pool_sequence: [AtomicU64; POOL_SIZE],    // Sequence numbers for slots
}
```

#### PoolSlot
```rust
pub struct PoolSlot {
    slot_index: usize,          // Index in the pre-allocated pool
    size: usize,                // Actual data size
    sequence: u64,              // Sequence number for tracking
    is_valid: bool,             // Slot validity flag
}

impl PoolSlot {
    pub fn new(slot_index: usize, size: usize, sequence: u64) -> Self
    pub fn slot_index(&self) -> usize
    pub fn size(&self) -> usize
    pub fn sequence(&self) -> u64
    pub fn is_valid(&self) -> bool
}
```

#### RingBuffer
```rust
pub struct RingBuffer {
    fd: i32,                    // Shared memory file descriptor
    ptr: *mut u8,              // Pointer to mapped memory
    size: usize,                // Total buffer size
    max_messages: usize,        // Maximum number of messages
    max_subscribers: usize,    // Maximum number of subscribers
}
```

## Implementation Patterns

### Pre-allocated Memory Pool Management

#### Pool Constants
- **Slot Size**: 35MB (matching performance requirements)
- **Pool Size**: 63 slots (maximum for u64 bitmap)
- **Bitmap Format**: 1 bit per slot (0 = free, 1 = allocated)
- **Allocation Strategy**: First available slot with atomic operations
- **Sequence Tracking**: Atomic sequence numbers per slot

#### Memory Layout
```
┌─────────────────────────────────────────────────────────────┐
│                 Shared Memory Pool                          │
├─────────────────────────────────────────────────────────────┤
│ Ring Buffer Structure                                        │
│ - head/tail pointers                                        │
│ - message metadata                                          │
│ - availability bitmap                                      │
├─────────────────────────────────────────────────────────────┤
│ Pre-allocated Pool Slots (35MB each)                       │
│ [Slot 0][Slot 1][Slot 2]...[Slot 62]                      │
└─────────────────────────────────────────────────────────────┘
```

#### Pool Slot Allocation Process
1. Atomically check pool availability bitmap
2. Find first available slot using bit operations
3. Atomically claim slot and assign sequence number
4. Return slot pointer and index to caller

#### Pool Slot Release Process
1. Validate slot index and sequence number
2. Mark slot as available in bitmap
3. Update sequence tracking
4. Return slot to pool

### Atomic Operation Integration

#### Head Pointer Updates
- **Operation**: Atomic compare-and-swap for head advancement
- **Memory Ordering**: Sequential consistency for cross-process visibility
- **Overflow Handling**: Wrap-around with sequence number tracking

#### Tail Pointer Updates
- **Operation**: Atomic loads and stores per subscriber
- **Memory Ordering**: Acquire-release semantics
- **Independent Tracking**: Each subscriber maintains separate tail pointer

#### Bitmap Operations
- **Availability Updates**: Atomic bitwise operations
- **Subscriber Tracking**: 16 separate bitmap words for 16 subscribers
- **Message Indexing**: 1024 message slots (16 × 64 bits)

### Multi-Process Architecture

#### Process Creation Flow
1. **Parent**: Create shared memory region with unique identifier
2. **Parent**: Initialize ring buffer with atomic head/tail pointers
3. **Parent**: Fork child process using `fork()`
4. **Child**: Attach to parent's shared memory using identifier
5. **Child**: Initialize subscriber with independent tail pointer
6. **Child**: Begin receiving messages from shared ring buffer

#### Shared Memory Naming
- **Pattern**: `/dev/shm/ultrapubsub_[identifier]`
- **Uniqueness**: Include process ID and timestamp
- **Cleanup**: Automatic unlink on last process exit

#### Shared Memory Attachment
```rust
// Parent creates shared memory
let shm_name = format!("/dev/shm/ultrapubsub_{}_{}", pid, timestamp);
let fd = shm_open(&shm_name, O_CREAT | O_RDWR, 0666);
ftruncate(fd, buffer_size);
let ptr = mmap(nullptr, buffer_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

// Child attaches to existing shared memory
let fd = shm_open(&shm_name, O_RDWR, 0666);
let ptr = mmap(nullptr, buffer_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
```

### Large Binary Blob Handling

#### Signature System
```rust
// Header signature: "ULTRAPUBSUB_BLOB_START_[SIZE]MB"
// Footer signature: "ULTRAPUBSUB_BLOB_END_[SIZE]MB"

struct BlobSignature {
    header: Vec<u8>,
    footer: Vec<u8>,
    size_mb: usize,
}
```

#### Blob Generation Process
1. Create header signature with size information
2. Generate deterministic payload data
3. Append footer signature
4. Calculate checksum for verification
5. Return complete blob with signatures

#### Verification Process
1. Check header signature matches expected size
2. Check footer signature matches expected size
3. Verify payload size is correct
4. Calculate and verify checksum
5. Return validation result

### Error Handling Strategy

#### Memory Allocation Errors
- **Out of Memory**: Return `Error::OutOfMemory`
- **Invalid Size**: Return `Error::InvalidSize`
- **Fragmentation**: Return `Error::Fragmentation`

#### Process Communication Errors
- **Attachment Failure**: Return `Error::AttachFailed`
- **Memory Sharing**: Return `Error::MemorySharingFailed`
- **Process Exit**: Handle cleanup automatically

#### Data Integrity Errors
- **Signature Mismatch**: Return `Error::SignatureMismatch`
- **Checksum Failure**: Return `Error::ChecksumFailed`
- **Corruption Detected**: Return `Error::DataCorruption`

## Performance Optimizations

### Memory Pool Optimizations
- **Bitmap Operations**: Use bit manipulation for efficiency
- **Contiguous Allocation**: Minimize memory fragmentation
- **Zero-Copy**: Avoid data copying between processes

### Zero-Copy Pool Management

#### Pool Slot Access
- **Direct Memory Access**: No copying between publisher and subscribers
- **Atomic Operations**: Lock-free slot allocation and release
- **Memory Safety**: Bounds checking and sequence number validation

#### Pool Slot Usage
1. Publisher allocates slot from pre-allocated pool
2. Publisher writes data directly to slot memory
3. Publisher publishes slot index with sequence number
4. Subscribers read data directly from shared slot memory
5. Publisher releases slot back to pool after all subscribers

### Multi-Process Optimizations
- **Shared Memory**: Direct memory access between processes
- **Efficient Signaling**: Use io_uring for process coordination
- **Resource Cleanup**: Automatic cleanup on process exit

## Security Considerations

### Memory Safety
- **Bounds Checking**: All array accesses are bounds-checked
- **Null Termination**: Proper handling of C-style strings
- **Reference Validation**: PoolSlot validation before use

### Process Isolation
- **Shared Memory Security**: Secure memory mapping with proper permissions
- **Memory Permissions**: Appropriate read/write permissions
- **Resource Limits**: Prevent resource exhaustion attacks

### Data Integrity
- **Signature Verification**: Cryptographic-style verification
- **Checksum Validation**: Additional integrity checks
- **Corruption Detection**: Detect and handle data corruption

## Testing Strategy

### Unit Tests
- **Memory Pool**: Test allocation, deallocation, error cases
- **PoolSlot**: Test creation, validation, operations
- **Bitmap**: Test bit manipulation, bounds checking
- **Shared Memory**: Test creation, attachment, cleanup
- **Atomic Operations**: Test head/tail pointer operations

### Integration Tests
- **Multi-Process**: Test fork/exec, shared memory attachment
- **Large Data**: Test 35MB blob generation, verification
- **Performance**: Test 40Hz frequency, 1.4GB/s throughput metrics
- **Error Handling**: Test error conditions, recovery
- **Zero-Copy**: Test pre-allocated pool operations

### Stress Tests
- **High Frequency**: Test sustained 40Hz messaging with 35MB payloads
- **Memory Pressure**: Test 6 subscribers × 35MB = 210MB memory footprint
- **Concurrent Access**: Test 6 independent subscriber processes
- **Long Duration**: Test stability over extended 40Hz operation

## Future Enhancements

### Performance Improvements
- **Adaptive Pool Sizes**: Dynamic slot size allocation
- **Memory Compression**: Optional compression for large data
- **Batch Processing**: More efficient slot allocation operations

### Feature Enhancements
- **Message Filtering**: Subscriber-side message filtering
- **Priority Queuing**: Priority-based message handling
- **Statistics**: Performance metrics and monitoring

### Platform Support
- **Alternative Atomic Implementations**: Support for different CPU architectures
- **Cross-Platform**: Windows/macOS support (if needed)
- **Hardware Acceleration**: GPU-assisted memory operations