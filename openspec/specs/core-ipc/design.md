# Core IPC Design Document

## Context

The Core IPC system implements high-performance inter-process communication using Linux io_uring with shared memory pools. This design document provides technical details on the architecture, patterns, and implementation decisions.

## Architecture Overview

### System Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Publisher     │    │   Subscriber 1  │    │   Subscriber 2  │
│   Process       │    │   Process       │    │   Process       │
│                 │    │                 │    │                 │
│  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │
│  │SharedMemory│  │    │  │SharedMemory│  │    │  │SharedMemory│  │
│  │   Pool    │  │    │  │   Pool    │  │    │  │   Pool    │  │
│  └───────────┘  │    │  └───────────┘  │    │  └───────────┘  │
│        │        │    │        │        │    │        │        │
│  ┌───────────┐  │    │  ┌───────────┐  │    │  ┌───────────┐  │
│  │  Hring    │  │    │  │  Hring    │  │    │  │  Hring    │  │
│  │ Ring      │  │    │  │ Ring      │  │    │  │ Ring      │  │
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

#### SharedMemoryPool
```rust
pub struct SharedMemoryPool {
    fd: i32,                    // File descriptor for shared memory
    ptr: *mut u8,              // Pointer to mapped memory
    size: usize,                // Total pool size
    block_size: usize,          // Size of each allocation block
    num_blocks: usize,          // Number of blocks in pool
    bitmap: *mut u8,            // Bitmap for tracking allocated blocks
    hring_id: String,           // Unique identifier for this pool
}
```

#### HringAddr
```rust
pub struct HringAddr(u64);

impl HringAddr {
    // 64-bit address format:
    // bits 63-32: size_part (32 bits for block size/alignment)
    // bits 31-0:  block_index (32 bits for block index)

    pub fn new(size: usize, block_index: u32) -> Self
    pub fn size(&self) -> usize
    pub fn block_index(&self) -> u32
    pub fn is_valid(&self) -> bool
}
```

#### Hring
```rust
pub struct Hring {
    fd: i32,                    // io_uring file descriptor
    sq_ptr: *mut u8,           // Submission queue pointer
    cq_ptr: *mut u8,           // Completion queue pointer
    sq_entries: u32,           // Submission queue entries
    cq_entries: u32,           // Completion queue entries
    flags: u32,                // Ring flags
    features: u32,             // Ring features
}
```

## Implementation Patterns

### Memory Pool Management

#### Bitmap Allocation Strategy
- **Block Size**: 4KB (matching vendor implementation)
- **Bitmap Representation**: 1 bit per block (0 = free, 1 = allocated)
- **Allocation Algorithm**: First-fit with bounds checking
- **Fragmentation Handling**: Contiguous block allocation only

#### Memory Layout
```
┌─────────────────────────────────────────────────────────────┐
│                     Shared Memory Pool                      │
├─────────────────────────────────────────────────────────────┤
│ Bitmap (1 bit per 4KB block)                               │
├─────────────────────────────────────────────────────────────┤
│ Available Memory Blocks (4KB each)                         │
│ [Block 0][Block 1][Block 2]...[Block N-1]                  │
└─────────────────────────────────────────────────────────────┘
```

#### Allocation Process
1. Calculate required blocks: `ceil(size / block_size)`
2. Find contiguous free blocks in bitmap
3. Mark blocks as allocated in bitmap
4. Return HringAddr with size and starting block index

#### Deallocation Process
1. Validate HringAddr bounds
2. Calculate block range from address
3. Mark blocks as free in bitmap
4. Update memory tracking

### io_uring Integration

#### Submission Queue Operations
- **Primary Operation**: `IORING_OP_NOP` for zero-copy IPC
- **User Data**: HringAddr (64-bit memory reference)
- **Flags**: `IOSQE_IO_LINK` for chained operations when needed

#### Completion Queue Processing
- **Event Handling**: Process completions in batches
- **Error Detection**: Check result codes for operation failures
- **Memory Cleanup**: Free resources on failed operations

#### Ring Sharing Architecture
```
Parent Process                     Child Process
┌─────────────┐                 ┌─────────────┐
│   Hring     │                 │   Hring     │
│   Ring      │                 │   Ring      │
│             │                 │             │
│ SQ ────────┐│                 │┌───────── SQ │
│ CQ ────────┘│                 │└───────── CQ │
└──────┬──────┘                 └───────┬─────┘
       │                               │
       └─────────── Shared Memory ──────┘
       │           Completion Ring       │
       └───────────────────────────────┘
```

### Multi-Process Architecture

#### Process Creation Flow
1. **Parent**: Create shared memory pool with unique hring_id
2. **Parent**: Initialize io_uring ring with proper parameters
3. **Parent**: Fork child process using `fork()`
4. **Child**: Attach to parent's shared memory using hring_id
5. **Child**: Use `pidfd_getfd()` to get ring file descriptor
6. **Child**: Map completion ring using obtained descriptor

#### Shared Memory Naming
- **Pattern**: `/dev/shm/ultrapubsub_[hring_id]`
- **Uniqueness**: Include process ID and timestamp
- **Cleanup**: Automatic unlink on last process exit

#### File Descriptor Sharing
```rust
// Parent creates pidfd for child
let pidfd = syscall(SYS_pidfd_open, child_pid, 0);

// Child uses pidfd_getfd to get ring fd
let ring_fd = syscall(SYS_pidfd_getfd, pidfd, parent_ring_fd, 0);
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
- **Ring Sharing**: Return `Error::RingSharingFailed`
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

### io_uring Optimizations
- **Batch Operations**: Process multiple completions at once
- **Ring Buffer Management**: Efficient head/tail pointer updates
- **System Call Reduction**: Minimize io_uring_enter calls

### Multi-Process Optimizations
- **Shared Memory**: Direct memory access between processes
- **Efficient Signaling**: Use io_uring for process coordination
- **Resource Cleanup**: Automatic cleanup on process exit

## Security Considerations

### Memory Safety
- **Bounds Checking**: All array accesses are bounds-checked
- **Null Termination**: Proper handling of C-style strings
- **Reference Validation**: HringAddr validation before use

### Process Isolation
- **File Descriptor Sharing**: Secure sharing using pidfd_getfd
- **Memory Permissions**: Appropriate read/write permissions
- **Resource Limits**: Prevent resource exhaustion attacks

### Data Integrity
- **Signature Verification**: Cryptographic-style verification
- **Checksum Validation**: Additional integrity checks
- **Corruption Detection**: Detect and handle data corruption

## Testing Strategy

### Unit Tests
- **Memory Pool**: Test allocation, deallocation, error cases
- **HringAddr**: Test creation, validation, operations
- **Bitmap**: Test bit manipulation, bounds checking
- **Shared Memory**: Test creation, attachment, cleanup

### Integration Tests
- **Multi-Process**: Test fork/exec, ring sharing
- **Large Data**: Test blob generation, verification
- **Performance**: Test latency, throughput metrics
- **Error Handling**: Test error conditions, recovery

### Stress Tests
- **High Frequency**: Test 40Hz messaging scenarios
- **Memory Pressure**: Test under memory constraints
- **Concurrent Access**: Test multiple processes
- **Long Duration**: Test stability over time

## Future Enhancements

### Performance Improvements
- **Adaptive Block Sizes**: Dynamic block size allocation
- **Memory Compression**: Optional compression for large data
- **Batch Processing**: More efficient batch operations

### Feature Enhancements
- **Message Filtering**: Subscriber-side message filtering
- **Priority Queuing**: Priority-based message handling
- **Statistics**: Performance metrics and monitoring

### Platform Support
- **Alternative Kernels**: Support for other async I/O interfaces
- **Cross-Platform**: Windows/macOS support (if needed)
- **Hardware Acceleration**: GPU-assisted operations