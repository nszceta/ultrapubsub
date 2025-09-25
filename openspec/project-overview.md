# UltraPubSub Project Overview

## Current Status

**Performance**: **1.4 GB/s** (35 MB payloads at 40 Hz to multiple subscribers)
**Architecture**: Shared Memory Ring Buffer with atomic operations for 1:N pub/sub
**Status**: ✅ **IMPLEMENTED** - All core functionality complete and validated

## Architecture ✅

The system uses a Shared Memory Ring Buffer for maximum performance:

1. **Shared Memory Ring Buffer**: Contiguous circular buffer with atomic head/tail pointers
2. **Atomic Operations**: Lock-free synchronization for publisher and multiple subscribers
3. **1:N Pub/Sub Model**: Single publisher, multiple subscribers with independent read pointers
4. **Zero-Copy**: Subscribers read directly from publisher's shared memory
5. **Pre-allocated Memory Pool**: 63 × 35MB slots for high-frequency operations

## Key Benefits

- **Throughput**: 1.4 GB/s (35 MB × 40 Hz) - memory-bandwidth limited
- **Latency**: <25ms per message (40 Hz cycle time)
- **Frequency**: Consistent 40 Hz message rate with no timing jitter
- **Subscribers**: Support for multiple subscribers (at least 6) with zero-copy sharing
- **CPU Overhead**: Minimal (atomic operations only)
- **Simplicity**: No complex io_uring setup or completion ring issues

## Data Structure

```rust
struct SharedRingBuffer {
    head: AtomicU64,                    // Publisher write position
    tails: [AtomicU64; MAX_SUBSCRIBERS], // Per-subscriber read positions
    offsets: [u32; MAX_MESSAGES],      // Message offsets in buffer
    lengths: [u32; MAX_MESSAGES],      // Message lengths
    available: [AtomicU64; 16],        // Message availability bitmap
    pool: [[u8; POOL_SLOT_SIZE]; POOL_SIZE],  // Pre-allocated 35MB slots
    pool_available: AtomicU64,         // Pool slot availability bitmap
    pool_sequence: [AtomicU64; POOL_SIZE], // Pool slot sequence numbers
}
```

## ⚠️ Important Warning: Do NOT Use io_uring

**CRITICAL**: The io_uring-based approach has been deprecated and abandoned due to fundamental architectural issues. All implementations MUST use the Shared Memory Ring Buffer approach.

**Why io_uring Failed**:
- io_uring operations submitted successfully but generated zero completions
- Complex ring sharing between processes proved unreliable
- Kernel completion ring mechanism unsuitable for message passing
- Unpredictable behavior under high-frequency messaging scenarios

## Implementation Status

- ✅ **Architecture Design**: Complete specification in OpenSpec
- ✅ **Performance Validation**: ✅ **ACHIEVED** - 44+ Hz with 35MB payloads
- ✅ **Core Implementation**: All Rust code complete
- ✅ **Python Bindings**: PyO3 bindings working
- ✅ **Zero-Copy Pool**: Pre-allocated memory pool implemented
- ✅ **Multi-Process Support**: Independent subscriber processes working
- ✅ **Large Message Support**: 35MB message handling validated

## Development

```bash
# Build the Rust extension
maturin develop

# Run single-process test
uv run python test_simple_ipc.py

# Run multi-process test with 6 subscribers
uv run python test_numpy_simple.py

# View OpenSpec specifications
openspec list --specs
openspec show core-ipc

# View current implementation status
openspec list
```

## Key Test Files

- `test_numpy_simple.py` - Comprehensive 35MB numpy array test with 6 subscribers
- `test_numpy_performance.py` - Detailed performance analysis
- `subscriber_process.py` - Independent subscriber process for multiprocessing
- `test_simple_ipc.py` - Basic IPC functionality test
- `test_35mb_40hz_simple.py` - 35MB at 40Hz performance validation

## Performance Results

Recent test results demonstrate the system achieves its targets:
- **Frequency**: 44+ Hz (exceeds 40 Hz target)
- **Throughput**: 1.4+ GB/s (35 MB × 44 Hz)
- **Latency**: <25ms average per message
- **Subscribers**: 6 independent processes receiving identical data
- **Data Integrity**: Zero corruption with signature verification