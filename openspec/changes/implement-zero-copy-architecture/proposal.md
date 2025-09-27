## Why
The existing Rust-based shared memory broadcast architecture, while functional, creates unnecessary complexity and fails to achieve true zero-copy performance for numpy arrays. By moving to a memory-mapped file architecture with minimal Rust synchronization, we can achieve true zero-copy numpy array sharing across processes while dramatically reducing code complexity and maintenance burden.

## What Changes
- **ADDED**: Memory-mapped file architecture for zero-copy numpy array sharing
- **ADDED**: Pure Python numpy array handling using np.memmap()
- **ADDED**: Minimal Rust synchronization layer (~200 lines) with only futex operations
- **REMOVED**: Complex Rust broadcast buffer management (1000+ lines)
- **REMOVED**: Shared memory allocation and management complexity
- **MODIFIED**: Architecture from Rust-heavy to Python-heavy with minimal sync layer
- **UPDATED**: Performance targets to reflect zero-copy capabilities

## Impact
- **Affected specs**: core-ipc
- **Affected code**:
  - NEW: src/sync.rs (minimal futex synchronization)
  - NEW: python/ultrapubsub/zerocopy.py (zero-copy numpy arrays)
  - REMOVED: src/broadcast_buffer.rs (complex Rust buffer management)
  - SIMPLIFIED: src/lib.rs (minimal Rust API)
- **Performance impact**: Expected 10-15x improvement through true zero-copy numpy array sharing
- **Complexity impact**: 80% reduction in Rust code complexity
- **Breaking changes**: Major - new architecture with memory-mapped numpy arrays
- **Migration requirements**: Complete migration to memory-mapped numpy array model