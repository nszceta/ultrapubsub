## Why
The current synchronous broadcast implementation works functionally but fails to meet performance targets. Testing shows we achieve only 0.356 GB/s and 1.8 Hz, far below the project requirements of 1.4 GB/s and 40 Hz. The bottleneck is primarily in memory copy overhead and complex synchronization. The architecture has evolved to use memory-mapped numpy arrays with minimal Rust synchronization for true zero-copy performance.

## What Changes
- **MODIFIED**: Architecture to use memory-mapped files instead of shared memory
- **MODIFIED**: Data handling from Rust to Python numpy arrays for zero-copy access
- **MODIFIED**: Synchronization layer to minimal Rust futex operations only
- **ADDED**: True zero-copy numpy array sharing across processes
- **ADDED**: Memory-mapped file management for 35MB payloads
- **ADDED**: Minimal Rust synchronization module (futex operations only)
- **REMOVED**: Complex Rust broadcast buffer data management

## Impact
- **Affected specs**: core-ipc
- **Affected code**: src/sync.rs, python/ultrapubsub/zerocopy.py, performance tests
- **Performance impact**: Expected 10-15x improvement in throughput and frequency via zero-copy
- **Breaking changes**: Major - new architecture with memory-mapped numpy arrays
- **Migration requirements**: Complete migration to memory-mapped numpy array model