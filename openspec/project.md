# Project Context

## Purpose
The purpose of the library is to create the fastest possible publish/subscribe message system on modern Linux machines.

Current implementations of fast publish/subscribe technologies like pynng do not go far enough to both guarantee message delivery to subscribers and reduce the number of memory copies and syscalls.

The system must handle large data packets (typically ~33 MB, 20 MB in testing) at high frequency (40 Hz) with multiple concurrent subscribers (6 in test scenario), implementing sophisticated memory management and backpressure mechanisms.

## Tech Stack
- **Core Implementation**: Rust for performance and memory safety
- **Language Bindings**: Python bindings using PyO3 for seamless integration
- **Python Management**: Astral uv for Python dependency management and execution
- **Kernel Interface**: io_uring for asynchronous I/O operations
- **Memory Management**: Shared memory pools with reference counting
- **Data Integration**: Zero-copy NumPy array handling using buffer protocol
- **Performance Target**: 800 MB/s sustained throughput (20 MB × 40 Hz)

## Project Conventions

### Code Style
- **Rust**: Follow standard Rust formatting (rustfmt), snake_case for functions and variables, PascalCase for types
- **Python**: PEP8 compliance, type hints required, docstrings for all public APIs
- **Performance**: Zero-copy patterns preferred, minimize allocations in hot paths
- **Safety**: Explicit error handling, no unwinding across FFI boundaries
- **Python Management**: Use astral uv for all Python operations (uv init, uv add, uv run)

### Architecture Patterns
- **Zero-Copy Messaging**: Direct memory sharing between publisher and subscribers
- **Reference Counting**: Track buffer ownership across multiple subscribers
- **Memory Pool Management**: Dynamic allocation with pressure monitoring and backpressure
- **Async/Await**: Rust async patterns for io_uring operations
- **Backpressure Propagation**: Progressive warnings → hard limits → data dropping

### Testing Strategy
- **Performance Benchmark**: 20 MB packets at 40 Hz (800 MB/s) with 6 concurrent subscribers
- **Memory Stress Testing**: Monitor buffer retention and warning system effectiveness
- **Warning System Validation**: Verify progressive warnings before hard limits are reached
- **Data Drop Scenarios**: Test graceful degradation when memory limits exceeded
- **Latency Requirements**: Ensure 40 Hz timing is maintained under all load conditions
- **Memory Safety**: Validate no double-free or use-after-free in multi-process scenarios

### Git Workflow
- **Branching**: feature/ branches for new capabilities, fix/ for bug fixes
- **Commits**: Conventional commits with scope (e.g., feat: add memory pressure monitoring)
- **PR Process**: All changes require review, performance benchmarks for optimizations
- **Release**: Semantic versioning, breaking changes only when absolutely necessary

## Domain Context
- **Linux Kernel io_uring**: Asynchronous I/O interface for high-performance operations
- **Shared Memory IPC**: Zero-copy data transfer between processes using shared memory pools
- **Memory Pressure Management**: Subscriber buffer monitoring and warning system
- **Multi-subscriber Coordination**: 6 concurrent processes sharing memory pool with reference counting
- **Real-time Constraints**: 40 Hz production requires 25ms processing window per message
- **Backpressure Strategy**: Progressive warnings → hard limits → data dropping to maintain system stability

## Important Constraints
- **Platform**: Linux-only (io_uring is Linux-specific)
- **Data Size**: Large packets (~33 MB typical, 20 MB in test scenario)
- **Performance Target**: 40 Hz production rate with 6 concurrent subscribers (800 MB/s)
- **Memory Management**: Warning system for subscriber buffer retention with progressive escalation
- **Hard Limits**: Publisher must drop data when memory thresholds exceeded to prevent system failure
- **Memory Safety**: Buffer memory must not be freed until all references are released
- **Real-time Requirements**: Must maintain 40 Hz timing under all conditions
- **Zero-Copy**: Minimize memory copies, especially for large NumPy arrays

## External Dependencies
- **io_uring**: Linux kernel interface for asynchronous I/O operations
- **PyO3**: Rust bindings for Python to create seamless Python APIs
- **Astral uv**: Python package and environment management
- **NumPy**: Buffer protocol integration for zero-copy array handling
- **Memory Monitoring**: Linux memory tracking APIs for pressure detection
- **Process Coordination**: Inter-process signaling for warning propagation
- **Performance Counters**: High-resolution timing for 40 Hz validation and benchmarking

The vendor/io-uring-ipc folder demonstrates an io_uring based IPC method. I would like to use its approach and lessons learned to implement my program, targeting similar performance (25.15 msgs/usec, 39.76 ns latency) but with much larger data packets and sophisticated memory management.
