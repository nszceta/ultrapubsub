# Project Context

## Purpose
The purpose of the library is to create the fastest possible synchronous broadcast message for modern Linux machines.

Current implementations of fast publish/subscribe technologies like pynng do not go far enough to both guarantee message delivery to subscribers and reduce the number of memory copies and syscalls.

The system must handle large data packets (typically about 35 MB) at high frequency (40 Hz) with multiple concurrent subscribers (6 in test scenario), implementing sophisticated memory management and backpressure mechanisms. These are examples and these values must never be hardcoded.

## Tech Stack
- **Core Implementation**: Rust for performance and memory safety
- **Language Bindings**: Python bindings using PyO3 for seamless integration
- **Python Management**: Astral uv for Python dependency management and execution
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
- **Event-Driven Processing**: libuv based instead of polling
- **Completion Ring Sharing**: Multiple subscribers accessing same completion ring
- **Backpressure Propagation**: Progressive warnings → hard limits → data dropping

### Testing Strategy
- **Performance Benchmark**: 35 MB packets at 40 Hz with 6 concurrent subscribers
- **Memory Stress Testing**: Monitor buffer retention and warning system effectiveness
- **Warning System Validation**: Verify progressive warnings before hard limits are reache
- **Data Drop Scenarios**: Test graceful degradation when memory limits exceeded
- **Latency Requirements**: Ensure 40 Hz timing is maintained under all load conditions
- **Memory Safety**: Validate no double-free or use-after-free in multi-process scenarios
- **IPC Communication**: Test end-to-end message passing by completion ring issue

### Git Workflow
- **Branching**: feature/ branches for new capabilities, fix/ for bug fixes
- **Commits**: Conventional commits with scope (e.g., feat: add memory pressure monitoring)
- **PR Process**: All changes require review, performance benchmarks for optimizations
- **Release**: Semantic versioning, breaking changes only when absolutely necessary

## Domain Context
- **Shared Memory IPC**: Zero-copy data transfer between processes using shared memory pools
- **Memory Pressure Management**: Subscriber buffer monitoring and warning system
- **Multi-subscriber Coordination**: 6 concurrent processes sharing memory pool with reference counting
- **Real-time Constraints**: 40 Hz production requires 25ms processing window per message
- **Backpressure Strategy**: Progressive warnings → hard limits → data dropping to maintain system stability

## Important Constraints
- **Platform**: Linux-only (libuv and Linux-specific shared memory semantics)
- **Data Size**: Large packets (~33 MB typical, 20 MB in test scenario)
- **Performance Target**: 40 Hz production rate with 6 concurrent subscribers (800 MB/s)
- **Memory Management**: Warning system for subscriber buffer retention with progressive escalation
- **Hard Limits**: Publisher must drop data when memory thresholds exceeded to prevent system failure
- **Memory Safety**: Buffer memory must not be freed until all references are released
- **Real-time Requirements**: Must maintain 40 Hz timing under all conditions
- **Zero-Copy**: Minimize memory copies, especially for large NumPy arrays

**Tools and Commands:**
- Build: `maturin develop`
- Spec management: `openspec list`, `openspec validate`, `openspec show`

Use uv extensively when dealing with Python code. Use `uv add` to add packages to the local virtual environment and launch python scripts with `uv run`

NEVER modify sys.path without my explicit permission.

## External Dependencies
- **PyO3**: Rust bindings for Python to create seamless Python APIs
- **Astral uv**: Python package and environment management
- **NumPy**: Buffer protocol integration for zero-copy array handling
- **Memory Monitoring**: Linux memory tracking APIs for pressure detection
- **Process Coordination**: Inter-process signaling for warning propagation
- **Performance Counters**: High-resolution timing for 40 Hz validation and benchmarking
