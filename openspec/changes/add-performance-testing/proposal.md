## Why
The Core IPC specification defines performance targets of 800 MB/s throughput (20MB × 40Hz) with 6 concurrent subscribers, but there are currently no tests to verify these targets are achieved in practice. We need comprehensive performance testing to validate that the system meets its design requirements and can handle the specified workload reliably.

## What Changes
- Add performance testing capability to verify 20MB message transmission at 40Hz with 6 subscribers
- Implement payload integrity verification using encoded timestamps and unique signatures
- Add message ordering validation to ensure subscribers receive messages in correct sequence
- Create latency measurement and reporting infrastructure
- Add stress testing scenarios to validate system stability under sustained load
- Implement Python-based performance tests that can be run as part of CI/CD

## Impact
- Affected specs: core-ipc (adding performance testing requirements)
- Affected code: New Python test modules, performance monitoring utilities
- Dependencies: Additional test dependencies for timing and performance measurement
- Build: Integration with existing test framework and CI/CD pipeline