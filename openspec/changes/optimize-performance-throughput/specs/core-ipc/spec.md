## MODIFIED Requirements

### Requirement: Optimized Synchronous Broadcast Throughput
The system SHALL achieve a minimum throughput of 0.8 GB/s when broadcasting 35MB messages to 6 subscribers at 40 Hz frequency.

#### Scenario: Performance Target Validation
- **WHEN** broadcasting 35MB messages to 6 subscribers for 10 seconds
- **THEN** the system shall achieve at least 0.8 GB/s total throughput
- **AND** the broadcast frequency shall be at least 40 Hz
- **AND** all subscribers shall receive identical messages

### Requirement: Dynamic Subscriber Count Architecture
The system SHALL NOT hardcode the number of subscribers anywhere in the implementation. Subscriber count SHALL be determined dynamically at runtime through explicit registration and SHALL support any number from 0 to 32 concurrent subscribers.

#### Scenario: Dynamic Subscriber Registration
- **WHEN** processes register as subscribers at runtime
- **THEN** the system shall accept any number of subscribers up to the maximum of 32
- **AND** no hardcoded subscriber limits shall exist in the codebase
- **AND** subscriber registration shall be completely dynamic and process-independent

### Requirement: Single Publisher Process Constraint
The system SHALL enforce that exactly one publisher instance exists per process, and multiple publishers SHALL NOT be created within the same process. Publishers MUST be launched as standalone spawned processes with no fork inheritance.

#### Scenario: Publisher Process Isolation
- **WHEN** a publisher is created in a process
- **THEN** no additional publishers shall be allowed in the same process
- **AND** attempts to create multiple publishers in one process shall fail
- **AND** each publisher shall be bound to exactly one process for its lifetime
- **AND** publishers MUST be launched as separate processes, not threads

### Requirement: One Process Per Subscriber Architecture
The system SHALL enforce that exactly one subscriber instance exists per process, and multiple subscribers SHALL NOT be created within the same process. Subscribers MUST be launched as standalone spawned processes with no fork inheritance.

#### Scenario: Subscriber Process Isolation
- **WHEN** a subscriber is created in a process
- **THEN** no additional subscribers shall be allowed in the same process
- **AND** attempts to create multiple subscribers in one process shall fail
- **AND** each subscriber shall be bound to exactly one process for its lifetime
- **AND** subscribers MUST be launched as separate processes, not threads

### Requirement: Process-Based Benchmark Architecture
Performance benchmarks MUST launch the publisher as a standalone spawned process and each subscriber as a separate standalone process. The orchestrator SHALL coordinate these processes with proper timeouts and resource management.

#### Scenario: Process Orchestration
- **WHEN** running performance benchmarks
- **THEN** the publisher SHALL be launched as a standalone spawned process
- **AND** each subscriber SHALL be launched as a separate standalone process
- **AND** the orchestrator SHALL implement proper process management with timeouts
- **AND** no processes shall be created through fork inheritance
- **AND** all processes shall have proper cleanup and resource management

### Requirement: Mandatory Benchmark Timeouts
Performance benchmarks SHALL implement reasonable timeouts to prevent system resource exhaustion and ensure tests complete within acceptable timeframes.

#### Scenario: Timeout Enforcement
- **WHEN** running performance benchmarks
- **THEN** publisher startup SHALL timeout within 30 seconds if no subscribers register
- **AND** subscriber processes SHALL timeout within 60 seconds of test start
- **AND** message receive operations SHALL timeout within 1 second
- **AND** overall test duration SHALL NOT exceed 120 seconds
- **AND** process cleanup SHALL timeout within 10 seconds
- **AND** orchestrator SHALL terminate all processes if total test time exceeds 150 seconds

#### Scenario: Reduced Acknowledgment Overhead
- **WHEN** a publisher broadcasts a message
- **THEN** the acknowledgment mechanism shall minimize atomic operations
- **AND** subscriber acknowledgments shall be processed individually with maximum efficiency
- **AND** the total acknowledgment processing time shall be less than 5ms per message

## ADDED Requirements

### Requirement: Zero-Copy Memory Access
The system SHALL implement zero-copy memory access patterns for large payload transfers to minimize memory copy overhead.

#### Scenario: Direct Memory Access
- **WHEN** broadcasting large messages (>1MB)
- **THEN** the system shall use direct memory access without intermediate copies
- **AND** memory mapping shall be optimized for cache efficiency
- **AND** the memory copy overhead shall be less than 1% of total transfer time

### Requirement: Futex-Based Synchronization
The system SHALL implement futex-based synchronization for maximum performance in cross-process coordination, replacing pthread mutexes with more efficient atomic operations.

#### Scenario: Futex Wait/Wake Optimization
- **WHEN** processes need to coordinate on shared memory access
- **THEN** the system shall use futex operations for minimum overhead
- **AND** uncontended operations shall complete with pure atomic instructions
- **AND** contended operations shall use efficient kernel wait/wake mechanisms

### Requirement: Optimized Atomic Operations
The system SHALL optimize atomic operations for subscriber coordination to reduce contention and improve cache efficiency.

#### Scenario: Efficient Subscriber Tracking
- **WHEN** multiple subscribers acknowledge receipt
- **THEN** atomic operations shall use cache-friendly memory layouts
- **AND** subscriber acknowledgment arrays shall be aligned to cache lines
- **AND** the average time per atomic operation shall be less than 100ns

### Requirement: Individual Acknowledgment Optimization
The system SHALL optimize individual acknowledgment processing to minimize latency and atomic operation overhead while maintaining synchronous wait-for-all semantics.

#### Scenario: Efficient Individual Acknowledgments
- **WHEN** subscribers acknowledge message receipt
- **THEN** each acknowledgment shall use optimized atomic operations
- **AND** acknowledgment processing shall minimize cache line contention
- **AND** the total acknowledgment time shall scale linearly with subscriber count

### Requirement: Performance Monitoring
The system SHALL provide performance monitoring capabilities to measure throughput, latency, and resource utilization in real-time.

#### Scenario: Performance Metrics Collection
- **WHEN** the system is operating
- **THEN** it shall collect metrics for messages sent, received, and acknowledgment latency
- **AND** metrics shall be available through a monitoring interface
- **AND** the overhead of metrics collection shall be less than 1% of total throughput

### Requirement: Adaptive Timing Optimization
The system SHALL adapt timing parameters based on observed performance to maintain optimal throughput under varying load conditions.

#### Scenario: Dynamic Timing Adjustment
- **WHEN** acknowledgment times vary significantly
- **THEN** the system shall adjust timing parameters dynamically
- **AND** wait times shall be optimized based on historical performance
- **AND** the system shall maintain consistent throughput even with variable subscriber response times