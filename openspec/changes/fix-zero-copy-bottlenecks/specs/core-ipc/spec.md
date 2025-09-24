## MODIFIED Requirements

### Requirement: Shared Memory Pool with Variable-Size Allocation
The system SHALL provide a shared memory pool that supports variable-size allocations to eliminate memory waste.

#### Scenario: Efficient 20MB message allocation
- **WHEN** a 20MB message is published
- **THEN** the memory pool allocates exactly 20MB (not 32MB)
- **AND** the allocation uses the smallest power-of-two block size that can accommodate the request

#### Scenario: Memory reuse after consumption
- **WHEN** a subscriber finishes processing a message
- **THEN** the memory is immediately returned to the pool for reuse
- **AND** the publisher can allocate it for the next message

### Requirement: True Zero-Copy Message Publishing
The system SHALL implement true zero-copy semantics by eliminating memory copying in the publisher.

#### Scenario: Direct shared memory placement
- **WHEN** a publisher sends a 20MB message
- **THEN** the data is placed directly into shared memory without copying
- **AND** only a 64-bit memory reference is passed through io_uring

#### Scenario: Subscriber zero-copy access
- **WHEN** a subscriber receives a message
- **THEN** the subscriber accesses the original data directly from shared memory
- **AND** no memory copying occurs on the subscriber side

### Requirement: Efficient io_uring Reference Passing
The system SHALL use io_uring efficiently for passing memory references between processes.

#### Scenario: Lightweight reference passing
- **WHEN** a publisher queues a message
- **THEN** io_uring NOP operation carries only the 64-bit memory address in user_data
- **AND** the actual data remains in shared memory without being copied

#### Scenario: Multi-subscriber reference sharing
- **WHEN** multiple subscribers process the same message
- **THEN** all subscribers receive the same memory reference
- **AND** each accesses the same shared memory location

### Requirement: High-Performance Target Throughput
The system SHALL achieve 800MB/s throughput for 20MB messages at 40Hz with 6 subscribers.

#### Scenario: Sustained performance under load
- **WHEN** publishing 20MB messages at 40Hz for 10 seconds
- **THEN** the system processes 400 messages (8000MB total)
- **AND** throughput remains above 800MB/s throughout the test
- **AND** memory usage remains stable with proper reuse

#### Scenario: Efficiency metrics
- **WHEN** measuring performance efficiency
- **THEN** the system achieves at least 95% of theoretical maximum throughput
- **AND** CPU usage remains reasonable for the workload

## ADDED Requirements

### Requirement: Bitmap-Based Memory Management
The system SHALL use bitmap-based allocation for fine-grained memory management.

#### Scenario: Minimum block allocation
- **WHEN** allocating memory of any size
- **THEN** the system allocates in 64-byte minimum blocks
- **AND** uses a bitmap to track allocation status of each block

#### Scenario: Contiguous allocation finding
- **WHEN** requesting a multi-block allocation
- **THEN** the system efficiently finds contiguous free blocks
- **AND** marks them as allocated atomically

### Requirement: Memory Pool Performance Monitoring
The system SHALL provide visibility into memory pool performance characteristics.

#### Scenario: Allocation efficiency tracking
- **WHEN** the memory pool is in use
- **THEN** allocation efficiency can be measured
- **AND** fragmentation levels are monitored
- **AND** waste due to block size rounding is minimized