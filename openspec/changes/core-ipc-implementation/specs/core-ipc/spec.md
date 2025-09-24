## ADDED Requirements

### Requirement: Core IPC Architecture
The system SHALL implement a proper io_uring-based inter-process communication architecture using shared memory pools and memory reference passing.

#### Scenario: Multi-process communication setup
- **WHEN** the system initializes a new IPC instance
- **THEN** it SHALL create a shared memory pool with bitmap-based allocation
- **AND** setup io_uring rings for cross-process communication
- **AND** use `IORING_OP_NOP` operations to send memory references (not data)

#### Scenario: Memory allocation from shared pool
- **WHEN** a publisher needs to send a message
- **THEN** it SHALL allocate a block from the shared memory pool using bitmap allocation
- **AND** receive a 64-bit memory reference (`HringAddr`)
- **AND** copy message data to the allocated shared memory block

#### Scenario: Zero-copy message transmission
- **WHEN** a publisher transmits a message
- **THEN** it SHALL queue the 64-bit memory reference using `IORING_OP_NOP`
- **AND** submit the operation to io_uring for cross-process delivery
- **AND** NOT copy the actual message data (zero-copy)

### Requirement: Shared Memory Pool Management
The system SHALL implement a bitmap-based shared memory pool for efficient block allocation and deallocation.

#### Scenario: Block allocation
- **WHEN** a process requests memory allocation
- **THEN** the system SHALL find the first free block using bitmap scanning
- **AND** mark the block as allocated atomically
- **AND** return a 64-bit address containing both size and block index

#### Scenario: Block deallocation
- **WHEN** a process releases memory back to the pool
- **THEN** the system SHALL mark the corresponding bitmap bit as free
- **AND** validate that the block was previously allocated
- **AND** make the block available for future allocations

#### Scenario: Memory pool initialization
- **WHEN** the shared memory pool is created
- **THEN** it SHALL initialize all bitmap bits to free (1)
- **AND** allocate the specified number of 4KB blocks
- **AND** setup proper shared memory mapping in `/dev/shm/`

### Requirement: Multi-Process Coordination
The system SHALL support true inter-process communication with proper process lifecycle management.

#### Scenario: Process creation and ring sharing
- **WHEN** the system needs multiple processes
- **THEN** it SHALL create child processes using fork/exec
- **AND** use `pidfd_getfd()` for cross-process io_uring ring access
- **AND** establish shared memory regions accessible by all processes

#### Scenario: Cross-process message delivery
- **WHEN** a parent process sends a memory reference
- **THEN** the child process SHALL receive the reference via io_uring completion
- **AND** access the shared memory using the 64-bit address
- **AND** process the message data directly from shared memory

#### Scenario: Process cleanup and resource management
- **WHEN** processes exit or communication ends
- **THEN** the system SHALL properly cleanup shared memory resources
- **AND** unlink shared memory objects from `/dev/shm/`
- **AND** close io_uring file descriptors
- **AND** ensure no memory leaks occur

### Requirement: Performance Targets
The system SHALL achieve real IPC performance comparable to vendor/io-uring-ipc implementation.

#### Scenario: Latency measurement
- **WHEN** benchmarking inter-process communication
- **THEN** the system SHALL achieve average latency of ~270ns per message
- **AND** sustain throughput of at least 3.69 messages per microsecond
- **AND** measure actual IPC (not stdout write performance)

#### Scenario: Large data handling
- **WHEN** transmitting large data packets (20MB+)
- **THEN** the system SHALL maintain zero-copy semantics
- **AND** achieve sustained throughput of 800 MB/s
- **AND** support 40 Hz production rate with 6 concurrent subscribers

## REMOVED Requirements

### Requirement: Flawed Single-Process Implementation
**Reason**: The previous implementation used `IORING_OP_WRITE` to stdout instead of real IPC, making performance metrics meaningless.
**Migration**: Complete rewrite required - no migration path from the flawed architecture.

### Requirement: Linear Buffer Message Queue
**Reason**: Simple linear buffers with atomic counters cannot support sophisticated memory management or multi-process coordination.
**Migration**: Replace with bitmap-based shared memory pool system.

### Requirement: Direct Data Copying
**Reason**: Copying message data instead of passing memory references defeats the purpose of high-performance IPC.
**Migration**: Implement zero-copy architecture using 64-bit memory references.