## ADDED Requirements

### Requirement: Shared Memory Ring Buffer Architecture
The system SHALL implement a shared memory ring buffer with atomic operations for lock-free 1:N pub/sub messaging.

#### Scenario: Ring Buffer Creation
- **WHEN** creating a shared memory region for pub/sub
- **THEN** the system SHALL allocate a contiguous circular buffer
- **AND** the system SHALL initialize atomic head pointer for publisher
- **AND** the system SHALL initialize atomic tail pointers for each subscriber
- **AND** the system SHALL create message availability bitmap

#### Scenario: Zero-Copy 1:N Publishing
- **WHEN** a publisher writes a message
- **THEN** the system SHALL write directly to shared memory buffer
- **AND** the system SHALL update atomic head pointer
- **AND** the system SHALL set availability bits for all subscribers
- **AND** no data copying SHALL occur between publisher and subscribers

#### Scenario: Concurrent Subscriber Access
- **WHEN** multiple subscribers read messages
- **THEN** each subscriber SHALL maintain independent tail pointers
- **AND** subscribers SHALL read directly from shared memory
- **AND** no contention SHALL occur between subscribers
- **AND** all subscribers SHALL see identical message data

### Requirement: Atomic Operations for Synchronization
The system SHALL use atomic operations for all synchronization to achieve lock-free performance.

#### Scenario: Atomic Head Updates
- **WHEN** publisher advances write position
- **THEN** head pointer SHALL be updated atomically
- **AND** the operation SHALL be wait-free
- **AND** no locks SHALL be acquired or contended

#### Scenario: Atomic Tail Updates
- **WHEN** subscriber advances read position
- **THEN** tail pointer SHALL be updated atomically
- **AND** the operation SHALL be wait-free
- **AND** no locks SHALL be acquired or contended

#### Scenario: Availability Bitmap Updates
- **WHEN** publisher marks message as available
- **THEN** bitmap SHALL be updated atomically
- **AND** subscribers SHALL see updates immediately
- **AND** no race conditions SHALL occur

### Requirement: High-Performance Memory Layout
The system SHALL use optimized memory layout for maximum cache efficiency and throughput.

#### Scenario: Contiguous Buffer Access
- **WHEN** accessing message data
- **THEN** all data SHALL reside in contiguous memory
- **AND** access patterns SHALL be cache-friendly
- **AND** memory prefetching SHALL be effective

#### Scenario: Minimal Metadata Overhead
- **WHEN** storing message metadata
- **THEN** metadata SHALL be compact and aligned
- **AND** metadata access SHALL not pollute data caches
- **AND** total overhead SHALL be < 5% of buffer size

#### Scenario: NUMA-Aware Allocation
- **WHEN** allocating shared memory on NUMA systems
- **THEN** memory SHALL be allocated on optimal NUMA node
- **AND** cross-node memory access SHALL be minimized
- **AND** performance SHALL be consistent across NUMA configurations

### Requirement: Notification Mechanisms
The system SHALL provide efficient notification mechanisms for subscriber wakeup.

#### Scenario: Event-Driven Notifications
- **WHEN** publisher writes new messages
- **THEN** the system SHALL notify waiting subscribers
- **AND** notification SHALL use eventfd or similar mechanism
- **AND** spurious wakeups SHALL be minimized

#### Scenario: Poll-Based Discovery
- **WHEN** subscribers check for new messages
- **THEN** subscribers SHALL poll availability bitmap
- **AND** polling SHALL use efficient atomic operations
- **AND** CPU usage SHALL be minimal during idle periods

#### Scenario: Batch Notification
- **WHEN** multiple messages are published rapidly
- **THEN** the system SHALL batch notifications
- **AND** notification frequency SHALL adapt to message rate
- **AND** system responsiveness SHALL be maintained

## MODIFIED Requirements

### Requirement: Performance Targets
The system SHALL achieve significantly higher performance targets with the new architecture.

#### Scenario: High-Frequency Messaging
- **WHEN** sending messages at high frequency
- **THEN** the system SHALL sustain 2-4 GB/s throughput
- **AND** latency SHALL remain below 1μs per message
- **AND** the system SHALL handle 6+ concurrent subscribers with linear scaling

#### Scenario: Large Data Performance
- **WHEN** transmitting large binary data
- **THEN** the system SHALL maintain memory-bandwidth-limited performance
- **AND** zero-copy semantics SHALL eliminate all copying overhead
- **AND** performance SHALL scale with data size up to available memory

#### Scenario: Multi-Subscriber Scalability
- **WHEN** multiple subscribers attach to the same publisher
- **THEN** the system SHALL support 10+ concurrent subscribers
- **AND** each additional subscriber SHALL add < 5% overhead
- **AND** all subscribers SHALL receive identical data with minimal latency difference

### Requirement: Large Binary Blob Transmission
The system SHALL support transmission of large binary data with enhanced zero-copy semantics.

#### Scenario: Large Blob Generation
- **WHEN** generating large binary data for transmission
- **THEN** the system SHALL create blobs directly in shared memory
- **AND** blobs SHALL be accessible to all subscribers without copying
- **AND** memory overhead SHALL be limited to metadata only

#### Scenario: Zero-Copy Blob Access
- **WHEN** subscribers access large blobs
- **THEN** subscribers SHALL read directly from shared memory
- **AND** no intermediate copying SHALL occur
- **AND** memory bandwidth SHALL be the only limiting factor

#### Scenario: Concurrent Blob Access
- **WHEN** multiple subscribers access the same large blob
- **THEN** all subscribers SHALL read simultaneously
- **AND** no contention SHALL occur between readers
- **AND** performance SHALL scale linearly with subscriber count

## REMOVED Requirements

### Requirement: io_uring-based Message Passing
**Reason**: io_uring operations fail to generate completions, making this approach fundamentally unworkable for message passing.
**Migration**: Replace with shared memory ring buffer using atomic operations.

#### Scenario: io_uring Setup
- **REMOVED**: io_uring instance creation with completion rings
- **Migration**: Use shared memory allocation with atomic pointers

#### Scenario: io_uring Operation Submission
- **REMOVED**: SQE filling and submission through io_uring_enter
- **Migration**: Direct atomic updates to shared memory pointers

#### Scenario: io_uring Completion Processing
- **REMOVED**: CQE processing and completion ring management
- **Migration**: Bitmap-based message availability tracking

### Requirement: Multi-Process io_uring Ring Sharing
**Reason**: io_uring ring sharing between processes proved unreliable and complex.
**Migration**: Use simple shared memory mapping with atomic operations.

#### Scenario: Process Creation with Ring Sharing
- **REMOVED**: Child process attachment to parent's io_uring rings
- **Migration**: Independent shared memory attachment with atomic synchronization

#### Scenario: Cross-Process File Descriptor Sharing
- **REMOVED**: pidfd_getfd() for io_uring descriptor sharing
- **Migration**: Direct shared memory file descriptor passing

## RENAMED Requirements
- FROM: `### Requirement: Shared Memory Pool Management`
- TO: `### Requirement: Shared Memory Ring Buffer Management`