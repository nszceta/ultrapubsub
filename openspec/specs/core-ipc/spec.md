# Core IPC Specification

## Purpose

The Core IPC capability provides high-performance inter-process communication using Shared Memory Broadcast Buffer with synchronous coordination. It enables zero-copy message passing between processes with process-shared mutex synchronization and support for large binary data transmission at 40 Hz frequency.

## Overview

This specification defines the requirements for implementing a complete Shared Memory Broadcast Buffer-based IPC system that implements synchronous 1:N messaging where all subscribers receive identical messages and the publisher waits for all subscribers to process each message before continuing.

## Requirements

### Requirement: Shared Memory Broadcast Buffer Management
The system SHALL provide a shared memory broadcast buffer with process-shared mutex for synchronous 1:N messaging.

#### Scenario: Broadcast Buffer Creation
- **WHEN** creating a shared memory region for synchronous broadcast
- **THEN** the system SHALL allocate a contiguous 35MB broadcast slot
- **AND** the system SHALL initialize process-shared mutex for cross-process synchronization
- **AND** the system SHALL create subscriber acknowledgment arrays
- **AND** the system SHALL initialize sequence number tracking

#### Scenario: Synchronous 1:N Broadcasting
- **WHEN** a publisher broadcasts a message
- **THEN** the system SHALL write directly to the shared 35MB broadcast slot
- **AND** the system SHALL update sequence number atomically
- **AND** the system SHALL wait for all subscribers to acknowledge receipt
- **AND** no data copying SHALL occur between publisher and subscribers

#### Scenario: Concurrent Subscriber Access
- **WHEN** multiple subscribers read messages
- **THEN** each subscriber SHALL read from the same broadcast slot
- **AND** subscribers SHALL read directly from shared memory
- **AND** all subscribers SHALL receive identical message data
- **AND** subscribers SHALL acknowledge receipt independently

### Requirement: Process-Shared Mutex Synchronization
The system SHALL use process-shared mutex with PTHREAD_PROCESS_SHARED for cross-process synchronization.

#### Scenario: Mutex Initialization
- **WHEN** initializing the broadcast buffer
- **THEN** the system SHALL create a process-shared mutex
- **AND** the mutex SHALL be cache-line aligned (64 bytes)
- **AND** the mutex SHALL support cross-process locking/unlocking

#### Scenario: Cross-Process Locking
- **WHEN** multiple processes access shared data
- **THEN** the mutex SHALL provide exclusive access
- **AND** lock operations SHALL be atomic across processes
- **AND** unlock operations SHALL be atomic across processes

#### Scenario: Performance Optimization
- **WHEN** mutex is uncontended
- **THEN** lock/unlock operations SHALL complete in 10-30 nanoseconds
- **AND** no system calls SHALL be required in fast path
- **AND** cache-line alignment SHALL prevent false sharing

### Requirement: Large Binary Blob Transmission
The system SHALL support transmission of large binary data (35 MB payloads) with synchronous acknowledgment at 40 Hz frequency.

#### Scenario: Large Blob Generation
- **WHEN** generating a large binary blob for transmission
- **THEN** the system SHALL create 35 MB payloads
- **AND** each blob SHALL fit within the broadcast slot
- **AND** the payload SHALL be written directly to shared memory
- **AND** generation SHALL sustain 40 Hz frequency

#### Scenario: Synchronous Transmission
- **WHEN** a blob is broadcast
- **THEN** all subscribers SHALL receive the identical payload
- **AND** the publisher SHALL wait for all acknowledgments
- **AND** transmission SHALL be synchronous with all subscribers
- **AND** the system SHALL handle up to 32 subscribers

#### Scenario: Payload Integrity
- **WHEN** subscribers receive broadcast data
- **THEN** the payload data SHALL be intact and unmodified
- **AND** the payload size SHALL match expected size
- **AND** all subscribers SHALL receive identical data

### Requirement: Zero-Copy Message Passing
The system SHALL implement zero-copy semantics using direct shared memory access instead of data copying.

#### Scenario: Direct Memory Access
- **WHEN** a message is broadcast
- **THEN** the system SHALL write directly to shared memory
- **AND** subscribers SHALL read directly from shared memory
- **AND** no data SHALL be copied between processes
- **AND** memory access SHALL be properly synchronized

#### Scenario: Memory Reference Safety
- **WHEN** multiple processes access shared memory
- **THEN** the system SHALL prevent race conditions
- **AND** the system SHALL validate all memory operations
- **AND** proper synchronization SHALL be enforced
- **AND** memory SHALL be properly mapped/unmapped

### Requirement: Dynamic Subscriber Management
The system SHALL provide dynamic subscriber registration and deregistration with acknowledgment tracking.

#### Scenario: Subscriber Registration
- **WHEN** a new subscriber connects
- **THEN** the system SHALL assign a unique subscriber ID (0-31)
- **AND** the system SHALL update registration arrays
- **AND** the system SHALL increment subscriber count
- **AND** the subscriber SHALL be ready to receive broadcasts

#### Scenario: Subscriber Deregistration
- **WHEN** a subscriber disconnects
- **THEN** the system SHALL clear registration status
- **AND** the system SHALL decrement subscriber count
- **AND** the system SHALL handle pending acknowledgments
- **AND** resources SHALL be properly cleaned up

#### Scenario: Acknowledgment Tracking
- **WHEN** subscribers receive messages
- **THEN** each subscriber SHALL acknowledge receipt independently
- **AND** the system SHALL track acknowledgment status per subscriber
- **AND** the publisher SHALL wait for all acknowledgments
- **AND** acknowledgment SHALL be atomic and synchronized

### Requirement: Shared Memory Naming and Management
The system SHALL provide proper shared memory management with unique naming conventions.

#### Scenario: Shared Memory Creation
- **WHEN** creating a shared memory region
- **THEN** the system SHALL use POSIX shared memory APIs
- **AND** each region SHALL have a unique identifier
- **AND** the system SHALL handle naming conflicts gracefully
- **AND** proper permissions SHALL be set

#### Scenario: Shared Memory Attachment
- **WHEN** a process attaches to existing shared memory
- **THEN** the system SHALL locate the memory by name
- **AND** the process SHALL gain read/write access
- **AND** the system SHALL validate memory integrity
- **AND** mapping SHALL be consistent across processes

#### Scenario: Memory Cleanup
- **WHEN** shared memory is no longer needed
- **THEN** the system SHALL properly unlink and clean up resources
- **AND** no memory leaks SHALL occur
- **AND** the system SHALL handle concurrent access safely
- **AND** cleanup SHALL be atomic

### Requirement: Performance Targets
The system SHALL achieve performance targets suitable for high-frequency synchronous messaging scenarios.

#### Scenario: High-Frequency Messaging
- **WHEN** sending 35 MB payloads at 40 Hz frequency
- **THEN** the system SHALL sustain synchronous delivery to all subscribers
- **AND** latency SHALL remain below 25ms per message (40 Hz cycle time)
- **AND** the system SHALL handle at least 32 concurrent subscribers
- **AND** all subscribers SHALL receive identical data

#### Scenario: Synchronous Coordination
- **WHEN** coordinating multiple subscribers
- **THEN** the system SHALL minimize acknowledgment overhead
- **AND** mutex operations SHALL complete in 10-30 nanoseconds
- **AND** memory access SHALL be cache-optimized
- **AND** synchronization SHALL be efficient across processes

#### Scenario: Multi-Subscriber Scalability
- **WHEN** multiple subscribers attach to the same publisher
- **THEN** all subscribers SHALL receive identical 35MB data simultaneously
- **AND** zero-copy semantics SHALL ensure no additional memory overhead per subscriber
- **AND** all subscribers SHALL complete processing within the 25ms cycle time
- **AND** performance SHALL scale from 1 to 32 subscribers

### Requirement: Error Handling and Recovery
The system SHALL provide robust error handling and recovery mechanisms.

#### Scenario: Memory Allocation Failure
- **WHEN** memory allocation fails
- **THEN** the system SHALL return a clear error indication
- **AND** the system SHALL not crash or become unstable
- **AND** the calling process SHALL be able to handle the error gracefully

#### Scenario: Process Communication Failure
- **WHEN** inter-process communication fails
- **THEN** the system SHALL detect the failure condition
- **AND** the system SHALL attempt reconnection if appropriate
- **AND** the system SHALL provide error information to calling processes

#### Scenario: Subscriber Timeout
- **WHEN** a subscriber fails to acknowledge
- **THEN** the system SHALL detect the timeout condition
- **AND** the system SHALL handle the failed subscriber appropriately
- **AND** the system SHALL continue operation with remaining subscribers
- **AND** timeout SHALL be configurable and adaptive

### Requirement: System Integration
The system SHALL integrate properly with the Linux operating system and development environment.

#### Scenario: Python Integration
- **WHEN** using the system from Python
- **THEN** PyO3 bindings SHALL provide seamless integration
- **AND** Python APIs SHALL follow Python conventions
- **AND** error handling SHALL use Python exception mechanisms

#### Scenario: Development Environment
- **WHEN** developing with the system
- **THEN** the system SHALL integrate with standard build tools
- **AND** the system SHALL provide clear compilation and linking
- **AND** debugging information SHALL be available when needed

#### Scenario: System Resource Usage
- **WHEN** the system is running
- **THEN** memory usage SHALL be reasonable and bounded
- **AND** CPU usage SHALL be efficient for the workload
- **AND** system resource consumption SHALL be predictable

## Non-Functional Requirements

### Performance Requirements
- **Throughput**: Support 35MB × 40Hz = 1.4 GB/s sustained data transfer
- **Latency**: Target <= 25ms per message (40 Hz cycle time)
- **Frequency**: Maintain consistent 40 Hz message rate with minimal jitter
- **Subscribers**: Support up to 32 concurrent subscribers with zero-copy sharing
- **Memory Efficiency**: Single 35MB broadcast slot shared by all subscribers
- **Payload Size**: Handle 35 MB payloads efficiently
- **Synchronization**: Process-shared mutex with 10-30 nanosecond lock/unlock

### Reliability Requirements
- **Stability**: System SHALL remain stable under continuous load
- **Memory Safety**: No memory leaks or use-after-free conditions
- **Data Integrity**: Guaranteed delivery of identical data to all subscribers
- **Error Recovery**: Graceful handling of subscriber failures and timeouts
- **Atomic Operations**: All shared data access SHALL be properly synchronized

### Compatibility Requirements
- **Platform**: Linux-only (POSIX shared memory and pthreads)
- **Kernel**: Require Linux kernel with POSIX shared memory support
- **Python**: Support Python 3.8+ with PyO3 bindings
- **Architecture**: Support x86_64 architecture with cache-line alignment

## Testing Requirements

### Unit Testing
- **Broadcast Buffer**: Test broadcast buffer operations and synchronization
- **Mutex Operations**: Test process-shared mutex locking/unlocking
- **Shared Memory**: Test creation, attachment, and cleanup
- **Subscriber Management**: Test registration, deregistration, acknowledgment

### Integration Testing
- **Multi-Process**: Test independent process attachment to shared memory
- **Large Data**: Test zero-copy transmission of 35 MB binary blobs
- **Performance**: Verify 40 Hz frequency and 1.4 GB/s throughput targets
- **Synchronous Coordination**: Test publisher wait for all subscribers
- **Python Integration**: Test all Python APIs and error handling

### Stress Testing
- **High Frequency**: Test sustained 40 Hz messaging with 35 MB payloads
- **Concurrent Access**: Test maximum 32 concurrent subscribers
- **Memory Pressure**: Test behavior with single 35MB shared memory footprint
- **Timing Accuracy**: Test 25ms cycle time precision over extended periods
- **Long Duration**: Test stability over extended 40 Hz operation

## Security Considerations

### Memory Safety
- **Bounds Checking**: All broadcast buffer accesses SHALL be bounds-checked
- **Synchronization**: All shared data access SHALL be properly synchronized
- **Memory Mapping**: All memory mappings SHALL be validated and proper

### Process Isolation
- **Memory Protection**: Shared memory regions SHALL have appropriate permissions
- **Independent Attachment**: Each process SHALL attach independently to shared memory
- **Process Cleanup**: Ensure proper cleanup when processes terminate unexpectedly

### Data Integrity
- **Synchronous Delivery**: Ensure all subscribers receive identical data
- **Acknowledgment Verification**: Verify all subscribers acknowledge each message
- **Error Detection**: Detect and handle subscriber failures appropriately