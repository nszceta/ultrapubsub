# Core IPC Specification

## Purpose

The Core IPC capability provides high-performance inter-process communication using Shared Memory Ring Buffer with atomic operations. It enables zero-copy message passing between processes with lock-free synchronization and support for large binary data transmission at 1.4 GB/s throughput (35 MB payloads at 40 Hz).

## Overview

This specification defines the requirements for implementing a complete Shared Memory Ring Buffer-based IPC system that replaces the previous flawed io_uring-based implementation with true zero-copy, lock-free multi-process communication achieving maximum performance targets.

## Requirements

### Requirement: Shared Memory Ring Buffer Management
The system SHALL provide a shared memory ring buffer with atomic operations for lock-free 1:N pub/sub messaging.

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

### Requirement: Large Binary Blob Transmission
The system SHALL support transmission of large binary data (35 MB payloads) with integrity verification at 40 Hz frequency.

#### Scenario: Large Blob Generation
- **WHEN** generating a large binary blob for transmission
- **THEN** the system SHALL create 35 MB payloads
- **AND** each blob SHALL include indisputable header and footer signatures
- **AND** the payload SHALL be deterministic and verifiable
- **AND** generation SHALL sustain 40 Hz frequency

#### Scenario: Signature Verification
- **WHEN** a blob is received
- **THEN** the system SHALL verify the header signature "ULTRAPUBSUB_BLOB_START_[SIZE]MB"
- **AND** the system SHALL verify the footer signature "ULTRAPUBSUB_BLOB_END_[SIZE]MB"
- **AND** verification SHALL fail if either signature is corrupted or missing

#### Scenario: Payload Integrity
- **WHEN** a blob passes signature verification
- **THEN** the payload data SHALL be intact and unmodified
- **AND** the payload size SHALL match expected size minus signature overhead
- **AND** checksum verification SHALL provide additional integrity validation

### Requirement: Zero-Copy Message Passing
The system SHALL implement zero-copy semantics using HringAddr references instead of data copying.

#### Scenario: HringAddr Creation
- **WHEN** a memory block is allocated
- **THEN** the system SHALL generate a 64-bit HringAddr containing size and block index
- **AND** the address SHALL uniquely identify the memory location
- **AND** the address SHALL be shareable between processes

#### Scenario: Zero-Copy Transmission
- **WHEN** sending a message using io_uring
- **THEN** the system SHALL use IORING_OP_NOP operations with HringAddr references
- **AND** no data SHALL be copied between processes
- **AND** the receiver SHALL access the original memory location

#### Scenario: Memory Reference Safety
- **WHEN** multiple processes access shared memory via HringAddr
- **THEN** the system SHALL prevent use-after-free conditions
- **AND** the system SHALL validate all memory references
- **AND** invalid addresses SHALL be rejected

### Requirement: Shared Memory Naming and Management
The system SHALL provide proper shared memory management with unique naming conventions.

#### Scenario: Shared Memory Creation
- **WHEN** creating a shared memory region
- **THEN** the system SHALL use /dev/shm/ naming conventions
- **AND** each region SHALL have a unique identifier
- **AND** the system SHALL handle naming conflicts gracefully

#### Scenario: Shared Memory Attachment
- **WHEN** a process attaches to existing shared memory
- **THEN** the system SHALL locate the memory by name
- **AND** the process SHALL gain read/write access
- **AND** the system SHALL validate memory integrity

#### Scenario: Memory Cleanup
- **WHEN** shared memory is no longer needed
- **THEN** the system SHALL properly unlink and clean up resources
- **AND** no memory leaks SHALL occur
- **AND** the system SHALL handle concurrent access safely

### Requirement: Performance Targets
The system SHALL achieve performance targets suitable for high-frequency messaging scenarios.

#### Scenario: High-Frequency Messaging
- **WHEN** sending 35 MB payloads at 40 Hz frequency
- **THEN** the system SHALL sustain 1.4 GB/s throughput (35 MB × 40 Hz)
- **AND** latency SHALL remain below 25ms per message (40 Hz cycle time)
- **AND** the system SHALL handle exactly 6 concurrent subscribers with zero-copy sharing

#### Scenario: Large Data Performance
- **WHEN** transmitting 35MB binary blobs at 40 Hz
- **THEN** the system SHALL maintain stable 1.4 GB/s transmission rates
- **AND** memory usage SHALL remain within expected limits for 6 subscribers
- **AND** no timing jitter SHALL occur in the 40 Hz cycle

#### Scenario: Multi-Subscriber Scalability
- **WHEN** multiple subscribers attach to the same publisher
- **THEN** all subscribers SHALL receive identical 35MB data simultaneously
- **AND** zero-copy semantics SHALL ensure no additional memory overhead per subscriber
- **AND** all subscribers SHALL complete processing within the 25ms cycle time
- **AND** performance SHALL scale linearly from 1 to at least 6 subscribers

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

#### Scenario: Corrupted Data Detection
- **WHEN** data corruption is detected
- **THEN** the system SHALL reject the corrupted data
- **AND** the system SHALL log the corruption event
- **AND** the system SHALL attempt recovery if possible

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
- **Throughput**: Support 1.4 GB/s sustained data transfer (35 MB × 40 Hz)
- **Latency**: Target < 25ms per message (40 Hz cycle time)
- **Frequency**: Maintain consistent 40 Hz message rate with no jitter
- **Subscribers**: Support multiple concurrent subscribers (at least 6) with zero-copy sharing
- **Memory Efficiency**: Maintain zero-copy semantics throughout the system
- **Payload Size**: Handle 35 MB payloads efficiently

### Reliability Requirements
- **Stability**: System SHALL remain stable under continuous load
- **Memory Safety**: No memory leaks or use-after-free conditions
- **Data Integrity**: Guaranteed delivery of uncorrupted data
- **Error Recovery**: Graceful handling of error conditions

### Compatibility Requirements
- **Platform**: Linux-only (atomic operations and shared memory)
- **Kernel**: Require Linux kernel 3.2+ for atomic operations support
- **Python**: Support Python 3.8+ with PyO3 bindings
- **Architecture**: Support x86_64 and ARM64 architectures

## Testing Requirements

### Unit Testing
- **Ring Buffer**: Test all ring buffer operations and wrap-around scenarios
- **Atomic Operations**: Test atomic head/tail pointer updates and synchronization
- **Shared Memory**: Test creation, attachment, and cleanup
- **Bitmap Management**: Test message availability bitmap operations

### Integration Testing
- **Multi-Process**: Test independent process attachment to shared memory
- **Large Data**: Test zero-copy transmission of 35 MB binary blobs
- **Performance**: Verify 1.4 GB/s performance targets are met (35 MB × 40 Hz)
- **Frequency Testing**: Verify consistent 40 Hz message rate
- **Python Integration**: Test all Python APIs and error handling

### Stress Testing
- **High Frequency**: Test sustained 40 Hz messaging with 35 MB payloads
- **Concurrent Access**: Test multiple concurrent subscribers (at least 6)
- **Memory Pressure**: Test behavior with 35 MB × 6 subscribers memory footprint
- **Timing Accuracy**: Test 25ms cycle time precision over extended periods
- **Long Duration**: Test stability over extended 40 Hz operation

## Security Considerations

### Memory Safety
- **Bounds Checking**: All ring buffer accesses SHALL be bounds-checked
- **Atomic Operations**: All atomic operations SHALL use proper memory ordering
- **Pointer Validation**: All head/tail pointers SHALL be validated before use

### Process Isolation
- **Memory Protection**: Shared memory regions SHALL have appropriate permissions
- **Independent Attachment**: Each process SHALL attach independently to shared memory
- **Process Cleanup**: Ensure proper cleanup when processes terminate unexpectedly

### Data Integrity
- **Signature Verification**: Verify blob signatures to ensure data integrity
- **Checksum Validation**: Use checksums for additional integrity verification
- **Corruption Detection**: Detect and handle corrupted data appropriately