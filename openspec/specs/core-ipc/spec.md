# Core IPC Specification

## Purpose

The Core IPC capability provides high-performance inter-process communication using Linux io_uring with shared memory pools. It enables zero-copy message passing between processes with sophisticated memory management and support for large binary data transmission up to 20MB+ at 40Hz frequency.

## Overview

This specification defines the requirements for implementing a complete io_uring-based IPC system that replaces the previous flawed stdout-based implementation with true multi-process communication achieving real IPC performance targets.

## Requirements

### Requirement: Shared Memory Pool Management
The system SHALL provide a bitmap-based shared memory pool for efficient block allocation and deallocation.

#### Scenario: Block Allocation Success
- **WHEN** a process requests a memory block from the pool
- **THEN** the system SHALL allocate a contiguous block and return its 64-bit HringAddr
- **AND** the block SHALL be marked as allocated in the bitmap

#### Scenario: Block Deallocation Success
- **WHEN** a process frees a memory block using its HringAddr
- **THEN** the system SHALL mark the block as available in the bitmap
- **AND** the memory SHALL be available for subsequent allocations

#### Scenario: Pool Exhaustion Handling
- **WHEN** the memory pool has no available blocks
- **THEN** the system SHALL return an allocation error
- **AND** no memory SHALL be allocated

### Requirement: Multi-Process Communication
The system SHALL support true inter-process communication using fork/exec with io_uring ring sharing.

#### Scenario: Process Creation with Ring Sharing
- **WHEN** a parent process creates a child process
- **THEN** the child SHALL be able to attach to the parent's io_uring rings
- **AND** both processes SHALL share the same completion ring
- **AND** message passing SHALL work bidirectionally between processes

#### Scenario: Cross-Process File Descriptor Sharing
- **WHEN** a child process needs access to the parent's io_uring ring file descriptor
- **THEN** the system SHALL use pidfd_getfd() to safely share the descriptor
- **AND** the shared descriptor SHALL function correctly in the child process

#### Scenario: Process Cleanup
- **WHEN** a process exits or crashes
- **THEN** all allocated memory blocks SHALL be properly freed
- **AND** shared memory resources SHALL be cleaned up

### Requirement: Large Binary Blob Transmission
The system SHALL support transmission of large binary data (up to 20MB+) with integrity verification.

#### Scenario: Large Blob Generation
- **WHEN** generating a large binary blob for transmission
- **THEN** the system SHALL create blobs of specified sizes (1MB, 5MB, 10MB, 20MB)
- **AND** each blob SHALL include indisputable header and footer signatures
- **AND** the payload SHALL be deterministic and verifiable

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
- **WHEN** sending messages at 40Hz frequency
- **THEN** the system SHALL sustain 800MB/s throughput (20MB × 40Hz)
- **AND** latency SHALL remain below 1ms per message
- **AND** the system SHALL handle 6 concurrent subscribers

#### Scenario: Large Data Performance
- **WHEN** transmitting 20MB binary blobs
- **THEN** the system SHALL maintain stable transmission rates
- **AND** memory usage SHALL remain within expected limits
- **AND** no significant performance degradation SHALL occur

#### Scenario: Multi-Subscriber Scalability
- **WHEN** multiple subscribers attach to the same publisher
- **THEN** the system SHALL support at least 6 concurrent subscribers
- **AND** each subscriber SHALL receive identical data
- **AND** performance SHALL scale linearly with subscriber count

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
- **Latency**: Target < 270ns for basic message passing (based on vendor benchmarks)
- **Throughput**: Support 800MB/s sustained data transfer
- **Scalability**: Handle 6+ concurrent subscribers without performance degradation
- **Memory Efficiency**: Maintain zero-copy semantics throughout the system

### Reliability Requirements
- **Stability**: System SHALL remain stable under continuous load
- **Memory Safety**: No memory leaks or use-after-free conditions
- **Data Integrity**: Guaranteed delivery of uncorrupted data
- **Error Recovery**: Graceful handling of error conditions

### Compatibility Requirements
- **Platform**: Linux-only (io_uring is Linux-specific)
- **Kernel**: Require Linux kernel 5.1+ for io_uring support
- **Python**: Support Python 3.8+ with PyO3 bindings
- **Architecture**: Support x86_64 and ARM64 architectures

## Testing Requirements

### Unit Testing
- **Memory Pool**: Test all allocation and deallocation scenarios
- **HringAddr**: Test address creation, validation, and usage
- **Shared Memory**: Test creation, attachment, and cleanup
- **Error Handling**: Test all error conditions and recovery paths

### Integration Testing
- **Multi-Process**: Test fork/exec scenarios with ring sharing
- **Large Data**: Test transmission of 1MB-20MB binary blobs
- **Performance**: Verify performance targets are met
- **Python Integration**: Test all Python APIs and error handling

### Stress Testing
- **High Frequency**: Test sustained 40Hz messaging
- **Concurrent Access**: Test multiple concurrent processes
- **Memory Pressure**: Test behavior under memory constraints
- **Long Duration**: Test stability over extended periods

## Security Considerations

### Memory Safety
- **Bounds Checking**: All memory accesses SHALL be bounds-checked
- **Null Termination**: String operations SHALL properly handle null termination
- **Reference Validation**: All HringAddr references SHALL be validated before use

### Process Isolation
- **File Descriptor Sharing**: Use pidfd_getfd() for secure descriptor sharing
- **Memory Protection**: Shared memory regions SHALL have appropriate permissions
- **Process Cleanup**: Ensure proper cleanup when processes terminate unexpectedly

### Data Integrity
- **Signature Verification**: Verify blob signatures to ensure data integrity
- **Checksum Validation**: Use checksums for additional integrity verification
- **Corruption Detection**: Detect and handle corrupted data appropriately