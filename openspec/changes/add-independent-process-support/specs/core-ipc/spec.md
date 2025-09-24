## MODIFIED Requirements

### Requirement: Multi-Process Communication
The system SHALL support true inter-process communication between independent processes using spawn() or direct execution, not just fork().

#### Scenario: Independent Process Creation
- **WHEN** multiple independent processes are started (via spawn(), exec, or direct execution)
- **THEN** each process SHALL be able to create or attach to shared memory regions by name
- **AND** processes SHALL be able to communicate without requiring fork() relationship
- **AND** the system SHALL work across process boundaries and container boundaries

#### Scenario: Shared Memory Discovery by Name
- **WHEN** a process needs to connect to existing shared memory
- **THEN** the system SHALL allow attachment using a well-known name
- **AND** the attaching process SHALL NOT need to be related to the creating process
- **AND** multiple independent processes SHALL attach to the same shared memory

#### Scenario: Process Cleanup and Resource Management
- **WHEN** any process exits or crashes
- **THEN** shared memory resources SHALL persist for other processes
- **AND** the system SHALL provide cleanup mechanisms when all processes detach
- **AND** the system SHALL handle process termination gracefully without leaks

## ADDED Requirements

### Requirement: Independent Process Support
The system MUST support communication between truly independent processes without requiring fork() relationships. This is a 100% mandatory requirement for production deployment.

#### Scenario: Spawn Process Compatibility
- **WHEN** processes are created using multiprocessing.spawn() or equivalent
- **THEN** the system SHALL allow shared memory communication between processes
- **AND** processes SHALL NOT need parent-child relationships
- **AND** performance SHALL meet targets of 800MB/s throughput

#### Scenario: Container and Process Boundary Support
- **WHEN** processes run in different containers or process namespaces
- **THEN** the system SHALL support cross-boundary communication where possible
- **AND** shared memory SHALL work within the same kernel space
- **AND** the system SHALL handle namespace differences appropriately

#### Scenario: Process Independence Requirements
- **WHEN** using the IPC system
- **THEN** processes SHALL be able to start and stop independently
- **AND** no process SHALL be a single point of failure for the entire system
- **AND** new processes SHALL be able to join existing communication channels

#### Scenario: Resource Cleanup on Process Exit
- **WHEN** a process exits unexpectedly
- **THEN** shared memory resources SHALL be automatically cleaned up
- **AND** the system SHALL detect stale resources and reclaim them
- **AND** other processes SHALL continue functioning normally

## REMOVED Requirements

### Requirement: Fork-Based Process Communication (REMOVED)
**Reason**: This requirement assumes fork() semantics which don't work with independent processes spawned via multiprocessing.spawn().
**Migration**: Replace with named shared memory approach that works with any process creation method.

- **FROM**: The system SHALL support true inter-process communication using fork/exec with io_uring ring sharing
- **TO**: **REMOVED** - Replaced by independent process support requirement above