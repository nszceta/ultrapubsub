## ADDED Requirements

### Requirement: Zero-Copy Memory-Mapped Architecture
The system SHALL implement zero-copy numpy array broadcasting using memory-mapped files with minimal Rust synchronization layer.

#### Scenario: Memory-Mapped File Creation
- **WHEN** creating a broadcast system
- **THEN** the system SHALL create a memory-mapped file for numpy array storage
- **AND** the file SHALL be sized to accommodate the specified numpy array dimensions
- **AND** the file SHALL be accessible across multiple processes
- **AND** the file SHALL support both read and write operations

#### Scenario: Zero-Copy Numpy Array Access
- **WHEN** a publisher writes data
- **THEN** the publisher SHALL write directly to the memory-mapped file as a numpy array
- **AND** no data copying SHALL occur between the numpy array and shared storage
- **AND** subscribers SHALL access the same memory location as read-only numpy arrays
- **AND** all processes SHALL see identical data simultaneously

#### Scenario: Minimal Rust Synchronization
- **WHEN** processes need to coordinate access
- **THEN** the system SHALL use futex operations for cross-process synchronization
- **AND** the Rust layer SHALL provide only timing coordination functions
- **AND** no data processing SHALL occur in the Rust layer
- **AND** the Rust implementation SHALL be less than 300 lines of code

### Requirement: Memory-Mapped File Management
The system SHALL manage memory-mapped files for numpy array broadcasting with proper lifecycle handling.

#### Scenario: File Lifecycle Management
- **WHEN** creating a broadcast system
- **THEN** the system SHALL create appropriately sized memory-mapped files
- **AND** the system SHALL clean up files when broadcasting completes
- **AND** the system SHALL handle concurrent access safely
- **AND** file permissions SHALL allow cross-process read/write access

#### Scenario: Array Integrity Preservation
- **WHEN** multiple processes access the same memory-mapped file
- **THEN** numpy array operations SHALL preserve data integrity
- **AND** array shapes and data types SHALL remain consistent
- **AND** memory alignment SHALL be appropriate for numpy operations
- **AND** access patterns SHALL be optimized for cache efficiency

### Requirement: Hybrid Python-Rust Architecture
The system SHALL use a hybrid architecture with Python for data handling and Rust for synchronization only.

#### Scenario: Python Data Layer
- **WHEN** handling numpy arrays
- **THEN** all data operations SHALL occur in Python using numpy
- **AND** memory-mapped files SHALL be exposed as numpy arrays
- **AND** Python SHALL handle all array creation, access, and manipulation
- **AND** no data copying SHALL occur between Python and memory-mapped storage

#### Scenario: Rust Synchronization Layer
- **WHEN** coordinating process timing
- **THEN** Rust SHALL provide only futex-based synchronization primitives
- **AND** Rust SHALL NOT handle any data processing or storage
- **AND** the Rust API SHALL expose minimal coordination functions
- **AND** synchronization overhead SHALL be less than 1% of total operation time

### Requirement: Cross-Process Numpy Array Sharing
The system SHALL enable true zero-copy sharing of numpy arrays across independent processes.

#### Scenario: Publisher-Subscriber Array Sharing
- **WHEN** a publisher writes a numpy array
- **THEN** all subscribers SHALL see the same array data without copying
- **AND** subscribers SHALL access data through read-only numpy array views
- **AND** array modifications SHALL be immediately visible to all processes
- **AND** memory usage SHALL be constant regardless of subscriber count

#### Scenario: Large Array Performance
- **WHEN** broadcasting 35MB numpy arrays
- **THEN** the system SHALL achieve throughput greater than 1.0 GB/s
- **AND** memory copy overhead SHALL be less than 1% of total time
- **AND** synchronization overhead SHALL be less than 5ms per operation
- **AND** the system SHALL support sustained 40Hz broadcasting

### Requirement: Performance Optimization
The system SHALL optimize for maximum zero-copy performance with minimal synchronization overhead.

#### Scenario: Zero-Copy Efficiency
- **WHEN** broadcasting numpy arrays
- **THEN** the system SHALL achieve true zero-copy data transfer
- **AND** memory bandwidth SHALL be the only limiting factor
- **AND** CPU overhead SHALL be less than 5% of total operation time
- **AND** cache efficiency SHALL be optimized through proper memory alignment

#### Scenario: Synchronization Efficiency
- **WHEN** coordinating across processes
- **THEN** futex operations SHALL complete in microseconds
- **AND** synchronization SHALL NOT block data access
- **AND** wait times SHALL be minimized through efficient wake mechanisms
- **AND** the system SHALL handle up to 32 concurrent subscribers efficiently

## MODIFIED Requirements

### Requirement: Broadcast Memory Management
**MODIFIED FROM**: Shared memory broadcast buffer
**MODIFIED TO**: Memory-mapped file numpy array management

The system SHALL manage memory for single broadcast messages using memory-mapped numpy arrays shared among all subscribers.

#### Scenario: Single Memory-Mapped Numpy Array
- **WHEN** preparing a broadcast message
- **THEN** the system SHALL create one memory-mapped file for the numpy array
- **AND** the file SHALL be sized to accommodate 35MB numpy arrays
- **AND** the file SHALL be accessible to all subscribers as read-only numpy arrays
- **AND** the file SHALL be reused for subsequent broadcasts

### Requirement: Process Architecture
**MODIFIED FROM**: Complex Rust-based process coordination
**MODIFIED TO**: Simple hybrid architecture with clear separation

The system SHALL enforce simple process isolation with minimal synchronization requirements.

#### Scenario: Simplified Process Management
- **WHEN** processes participate in broadcasting
- **THEN** each process SHALL manage its own numpy array access
- **AND** synchronization SHALL be limited to timing coordination only
- **AND** no complex process lifecycle management SHALL be required
- **AND** processes SHALL operate independently with minimal coordination

## REMOVED Requirements

### Requirement: Complex Rust Broadcast Buffer
**Reason**: The complex Rust broadcast buffer implementation creates unnecessary complexity and prevents true zero-copy numpy array access. Memory-mapped files with simple synchronization provide better performance and maintainability.

**Migration**: Replace with simple memory-mapped file management handled by Python numpy operations.

#### Scenario: Rust Buffer Management
- **REMOVED**: Complex Rust-based shared memory buffer management
- **REMOVED**: Manual memory allocation and deallocation in Rust
- **REMOVED**: Complex data structure management for broadcasting
- **REMOVED**: Rust-side numpy array handling and conversion

### Requirement: Heavy Rust Data Processing
**Reason**: Moving data processing to Python with numpy eliminates conversion overhead and leverages numpy's optimized array operations.

**Migration**: Implement all data operations in Python using numpy, keeping Rust for synchronization only.

#### Scenario: Rust Data Handling
- **REMOVED**: Rust-side numpy array manipulation
- **REMOVED**: Data type conversion between Rust and Python
- **REMOVED**: Complex serialization/deserialization in Rust
- **REMOVED**: Rust-side memory copy operations