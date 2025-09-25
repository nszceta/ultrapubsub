## MODIFIED Requirements

### Requirement: Multi-Subscriber Support
The system SHALL support multiple concurrent subscribers with unique identifiers, where each subscriber receives independent copies of all published messages without contention or message loss.

#### Scenario: Multiple Subscribers with Unique IDs
- **WHEN** 6 subscriber processes connect with unique IDs (0-5)
- **AND** a publisher publishes messages
- **THEN** each subscriber SHALL receive all messages addressed to their specific ID
- **AND** message delivery rate SHALL exceed 90% for all subscribers
- **AND** subscribers SHALL NOT contend with each other for messages

### Requirement: Subscriber Registration
Subscribers SHALL be able to register with specific IDs that the publisher recognizes for targeted message delivery.

#### Scenario: Subscriber ID Registration
- **WHEN** a subscriber process calls `create_subscriber_with_id(name, id)`
- **THEN** the subscriber SHALL be assigned the specified ID
- **AND** the subscriber SHALL only receive messages marked as available for that ID
- **AND** the publisher SHALL correctly count this subscriber in its subscriber count

## ADDED Requirements

### Requirement: Dynamic Subscriber ID Assignment
The system SHALL provide mechanisms for creating subscribers with specific IDs rather than hard-coding all subscribers to ID 0.

#### Scenario: Zero-Copy Subscriber with Specific ID
- **WHEN** a subscriber is created with `create_subscriber_with_id(test_name, 3)`
- **AND** the publisher allocates a pool slot
- **THEN** the subscriber SHALL receive messages marked as available for ID 3
- **AND** the subscriber SHALL NOT receive messages intended for other IDs
- **AND** slot reclamation SHALL work correctly for this specific subscriber

### Requirement: Efficient Slot Allocation
The slot allocation algorithm SHALL properly manage slot availability across multiple subscribers without race conditions or bitmap corruption.

#### Scenario: High-Frequency Slot Allocation
- **WHEN** multiple subscribers are actively receiving 35MB messages at 40 Hz
- **AND** slot allocation occurs concurrently with slot reclamation
- **THEN** the available slot count SHALL remain stable
- **AND** allocation SHALL NOT fail due to bitmap corruption
- **AND** performance SHALL sustain the target 40 Hz rate