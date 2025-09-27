## Why
The current implementation assumes a traditional pub/sub model where subscribers can receive messages independently and at different rates. However, the actual requirement is for a synchronous broadcast system where all subscribers must receive the SAME message before the publisher can continue, with the publisher waiting for all subscribers to process each message.

## What Changes
- **MODIFIED**: Core architecture from asynchronous pub/sub to synchronous broadcast
- **MODIFIED**: Message flow from individual subscriber messages to single broadcast message
- **ADDED**: Publisher synchronization mechanism to wait for all subscribers
- **ADDED**: Subscriber acknowledgment mechanism for broadcast completion
- **ADDED**: Subscriber registration and deregistration lifecycle management
- **ADDED**: Dynamic subscriber count tracking with atomic operations
- **MODIFIED**: Memory model from multiple message slots to single broadcast slot
- **MODIFIED**: Performance requirements to reflect synchronous nature

## Impact
- **Affected specs**: core-ipc
- **Affected code**: src/broadcast_buffer.rs, src/lib.rs, python/ultrapubsub/api.py, test scripts
- **Performance impact**: May reduce maximum throughput due to synchronization requirements
- **Breaking changes**: Yes - fundamental architecture change from async to sync
- **Migration requirements**: Complete redesign of message handling logic