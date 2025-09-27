## Why
The current implementation assumes a traditional pub/sub model where subscribers can receive messages independently and at different rates. However, the actual requirement is for a synchronous broadcast system where all subscribers must receive the SAME message before the publisher can continue, with the publisher waiting for all subscribers to process each message. The architecture has evolved to use memory-mapped files for zero-copy numpy array sharing with minimal Rust synchronization.

## What Changes
- **MODIFIED**: Core architecture from asynchronous pub/sub to synchronous broadcast
- **MODIFIED**: Message flow from individual subscriber messages to single broadcast message
- **ADDED**: Publisher synchronization mechanism to wait for all subscribers
- **ADDED**: Subscriber acknowledgment mechanism for broadcast completion
- **ADDED**: Subscriber registration and deregistration lifecycle management
- **ADDED**: Dynamic subscriber count tracking with atomic operations
- **MODIFIED**: Memory model from multiple message slots to single memory-mapped file
- **MODIFIED**: Performance requirements to reflect synchronous nature
- **UPDATED**: Architecture to use memory-mapped numpy arrays with minimal Rust sync layer

## Impact
- **Affected specs**: core-ipc
- **Affected code**: src/sync.rs, python/ultrapubsub/zerocopy.py, test scripts
- **Performance impact**: Zero-copy architecture eliminates memory copying overhead
- **Breaking changes**: Yes - fundamental architecture change from async to sync with new data model
- **Migration requirements**: Complete redesign of message handling logic with memory-mapped arrays