## Why
The subscriber registration mechanism was fundamentally broken. All subscribers were hard-coded to use ID 0, causing them to compete for the same messages and resulting in poor delivery rates (1.1% in tests). The publisher had no knowledge of actual subscriber count, marking messages available for only 1 subscriber instead of the expected 6.

## What Changes
- **FIXED**: Subscriber hard-coded ID issue - added `Subscriber::with_id()` method in Rust
- **ADDED**: Python bindings `create_subscriber_with_id()` function
- **ADDED**: SharedMemory API method `create_subscriber_with_id()` for high-level interface
- **ADDED**: Subscriber classmethod `with_id()` for creating subscribers with specific IDs
- **IMPROVED**: Slot allocation algorithm with cleaner, more efficient bitmap operations
- **FIXED**: Race conditions in slot reclamation logic

## Impact
- **Affected specs**: core-ipc, shared-memory-ring-buffer
- **Affected code**: src/lib.rs, src/ring_buffer.rs, python/ultrapubsub/api.py, test scripts
- **Performance impact**: Expected to improve delivery rate from 1.1% to near 100% for multi-subscriber scenarios
- **Breaking changes**: None - additive changes only