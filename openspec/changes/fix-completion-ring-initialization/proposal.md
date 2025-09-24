## Why
The UltraPubSub system has achieved significant performance improvements (74MB/s up from 29MB/s) but is blocked from reaching the target 800MB/s by a fundamental io_uring completion ring initialization issue. The completion ring has `ring_entries: 0` and `ring_mask: 0`, preventing proper message passing between publishers and subscribers.

## What Changes
- **Fix io_uring setup parameters**: Ensure completion ring is properly initialized by the kernel
- **Implement completion ring sharing**: Allow multiple subscribers to share the same completion ring
- **Add completion ring debugging**: Enhanced visibility into ring initialization state
- **Fix ring memory mapping**: Ensure ring_entries and ring_mask are correctly read from kernel memory
- **Validate io_uring setup**: Confirm that kernel properly populates completion ring parameters

## Impact
- **Performance**: Expected improvement from 74MB/s to 800MB/s (10x increase)
- **IPC functionality**: Enable actual message passing between publishers and subscribers
- **Architecture**: Complete the intended io_uring-based zero-copy IPC system
- **Affected specs**: core-ipc
- **Affected code**: src/lib.rs io_uring setup, completion ring mapping, Hring initialization

## Technical Details

The current issue is that after `io_uring_setup`, the completion ring parameters remain uninitialized:
- `ring_entries`: 0 (should be populated by kernel)
- `ring_mask`: 0 (should be populated by kernel based on ring_entries)

This prevents the completion ring from functioning as a message queue between publishers and subscribers.