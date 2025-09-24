## Why
The UltraPubSub implementation was only achieving 29MB/s instead of the target 800MB/s (3.6% efficiency). Analysis revealed fundamental violations of the zero-copy architecture, with heavy memory copying occurring in the publisher despite the intended io_uring reference-passing design.

## What Changes
- **Fix memory pool allocation**: Replace 32MB fixed blocks with variable-sized allocations to eliminate massive memory waste
- **Implement true zero-copy**: Remove `ptr::copy_nonoverlapping` from publisher that copies 20MB per message
- **Preserve io_uring reference passing**: Ensure io_uring only carries 64-bit memory addresses in `user_data` field
- **Fix memory management**: Proper memory reuse instead of pool exhaustion after 15 messages
- **Update OpenSpec specs**: Reflect current architectural reality and intended fixes

## Impact
- **Performance**: Expected improvement from 29MB/s to 800MB/s (27x increase)
- **Memory efficiency**: Eliminate 12MB waste per 20MB message
- **Architecture**: Restore intended zero-copy semantics
- **Affected specs**: core-ipc
- **Affected code**: src/lib.rs memory pool, publisher, and io_uring integration