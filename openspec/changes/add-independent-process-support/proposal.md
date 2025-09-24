## Why

The current UltraPubSub specification has a critical flaw - it assumes fork() semantics for shared memory sharing, but this doesn't work with independent processes created via spawn(). This is a fundamental requirement for any production IPC system that must work with modern process management (containers, process supervisors, etc.).

## What Changes

- **BREAKING**: Replace fork()-based shared memory approach with independent process support
- Add mandatory requirement for spawn() compatibility
- Update multi-process communication requirements to work with truly independent processes
- Add proper shared memory naming and discovery mechanisms
- Ensure cleanup works when processes exit independently

## Impact

- Affected specs: core-ipc (Multi-Process Communication, Shared Memory Management)
- Affected code: Shared memory creation/attachment logic, process management
- This is a fundamental architectural change required for production viability