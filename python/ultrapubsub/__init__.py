"""
ultrapubsub - Ultra-fast pub/sub using Shared Memory Ring Buffer with Atomic Operations

⚠️ IMPORTANT WARNING: DO NOT USE io_URING ⚠️

This module provides high-performance inter-process communication using
Shared Memory Ring Buffer with atomic operations for zero-copy semantics.

CRITICAL: The io_uring-based approach has been deprecated and abandoned due to
fundamental architectural issues. All implementations MUST use the Shared Memory
Ring Buffer approach described in the specifications.

Why io_uring Failed:
- io_uring operations submitted successfully but generated zero completions
- Complex ring sharing between processes proved unreliable
- Kernel completion ring mechanism unsuitable for message passing
- Unpredictable behavior under high-frequency messaging scenarios

Current Implementation: Shared Memory Ring Buffer with atomic operations only
"""

# Low-level PyO3 bindings
from .ultrapubsub import PyPublisher, PySubscriber, PyEventLoop

# High-level Python API
from .api import SharedMemory, Publisher, Subscriber, create_process_pair, fork_process_with_ipc

__all__ = [
    # Low-level
    'PyPublisher', 'PySubscriber', 'PyEventLoop',
    # High-level
    'SharedMemory', 'Publisher', 'Subscriber', 'create_process_pair', 'fork_process_with_ipc'
]