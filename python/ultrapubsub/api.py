"""
High-level Python API for ultrapubsub

This module provides a user-friendly interface for the ultrapubsub IPC system,
simplifying the creation of publishers and subscribers for inter-process communication.
"""

import os
import multiprocessing
from typing import Optional, Callable, Any
from .ultrapubsub import PyHring, PyPublisher, PySubscriber


class SharedMemory:
    """
    High-level wrapper for shared memory management using Hring.

    This class provides a simplified interface for creating and managing
    shared memory regions for inter-process communication.
    """

    def __init__(self, name: str, entries: int = 32, flags: int = 0, sq_thread_cpu: int = 0):
        """
        Initialize a new shared memory region.

        Args:
            name: Unique name for the shared memory region
            entries: Number of entries in the io_uring ring (default: 32)
            flags: io_uring setup flags (default: 0)
            sq_thread_cpu: CPU for SQ thread (default: 0)
        """
        self.name = name
        self.hring = PyHring(name, entries, flags, sq_thread_cpu)
        self._publisher = None
        self._subscriber = None

    @classmethod
    def attach(cls, name: str) -> 'SharedMemory':
        """
        Attach to an existing shared memory region.

        Args:
            name: Name of the existing shared memory region

        Returns:
            SharedMemory instance attached to the existing region
        """
        instance = cls.__new__(cls)
        instance.name = name
        instance.hring = PyHring.attach(name)
        instance._publisher = None
        instance._subscriber = None
        return instance

    def create_publisher(self) -> 'Publisher':
        """
        Create a publisher for this shared memory region.

        Returns:
            Publisher instance
        """
        if self._publisher is None:
            self._publisher = Publisher(self.hring)
        return self._publisher

    def create_subscriber(self) -> 'Subscriber':
        """
        Create a subscriber for this shared memory region.

        Returns:
            Subscriber instance
        """
        if self._subscriber is None:
            self._subscriber = Subscriber(self.hring)
        return self._subscriber


class Publisher:
    """
    High-level publisher for sending messages to shared memory.

    This class provides a simple interface for publishing messages
    to subscribers through shared memory.
    """

    def __init__(self, hring: PyHring):
        """
        Initialize publisher with an existing Hring.

        Args:
            hring: PyHring instance for shared memory access
        """
        self._publisher = PyPublisher(hring)

    def publish(self, data: bytes) -> None:
        """
        Publish data to shared memory.

        Args:
            data: Bytes data to publish

        Raises:
            RuntimeError: If publishing fails
        """
        if not isinstance(data, bytes):
            raise TypeError("Data must be bytes")

        self._publisher.publish(data)

    def publish_string(self, text: str, encoding: str = 'utf-8') -> None:
        """
        Publish a string message.

        Args:
            text: String to publish
            encoding: Text encoding (default: utf-8)
        """
        self.publish(text.encode(encoding))

    def publish_json(self, obj: Any) -> None:
        """
        Publish a JSON-serializable object.

        Args:
            obj: Object to serialize and publish

        Raises:
            RuntimeError: If JSON serialization fails
        """
        import json
        json_str = json.dumps(obj)
        self.publish_string(json_str)


class Subscriber:
    """
    High-level subscriber for receiving messages from shared memory.

    This class provides a simple interface for receiving messages
    from publishers through shared memory.
    """

    def __init__(self, hring: PyHring):
        """
        Initialize subscriber with an existing Hring.

        Args:
            hring: PyHring instance for shared memory access
        """
        self._subscriber = PySubscriber(hring)

    def receive(self, timeout: Optional[float] = None) -> Optional[bytes]:
        """
        Receive a message from shared memory.

        Args:
            timeout: Timeout in seconds (default: None for no timeout)

        Returns:
            Received data as bytes, or None if timeout
        """
        # Note: The current implementation doesn't support timeout
        # This is a limitation of the underlying Rust implementation
        try:
            result = self._subscriber.receive()
            return result
        except Exception:
            return None

    def receive_string(self, timeout: Optional[float] = None, encoding: str = 'utf-8') -> Optional[str]:
        """
        Receive a string message.

        Args:
            timeout: Timeout in seconds (default: None for no timeout)
            encoding: Text encoding (default: utf-8)

        Returns:
            Received string, or None if timeout
        """
        data = self.receive(timeout)
        return data.decode(encoding) if data else None

    def receive_json(self, timeout: Optional[float] = None) -> Optional[Any]:
        """
        Receive a JSON-serialized object.

        Args:
            timeout: Timeout in seconds (default: None for no timeout)

        Returns:
            Deserialized object, or None if timeout
        """
        import json
        json_str = self.receive_string(timeout)
        return json.loads(json_str) if json_str else None

    def listen(self, callback: Callable[[bytes], None], timeout: Optional[float] = None) -> None:
        """
        Listen for messages and call callback for each received message.

        Args:
            callback: Function to call with received data
            timeout: Timeout in seconds (default: None for no timeout)
        """
        import time
        start_time = time.time()

        while True:
            data = self.receive(timeout)
            if data:
                callback(data)

            if timeout and (time.time() - start_time) > timeout:
                break

            # Small sleep to avoid busy waiting
            time.sleep(0.001)


def create_process_pair(name: str) -> tuple['SharedMemory', 'SharedMemory']:
    """
    Create a parent-child process pair with shared memory.

    This function creates a shared memory region and returns instances
    for both parent and child processes.

    Args:
        name: Base name for the shared memory region

    Returns:
        Tuple of (parent_shm, child_shm) for parent and child processes
    """
    # Create parent shared memory
    parent_shm = SharedMemory(f"{name}_parent")

    # Child will attach to parent's shared memory
    child_shm = SharedMemory.attach(f"{name}_parent")

    return parent_shm, child_shm


def fork_process_with_ipc(name: str) -> tuple[int, 'SharedMemory']:
    """
    Fork a new process with IPC communication setup.

    This function creates a shared memory region and forks a child process
    that can communicate with the parent through IPC.

    Args:
        name: Name for the shared memory region

    Returns:
        Tuple of (child_pid, parent_shm) where child_pid is the PID of the
        child process and parent_shm is the shared memory instance
    """
    parent_shm = SharedMemory(name)

    pid = os.fork()
    if pid == 0:
        # Child process
        return (0, parent_shm)
    else:
        # Parent process
        return (pid, parent_shm)


# Backwards compatibility aliases
PySharedMemory = SharedMemory
PyMessage = bytes  # Messages are just bytes in the new architecture