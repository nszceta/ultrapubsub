"""
High-level Python API for ultrapubsub

This module provides a user-friendly interface for the ultrapubsub IPC system,
simplifying the creation of publishers and subscribers for inter-process communication.
"""

import os
import sys
import multiprocessing
from typing import Optional, Callable, Any
from .ultrapubsub import PyPublisher, PySubscriber


class SharedMemory:
    """
    High-level wrapper for shared memory management using Ring Buffer.

    This class provides a simplified interface for creating and managing
    shared memory regions for inter-process communication.
    """

    def __init__(self, name: str):
        """
        Initialize a new shared memory region.

        Args:
            name: Unique name for the shared memory region
        """
        self.name = name
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
            self._publisher = Publisher(self.name)
        return self._publisher

    def create_subscriber(self) -> 'Subscriber':
        """
        Create a subscriber for this shared memory region.

        Returns:
            Subscriber instance
        """
        if self._subscriber is None:
            self._subscriber = Subscriber(self.name)
        return self._subscriber


class Publisher:
    """
    High-level publisher for sending messages to shared memory.

    This class provides a simple interface for publishing messages
    to subscribers through shared memory.
    """

    def __init__(self, name: str):
        """
        Initialize publisher with a shared memory name.

        Args:
            name: Name for the shared memory region
        """
        self._publisher = PyPublisher(name)
        self._publisher.initialize()

    def publish(self, data: bytes) -> int:
        """
        Publish data to shared memory.

        Args:
            data: Bytes data to publish

        Returns:
            Sequence number of the published message

        Raises:
            RuntimeError: If publishing fails
        """
        if not isinstance(data, bytes):
            raise TypeError("Data must be bytes")

        return self._publisher.publish(data)

    def try_publish(self, data: bytes) -> bool:
        """
        Try to publish data to shared memory without blocking.

        Args:
            data: Bytes data to publish

        Returns:
            True if published successfully, False if buffer is full

        Raises:
            RuntimeError: If publishing fails for reasons other than full buffer
        """
        if not isinstance(data, bytes):
            raise TypeError("Data must be bytes")

        result = self._publisher.try_publish(data)
        return result is not None

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

    def publish_batch(self, messages: list[bytes]) -> None:
        """
        Publish multiple messages in a batch for better performance.

        Args:
            messages: List of byte messages to publish

        Raises:
            RuntimeError: If batch publishing fails
        """
        if not messages:
            return

        for msg in messages:
            if not isinstance(msg, bytes):
                raise TypeError("All messages must be bytes")

        self._publisher.publish_batch(messages)

    def allocate_and_write(self, size: int) -> int:
        """
        Allocate shared memory and return pointer for direct writing.

        Args:
            size: Size of memory to allocate in bytes

        Returns:
            Memory address as integer for direct writing

        Raises:
            RuntimeError: If allocation fails
        """
        return self._publisher.allocate_and_write(size)

    def publish_allocation(self, ptr: int, size: int) -> None:
        """
        Publish pre-allocated shared memory.

        Args:
            ptr: Memory address from allocate_and_write
            size: Size of the allocated memory

        Raises:
            RuntimeError: If publishing fails
        """
        self._publisher.publish_allocation(ptr, size)

    def free_memory(self, ptr: int, size: int) -> None:
        """
        Free previously allocated memory.

        Args:
            ptr: Memory address to free
            size: Size of the allocated memory

        Raises:
            RuntimeError: If freeing fails
        """
        self._publisher.free_memory(ptr, size)


class Subscriber:
    """
    High-level subscriber for receiving messages from shared memory.

    This class provides a simple interface for receiving messages
    from publishers through shared memory.
    """

    def __init__(self, name: str):
        """
        Initialize subscriber with a shared memory name.

        Args:
            name: Name for the shared memory region
        """
        self._subscriber = PySubscriber(name)
        self._subscriber.initialize()

    def receive(self, timeout: Optional[float] = None) -> Optional[bytes]:
        """
        Receive a message from shared memory.

        Args:
            timeout: Timeout in seconds (default: None for no timeout)
                     Note: The implementation now uses blocking io_uring behavior,
                     so timeout may not be exact but should be respected approximately

        Returns:
            Received data as bytes, or None if timeout
        """
        import time
        start_time = time.time()


        # Use blocking event-driven behavior with timeout
        while True:
            # Try non-blocking receive first
            try:
                result = self._subscriber.try_receive()
                if result:
                    return result
            except Exception as e:
                pass

            # Check timeout
            if timeout and (time.time() - start_time) > timeout:
                return None

            # Small sleep to avoid busy waiting
            time.sleep(0.001)  # 1ms sleep

        return None

    def try_receive(self) -> Optional[bytes]:
        """
        Try to receive a message without blocking.

        Returns:
            Received data as bytes, or None if no message available
        """
        try:
            return self._subscriber.try_receive()
        except Exception:
            return None

    def set_prefix_filter(self, prefix: bytes) -> None:
        """
        Set a prefix filter for messages.

        Args:
            prefix: Only messages starting with this prefix will be received
        """
        self._subscriber.set_prefix_filter(prefix)

    def set_size_filter(self, min_size: int, max_size: int) -> None:
        """
        Set a size filter for messages.

        Args:
            min_size: Minimum message size in bytes
            max_size: Maximum message size in bytes
        """
        self._subscriber.set_size_filter(min_size, max_size)

    def clear_filter(self) -> None:
        """
        Remove any active message filter.
        """
        self._subscriber.clear_filter()

    def has_filter(self) -> bool:
        """
        Check if this subscriber has an active filter.

        Returns:
            True if a filter is active, False otherwise
        """
        return self._subscriber.has_filter()

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