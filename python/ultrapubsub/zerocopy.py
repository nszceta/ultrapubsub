"""
Zero-Copy Numpy Array Broadcasting Module

This module provides zero-copy numpy array broadcasting across processes
using memory-mapped files and minimal Rust synchronization.
"""

import os
import mmap
import numpy as np
import time
import tempfile
from pathlib import Path
from typing import Optional, Tuple

# Import Rust synchronization functions
try:
    from . import ultrapubsub
except ImportError:
    import ultrapubsub


class ZeroCopyPublisher:
    """Zero-copy publisher for numpy arrays using memory-mapped files."""

    def __init__(self, name: str, array_shape: Tuple[int, ...], dtype: np.dtype = np.uint8):
        self.name = name
        self.array_shape = array_shape
        self.dtype = dtype
        self.sequence = 0

        # Create memory-mapped file for the numpy array
        self.data_file = f"/tmp/ultrapubsub_{name}_data.bin"
        self._create_data_file()

        # Create Rust synchronization coordinator
        ultrapubsub.create_coordinator(name)

    def _create_data_file(self):
        """Create and initialize the memory-mapped data file."""
        # Calculate required size
        array_size = int(np.prod(self.array_shape) * self.dtype.itemsize)

        # Create file with appropriate size
        with open(self.data_file, 'wb') as f:
            f.write(b'\x00' * array_size)

    def get_array(self) -> np.ndarray:
        """Get writeable numpy array view of the memory-mapped file."""
        return np.memmap(
            self.data_file,
            dtype=self.dtype,
            mode='r+',
            shape=self.array_shape
        )

    def wait_for_subscribers(self, timeout_ms: int = 30000) -> bool:
        """Wait for subscribers to be ready."""
        return ultrapubsub.wait_for_subscribers(timeout_ms)

    def broadcast(self) -> int:
        """Notify subscribers that new data is ready."""
        self.sequence += 1
        ultrapubsub.notify_broadcast(self.sequence)
        return self.sequence

    def wait_for_acknowledgments(self, timeout_ms: int = 5000) -> bool:
        """Wait for all subscribers to acknowledge."""
        return ultrapubsub.wait_for_acknowledgments(timeout_ms)

    def subscriber_count(self) -> int:
        """Get number of registered subscribers."""
        return ultrapubsub.get_subscriber_count()

    def cleanup(self):
        """Clean up resources."""
        ultrapubsub.cleanup_coordinator()
        try:
            os.unlink(self.data_file)
        except FileNotFoundError:
            pass


class ZeroCopySubscriber:
    """Zero-copy subscriber for numpy arrays using memory-mapped files."""

    def __init__(self, name: str, array_shape: Tuple[int, ...], dtype: np.dtype = np.uint8):
        self.name = name
        self.array_shape = array_shape
        self.dtype = dtype
        self.subscriber_id = None

        # Connect to existing memory-mapped file
        self.data_file = f"/tmp/ultrapubsub_{name}_data.bin"

        # Connect to Rust synchronization coordinator
        ultrapubsub.connect_coordinator(name)

        # Register as subscriber
        self.subscriber_id = ultrapubsub.register_subscriber()

    def get_array(self) -> np.ndarray:
        """Get read-only numpy array view of the memory-mapped file."""
        return np.memmap(
            self.data_file,
            dtype=self.dtype,
            mode='r',
            shape=self.array_shape
        )

    def wait_for_broadcast(self, timeout_ms: int = 5000) -> Optional[int]:
        """Wait for broadcast notification from publisher."""
        return ultrapubsub.wait_for_broadcast(timeout_ms)

    def acknowledge(self):
        """Acknowledge broadcast receipt."""
        ultrapubsub.acknowledge_broadcast()

    def cleanup(self):
        """Clean up resources."""
        ultrapubsub.cleanup_coordinator()


def create_test_array(shape: Tuple[int, ...] = (3480, 3480, 3), dtype: np.dtype = np.uint8) -> np.ndarray:
    """Create a test numpy array with known pattern."""
    array = np.zeros(shape, dtype=dtype)

    # Create test pattern - gradient fill
    if len(shape) == 3:  # RGB image
        array[:, :, 0] = np.linspace(0, 255, shape[0], dtype=dtype)[:, np.newaxis]
        array[:, :, 1] = np.linspace(0, 255, shape[1], dtype=dtype)[np.newaxis, :]
        array[:, :, 2] = 128
    elif len(shape) == 2:  # Grayscale
        array = np.linspace(0, 255, shape[0] * shape[1], dtype=dtype).reshape(shape)
    else:  # 1D
        array = np.linspace(0, 255, shape[0], dtype=dtype)

    return array


def verify_array_integrity(array: np.ndarray, expected_shape: Tuple[int, ...], expected_dtype: np.dtype) -> bool:
    """Verify that array matches expected shape and dtype."""
    return array.shape == expected_shape and array.dtype == expected_dtype


def benchmark_zero_copy_performance(
    name: str = "benchmark",
    array_shape: Tuple[int, ...] = (3480, 3480, 3),
    dtype: np.dtype = np.uint8,
    duration_seconds: int = 10,
    num_subscribers: int = 6
) -> dict:
    """
    Benchmark zero-copy broadcasting performance.

    Returns:
        dict: Performance metrics including throughput, frequency, etc.
    """
    import subprocess
    import json
    import time

    test_name = f"{name}_{int(time.time())}"
    results = {}

    try:
        # Start publisher process
        publisher_cmd = [
            'python', '-c', f'''
import sys
sys.path.insert(0, "/root/ultrapubsub")
from python.ultrapubsub.zerocopy import ZeroCopyPublisher, create_test_array
import time
import numpy as np

# Create publisher
publisher = ZeroCopyPublisher("{test_name}", {array_shape}, np.{dtype.__name__})
print("Publisher created")

# Wait for subscribers
if not publisher.wait_for_subscribers(30000):
    print("Timeout waiting for subscribers")
    exit(1)

print(f"Starting benchmark with {{publisher.subscriber_count()}} subscribers")

# Create test array
test_array = create_test_array({array_shape}, np.{dtype.__name__})
array_size = test_array.nbytes

# Benchmark loop
start_time = time.time()
end_time = start_time + {duration_seconds}
message_count = 0

while time.time() < end_time:
    # Write new data to memory-mapped array
    publisher_array = publisher.get_array()
    publisher_array[:] = test_array[:]

    # Notify subscribers
    publisher.broadcast()

    # Wait for acknowledgments
    if not publisher.wait_for_acknowledgments(5000):
        print("Timeout waiting for acknowledgments")
        break

    message_count += 1

    # Progress update
    if message_count % 5 == 0:
        elapsed = time.time() - start_time
        freq = message_count / elapsed
        throughput = (message_count * array_size) / elapsed / (1024 * 1024)
        print(f"Message {{message_count}}: {{freq:.1f}} Hz, {{throughput:.1f}} MB/s")

# Calculate results
total_time = time.time() - start_time
frequency = message_count / total_time
throughput_mbps = (message_count * array_size) / total_time / (1024 * 1024)
throughput_gbps = throughput_mbps / 1024

result = {{
    "type": "publisher",
    "messages_sent": message_count,
    "array_size_bytes": array_size,
    "total_bytes": message_count * array_size,
    "duration": total_time,
    "frequency_hz": frequency,
    "throughput_mbps": throughput_mbps,
    "throughput_gbps": throughput_gbps,
    "subscribers": publisher.subscriber_count()
}}

print(json.dumps(result))
publisher.cleanup()
'''
        ]

        publisher_proc = subprocess.Popen(publisher_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Start subscriber processes
        subscriber_procs = []
        for i in range(num_subscribers):
            subscriber_cmd = [
                'python', '-c', f'''
import sys
sys.path.insert(0, "/root/ultrapubsub")
from python.ultrapubsub.zerocopy import ZeroCopySubscriber
import time
import numpy as np

# Create subscriber
subscriber = ZeroCopySubscriber("{test_name}", {array_shape}, np.{dtype.__name__})
print(f"Subscriber {{i}} created")

# Receive messages
start_time = time.time()
messages_received = 0
total_bytes = 0

while time.time() - start_time < {duration_seconds + 5}:
    # Wait for broadcast
    seq = subscriber.wait_for_broadcast(1000)
    if seq is not None:
        # Read data from memory-mapped array
        array = subscriber.get_array()

        # Verify array integrity
        expected_size = {int(np.prod(array_shape))}
        if array.size == expected_size:
            messages_received += 1
            total_bytes += array.nbytes

            # Acknowledge
            subscriber.acknowledge()

            # Progress update
            if messages_received % 5 == 0:
                elapsed = time.time() - start_time
                freq = messages_received / elapsed
                throughput = (messages_received * array.nbytes) / elapsed / (1024 * 1024)
                print(f"Sub {{i}}: {{messages_received}} messages, {{freq:.1f}} Hz, {{throughput:.1f}} MB/s")

# Calculate results
total_time = time.time() - start_time
frequency = messages_received / total_time if total_time > 0 else 0
throughput_mbps = (messages_received * array.nbytes) / total_time / (1024 * 1024) if total_time > 0 else 0
throughput_gbps = throughput_mbps / 1024

result = {{
    "type": "subscriber",
    "subscriber_id": i,
    "messages_received": messages_received,
    "array_size_bytes": array.nbytes if messages_received > 0 else 0,
    "total_bytes": total_bytes,
    "duration": total_time,
    "frequency_hz": frequency,
    "throughput_mbps": throughput_mbps,
    "throughput_gbps": throughput_gbps
}}

print(json.dumps(result))
subscriber.cleanup()
'''
            ]

            proc = subprocess.Popen(subscriber_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            subscriber_procs.append(proc)

        # Wait for completion
        try:
            publisher_proc.wait(timeout=duration_seconds + 30)
            for proc in subscriber_procs:
                proc.wait(timeout=duration_seconds + 30)
        except subprocess.TimeoutExpired:
            publisher_proc.terminate()
            for proc in subscriber_procs:
                proc.terminate()

        # Collect results
        pub_stdout, pub_stderr = publisher_proc.communicate()

        results['publisher'] = json.loads(pub_stdout.strip())
        results['subscribers'] = []

        for i, proc in enumerate(subscriber_procs):
            sub_stdout, sub_stderr = proc.communicate()
            try:
                sub_result = json.loads(sub_stdout.strip())
                results['subscribers'].append(sub_result)
            except:
                print(f"Failed to parse subscriber {i} output")

        # Calculate aggregate metrics
        if results['subscribers']:
            avg_subscriber_freq = np.mean([sub['frequency_hz'] for sub in results['subscribers']])
            min_subscriber_freq = np.min([sub['frequency_hz'] for sub in results['subscribers']])
            max_subscriber_freq = np.max([sub['frequency_hz'] for sub in results['subscribers']])

            results['aggregate'] = {
                'avg_subscriber_frequency_hz': avg_subscriber_freq,
                'min_subscriber_frequency_hz': min_subscriber_freq,
                'max_subscriber_frequency_hz': max_subscriber_freq,
                'efficiency_vs_target': (results['publisher']['throughput_mbps'] / 1433.6) * 100,  # 1.4 GB/s target
                'message_loss_pct': ((results['publisher']['messages_sent'] - results['subscribers'][0]['messages_received']) / results['publisher']['messages_sent'] * 100) if results['publisher']['messages_sent'] > 0 else 0
            }

    except Exception as e:
        results['error'] = str(e)

    finally:
        # Cleanup
        try:
            data_file = f"/tmp/ultrapubsub_{test_name}_data.bin"
            if os.path.exists(data_file):
                os.unlink(data_file)
        except:
            pass

    return results


if __name__ == "__main__":
    # Simple test
    print("Testing Zero-Copy Numpy Array Broadcasting")

    # Test parameters
    test_shape = (1000, 1000, 3)  # Smaller for testing
    test_name = "zerocopy_test"

    # Create test array
    test_array = create_test_array(test_shape)
    print(f"Created test array: {test_array.shape}, {test_array.dtype}, {test_array.nbytes} bytes")

    # Create publisher
    publisher = ZeroCopyPublisher(test_name, test_shape)
    print(f"Publisher created: {publisher.data_file}")

    # Create subscriber
    subscriber = ZeroCopySubscriber(test_name, test_shape)
    print(f"Subscriber created: {subscriber.subscriber_id}")

    # Test broadcasting
    publisher_array = publisher.get_array()
    publisher_array[:] = test_array[:]

    seq = publisher.broadcast()
    print(f"Broadcast sequence: {seq}")

    # Subscriber receives
    received_seq = subscriber.wait_for_broadcast(5000)
    print(f"Subscriber received sequence: {received_seq}")

    if received_seq == seq:
        subscriber_array = subscriber.get_array()
        if np.array_equal(subscriber_array, test_array):
            print("✅ Zero-copy broadcast successful!")
        else:
            print("❌ Array data mismatch")
    else:
        print("❌ Sequence mismatch")

    # Cleanup
    publisher.cleanup()
    subscriber.cleanup()
    print("✅ Test completed")