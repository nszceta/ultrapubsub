#!/usr/bin/env python3
"""
Publisher process for numpy array test
"""
import sys
import time
import ctypes
import os
import numpy as np
from datetime import datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ultrapubsub import SharedMemory

# Test configuration - exactly 35MB
ARRAY_SHAPE = (3480, 3480, 3)  # 3480*3480*3 = 36,331,200 bytes (~35MB)
DTYPE = np.uint8

def create_test_array(index):
    """Create a test numpy array with timestamp and index encoded"""
    array = np.zeros(ARRAY_SHAPE, dtype=DTYPE)

    # Fill with test pattern
    array[:, :, 0] = (index * 10) % 256  # Red channel encodes index
    array[:, :, 1] = int(time.time() * 1000) % 256  # Green channel encodes ms timestamp
    array[:, :, 2] = 128  # Blue channel fixed

    # Encode actual timestamp and index in first few pixels
    timestamp_bytes = int(time.time() * 1000000).to_bytes(8, 'little')  # microseconds
    index_bytes = index.to_bytes(4, 'little')

    # Write timestamp to first 8 pixels of first row
    for i, byte in enumerate(timestamp_bytes):
        array[0, i, 0] = byte
        array[0, i, 1] = byte ^ 0x80  # Complement for verification

    # Write index to next 4 pixels
    for i, byte in enumerate(index_bytes):
        array[0, i+8, 0] = byte
        array[0, i+8, 1] = byte ^ 0x80

    return array

def publisher_process(test_name, duration):
    """Publisher process for sending numpy arrays"""
    try:
        # Create publisher
        print(f"  📤 Publisher process starting with test_name: {test_name}")
        shm = SharedMemory(test_name)
        publisher = shm.create_publisher()
        print(f"  📤 Publisher created successfully")

        # Publish a small test message to ensure shared memory is fully initialized
        publisher.publish(b'init')
        print(f"  📤 Initialization message published")

        # Give subscribers time to start and register
        time.sleep(2.0)

        # Start publishing
        start_time = time.time()
        count = 0
        array_size = ARRAY_SHAPE[0] * ARRAY_SHAPE[1] * ARRAY_SHAPE[2] * DTYPE().itemsize

        print(f"  📤 Starting to publish arrays for {duration} seconds...")
        print(f"  📤 Array size: {array_size:,} bytes ({array_size/1024/1024:.1f}MB)")

        last_report_time = start_time
        report_interval = 1.0

        try:
            while time.time() - start_time < duration:
                # Create test array with current timestamp
                test_array = create_test_array(count)

                # Allocate pool slot
                slot, ptr = publisher.allocate_pool_slot()

                # Copy numpy array directly to pool slot
                pool_mem = ctypes.cast(ptr, ctypes.POINTER(ctypes.c_char * array_size))
                array_ptr = test_array.ctypes.data_as(ctypes.POINTER(ctypes.c_char * array_size))
                ctypes.memmove(pool_mem, array_ptr, array_size)

                # Publish the pool slot
                seq = publisher.publish_pool_slot(slot, array_size)
                count += 1

                # Report progress
                current_time = time.time()
                if current_time - last_report_time >= report_interval:
                    elapsed = current_time - start_time
                    freq = count / elapsed
                    throughput = (count * array_size) / elapsed / 1024 / 1024  # MB/s
                    print(f"  📈 Progress: {count} arrays, {freq:.1f} Hz, {throughput:.1f} MB/s")
                    last_report_time = current_time

        except Exception as e:
            print(f"  ❌ Publisher error: {e}")

        # Final stats
        total_time = time.time() - start_time
        avg_frequency = count / total_time if total_time > 0 else 0
        throughput = (count * array_size) / total_time / 1024 / 1024

        result = {
            'process_type': 'publisher',
            'arrays_sent': count,
            'duration': total_time,
            'frequency': avg_frequency,
            'throughput': throughput
        }

        print(f"  📤 Publisher Results:")
        print(f"     Arrays sent: {count}")
        print(f"     Duration: {total_time:.2f}s")
        print(f"     Frequency: {avg_frequency:.1f} Hz")
        print(f"     Throughput: {throughput:.1f} MB/s")
        print(f"RESULT:{result}")

    except Exception as e:
        print(f"  ❌ Publisher process failed: {e}")
        error_result = {
            'process_type': 'publisher',
            'arrays_sent': 0,
            'duration': 0,
            'frequency': 0,
            'throughput': 0,
            'error': str(e)
        }
        print(f"RESULT:{error_result}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: publisher_process.py <test_name> <duration>")
        sys.exit(1)

    test_name = sys.argv[1]
    duration = float(sys.argv[2])

    publisher_process(test_name, duration)