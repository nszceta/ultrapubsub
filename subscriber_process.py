#!/usr/bin/env python3
"""
Subscriber process for numpy array test
"""
import sys
import time
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
from ultrapubsub import SharedMemory

# Import test configuration
ARRAY_SHAPE = (3480, 3480, 3)
DTYPE = np.uint8

def decode_array_info(array):
    """Decode timestamp and index from numpy array"""
    # Read timestamp from first 8 pixels
    timestamp_bytes = bytes()
    for i in range(8):
        byte = array[0, i, 0]
        complement = array[0, i, 1]
        if byte != (complement ^ 0x80):
            return None, None, "Data corruption detected"
        timestamp_bytes += bytes([byte])

    # Read index from next 4 pixels
    index_bytes = bytes()
    for i in range(4):
        byte = array[0, i+8, 0]
        complement = array[0, i+8, 1]
        if byte != (complement ^ 0x80):
            return None, None, "Data corruption detected"
        index_bytes += bytes([byte])

    timestamp = int.from_bytes(timestamp_bytes, 'little') / 1000000.0
    index = int.from_bytes(index_bytes, 'little')

    return timestamp, index, "OK"

def subscriber_process(subscriber_id, test_name, duration):
    """Completely independent subscriber process for receiving numpy arrays"""
    try:
        print(f"  📡 Sub {subscriber_id}: Starting independent process...")

        # Wait for publisher to create shared memory
        print(f"  📡 Sub {subscriber_id}: Waiting for shared memory to be available...")
        max_attempts = 30  # 30 seconds max wait
        attempts = 0

        while attempts < max_attempts:
            try:
                # Try to create subscriber
                print(f"  📡 Sub {subscriber_id}: Attempting to create subscriber with ID {subscriber_id}")
                shm = SharedMemory(test_name)
                subscriber = shm.create_subscriber_with_id(subscriber_id)
                print(f"  📡 Sub {subscriber_id}: Subscriber {subscriber_id} created successfully")
                break
            except Exception as e:
                attempts += 1
                if attempts >= max_attempts:
                    raise Exception(f"Failed to connect to shared memory after {max_attempts} attempts: {e}")
                print(f"  📡 Sub {subscriber_id}: Waiting for publisher (attempt {attempts}/{max_attempts})...")
                time.sleep(1.0)

        received = 0
        latencies = []
        errors = 0
        first_message_time = None
        last_message_time = None
        start_time = time.time()

        print(f"  📡 Sub {subscriber_id} process started...")

        while time.time() - start_time < duration:
            try:
                msg_start = time.time()
                data = subscriber.receive(timeout=0.1)
                msg_end = time.time()

                if data:
                    # Convert bytes back to numpy array
                    array = np.frombuffer(data, dtype=DTYPE).reshape(ARRAY_SHAPE)

                    # Decode embedded information
                    timestamp, index, status = decode_array_info(array)

                    if timestamp:
                        latency = (msg_end - timestamp) * 1000  # ms
                        latencies.append(latency)

                        if first_message_time is None:
                            first_message_time = msg_end
                        last_message_time = msg_end

                    received += 1

                    if status != "OK":
                        errors += 1

                    # Log every 50th message
                    if received % 50 == 0:
                        avg_lat = sum(latencies) / len(latencies) if latencies else 0
                        print(f"    📡 Sub {subscriber_id}: {received} arrays, avg latency: {avg_lat:.1f}ms")

            except Exception as e:
                errors += 1
                time.sleep(0.001)  # Brief pause on error

        # Calculate final statistics
        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            min_latency = min(latencies)
            max_latency = max(latencies)
            p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
            p99_latency = sorted(latencies)[int(len(latencies) * 0.99)]
        else:
            avg_latency = min_latency = max_latency = p95_latency = p99_latency = 0

        # Calculate message rate
        if first_message_time and last_message_time:
            active_duration = last_message_time - first_message_time
            msg_rate = received / active_duration if active_duration > 0 else 0
        else:
            msg_rate = 0

        result = {
            'subscriber_id': subscriber_id,
            'arrays_received': received,
            'errors': errors,
            'avg_latency_ms': avg_latency,
            'min_latency_ms': min_latency,
            'max_latency_ms': max_latency,
            'p95_latency_ms': p95_latency,
            'p99_latency_ms': p99_latency,
            'message_rate_hz': msg_rate,
            'data_integrity_ok': errors == 0
        }

        # Print result to stdout for parent process to capture
        print(f"RESULT:{result}")
        print(f"  📡 Sub {subscriber_id} process completed: {received} arrays")

    except Exception as e:
        print(f"  ❌ Sub {subscriber_id} process failed: {e}")
        error_result = {
            'subscriber_id': subscriber_id,
            'arrays_received': 0,
            'errors': 999,
            'avg_latency_ms': 0,
            'min_latency_ms': 0,
            'max_latency_ms': 0,
            'p95_latency_ms': 0,
            'p99_latency_ms': 0,
            'message_rate_hz': 0,
            'data_integrity_ok': False,
            'error': str(e)
        }
        print(f"RESULT:{error_result}")

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: subscriber_process.py <subscriber_id> <test_name> <duration>")
        sys.exit(1)

    subscriber_id = int(sys.argv[1])
    test_name = sys.argv[2]
    duration = float(sys.argv[3])

    subscriber_process(subscriber_id, test_name, duration)