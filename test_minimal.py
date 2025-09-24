#!/usr/bin/env python3
import sys
import time
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_one_publish():
    shm = SharedMemory('minimal_test')
    publisher = shm.create_publisher()

    small_msg = b'hello world'
    print(f"Publishing small message ({len(small_msg)} bytes)...")

    start_time = time.time()
    seq = publisher.publish(small_msg)
    end_time = time.time()

    print(f"Small message: seq={seq}, time={(end_time-start_time)*1000:.2f}ms")

    large_msg = b'X' * (1024 * 1024)  # 1MB
    print(f"Publishing large message ({len(large_msg)} bytes)...")

    start_time = time.time()
    seq = publisher.publish(large_msg)
    end_time = time.time()

    print(f"Large message: seq={seq}, time={(end_time-start_time)*1000:.2f}ms")

if __name__ == "__main__":
    test_one_publish()