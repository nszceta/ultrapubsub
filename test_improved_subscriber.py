#!/usr/bin/env python3
"""
Test improved subscriber performance.
"""

import time
import sys
from ultrapubsub import SharedMemory

def main():
    print("Testing improved subscriber performance...")
    print("This will run for 30 seconds to measure CPU usage")

    # Create subscriber
    shm = SharedMemory("improved_subscriber_test")
    subscriber = shm.create_subscriber()

    # Poll for 30 seconds with the improved implementation
    start_time = time.time()
    poll_count = 0

    while time.time() - start_time < 30:
        try:
            message = subscriber.receive(timeout=0.001)
            if message:
                print(f"Received message: {len(message)} bytes")
            poll_count += 1
        except Exception:
            poll_count += 1

    end_time = time.time()
    print(f"Completed: {poll_count} polls in {end_time - start_time:.1f}s")
    print(f"Poll rate: {poll_count / (end_time - start_time):.0f} polls/sec")

if __name__ == "__main__":
    main()