#!/usr/bin/env python3
"""
Simple test to verify subscriber process creation works
"""

import sys
import os
sys.path.insert(0, 'python')

import ultrapubsub
import time
import multiprocessing as mp

def simple_subscriber_process(subscriber_id: int, test_name: str):
    """Simple subscriber process that just tests basic functionality."""
    print(f"[Subscriber {subscriber_id} PID:{os.getpid()}] Process starting...")
    print(f"[Subscriber {subscriber_id}] Test name: {test_name}")

    try:
        # Test basic import
        print(f"[Subscriber {subscriber_id}] Import successful...")

        # Test creating subscriber
        print(f"[Subscriber {subscriber_id}] Creating subscriber...")
        subscriber = ultrapubsub.Subscriber.with_id(test_name, subscriber_id)
        print(f"[Subscriber {subscriber_id}] Subscriber created successfully")

        # Test deregister
        print(f"[Subscriber {subscriber_id}] Deregistering...")
        subscriber.deregister()
        print(f"[Subscriber {subscriber_id}] Deregistered successfully")

        print(f"[Subscriber {subscriber_id}] Process completed successfully")

    except Exception as e:
        print(f"[Subscriber {subscriber_id}] Error: {e}")
        import traceback
        traceback.print_exc()

def test_subscriber_creation():
    """Test creating subscriber processes."""
    test_name = "/simple_subscriber_test"

    print("=== Testing Subscriber Process Creation ===")

    # Create publisher first to initialize shared memory
    print("Creating publisher...")
    publisher = ultrapubsub.Publisher(test_name)
    print("Publisher created successfully")

    # Start subscriber processes
    subscriber_processes = []
    print("Starting 2 subscriber processes...")

    for i in range(2):
        print(f"Creating subscriber process {i}...")
        p = mp.Process(target=simple_subscriber_process, args=(i, test_name))
        subscriber_processes.append(p)
        p.start()
        print(f"Started subscriber process {i}")

    # Wait for processes to complete
    print("Waiting for subscriber processes to complete...")
    for i, p in enumerate(subscriber_processes):
        p.join(timeout=10)
        if p.is_alive():
            print(f"Terminating hanging subscriber {i}")
            p.terminate()
            p.join()
        else:
            print(f"Subscriber {i} completed")

    # Cleanup
    try:
        publisher.cleanup()
        print("Publisher cleanup completed")
    except Exception as e:
        print(f"Publisher cleanup error: {e}")

    print("Test completed")

if __name__ == "__main__":
    test_subscriber_creation()