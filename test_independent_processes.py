#!/usr/bin/env python3
"""
Test script to verify independent process support for UltraPubSub.
This script tests if spawned processes can attach to shared memory.
"""

import os
import sys
import time
import multiprocessing as mp
from multiprocessing import Process, Queue

# Add the path to the UltraPubSub module
sys.path.append('/home/adam/Documents/src/ultrapubsub/python')
sys.path.append('/home/adam/Documents/src/ultrapubsub')

def test_subscriber_process(result_queue):
    """Test subscriber process functionality."""
    try:
        import ultrapubsub

        print(f"[Subscriber {os.getpid()}] Attempting to attach to shared memory...")

        # Try to attach to existing shared memory
        shm = ultrapubsub.SharedMemory("independent_test")
        subscriber = shm.create_subscriber()

        print(f"[Subscriber {os.getpid()}] Successfully attached to shared memory")

        # Try to receive a message (should timeout if no publisher)
        try:
            msg = subscriber.receive(timeout=1000)  # 1 second timeout
            print(f"[Subscriber {os.getpid()}] Received message: {msg}")
            result_queue.put(("success", f"Received message: {msg}"))
        except Exception as e:
            print(f"[Subscriber {os.getpid()}] No message received (expected): {e}")
            result_queue.put(("success", "Attached successfully, no message (expected)"))

    except Exception as e:
        print(f"[Subscriber {os.getpid()}] Failed to attach: {e}")
        result_queue.put(("error", str(e)))

def test_publisher_process():
    """Test publisher process functionality."""
    try:
        import ultrapubsub

        print(f"[Publisher {os.getpid()}] Creating shared memory...")

        # Create shared memory
        shm = ultrapubsub.SharedMemory("independent_test")
        publisher = shm.create_publisher()

        print(f"[Publisher {os.getpid()}] Successfully created shared memory and publisher")

        # Send a test message
        test_msg = b"Hello from independent process!"
        publisher.publish(test_msg)

        print(f"[Publisher {os.getpid()}] Successfully published message")
        return True

    except Exception as e:
        print(f"[Publisher {os.getpid()}] Failed: {e}")
        return False

def main():
    print("Testing UltraPubSub independent process support...")
    print(f"Main process PID: {os.getpid()}")
    print(f"Multiprocessing start method: {mp.get_start_method()}")

    # Force spawn method for independent processes
    try:
        mp.set_start_method('spawn', force=True)
        print("✓ Successfully set spawn method")
    except RuntimeError as e:
        print(f"Could not set spawn method: {e}")
        print("Falling back to current method")
        # Continue with current method

    result_queue = Queue()

    # Test 1: Create publisher in main process
    print("\n=== Test 1: Creating publisher ===")
    if test_publisher_process():
        print("✓ Publisher creation successful")
    else:
        print("✗ Publisher creation failed")
        return

    # Test 2: Create subscriber process
    print("\n=== Test 2: Creating subscriber process ===")
    subscriber_proc = Process(target=test_subscriber_process, args=(result_queue,))
    subscriber_proc.start()

    # Wait for subscriber to complete
    subscriber_proc.join(timeout=10)

    if subscriber_proc.is_alive():
        print("✗ Subscriber process timed out")
        subscriber_proc.terminate()
        return

    # Get result from subscriber
    try:
        result = result_queue.get(timeout=1)
        status, message = result
        if status == "success":
            print(f"✓ Subscriber process successful: {message}")
        else:
            print(f"✗ Subscriber process failed: {message}")
    except:
        print("✗ No result from subscriber process")

    print("\n=== Test Summary ===")
    print("Independent process support test completed")

if __name__ == "__main__":
    main()