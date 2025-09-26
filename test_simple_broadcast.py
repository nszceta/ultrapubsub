#!/usr/bin/env python3
"""
Simple test to verify broadcast functionality
"""

import sys
import os
sys.path.insert(0, 'python')

import ultrapubsub
import time
import multiprocessing as mp

def simple_subscriber_process(subscriber_id: int, results_queue: mp.Queue, test_name: str):
    """Simple subscriber process that tests broadcast receive."""
    print(f"[Subscriber {subscriber_id} PID:{os.getpid()}] Process starting...")
    print(f"[Subscriber {subscriber_id}] Test name: {test_name}")

    try:
        # Give publisher time to start
        print(f"[Subscriber {subscriber_id}] Waiting for publisher...")
        time.sleep(3)

        # Create subscriber
        print(f"[Subscriber {subscriber_id}] Creating subscriber...")
        subscriber = ultrapubsub.Subscriber.with_id(test_name, subscriber_id)
        print(f"[Subscriber {subscriber_id}] Subscriber created successfully")

        # Try to receive a message
        print(f"[Subscriber {subscriber_id}] Attempting to receive message...")
        start_time = time.time()
        try:
            message = subscriber.receive()
            end_time = time.time()
            if message:
                print(f"[Subscriber {subscriber_id}] Received message: {len(message)} bytes in {end_time - start_time:.3f}s")
                results_queue.put((subscriber_id, len(message)))
            else:
                print(f"[Subscriber {subscriber_id}] No message received")
                results_queue.put((subscriber_id, None))
        except Exception as e:
            print(f"[Subscriber {subscriber_id}] Receive error: {e}")
            results_queue.put((subscriber_id, None))

        # Deregister
        print(f"[Subscriber {subscriber_id}] Deregistering...")
        subscriber.deregister()
        print(f"[Subscriber {subscriber_id}] Deregistered successfully")

    except Exception as e:
        print(f"[Subscriber {subscriber_id}] Fatal error: {e}")
        import traceback
        traceback.print_exc()
        results_queue.put((subscriber_id, None))

    print(f"[Subscriber {subscriber_id}] Process completed")

def test_simple_broadcast():
    """Test simple broadcast functionality."""
    test_name = "/simple_broadcast_test"

    print("=== Testing Simple Broadcast ===")

    # Start subscriber processes
    subscriber_processes = []
    results_queue = mp.Queue()

    print("Starting 2 subscriber processes...")
    for i in range(2):
        p = mp.Process(target=simple_subscriber_process, args=(i, results_queue, test_name))
        subscriber_processes.append(p)
        p.start()
        print(f"Started subscriber process {i}")

    # Give subscribers time to start and register
    print("Waiting for subscribers to register...")
    time.sleep(2)

    # Create publisher
    print("Creating publisher...")
    publisher = ultrapubsub.Publisher(test_name)
    print("Publisher created successfully")

    # Wait for subscribers to register
    print("Waiting for subscriber count...")
    start_time = time.time()
    while publisher.subscriber_count() < 2:
        if time.time() - start_time > 10:
            print(f"Timeout waiting for subscribers, got {publisher.subscriber_count()}")
            break
        time.sleep(0.1)
        print(f"Current count: {publisher.subscriber_count()}")

    print(f"Final subscriber count: {publisher.subscriber_count()}")

    # Broadcast a message
    test_message = b"Hello from simple broadcast!" * 100  # Small message for testing
    print(f"Broadcasting {len(test_message)} bytes...")
    sequence = publisher.broadcast(test_message)
    print(f"Broadcast completed with sequence {sequence}")

    # Wait a bit for subscribers to process
    print("Waiting for subscribers to process...")
    time.sleep(2)

    # Cleanup
    try:
        publisher.cleanup()
        print("Publisher cleanup completed")
    except Exception as e:
        print(f"Publisher cleanup error: {e}")

    # Wait for subscriber processes to complete
    print("Waiting for subscriber processes to complete...")
    for i, p in enumerate(subscriber_processes):
        p.join(timeout=10)
        if p.is_alive():
            print(f"Terminating hanging subscriber {i}")
            p.terminate()
            p.join()
        else:
            print(f"Subscriber {i} completed")

    # Collect results
    print("Collecting results...")
    results = []
    try:
        while not results_queue.empty():
            results.append(results_queue.get())
    except Exception as e:
        print(f"Error collecting results: {e}")

    print(f"Results: {results}")

if __name__ == "__main__":
    test_simple_broadcast()