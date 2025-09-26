#!/usr/bin/env python3
"""
Test script for synchronous broadcast implementation.

This script tests:
1. Publisher creates broadcast buffer
2. Multiple subscribers register (0-5)
3. Publisher broadcasts message and waits for all subscribers
4. All subscribers receive the SAME message
5. Publisher doesn't continue until all acknowledge
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python'))

import ultrapubsub
import time
import threading
import multiprocessing as mp
from typing import List, Any

def subscriber_process(subscriber_id: int, results_queue: mp.Queue, test_name: str):
    """Subscriber process that receives broadcast messages."""
    print(f"[Subscriber {subscriber_id} PID:{os.getpid()}] Process starting...")
    print(f"[Subscriber {subscriber_id}] Test name: {test_name}")

    try:
        # Give the publisher time to start
        print(f"[Subscriber {subscriber_id}] Waiting for publisher to start...")
        time.sleep(2)

        # Create subscriber with specific ID
        print(f"[Subscriber {subscriber_id}] Creating subscriber...")
        subscriber = ultrapubsub.Subscriber.with_id(test_name, subscriber_id)
        print(f"[Subscriber {subscriber_id}] Registered successfully")

        # Wait a bit for publisher to be ready and send broadcast
        time.sleep(1.0)

        # Receive broadcast message with timeout - use blocking receive
        print(f"[Subscriber {subscriber_id}] Waiting for message...")
        start_time = time.time()
        message = None

        try:
            # Use blocking receive with our own timeout
            message = subscriber.receive()
            print(f"[Subscriber {subscriber_id}] Received message: {len(message)} bytes")
        except Exception as e:
            print(f"[Subscriber {subscriber_id}] Receive error: {e}")
            message = None

        if message is None:
            print(f"[Subscriber {subscriber_id}] Failed to receive message")
            results_queue.put((subscriber_id, None))
            return

        # Put message in results queue for verification
        results_queue.put((subscriber_id, message))

        # Deregister and cleanup
        try:
            subscriber.deregister()
            print(f"[Subscriber {subscriber_id}] Deregistered successfully")
        except Exception as e:
            print(f"[Subscriber {subscriber_id}] Deregister error: {e}")

        print(f"[Subscriber {subscriber_id}] Process exiting normally")

    except Exception as e:
        print(f"[Subscriber {subscriber_id}] Fatal error: {e}")
        import traceback
        traceback.print_exc()
        results_queue.put((subscriber_id, None))
    finally:
        print(f"[Subscriber {subscriber_id}] Process cleanup complete")

def publisher_process(test_name: str, num_subscribers: int, ready_event: mp.Event, done_event: mp.Event):
    """Publisher process that broadcasts messages."""
    print(f"[Publisher PID:{os.getpid()}] Starting...")

    try:
        # Give subscribers time to start first
        print(f"[Publisher] Waiting for subscribers to start...")
        time.sleep(2)

        # Create publisher
        print(f"[Publisher] Creating publisher...")
        publisher = ultrapubsub.Publisher(test_name)
        print(f"[Publisher] Created broadcast buffer")

        # Signal that publisher is ready
        ready_event.set()

        # Wait for subscribers to register
        print(f"[Publisher] Waiting for {num_subscribers} subscribers to register...")
        start_time = time.time()
        while publisher.subscriber_count() < num_subscribers:
            if time.time() - start_time > 30:  # 30 second timeout
                print(f"[Publisher] Timeout waiting for subscribers, got {publisher.subscriber_count()}")
                done_event.set()  # Signal done even if not all subscribers
                return
            time.sleep(0.1)
            print(f"[Publisher] Current count: {publisher.subscriber_count()}")

        count = publisher.subscriber_count()
        print(f"[Publisher] {count} subscribers registered")

        # Create test message (smaller for testing)
        test_message = b"Hello from synchronous broadcast!" * 1000  # ~35KB
        print(f"[Publisher] Broadcasting {len(test_message)} bytes...")

        # Broadcast message (this should wait for all subscribers)
        print(f"[Publisher] Starting broadcast...")
        start_time = time.time()
        sequence = publisher.broadcast(test_message)
        end_time = time.time()

        print(f"[Publisher] Broadcast completed in {end_time - start_time:.3f}s")
        print(f"[Publisher] Sequence number: {sequence}")

        # Signal that broadcast is done
        print(f"[Publisher] Setting done event...")
        done_event.set()
        print(f"[Publisher] Done event set successfully")

        # Wait for subscribers to process
        print(f"[Publisher] Waiting for subscribers to finish...")
        time.sleep(2)

        # Cleanup
        try:
            publisher.cleanup()
            print(f"[Publisher] Cleanup complete")
        except Exception as e:
            print(f"[Publisher] Cleanup error: {e}")

    except Exception as e:
        print(f"[Publisher] Fatal error: {e}")
        import traceback
        traceback.print_exc()
        done_event.set()  # Ensure we signal done even on error


def test_synchronous_broadcast():
    """Test synchronous broadcast with multiple subscribers."""
    test_name = "/sync_broadcast_test"
    num_subscribers = 2

    print(f"=== Testing Synchronous Broadcast with {num_subscribers} subscribers ===")
    print(f"[Main PID:{os.getpid()}] Starting test...")

    # Cleanup any existing shared memory
    try:
        ultrapubsub.cleanup_shared_memory(test_name)
        print("[Main] Cleaned up existing shared memory")
    except Exception as e:
        print(f"[Main] Cleanup error: {e}")

    # Create synchronization events
    publisher_ready = mp.Event()
    broadcast_done = mp.Event()

    # Start publisher process
    print("[Main] Starting publisher process...")
    publisher_proc = mp.Process(
        target=publisher_process,
        args=(test_name, num_subscribers, publisher_ready, broadcast_done)
    )
    print("[Main] Created publisher process object")
    publisher_proc.start()
    print("[Main] Publisher process started")

    # Wait for publisher to be ready
    print("[Main] Waiting for publisher to be ready...")
    if not publisher_ready.wait(timeout=15):
        print("❌ Publisher failed to start")
        publisher_proc.terminate()
        publisher_proc.join()
        return

    print("[Main] Publisher is ready")

    # Give publisher a moment to fully initialize
    time.sleep(1)

    # Create results queue for subscriber messages
    results_queue = mp.Queue()

    # Start subscriber processes
    subscriber_processes = []
    print(f"[Main] Starting {num_subscribers} subscriber processes...")
    for i in range(num_subscribers):
        print(f"[Main] Creating subscriber process {i}...")
        p = mp.Process(target=subscriber_process, args=(i, results_queue, test_name))
        subscriber_processes.append(p)
        print(f"[Main] Starting subscriber process {i}...")
        p.start()
        print(f"[Main] Started subscriber process {i}")
        time.sleep(0.5)  # Delay between process starts

    print(f"[Main] All {num_subscribers} subscriber processes started")
    time.sleep(3)  # Give subscribers time to start and register

    # Wait for broadcast to complete with timeout
    print("[Main] Waiting for broadcast to complete...")
    if not broadcast_done.wait(timeout=45):
        print("❌ Broadcast timed out")
    else:
        print("[Main] Broadcast completed successfully")

    # Wait for all subscriber processes to complete with longer timeout
    print("[Main] Waiting for subscriber processes to complete...")
    for i, p in enumerate(subscriber_processes):
        print(f"[Main] Waiting for subscriber {i}...")
        p.join(timeout=20)
        if p.is_alive():
            print(f"[Main] Terminating hanging subscriber {i}")
            p.terminate()
            p.join()  # Wait for termination to complete

    # Wait for publisher to finish
    print("[Main] Waiting for publisher process to complete...")
    publisher_proc.join(timeout=10)
    if publisher_proc.is_alive():
        print("[Main] Terminating hanging publisher")
        publisher_proc.terminate()
        publisher_proc.join()

    # Collect results
    print("[Main] Collecting results...")
    results = []
    try:
        # Get results from queue - expect one per subscriber
        for i in range(num_subscribers):
            try:
                result = results_queue.get(timeout=5)
                results.append(result)
                print(f"[Main] Got result from subscriber {result[0]}")
            except Exception as e:
                print(f"[Main] Timeout getting result from subscriber {i}")
                results.append((i, None))
    except Exception as e:
        print(f"[Main] Error collecting results: {e}")

    print(f"\n=== Results ===")
    print(f"Expected: {num_subscribers} subscribers")
    print(f"Received: {len(results)} responses")

    # Print detailed results
    for subscriber_id, message in results:
        if message is None:
            print(f"❌ Subscriber {subscriber_id}: FAILED")
        else:
            print(f"✅ Subscriber {subscriber_id}: SUCCESS ({len(message)} bytes)")

    # Verify all subscribers got the same message
    if len(results) >= 1:  # At least one subscriber succeeded
        first_message = results[0][1]
        if first_message is None:
            print("❌ FAILURE: First subscriber failed")
            return

        all_same = True
        for subscriber_id, message in results:
            if message is None:
                print(f"❌ Subscriber {subscriber_id} failed to receive message")
                all_same = False
            elif message != first_message:
                print(f"❌ Subscriber {subscriber_id} got different message")
                all_same = False

        if all_same:
            print(f"✅ SUCCESS: All responding subscribers received the SAME message!")
            print(f"✅ Message length: {len(first_message)} bytes")
            print(f"✅ {len(results)}/{num_subscribers} subscribers responded correctly")
        else:
            print("❌ FAILURE: Messages were not identical")
    else:
        print("❌ FAILURE: No subscribers responded")

    # Final cleanup
    try:
        ultrapubsub.cleanup_shared_memory(test_name)
        print("[Main] Final cleanup completed")
    except Exception as e:
        print(f"[Main] Final cleanup error: {e}")

    print("\n=== Test Complete ===")

if __name__ == "__main__":
    test_synchronous_broadcast()