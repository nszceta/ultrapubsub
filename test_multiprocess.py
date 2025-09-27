#!/usr/bin/env python3
"""
Test pthread mutex with multiple processes: 1 producer, 6 subscribers
"""
import subprocess
import time
import sys
import os
import signal

def run_producer(test_name):
    """Producer process"""
    import ultrapubsub

    print(f"[PRODUCER] Starting producer for {test_name}")

    try:
        # Clean up first
        try:
            ultrapubsub.cleanup_shared_memory(test_name)
        except:
            pass

        # Create publisher
        publisher = ultrapubsub.create_publisher(test_name)
        print(f"[PRODUCER] Publisher created, waiting for subscribers...")

        # Wait for subscribers to register
        start_time = time.time()
        while publisher.subscriber_count() < 6 and time.time() - start_time < 10:
            print(f"[PRODUCER] Waiting for subscribers... current count: {publisher.subscriber_count()}")
            time.sleep(0.5)

        if publisher.subscriber_count() < 6:
            print(f"[PRODUCER] ERROR: Only {publisher.subscriber_count()} subscribers registered")
            return

        print(f"[PRODUCER] All {publisher.subscriber_count()} subscribers registered!")

        # Send test messages
        messages = [
            b"Message 1 from producer",
            b"Message 2 from producer",
            b"Message 3 from producer"
        ]

        for i, msg in enumerate(messages):
            print(f"[PRODUCER] Broadcasting message {i+1}: {msg.decode()}")
            publisher.broadcast(msg)
            print(f"[PRODUCER] Message {i+1} broadcast completed")
            time.sleep(1)  # Give subscribers time to process

        print(f"[PRODUCER] All messages sent, waiting 2 seconds before cleanup...")
        time.sleep(2)

        ultrapubsub.cleanup_shared_memory(test_name)
        print(f"[PRODUCER] Test completed successfully")

    except Exception as e:
        print(f"[PRODUCER] ERROR: {e}")
        import traceback
        traceback.print_exc()

def run_subscriber(test_name, subscriber_id):
    """Subscriber process"""
    import ultrapubsub

    print(f"[SUBSCRIBER-{subscriber_id}] Starting subscriber for {test_name}")

    try:
        # Create subscriber
        subscriber = ultrapubsub.create_subscriber_with_id(test_name, subscriber_id)
        print(f"[SUBSCRIBER-{subscriber_id}] Subscriber created")

        # Register
        subscriber.register()
        print(f"[SUBSCRIBER-{subscriber_id}] Registered successfully")

        # Receive messages
        message_count = 0
        start_time = time.time()

        print(f"[SUBSCRIBER-{subscriber_id}] Waiting for messages...")

        while message_count < 3 and time.time() - start_time < 15:
            try:
                received = subscriber.receive()
                message_count += 1
                elapsed = time.time() - start_time
                print(f"[SUBSCRIBER-{subscriber_id}] Received message {message_count}: {received.decode()} (elapsed: {elapsed:.2f}s)")
            except Exception as e:
                print(f"[SUBSCRIBER-{subscriber_id}] Error receiving: {e}")
                time.sleep(0.1)

        print(f"[SUBSCRIBER-{subscriber_id}] Completed! Received {message_count} messages")

    except Exception as e:
        print(f"[SUBSCRIBER-{subscriber_id}] ERROR: {e}")
        import traceback
        traceback.print_exc()

def main():
    test_name = "/multiprocess_test"

    print("=== Multi-Process Test: 1 Producer, 6 Subscribers ===")

    # Start subscriber processes
    subscriber_processes = []
    for i in range(6):
        print(f"Starting subscriber process {i}...")
        proc = subprocess.Popen([
            sys.executable, __file__, test_name, str(i), "subscriber"
        ])
        subscriber_processes.append(proc)
        time.sleep(0.2)  # Small delay between starting subscribers

    # Start producer process
    print("Starting producer process...")
    producer_proc = subprocess.Popen([
        sys.executable, __file__, test_name, "producer"
    ])

    # Wait for all processes to complete
    print("Waiting for all processes to complete...")

    # Wait for producer first (it should finish first)
    producer_proc.wait(timeout=30)
    print("Producer process completed")

    # Then wait for subscribers
    for i, proc in enumerate(subscriber_processes):
        try:
            proc.wait(timeout=10)
            print(f"Subscriber {i} process completed")
        except subprocess.TimeoutExpired:
            print(f"Subscriber {i} process timed out, terminating...")
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except:
                proc.kill()

    print("=== Test completed ===")

if __name__ == "__main__":
    if len(sys.argv) == 4 and sys.argv[2] == "producer":
        run_producer(sys.argv[1])
    elif len(sys.argv) == 4 and sys.argv[2] == "subscriber":
        run_subscriber(sys.argv[1], int(sys.argv[3]))
    else:
        main()