#!/usr/bin/env python3
"""
Simple test to check if completion ring issue is fixed
"""
import sys
import time
import multiprocessing as mp
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def publisher_test():
    """Simple publisher test"""
    try:
        shm = SharedMemory('test', 32, 0, 0)
        publisher = shm.create_publisher()

        # Send a simple message
        data = b"Hello, World!"
        publisher.publish(data)
        print(f"Publisher sent {len(data)} bytes")

        time.sleep(0.1)  # Give subscriber time

        return True
    except Exception as e:
        print(f"Publisher error: {e}")
        return False

def subscriber_test():
    """Simple subscriber test"""
    try:
        # Retry attaching to shared memory
        for attempt in range(5):
            try:
                shm = SharedMemory.attach('test')
                subscriber = shm.create_subscriber()
                break
            except Exception as e:
                if attempt < 4:
                    print(f"Subscriber attach attempt {attempt + 1} failed, retrying...")
                    time.sleep(0.2)
                else:
                    raise e

        # Try to receive with timeout
        result = subscriber.receive(timeout=2.0)
        if result:
            print(f"Subscriber received {len(result)} bytes: {result}")
            return True
        else:
            print("Subscriber received nothing")
            return False
    except Exception as e:
        print(f"Subscriber error: {e}")
        return False

if __name__ == "__main__":
    print("Testing completion ring fix...")

    # Set up multiprocessing
    mp.set_start_method('spawn')

    # Start subscriber first (it will wait for the shared memory to be created)
    print("Starting subscriber process...")
    sub_process = mp.Process(target=subscriber_test)
    sub_process.start()

    # Give subscriber time to start
    time.sleep(0.5)

    # Now run publisher test
    print("Running publisher test...")
    pub_success = publisher_test()

    # Wait for subscriber to complete
    sub_process.join(timeout=3.0)

    if sub_process.exitcode == 0:
        print("✅ Test completed successfully")
    else:
        print("❌ Test failed")

    print(f"Publisher success: {pub_success}")
    print(f"Subscriber process exit code: {sub_process.exitcode}")