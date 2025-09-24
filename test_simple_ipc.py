#!/usr/bin/env python3
"""
Simple test to verify UltraPubSub IPC is working with spawn()
"""

import sys
import time
import multiprocessing as mp

# Add the path to the UltraPubSub module
sys.path.append('/home/adam/Documents/src/ultrapubsub/python')
sys.path.append('/home/adam/Documents/src/ultrapubsub')

def test_subscriber(result_queue):
    """Test subscriber function"""
    try:
        from ultrapubsub import SharedMemory

        # Attach to shared memory
        shm = SharedMemory.attach("simple_test")
        subscriber = shm.create_subscriber()

        # Try to receive a message
        msg = subscriber.receive(timeout=2000)  # 2 second timeout
        result_queue.put(("success", msg))

    except Exception as e:
        result_queue.put(("error", str(e)))

def main():
    print("Testing UltraPubSub IPC with spawn()...")

    # Force spawn method
    try:
        mp.set_start_method('spawn', force=True)
        print("✓ Using spawn method")
    except:
        print("Could not set spawn method")
        return

    # Create shared memory and publisher in main process
    try:
        from ultrapubsub import SharedMemory

        shm = SharedMemory("simple_test")
        publisher = shm.create_publisher()
        print("✓ Created shared memory and publisher")

        # Start subscriber process
        result_queue = mp.Queue()
        subscriber_proc = mp.Process(target=test_subscriber, args=(result_queue,))
        subscriber_proc.start()

        # Give subscriber time to start
        time.sleep(0.5)

        # Send a test message
        test_msg = b"Hello IPC test!"
        publisher.publish(test_msg)
        print(f"✓ Published message: {test_msg}")

        # Wait for subscriber result
        subscriber_proc.join(timeout=5)

        if subscriber_proc.is_alive():
            print("✗ Subscriber timed out")
            subscriber_proc.terminate()
            return

        # Get result
        try:
            result = result_queue.get(timeout=1)
            status, message = result

            if status == "success":
                print(f"✓ Subscriber received: {message}")
                print("✓ IPC test successful!")
            else:
                print(f"✗ Subscriber error: {message}")
        except:
            print("✗ No result from subscriber")

    except Exception as e:
        print(f"✗ Test failed: {e}")

if __name__ == "__main__":
    main()