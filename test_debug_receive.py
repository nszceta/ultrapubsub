#!/usr/bin/env python3
"""
Debug test to understand why messages aren't being received
"""
import sys
import time
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_debug_receive():
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_debug')

        print("Creating publisher and subscriber...")
        publisher = shm.create_publisher()
        subscriber = shm.create_subscriber()

        print("Testing with small message first...")
        small_msg = b"hello world"
        publisher.publish(small_msg)
        print("✅ Published small message")

        # Try to receive with timeout
        for i in range(10):
            received = subscriber.try_receive()
            if received:
                print(f"✅ Received small message: {received}")
                break
            else:
                print(f"Attempt {i+1}: No message, waiting...")
                time.sleep(0.1)
        else:
            print("❌ Never received small message")
            return False

        print("\nTesting with larger message...")
        larger_msg = b'X' * (1024 * 1024)  # 1MB
        publisher.publish(larger_msg)
        print("✅ Published 1MB message")

        for i in range(10):
            received = subscriber.try_receive()
            if received:
                print(f"✅ Received 1MB message: {len(received)} bytes")
                break
            else:
                print(f"Attempt {i+1}: No message, waiting...")
                time.sleep(0.1)
        else:
            print("❌ Never received 1MB message")
            return False

        print("\nTesting subscriber state...")
        print(f"Has filter: {subscriber.has_filter()}")
        print(f"Has messages: {subscriber.try_receive() is not None}")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Debugging receive functionality...")
    success = test_debug_receive()
    if success:
        print("✅ Debug test completed successfully!")
    else:
        print("💥 Debug test FAILED!")