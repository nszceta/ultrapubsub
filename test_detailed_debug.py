#!/usr/bin/env python3
"""
Debug test to understand what's happening with large messages
"""
import sys
import time
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_detailed_debug():
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_detailed_debug')

        print("Creating publisher and subscriber...")
        publisher = shm.create_publisher()
        subscriber = shm.create_subscriber()

        # Clear any filter
        subscriber.clear_filter()
        print(f"Filter cleared. Has filter: {subscriber.has_filter()}")

        print("Testing with small message first...")
        small_msg = b"hello world"
        print(f"Publishing small message: {len(small_msg)} bytes")
        publisher.publish(small_msg)

        received = subscriber.receive(timeout=2.0)
        if received:
            print(f"✅ Received small message: {len(received)} bytes")
        else:
            print("❌ Failed to receive small message")
            return False

        print("\nTesting with 1MB message...")
        msg_1mb = b'X' * (1024 * 1024)
        print(f"Publishing 1MB message: {len(msg_1mb)} bytes")
        publisher.publish(msg_1mb)

        print("Trying to receive...")
        received = subscriber.receive(timeout=2.0)
        if received:
            print(f"✅ Received 1MB message: {len(received)} bytes")
        else:
            print("❌ Failed to receive 1MB message")

            # Try multiple times to see if it's a timing issue
            print("Trying multiple receives...")
            for i in range(5):
                received = subscriber.try_receive()
                if received:
                    print(f"✅ Received on attempt {i+1}: {len(received)} bytes")
                    break
                else:
                    print(f"Attempt {i+1}: No message")
                    time.sleep(0.1)
            else:
                print("❌ Never received the message")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Detailed debugging...")
    success = test_detailed_debug()
    if success:
        print("✅ Debug test completed!")
    else:
        print("💥 Debug test FAILED!")