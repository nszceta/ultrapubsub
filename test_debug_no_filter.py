#!/usr/bin/env python3
"""
Test to verify functionality without filtering
"""
import sys
import time
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_no_filter():
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_no_filter')

        print("Creating publisher and subscriber...")
        publisher = shm.create_publisher()
        subscriber = shm.create_subscriber()

        # Clear any filter
        subscriber.clear_filter()
        print(f"Filter cleared. Has filter: {subscriber.has_filter()}")

        print("Testing with small message...")
        small_msg = b"test message"
        publisher.publish(small_msg)

        received = subscriber.receive(timeout=2.0)
        if received:
            print(f"✅ Received: {received}")
        else:
            print("❌ Failed to receive small message")
            return False

        print("\nTesting with 1MB message...")
        msg_1mb = b'X' * (1024 * 1024)
        publisher.publish(msg_1mb)

        received = subscriber.receive(timeout=2.0)
        if received:
            print(f"✅ Received 1MB message: {len(received)} bytes")
        else:
            print("❌ Failed to receive 1MB message")
            return False

        print("\nTesting with 5MB message...")
        msg_5mb = b'Y' * (5 * 1024 * 1024)
        publisher.publish(msg_5mb)

        received = subscriber.receive(timeout=2.0)
        if received:
            print(f"✅ Received 5MB message: {len(received)} bytes")
        else:
            print("❌ Failed to receive 5MB message")
            return False

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Testing without filter...")
    success = test_no_filter()
    if success:
        print("✅ No-filter test PASSED!")
    else:
        print("💥 No-filter test FAILED!")