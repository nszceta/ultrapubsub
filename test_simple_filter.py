#!/usr/bin/env python3
"""
Simple test to debug filtering
"""
import sys
import time
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_simple_filter():
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_simple_filter')

        print("Creating publisher and subscriber...")
        publisher = shm.create_publisher()
        subscriber = shm.create_subscriber()

        # Clear any existing messages
        while subscriber.try_receive():
            pass

        print("Testing without filter...")
        publisher.publish(b"test message 24 bytes long")
        time.sleep(0.1)

        msg = subscriber.try_receive()
        if msg:
            print(f"✅ Received without filter: {msg} (length: {len(msg)})")
        else:
            print("❌ No message received without filter")

        # Now test with size filter
        print("\nTesting with size filter (20-30 bytes)...")
        subscriber.set_size_filter(20, 30)
        print(f"Filter set: {subscriber.has_filter()}")

        publisher.publish(b"another test message 24 bytes")
        time.sleep(0.1)

        msg = subscriber.try_receive()
        if msg:
            print(f"✅ Received with filter: {msg} (length: {len(msg)})")
        else:
            print("❌ No message received with filter")

        # Test with wrong size
        print("\nTesting with wrong size message...")
        publisher.publish(b"short")  # 5 bytes
        time.sleep(0.1)

        msg = subscriber.try_receive()
        if msg:
            print(f"❌ Should not have received: {msg} (length: {len(msg)})")
        else:
            print("✅ Correctly filtered out short message")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_simple_filter()