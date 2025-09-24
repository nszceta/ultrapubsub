#!/usr/bin/env python3
"""
Test batch publishing functionality
"""
import sys
import time
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_batch_publish():
    """Test batch publishing functionality"""
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_batch')

        print("Creating publisher and subscriber...")
        publisher = shm.create_publisher()
        subscriber = shm.create_subscriber()

        # Create multiple messages
        messages = [
            b"Message 1",
            b"Message 2",
            b"Message 3",
            b"Message 4",
            b"Message 5"
        ]

        print(f"Publishing batch of {len(messages)} messages...")
        sequences = publisher.publish_batch(messages)
        print(f"Published sequences: {sequences}")

        # Receive all messages
        received_count = 0
        for i, expected_msg in enumerate(messages):
            result = subscriber.receive(timeout=2.0)
            if result:
                print(f"✅ Received message {i+1}: {result}")
                if result == expected_msg:
                    print("✅ Data integrity verified")
                else:
                    print(f"❌ Data corruption! Expected: {expected_msg}, Got: {result}")
                    return False
                received_count += 1
            else:
                print(f"❌ Failed to receive message {i+1}")
                return False

        if received_count == len(messages):
            print("🎉 Batch publish test PASSED!")
            return True
        else:
            print(f"❌ Expected {len(messages)} messages, got {received_count}")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Testing batch publishing functionality...")
    success = test_batch_publish()
    if success:
        print("✅ Batch publishing test completed successfully!")
    else:
        print("💥 Batch publishing test FAILED!")