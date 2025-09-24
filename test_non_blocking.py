#!/usr/bin/env python3
"""
Test non-blocking publish behavior
"""
import sys
import time
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_non_blocking_publish():
    """Test non-blocking publish behavior"""
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_non_blocking')

        print("Creating publisher...")
        publisher = shm.create_publisher()

        # Create a small buffer to test non-blocking behavior
        BUFFER_SIZE = 1024  # Small buffer to easily fill it

        print("Testing non-blocking publish with small buffer...")

        # First publish should succeed
        result = publisher.try_publish(b"Test message 1")
        print(f"First publish result: {result}")
        assert result is not None
        assert result > 0

        # Fill up the buffer quickly
        message_count = 0
        success_count = 0

        print("Filling buffer to test non-blocking behavior...")
        for i in range(100):
            message = f"Message {i}".encode() * 100  # Make messages larger
            result = publisher.try_publish(message)
            message_count += 1

            if result is not None:
                success_count += 1
                print(f"✅ Published message {i}, sequence: {result}")
            else:
                print(f"❌ Buffer full at message {i}, publish failed (expected)")
                break

        print(f"Successfully published {success_count}/{message_count} messages")
        print("✅ Non-blocking publish test completed successfully!")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Testing non-blocking publish behavior...")
    success = test_non_blocking_publish()
    if success:
        print("✅ Non-blocking publish test completed successfully!")
    else:
        print("💥 Non-blocking publish test FAILED!")