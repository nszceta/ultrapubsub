#!/usr/bin/env python3
"""
Test circular buffer wrap-around functionality by publishing large messages
"""
import sys
import time
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_wrap_around():
    """Test circular buffer wrap-around with large messages"""
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_wrap')

        print("Creating publisher and subscriber...")
        publisher = shm.create_publisher()
        subscriber = shm.create_subscriber()

        # Create a large message (1MB) that will help test wrap-around
        large_msg = b'X' * (1024 * 1024)  # 1MB message

        print(f"Publishing {len(large_msg)} byte messages to test wrap-around...")

        # Publish multiple large messages to force wrap-around
        for i in range(10):
            print(f"Publishing message {i+1}...")
            publisher.publish(large_msg)

            # Try to receive immediately
            result = subscriber.receive(timeout=1.0)
            if result:
                print(f"✅ Received message {i+1}: {len(result)} bytes")
                # Verify the data integrity
                if result == large_msg:
                    print("✅ Data integrity verified")
                else:
                    print(f"❌ Data corruption detected! Expected {len(large_msg)} bytes, got {len(result)}")
                    return False
            else:
                print(f"❌ Failed to receive message {i+1}")
                return False

        print("🎉 Wrap-around test PASSED!")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Testing circular buffer wrap-around functionality...")
    success = test_wrap_around()
    if success:
        print("✅ All wrap-around tests completed successfully!")
    else:
        print("💥 Wrap-around test FAILED!")