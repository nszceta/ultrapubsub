#!/usr/bin/env python3
"""
Single process test to check basic functionality
"""
import sys
import time
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_single_process():
    """Test both publisher and subscriber in same process"""
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_single', 32, 0, 0)

        print("Creating publisher...")
        publisher = shm.create_publisher()

        print("Creating subscriber...")
        subscriber = shm.create_subscriber()

        # Send a message
        data = b"Hello, World!"
        print(f"Publishing message: {data}")
        publisher.publish(data)

        # Try to receive immediately
        print("Trying to receive message...")
        result = subscriber.receive(timeout=1.0)

        if result:
            print(f"✅ Success! Received {len(result)} bytes: {result}")
            return True
        else:
            print("❌ Failed: No message received")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Testing single process functionality...")
    success = test_single_process()
    if success:
        print("🎉 Single process test PASSED!")
    else:
        print("💥 Single process test FAILED!")