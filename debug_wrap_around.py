#!/usr/bin/env python3
"""
Debug circular buffer wrap-around functionality
"""
import sys
import time
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def debug_wrap_around():
    """Debug wrap-around with detailed output"""
    try:
        print("Creating shared memory...")
        shm = SharedMemory('debug_wrap')

        print("Creating publisher and subscriber...")
        publisher = shm.create_publisher()
        subscriber = shm.create_subscriber()

        # Start with a small message first
        small_msg = b'Hello, World!'
        print(f"Publishing small message: {len(small_msg)} bytes")
        publisher.publish(small_msg)

        print("Trying to receive small message...")
        result = subscriber.receive(timeout=2.0)
        if result:
            print(f"✅ Received small message: {len(result)} bytes")
        else:
            print("❌ Failed to receive small message")
            return False

        # Now try a larger message
        medium_msg = b'X' * 10000  # 10KB
        print(f"Publishing medium message: {len(medium_msg)} bytes")
        publisher.publish(medium_msg)

        print("Trying to receive medium message...")
        result = subscriber.receive(timeout=2.0)
        if result:
            print(f"✅ Received medium message: {len(result)} bytes")
        else:
            print("❌ Failed to receive medium message")
            return False

        print("🎉 Debug test PASSED!")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Debugging wrap-around functionality...")
    success = debug_wrap_around()
    if success:
        print("✅ Debug test completed successfully!")
    else:
        print("💥 Debug test FAILED!")