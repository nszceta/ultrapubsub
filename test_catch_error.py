#!/usr/bin/env python3
"""
Debug test to catch the actual error
"""
import sys
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_catch_error():
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_catch_error')

        print("Creating publisher...")
        publisher = shm.create_publisher()

        print("Testing large message...")
        msg_large = b'X' * (1024 * 1024)
        print(f"Publishing large message: {len(msg_large)} bytes")

        try:
            seq = publisher.publish(msg_large)
            print(f"Sequence returned: {seq}")
            print(f"Type: {type(seq)}")
        except Exception as e:
            print(f"Exception caught: {e}")
            print(f"Exception type: {type(e)}")
            import traceback
            traceback.print_exc()

        return True

    except Exception as e:
        print(f"❌ Setup error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Catching error test...")
    success = test_catch_error()
    if success:
        print("✅ Catch error test completed!")
    else:
        print("💥 Catch error test FAILED!")