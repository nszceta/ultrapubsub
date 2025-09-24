#!/usr/bin/env python3
"""
Debug test to check for errors
"""
import sys
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_error_debug():
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_error_debug')

        print("Creating publisher and subscriber...")
        publisher = shm.create_publisher()
        subscriber = shm.create_subscriber()

        # Clear any filter
        subscriber.clear_filter()

        print("Testing error handling...")

        # Test large message with error checking
        msg_large = b'X' * (1024 * 1024)
        print(f"Publishing large message: {len(msg_large)} bytes")

        try:
            seq = publisher.publish(msg_large)
            print(f"Sequence returned: {seq}")
            print(f"Type: {type(seq)}")
        except Exception as e:
            print(f"Error publishing: {e}")
            import traceback
            traceback.print_exc()

        # Try to receive
        try:
            received = subscriber.receive(timeout=2.0)
            if received:
                print(f"✅ Received large message: {len(received)} bytes")
            else:
                print("❌ No message received")
        except Exception as e:
            print(f"Error receiving: {e}")
            import traceback
            traceback.print_exc()

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Error debugging...")
    success = test_error_debug()
    if success:
        print("✅ Error debug completed!")
    else:
        print("💥 Error debug FAILED!")