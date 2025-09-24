#!/usr/bin/env python3
"""
Test to isolate the binding issue
"""
import sys
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_binding_issue():
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_binding')

        print("Creating publisher...")
        publisher = shm.create_publisher()

        # Test with very small message
        test_msgs = [
            b"a",
            b"hello",
            b"this is a test message"
        ]

        for i, msg in enumerate(test_msgs):
            print(f"\nTest {i+1}: Publishing {len(msg)} bytes")

            try:
                seq = publisher.publish(msg)
                print(f"  Raw result: {seq}")
                print(f"  Type: {type(seq)}")

                if seq is None:
                    print("  ❌ Got None - this indicates a binding issue")
                elif isinstance(seq, int):
                    print(f"  ✅ Got integer: {seq}")
                else:
                    print(f"  ❓ Got unexpected type: {type(seq)}")

            except Exception as e:
                print(f"  Exception: {e}")
                import traceback
                traceback.print_exc()

        return True

    except Exception as e:
        print(f"❌ Setup error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Testing binding issue...")
    success = test_binding_issue()
    if success:
        print("✅ Binding test completed!")
    else:
        print("💥 Binding test FAILED!")