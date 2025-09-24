#!/usr/bin/env python3
"""
Test to check if publisher initialization is working
"""
import sys
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_init_debug():
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_init')

        print("Creating publisher...")
        publisher = shm.create_publisher()

        print("Initializing publisher...")
        try:
            publisher.initialize()
            print("✅ Publisher initialized successfully")
        except Exception as e:
            print(f"❌ Publisher initialization failed: {e}")
            import traceback
            traceback.print_exc()
            return False

        print("Testing publish...")
        msg = b"test message"
        try:
            seq = publisher.publish(msg)
            print(f"✅ Publish returned: {seq} (type: {type(seq)})")
        except Exception as e:
            print(f"❌ Publish failed: {e}")
            import traceback
            traceback.print_exc()
            return False

        return True

    except Exception as e:
        print(f"❌ Setup error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Testing initialization...")
    success = test_init_debug()
    if success:
        print("✅ Init debug completed!")
    else:
        print("💥 Init debug FAILED!")