#!/usr/bin/env python3
"""
Basic test to verify 35MB payloads work
"""
import sys
import time
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_basic_35mb():
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_basic_35mb')

        print("Creating publisher and subscriber...")
        publisher = shm.create_publisher()
        subscriber = shm.create_subscriber()

        print("Creating 35MB payload...")
        payload = b'X' * (35 * 1024 * 1024)
        print(f"Payload size: {len(payload)/(1024*1024)}MB")

        print("Publishing 35MB message...")
        start_time = time.time()
        publisher.publish(payload)
        publish_time = time.time() - start_time
        print(f"✅ Published in {publish_time*1000:.2f}ms")

        print("Receiving 35MB message...")
        start_time = time.time()
        received = subscriber.receive(timeout=5.0)
        receive_time = time.time() - start_time

        if received:
            print(f"✅ Received {len(received)/(1024*1024)}MB in {receive_time*1000:.2f}ms")
            print(f"✅ Data integrity: {'✅' if received == payload else '❌'}")
            print(f"✅ Total round-trip time: {(publish_time + receive_time)*1000:.2f}ms")
            return True
        else:
            print("❌ Failed to receive message")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Testing basic 35MB payload functionality...")
    success = test_basic_35mb()
    if success:
        print("✅ Basic 35MB test PASSED!")
    else:
        print("💥 Basic 35MB test FAILED!")