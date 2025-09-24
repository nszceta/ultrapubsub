#!/usr/bin/env python3
"""
Simple test to validate zero-copy pool functionality
"""
import sys
import time
import ctypes
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_pool_functionality():
    print("🧪 Testing zero-copy pool functionality...")

    shm = SharedMemory('simple_pool_test')
    publisher = shm.create_publisher()
    subscriber = shm.create_subscriber()

    # Test data
    test_data = b'X' * (35 * 1024 * 1024)  # 35MB

    print(f"📦 Test payload: {len(test_data)} bytes")

    # Test 1: Allocate and publish to pool
    print("\n📤 Testing pool allocation and publishing...")
    start_time = time.time()

    slot, ptr = publisher.allocate_pool_slot()
    print(f"  Allocated pool slot {slot} at address {ptr}")

    # Copy data directly to pool
    pool_mem = ctypes.cast(ptr, ctypes.POINTER(ctypes.c_char * len(test_data)))
    ctypes.memmove(pool_mem, test_data, len(test_data))
    print("  ✅ Copied data to pool slot")

    seq = publisher.publish_pool_slot(slot, len(test_data))
    print(f"  ✅ Published pool slot with sequence {seq}")

    publish_time = time.time() - start_time
    print(f"  📊 Publish time: {publish_time*1000:.2f}ms")

    # Test 2: Receive the message
    print("\n📡 Testing message reception...")
    start_time = time.time()

    received = subscriber.receive(timeout=5.0)
    if received:
        print(f"  ✅ Received {len(received)} bytes")
        if received == test_data:
            print("  ✅ Data integrity verified")
        else:
            print("  ❌ Data integrity check failed")
    else:
        print("  ❌ No message received")

    receive_time = time.time() - start_time
    print(f"  📊 Receive time: {receive_time*1000:.2f}ms")

    # Test 3: Performance test
    print("\n🚀 Testing performance with 5 messages...")
    start_time = time.time()

    for i in range(5):
        slot, ptr = publisher.allocate_pool_slot()
        pool_mem = ctypes.cast(ptr, ctypes.POINTER(ctypes.c_char * len(test_data)))
        ctypes.memmove(pool_mem, test_data, len(test_data))
        seq = publisher.publish_pool_slot(slot, len(test_data))

        # Receive immediately
        received = subscriber.receive(timeout=1.0)
        if not received:
            print(f"  ❌ Failed to receive message {i+1}")

    total_time = time.time() - start_time
    avg_time = total_time / 5
    print(f"  📊 Average time per message: {avg_time*1000:.2f}ms")
    print(f"  📊 Achieved frequency: {1/avg_time:.1f} Hz")

    print("\n✅ Pool functionality test completed!")

if __name__ == "__main__":
    test_pool_functionality()