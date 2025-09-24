#!/usr/bin/env python3
"""
Simple profiling test to identify bottlenecks
"""
import sys
import time
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_publish_speed():
    """Test how fast we can publish messages"""
    print("🧪 Testing publish speed...")

    shm = SharedMemory('profile_test')
    publisher = shm.create_publisher()

    # Test with different message sizes
    sizes = [1024, 1024*1024, 35*1024*1024]  # 1KB, 1MB, 35MB

    for size in sizes:
        print(f"\n📊 Testing {size//1024//1024}MB messages...")

        payload = b'X' * size
        count = 100

        start_time = time.time()
        for i in range(count):
            seq = publisher.publish(payload)
            if i % 10 == 0:
                print(f"  Published {i+1}/{count} (seq: {seq})")

        end_time = time.time()
        duration = end_time - start_time

        print(f"  Results: {count} messages in {duration:.2f}s")
        print(f"  Rate: {count/duration:.1f} messages/sec")
        print(f"  Throughput: {(count * size) / duration / 1024 / 1024:.1f} MB/s")

def test_receive_speed():
    """Test how fast we can receive messages"""
    print("\n🧪 Testing receive speed...")

    shm = SharedMemory('profile_test')
    subscriber = shm.create_subscriber()

    count = 50
    start_time = time.time()

    for i in range(count):
        data = subscriber.receive(timeout=5.0)
        if data:
            print(f"  Received {i+1}/{count}: {len(data)} bytes")
        else:
            print(f"  Timeout on message {i+1}")
            break

    end_time = time.time()
    duration = end_time - start_time

    print(f"  Results: {count} messages in {duration:.2f}s")
    print(f"  Rate: {count/duration:.1f} messages/sec")

if __name__ == "__main__":
    print("🚀 UltraPubSub Simple Profiling Test")
    print("=" * 50)

    test_publish_speed()
    test_receive_speed()

    print("\n✅ Profiling completed")