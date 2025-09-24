#!/usr/bin/env python3
"""
Direct performance test without multiprocessing complexity
"""
import sys
import time
import ctypes
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_traditional_performance():
    """Test traditional publish performance"""
    print("🧪 Testing Traditional Memory Copy Performance")
    print("-" * 50)

    shm = SharedMemory('traditional_test')
    publisher = shm.create_publisher()
    subscriber = shm.create_subscriber()

    # 35MB test payload
    payload = b'X' * (35 * 1024 * 1024)
    message_count = 10

    print(f"📦 Test payload: {len(payload)} bytes")
    print(f"🎯 Messages to send: {message_count}")

    # Measure publish performance
    start_time = time.time()
    sequences = []

    for i in range(message_count):
        seq = publisher.publish(payload)
        sequences.append(seq)
        print(f"  Published message {i+1}/{message_count} (seq: {seq})")

    publish_time = time.time() - start_time
    avg_publish_time = publish_time / message_count * 1000  # Convert to ms
    frequency = message_count / publish_time

    print(f"\n📊 Traditional Results:")
    print(f"   Total publish time: {publish_time:.3f}s")
    print(f"   Average per message: {avg_publish_time:.1f}ms")
    print(f"   Achieved frequency: {frequency:.1f} Hz")
    print(f"   Throughput: {(message_count * 35) / publish_time:.1f} MB/s")

    # Measure receive performance
    print(f"\n📡 Testing receive performance...")
    start_time = time.time()
    received_count = 0

    for i in range(message_count):
        data = subscriber.receive(timeout=5.0)
        if data:
            received_count += 1
            print(f"  Received message {received_count}/{message_count}: {len(data)} bytes")
        else:
            print(f"  ❌ Failed to receive message {i+1}")

    receive_time = time.time() - start_time
    success_rate = (received_count / message_count) * 100

    print(f"\n📊 Receive Results:")
    print(f"   Messages received: {received_count}/{message_count}")
    print(f"   Success rate: {success_rate:.1f}%")
    print(f"   Total receive time: {receive_time:.3f}s")

    return {
        'method': 'traditional',
        'publish_time_ms': avg_publish_time,
        'frequency': frequency,
        'throughput_mb_s': (message_count * 35) / publish_time,
        'success_rate': success_rate
    }

def test_pool_performance():
    """Test zero-copy pool performance"""
    print("\n🧪 Testing Zero-Copy Pool Performance")
    print("-" * 50)

    shm = SharedMemory('pool_test')
    publisher = shm.create_publisher()
    subscriber = shm.create_subscriber()

    # 35MB test payload
    payload = b'X' * (35 * 1024 * 1024)
    message_count = 10

    print(f"📦 Test payload: {len(payload)} bytes")
    print(f"🎯 Messages to send: {message_count}")

    # Measure pool publish performance
    start_time = time.time()
    sequences = []

    for i in range(message_count):
        try:
            # Allocate pool slot
            slot, ptr = publisher.allocate_pool_slot()
            print(f"  Allocated pool slot {slot}")

            # Copy data directly to pool
            pool_mem = ctypes.cast(ptr, ctypes.POINTER(ctypes.c_char * len(payload)))
            ctypes.memmove(pool_mem, payload, len(payload))
            print(f"  Copied data to pool slot {slot}")

            # Publish the pool slot
            seq = publisher.publish_pool_slot(slot, len(payload))
            sequences.append(seq)
            print(f"  Published pool slot {slot} with sequence {seq}")

        except Exception as e:
            print(f"  ❌ Error on message {i+1}: {e}")
            break

    publish_time = time.time() - start_time
    messages_sent = len(sequences)
    avg_publish_time = publish_time / messages_sent * 1000 if messages_sent > 0 else 0
    frequency = messages_sent / publish_time if messages_sent > 0 else 0

    print(f"\n📊 Pool Results:")
    print(f"   Messages sent: {messages_sent}/{message_count}")
    print(f"   Total publish time: {publish_time:.3f}s")
    print(f"   Average per message: {avg_publish_time:.1f}ms")
    print(f"   Achieved frequency: {frequency:.1f} Hz")
    print(f"   Throughput: {(messages_sent * 35) / publish_time:.1f} MB/s")

    # Measure receive performance
    print(f"\n📡 Testing receive performance...")
    start_time = time.time()
    received_count = 0

    for i in range(message_count):
        data = subscriber.receive(timeout=5.0)
        if data:
            received_count += 1
            print(f"  Received message {received_count}/{message_count}: {len(data)} bytes")

            # Verify data integrity
            if data == payload:
                print(f"  ✅ Data integrity verified")
            else:
                print(f"  ❌ Data integrity check failed")
        else:
            print(f"  ❌ Failed to receive message {i+1}")

    receive_time = time.time() - start_time
    success_rate = (received_count / messages_sent) * 100 if messages_sent > 0 else 0

    print(f"\n📊 Receive Results:")
    print(f"   Messages received: {received_count}/{messages_sent}")
    print(f"   Success rate: {success_rate:.1f}%")
    print(f"   Total receive time: {receive_time:.3f}s")

    return {
        'method': 'pool',
        'publish_time_ms': avg_publish_time,
        'frequency': frequency,
        'throughput_mb_s': (messages_sent * 35) / publish_time if messages_sent > 0 else 0,
        'success_rate': success_rate
    }

def main():
    """Run performance comparison"""
    print("🚀 UltraPubSub Real Performance Test")
    print("=" * 60)

    # Test traditional method
    traditional_results = test_traditional_performance()

    # Test pool method
    pool_results = test_pool_performance()

    # Performance comparison
    print("\n" + "=" * 60)
    print("📊 FINAL PERFORMANCE COMPARISON")
    print("=" * 60)

    print(f"\n📤 Traditional Memory Copy:")
    print(f"   Average publish time: {traditional_results['publish_time_ms']:.1f} ms")
    print(f"   Frequency: {traditional_results['frequency']:.1f} Hz")
    print(f"   Throughput: {traditional_results['throughput_mb_s']:.1f} MB/s")
    print(f"   Success rate: {traditional_results['success_rate']:.1f}%")

    print(f"\n🚀 Zero-Copy Pool:")
    print(f"   Average publish time: {pool_results['publish_time_ms']:.1f} ms")
    print(f"   Frequency: {pool_results['frequency']:.1f} Hz")
    print(f"   Throughput: {pool_results['throughput_mb_s']:.1f} MB/s")
    print(f"   Success rate: {pool_results['success_rate']:.1f}%")

    # Calculate improvement
    if traditional_results['publish_time_ms'] > 0 and pool_results['publish_time_ms'] > 0:
        speed_improvement = traditional_results['publish_time_ms'] / pool_results['publish_time_ms']
        freq_improvement = pool_results['frequency'] / traditional_results['frequency']

        print(f"\n🎯 PERFORMANCE IMPROVEMENT:")
        print(f"   Speed improvement: {speed_improvement:.1f}x faster")
        print(f"   Frequency improvement: {freq_improvement:.1f}x faster")
        print(f"   Time reduction: {(1 - 1/speed_improvement) * 100:.0f}%")

    # Target analysis
    target_freq = 40.0
    print(f"\n🎯 TARGET ANALYSIS (40Hz = 25ms per message):")
    print(f"Traditional: {traditional_results['frequency']/target_freq*100:.1f}% of target")
    print(f"Pool: {pool_results['frequency']/target_freq*100:.1f}% of target")

    if pool_results['frequency'] >= target_freq:
        print("✅ ZERO-COPY POOL ACHIEVES TARGET PERFORMANCE!")
    elif pool_results['publish_time_ms'] <= 25:
        print("✅ ZERO-COPY POOL MEETS TIMING TARGET!")
    else:
        gap = pool_results['publish_time_ms'] - 25
        print(f"❌ Pool method {gap:.1f}ms above target per message")

if __name__ == "__main__":
    main()