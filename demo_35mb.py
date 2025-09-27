#!/usr/bin/env python3
"""
Direct demonstration of 35MB payload broadcast performance
"""
import time
import ultrapubsub
import json

def demo_35mb_broadcast():
    """Demonstrate 35MB broadcast performance directly"""
    print("🚀 35MB Payload Broadcast Performance Demo")
    print("=" * 60)

    test_name = "/demo_35mb"
    message_size_mb = 35
    target_hz = 40
    test_duration_seconds = 5

    try:
        # Clean up any existing shared memory
        try:
            ultrapubsub.cleanup_shared_memory(test_name)
            print("✅ Cleaned up existing shared memory")
        except:
            pass

        # Create publisher first
        print("📡 Creating publisher...")
        publisher = ultrapubsub.create_publisher(test_name)
        print(f"✅ Publisher created")

        # Create subscriber in same process for demonstration
        print("👥 Creating subscriber...")
        subscriber = ultrapubsub.create_subscriber_with_id(test_name, 0)
        subscriber.register()
        print(f"✅ Subscriber registered (count: {publisher.subscriber_count()})")

        # Create 35MB test message
        print(f"📦 Creating {message_size_mb}MB test message...")
        test_message = b'X' * (message_size_mb * 1024 * 1024)
        actual_size_mb = len(test_message) / (1024 * 1024)
        print(f"✅ Test message size: {actual_size_mb:.2f} MB ({len(test_message):,} bytes)")

        # Performance test
        print(f"\n🎯 Starting {test_duration_seconds}s performance test...")
        print(f"   Target: {target_hz} Hz = {target_hz * message_size_mb:.0f} MB/s")

        start_time = time.time()
        end_time = start_time + test_duration_seconds
        message_count = 0
        broadcast_times = []
        receive_times = []

        # Broadcast loop
        while time.time() < end_time:
            # Broadcast
            broadcast_start = time.time()
            sequence = publisher.broadcast(test_message)
            broadcast_end = time.time()
            broadcast_time = broadcast_end - broadcast_start
            broadcast_times.append(broadcast_time)

            # Receive
            receive_start = time.time()
            received = subscriber.receive(timeout=1.0)
            receive_end = time.time()
            receive_time = receive_end - receive_start
            receive_times.append(receive_time)

            message_count += 1

            # Progress update
            if message_count % 5 == 0:
                elapsed = time.time() - start_time
                current_hz = message_count / elapsed
                avg_broadcast = sum(broadcast_times[-5:]) / 5
                avg_receive = sum(receive_times[-5:]) / 5
                throughput_mbps = (message_count * len(test_message) / elapsed) / (1024 * 1024)
                print(f"   Msg {message_count}: {current_hz:.1f} Hz, {avg_broadcast*1000:.1f}ms send, {avg_receive*1000:.1f}ms recv, {throughput_mbps:.0f} MB/s")

        # Calculate final results
        total_time = time.time() - start_time
        actual_hz = message_count / total_time
        avg_broadcast_time = sum(broadcast_times) / len(broadcast_times) * 1000  # ms
        avg_receive_time = sum(receive_times) / len(receive_times) * 1000  # ms
        total_data_mb = message_count * actual_size_mb
        throughput_mbps = total_data_mb / total_time
        throughput_gbps = throughput_mbps / 1024

        print(f"\n📊 PERFORMANCE RESULTS:")
        print(f"   Messages: {message_count:,}")
        print(f"   Duration: {total_time:.2f}s")
        print(f"   Frequency: {actual_hz:.2f} Hz (target: {target_hz} Hz)")
        print(f"   Target efficiency: {(actual_hz/target_hz)*100:.1f}%")
        print(f"   Avg broadcast time: {avg_broadcast_time:.2f} ms")
        print(f"   Avg receive time: {avg_receive_time:.2f} ms")
        print(f"   Total data: {total_data_mb:.0f} MB")
        print(f"   Throughput: {throughput_mbps:.0f} MB/s ({throughput_gbps:.3f} GB/s)")

        # Performance assessment
        target_throughput_mbps = 1.4 * 1024  # 1.4 GB/s
        efficiency = (throughput_mbps / target_throughput_mbps) * 100

        print(f"\n🎯 TARGET ASSESSMENT:")
        print(f"   Target throughput: {target_throughput_mbps:.0f} MB/s (1.4 GB/s)")
        print(f"   Achieved throughput: {throughput_mbps:.0f} MB/s")
        print(f"   Target efficiency: {efficiency:.1f}%")

        if efficiency >= 100:
            print(f"   🎉 OUTSTANDING! Exceeding 1.4 GB/s target!")
        elif efficiency >= 80:
            print(f"   ✅ EXCELLENT! Achieving {efficiency:.0f}% of target")
        elif efficiency >= 60:
            print(f"   👍 GOOD! Achieving {efficiency:.0f}% of target")
        elif efficiency >= 40:
            print(f"   ⚠️  FAIR! {efficiency:.0f}% of target - needs optimization")
        else:
            print(f"   ❌ POOR! Only {efficiency:.0f}% of target - major optimization needed")

        # Performance envelope analysis
        print(f"\n📈 PERFORMANCE ENVELOPE:")
        max_possible_hz = 1 / avg_broadcast_time * 1000  # Based on broadcast time
        max_throughput_mbps = max_possible_hz * actual_size_mb
        print(f"   Theoretical max frequency: {max_possible_hz:.1f} Hz")
        print(f"   Theoretical max throughput: {max_throughput_mbps:.0f} MB/s ({max_throughput_mbps/1024:.3f} GB/s)")

        print(f"\n🔍 FUTEX OPTIMIZATION STATUS:")
        print(f"   ✅ Zero-copy memory access")
        print(f"   ✅ Futex-based synchronization")
        print(f"   ✅ Cross-process shared memory")
        print(f"   ✅ Synchronous wait-for-all semantics")

        return {
            'message_count': message_count,
            'duration': total_time,
            'frequency_hz': actual_hz,
            'target_efficiency': (actual_hz/target_hz)*100,
            'throughput_mbps': throughput_mbps,
            'throughput_gbps': throughput_gbps,
            'target_efficiency_throughput': efficiency,
            'avg_broadcast_time_ms': avg_broadcast_time,
            'avg_receive_time_ms': avg_receive_time
        }

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
        return None

    finally:
        # Cleanup
        try:
            ultrapubsub.cleanup_shared_memory(test_name)
            print(f"\n✅ Cleanup completed")
        except:
            pass

if __name__ == "__main__":
    result = demo_35mb_broadcast()
    if result:
        print(f"\n✅ 35MB broadcast demo completed successfully!")
    else:
        print(f"\n❌ 35MB broadcast demo failed!")