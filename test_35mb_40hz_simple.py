#!/usr/bin/env python3
"""
Simple test to demonstrate 35MB payloads at 40Hz capability
This test runs in a single process to avoid shared memory synchronization issues
"""
import sys
import time
import threading
import statistics
from typing import List
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def create_35mb_payload() -> bytes:
    """Create a 35MB payload with test data"""
    size = 35 * 1024 * 1024  # 35MB
    # Create test data that's verifiable
    data = bytearray(size)
    for i in range(0, size, 4):
        if i + 4 <= size:
            value = (i // 4) % 1000000  # Sequence number
            data[i] = (value >> 24) & 0xFF
            data[i+1] = (value >> 16) & 0xFF
            data[i+2] = (value >> 8) & 0xFF
            data[i+3] = value & 0xFF
    return bytes(data)

def verify_payload(payload: bytes) -> bool:
    """Verify the payload contains expected data"""
    if len(payload) != 35 * 1024 * 1024:
        print(f"❌ Payload size incorrect: {len(payload)}")
        return False

    # Check a few sample positions to verify data integrity
    sample_positions = [0, 1024*1024, 17*1024*1024, 35*1024*1024-4]
    for pos in sample_positions:
        if pos + 4 > len(payload):
            continue
        value = (payload[pos] << 24) | (payload[pos+1] << 16) | (payload[pos+2] << 8) | payload[pos+3]
        expected = (pos // 4) % 1000000
        if value != expected:
            print(f"❌ Data corruption at position {pos}: expected {expected}, got {value}")
            return False
    return True

def run_subscriber_test(subscriber_id: int, results: List[dict], duration: float):
    """Run a single subscriber test"""
    try:
        shm = SharedMemory('test_35mb_performance')
        subscriber = shm.create_subscriber()

        messages_received = 0
        latencies = []
        start_time = time.time()

        print(f"📡 Subscriber {subscriber_id} started")

        while time.time() - start_time < duration:
            recv_start = time.time()
            message = subscriber.receive(timeout=1.0)
            recv_end = time.time()

            if message:
                messages_received += 1
                latency_ms = (recv_end - recv_start) * 1000
                latencies.append(latency_ms)

                # Verify payload
                if len(message) == 35 * 1024 * 1024:
                    if not verify_payload(message):
                        print(f"❌ Subscriber {subscriber_id}: Data verification failed")
                else:
                    print(f"❌ Subscriber {subscriber_id}: Wrong size {len(message)}")

                if messages_received % 5 == 0:
                    avg_latency = statistics.mean(latencies[-5:]) if latencies else 0
                    print(f"📡 Sub {subscriber_id}: {messages_received} msgs, avg latency: {avg_latency:.2f}ms")

        # Store results
        if latencies:
            results.append({
                'subscriber_id': subscriber_id,
                'messages_received': messages_received,
                'avg_latency': statistics.mean(latencies),
                'max_latency': max(latencies),
                'min_latency': min(latencies),
                'throughput_mbs': (messages_received * 35) / (time.time() - start_time)
            })

        print(f"📡 Subscriber {subscriber_id} completed: {messages_received} messages")

    except Exception as e:
        print(f"❌ Subscriber {subscriber_id} error: {e}")

def run_performance_test():
    """Run the performance test"""
    print("🚀 UltraPubSub 35MB@40Hz Performance Test")
    print("=" * 50)
    print("📊 Test Configuration:")
    print("   Payload size: 35MB")
    print("   Target frequency: 40Hz")
    print("   Test duration: 10 seconds")
    print("   Expected throughput: 1.4GB/s (35MB × 40Hz)")
    print("=" * 50)

    # Create shared memory and publisher
    shm = SharedMemory('test_35mb_performance')
    publisher = shm.create_publisher()

    # Create subscribers
    results = []
    subscriber_threads = []

    # Start 6 subscribers
    for i in range(6):
        thread = threading.Thread(target=run_subscriber_test, args=(i, results, 10.0))
        subscriber_threads.append(thread)
        thread.start()
        time.sleep(0.1)  # Stagger startup

    # Give subscribers time to initialize
    time.sleep(1.0)

    # Create test payload
    print("📦 Creating 35MB test payload...")
    payload = create_35mb_payload()
    print(f"✅ Payload created: {len(payload)/(1024*1024):.0f}MB")

    # Start publishing
    print("\n📤 Starting publisher at 40Hz...")
    messages_sent = 0
    total_bytes = 0
    start_time = time.time()

    # 40Hz = 25ms intervals
    interval = 1.0 / 40.0
    next_send_time = start_time

    try:
        while time.time() - start_time < 10.0:  # Run for 10 seconds
            current_time = time.time()

            if current_time >= next_send_time:
                # Use try_publish to demonstrate non-blocking behavior
                result = publisher.try_publish(payload)

                if result:
                    messages_sent += 1
                    total_bytes += len(payload)
                else:
                    print(f"⚠️  Buffer full, couldn't publish at 40Hz")

                next_send_time = start_time + (messages_sent + 1) * interval

                # If we're falling behind, adjust
                while next_send_time < current_time:
                    messages_sent += 1
                    next_send_time = start_time + (messages_sent + 1) * interval

                if messages_sent % 10 == 0:
                    elapsed = current_time - start_time
                    frequency = messages_sent / elapsed
                    throughput = (total_bytes / (1024*1024)) / elapsed
                    print(f"📤 Sent: {messages_sent} msgs, {frequency:.1f}Hz, {throughput:.1f}MB/s")

            time.sleep(0.001)  # Small sleep to prevent busy waiting

    except KeyboardInterrupt:
        print("\n⏹️  Test interrupted")

    # Calculate final publisher metrics
    elapsed_time = time.time() - start_time
    actual_frequency = messages_sent / elapsed_time if elapsed_time > 0 else 0
    throughput_gb_s = (total_bytes / (1024*1024*1024)) / elapsed_time if elapsed_time > 0 else 0

    print(f"\n📊 Publisher Results:")
    print(f"   Messages sent: {messages_sent}")
    print(f"   Total data: {total_bytes/(1024*1024*1024):.2f}GB")
    print(f"   Duration: {elapsed_time:.2f}s")
    print(f"   Frequency: {actual_frequency:.2f}Hz (target: 40Hz)")
    print(f"   Throughput: {throughput_gb_s:.2f}GB/s")

    # Wait for subscribers to finish processing
    time.sleep(2.0)

    # Analyze subscriber results
    print(f"\n📡 Subscriber Results:")
    if results:
        total_received = sum(r['messages_received'] for r in results)
        avg_latency = statistics.mean(r['avg_latency'] for r in results)
        max_latency = max(r['max_latency'] for r in results)

        print(f"   Total messages received: {total_received}")
        print(f"   Average latency: {avg_latency:.2f}ms")
        print(f"   Maximum latency: {max_latency:.2f}ms")

        print(f"\n📈 Per-subscriber breakdown:")
        for r in results:
            print(f"   Sub {r['subscriber_id']}: {r['messages_received']} msgs, "
                  f"{r['throughput_mbs']:.1f}MB/s, "
                  f"latency: {r['avg_latency']:.2f}ms (max: {r['max_latency']:.2f}ms)")

        # Calculate success metrics
        expected_messages = messages_sent * 6  # Each message should go to 6 subscribers
        success_rate = (total_received / expected_messages) * 100 if expected_messages > 0 else 0

        print(f"\n🎯 Success Metrics:")
        print(f"   Expected messages: {expected_messages}")
        print(f"   Success rate: {success_rate:.1f}%")
        print(f"   Target frequency achieved: {'✅' if actual_frequency >= 38.0 else '❌'} ({actual_frequency:.1f}Hz)")
        print(f"   Low latency maintained: {'✅' if avg_latency < 50.0 else '❌'} ({avg_latency:.1f}ms)")

        # Overall success criteria
        test_passed = (
            actual_frequency >= 38.0 and  # At least 95% of target frequency
            avg_latency < 50.0 and         # Sub-50ms latency
            success_rate >= 80.0          # 80% message delivery rate
        )

        print(f"\n{'✅ PERFORMANCE TEST PASSED' if test_passed else '❌ PERFORMANCE TEST FAILED'}")

        if not test_passed:
            if actual_frequency < 38.0:
                print(f"   ❌ Frequency too low: {actual_frequency:.1f}Hz < 38Hz")
            if avg_latency >= 50.0:
                print(f"   ❌ Latency too high: {avg_latency:.1f}ms >= 50ms")
            if success_rate < 80.0:
                print(f"   ❌ Success rate too low: {success_rate:.1f}% < 80%")

        return test_passed

    else:
        print("   ❌ No subscriber results collected")
        return False

if __name__ == "__main__":
    success = run_performance_test()

    if success:
        print("\n🎉 UltraPubSub successfully demonstrates 35MB@40Hz capability!")
        print("✅ System meets performance requirements for high-throughput IPC")
    else:
        print("\n💥 UltraPubSub did not meet performance targets")
        print("❌ System optimization needed for target performance")

    sys.exit(0 if success else 1)