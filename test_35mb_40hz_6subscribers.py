#!/usr/bin/env python3
"""
Test 35MB payload at 40Hz from 1 publisher to 6 subscribers
This demonstrates the core performance requirement of UltraPubSub
"""
import sys
import time
import multiprocessing
import threading
import statistics
from typing import List, Dict, Any
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def create_35mb_payload() -> bytes:
    """Create a 35MB payload with test data"""
    size = 35 * 1024 * 1024  # 35MB
    # Create test data that's compressible but verifiable
    data = bytearray(size)
    for i in range(0, size, 4):
        # Write sequence numbers in 4-byte chunks
        if i + 4 <= size:
            value = (i // 4) % 1000000  # Sequence number
            data[i] = (value >> 24) & 0xFF
            data[i+1] = (value >> 16) & 0xFF
            data[i+2] = (value >> 8) & 0xFF
            data[i+3] = value & 0xFF
    return bytes(data)

def verify_payload(payload: bytes, expected_sequence: int) -> bool:
    """Verify the payload contains the expected sequence numbers"""
    if len(payload) != 35 * 1024 * 1024:
        print(f"❌ Payload size incorrect: {len(payload)} bytes")
        return False

    # Check a few sample positions
    sample_positions = [0, 1024, 1024*1024, 20*1024*1024, 35*1024*1024-4]
    for pos in sample_positions:
        if pos + 4 > len(payload):
            continue
        value = (payload[pos] << 24) | (payload[pos+1] << 16) | (payload[pos+2] << 8) | payload[pos+3]
        expected = (pos // 4) % 1000000
        if value != expected:
            print(f"❌ Data corruption at position {pos}: expected {expected}, got {value}")
            return False
    return True

def subscriber_worker(subscriber_id: int, results: List[Dict[str, Any]], stop_event: threading.Event):
    """Worker function for subscriber process"""
    try:
        shm = SharedMemory.attach('test_35mb_40hz')
        subscriber = shm.create_subscriber()

        messages_received = 0
        total_bytes = 0
        latencies = []
        sequence_errors = 0

        print(f"📡 Subscriber {subscriber_id} started")

        while not stop_event.is_set():
            start_time = time.time()
            message = subscriber.receive(timeout=0.1)
            end_time = time.time()

            if message:
                messages_received += 1
                total_bytes += len(message)

                # Verify payload size
                if len(message) == 35 * 1024 * 1024:
                    latency_ms = (end_time - start_time) * 1000
                    latencies.append(latency_ms)

                    # Verify every 10th message to avoid excessive CPU usage
                    if messages_received % 10 == 0:
                        if not verify_payload(message, messages_received):
                            sequence_errors += 1
                else:
                    print(f"❌ Subscriber {subscriber_id}: Invalid payload size {len(message)}")

                if messages_received % 10 == 0:
                    avg_latency = statistics.mean(latencies[-10:]) if latencies else 0
                    print(f"📡 Subscriber {subscriber_id}: {messages_received} msgs, "
                          f"{total_bytes//(1024*1024)}MB, avg latency: {avg_latency:.2f}ms")

        # Store results
        results.append({
            'subscriber_id': subscriber_id,
            'messages_received': messages_received,
            'total_bytes': total_bytes,
            'avg_latency': statistics.mean(latencies) if latencies else 0,
            'max_latency': max(latencies) if latencies else 0,
            'min_latency': min(latencies) if latencies else 0,
            'sequence_errors': sequence_errors
        })

        print(f"📡 Subscriber {subscriber_id} finished: {messages_received} messages, "
              f"{total_bytes//(1024*1024)}MB")

    except Exception as e:
        print(f"❌ Subscriber {subscriber_id} error: {e}")
        import traceback
        traceback.print_exc()

def publisher_worker(duration: float, stop_event: threading.Event):
    """Worker function for publisher process"""
    try:
        shm = SharedMemory('test_35mb_40hz')
        publisher = shm.create_publisher()

        payload = create_35mb_payload()
        messages_sent = 0
        total_bytes = 0

        print(f"📤 Publisher started with 35MB payload")

        # 40Hz = 25ms per message
        interval = 1.0 / 40.0  # 25ms in seconds
        start_time = time.time()
        next_send_time = start_time

        while time.time() - start_time < duration and not stop_event.is_set():
            current_time = time.time()

            if current_time >= next_send_time:
                # Send the message
                publisher.publish(payload)
                messages_sent += 1
                total_bytes += len(payload)

                # Calculate next send time
                next_send_time = start_time + (messages_sent * interval)

                # If we're falling behind, skip messages to maintain frequency
                while next_send_time < current_time and not stop_event.is_set():
                    messages_sent += 1
                    next_send_time = start_time + (messages_sent * interval)
                    print(f"⚠️  Skipping message {messages_sent} to maintain 40Hz")

                if messages_sent % 10 == 0:
                    elapsed = current_time - start_time
                    actual_freq = messages_sent / elapsed if elapsed > 0 else 0
                    throughput_mb = (total_bytes / (1024*1024)) / elapsed if elapsed > 0 else 0
                    print(f"📤 Publisher: {messages_sent} msgs sent, "
                          f"{actual_freq:.1f}Hz, {throughput_mb:.1f}MB/s")

            # Small sleep to prevent busy waiting
            time.sleep(0.001)

        elapsed_time = time.time() - start_time
        actual_frequency = messages_sent / elapsed_time if elapsed_time > 0 else 0
        throughput_gb = (total_bytes / (1024*1024*1024)) / elapsed_time if elapsed_time > 0 else 0

        print(f"📤 Publisher finished:")
        print(f"   Messages sent: {messages_sent}")
        print(f"   Total data: {total_bytes/(1024*1024*1024):.2f}GB")
        print(f"   Duration: {elapsed_time:.2f}s")
        print(f"   Frequency: {actual_frequency:.2f}Hz (target: 40Hz)")
        print(f"   Throughput: {throughput_gb:.2f}GB/s")

        return {
            'messages_sent': messages_sent,
            'total_bytes': total_bytes,
            'duration': elapsed_time,
            'actual_frequency': actual_frequency,
            'throughput_gb_s': throughput_gb
        }

    except Exception as e:
        print(f"❌ Publisher error: {e}")
        import traceback
        traceback.print_exc()
        return None

def run_benchmark(duration: float = 10.0):
    """Run the full benchmark test"""
    print("🚀 Starting UltraPubSub 35MB@40Hz Benchmark")
    print("=" * 60)
    print(f"📊 Test configuration:")
    print(f"   Payload size: 35MB")
    print(f"   Target frequency: 40Hz")
    print(f"   Subscribers: 6")
    print(f"   Duration: {duration}s")
    print(f"   Total expected data: {35 * 40 * 6 * duration / (1024*1024*1024):.1f}GB")
    print("=" * 60)

    # Clean up any existing shared memory
    try:
        import shutil
        shm = SharedMemory('test_35mb_40hz')
        del shm
    except:
        pass

    # Create shared memory first
    shm = SharedMemory('test_35mb_40hz')
    print("📋 Shared memory created")

    stop_event = threading.Event()
    results = []

    # Start subscriber threads
    subscriber_threads = []
    for i in range(6):
        thread = threading.Thread(target=subscriber_worker, args=(i, results, stop_event))
        subscriber_threads.append(thread)
        thread.start()
        time.sleep(0.1)  # Stagger startup

    # Give subscribers time to initialize
    time.sleep(1.0)

    # Start publisher
    publisher_thread = threading.Thread(target=publisher_worker, args=(duration, stop_event))
    publisher_thread.start()

    # Wait for publisher to finish
    publisher_thread.join()

    # Signal subscribers to stop
    stop_event.set()

    # Wait for subscribers to finish
    for thread in subscriber_threads:
        thread.join()

    # Analyze results
    print("\n" + "=" * 60)
    print("📊 BENCHMARK RESULTS")
    print("=" * 60)

    if results:
        total_messages = sum(r['messages_received'] for r in results)
        total_bytes = sum(r['total_bytes'] for r in results)
        avg_latency = statistics.mean(r['avg_latency'] for r in results)
        max_latency = max(r['max_latency'] for r in results)
        total_errors = sum(r['sequence_errors'] for r in results)

        print(f"📈 Aggregate Results:")
        print(f"   Total messages received: {total_messages}")
        print(f"   Total data received: {total_bytes/(1024*1024*1024):.2f}GB")
        print(f"   Average latency: {avg_latency:.2f}ms")
        print(f"   Max latency: {max_latency:.2f}ms")
        print(f"   Sequence errors: {total_errors}")

        print(f"\n📡 Per-Subscriber Results:")
        for r in results:
            print(f"   Subscriber {r['subscriber_id']}: {r['messages_received']} msgs, "
                  f"{r['total_bytes']/(1024*1024):.1f}MB, "
                  f"avg latency: {r['avg_latency']:.2f}ms")

        # Calculate success metrics
        expected_messages = 40 * duration * 6  # 40Hz * duration * 6 subscribers
        success_rate = (total_messages / expected_messages) * 100 if expected_messages > 0 else 0

        print(f"\n🎯 Success Metrics:")
        print(f"   Expected messages: {expected_messages}")
        print(f"   Success rate: {success_rate:.1f}%")
        print(f"   Error rate: {(total_errors/total_messages*100) if total_messages > 0 else 0:.3f}%")

        # Determine if test passed
        test_passed = success_rate >= 95.0 and total_errors == 0 and avg_latency < 100.0

        print(f"\n{'✅ TEST PASSED' if test_passed else '❌ TEST FAILED'}")
        if not test_passed:
            if success_rate < 95.0:
                print(f"   ❌ Success rate too low: {success_rate:.1f}% < 95%")
            if total_errors > 0:
                print(f"   ❌ Data errors detected: {total_errors}")
            if avg_latency >= 100.0:
                print(f"   ❌ Latency too high: {avg_latency:.2f}ms >= 100ms")

        return test_passed
    else:
        print("❌ No results collected")
        return False

if __name__ == "__main__":
    # Run benchmark for 15 seconds (enough to get meaningful metrics)
    success = run_benchmark(duration=15.0)

    if success:
        print("\n🎉 UltraPubSub 35MB@40Hz benchmark PASSED!")
        print("✅ System meets performance requirements")
    else:
        print("\n💥 UltraPubSub 35MB@40Hz benchmark FAILED!")
        print("❌ System does not meet performance requirements")

    sys.exit(0 if success else 1)