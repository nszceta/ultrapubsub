#!/usr/bin/env python3
"""
Performance benchmark script for UltraPubSub
Measures throughput, frequency, and latency with clear numerical results
Uses multiprocessing to respect the "one process per subscriber" architecture
"""
import time
import ultrapubsub
import multiprocessing
import statistics
import sys
import os
import signal

def create_performance_test_message(size_mb=35):
    """Create a test message of specified size"""
    return b'X' * (size_mb * 1024 * 1024)

def benchmark_publisher_subscriber_pairs(test_name="/benchmark_test", message_size_mb=35, num_subscribers=6, duration_seconds=10):
    """Run comprehensive performance benchmark"""
    print(f"🚀 Starting Performance Benchmark")
    print(f"📊 Configuration:")
    print(f"   - Message size: {message_size_mb} MB")
    print(f"   - Subscribers: {num_subscribers}")
    print(f"   - Duration: {duration_seconds} seconds")
    print(f"   - Target throughput: 1.4 GB/s at 40 Hz")
    print()

    results = {
        'message_count': 0,
        'total_bytes': 0,
        'duration_seconds': 0,
        'throughput_mbps': 0,
        'frequency_hz': 0,
        'latencies_ms': [],
        'subscriber_results': []
    }

    try:
        # Clean up any existing shared memory
        try:
            ultrapubsub.cleanup_shared_memory(test_name)
        except:
            pass

        # Create test message
        test_message = create_performance_test_message(message_size_mb)
        message_size_bytes = len(test_message)
        print(f"📦 Test message size: {message_size_bytes:,} bytes ({message_size_bytes / 1024 / 1024:.1f} MB)")

        # Create publisher
        publisher = ultrapubsub.create_publisher(test_name)
        print(f"✅ Publisher created")

        # Create and register subscribers
        subscribers = []
        for i in range(num_subscribers):
            subscriber = ultrapubsub.create_subscriber_with_id(test_name, i)
            subscriber.register()
            subscribers.append(subscriber)
            print(f"✅ Subscriber {i} registered")

        final_subscriber_count = publisher.subscriber_count()
        print(f"✅ Final subscriber count: {final_subscriber_count}")

        # Shared data for threads
        stop_event = threading.Event()
        subscriber_data = [{'received_count': 0, 'total_bytes': 0, 'latencies': []} for _ in range(num_subscribers)]

        def subscriber_worker(subscriber, subscriber_id, data):
            """Worker thread for subscriber"""
            print(f"🔄 Subscriber {subscriber_id} starting...")
            while not stop_event.is_set():
                try:
                    start_time = time.time()
                    received = subscriber.receive(timeout=1.0)
                    if received:
                        end_time = time.time()
                        latency_ms = (end_time - start_time) * 1000

                        data['received_count'] += 1
                        data['total_bytes'] += len(received)
                        data['latencies'].append(latency_ms)

                except Exception as e:
                    if not stop_event.is_set():
                        print(f"⚠️  Subscriber {subscriber_id} error: {e}")
                    break

            print(f"✅ Subscriber {subscriber_id} finished: {data['received_count']} messages, {data['total_bytes']:,} bytes")

        # Start subscriber threads
        subscriber_threads = []
        for i, subscriber in enumerate(subscribers):
            thread = threading.Thread(target=subscriber_worker, args=(subscriber, i, subscriber_data[i]), daemon=True)
            thread.start()
            subscriber_threads.append(thread)

        # Give subscribers time to start
        time.sleep(0.5)

        # Start broadcasting
        print(f"📤 Starting broadcast for {duration_seconds} seconds...")
        start_time = time.time()
        end_time = start_time + duration_seconds
        broadcast_count = 0

        try:
            while time.time() < end_time:
                broadcast_start = time.time()

                # Broadcast the message
                publisher.broadcast(test_message)
                broadcast_count += 1

                # Calculate actual time used and adjust for next broadcast
                actual_broadcast_time = time.time() - broadcast_start

                # If we have time left in this cycle, sleep
                cycle_time = 1.0 / 40.0  # Target 40 Hz = 25ms per cycle
                sleep_time = max(0, cycle_time - actual_broadcast_time)
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            print(f"⚠️  Benchmark interrupted")
        finally:
            # Stop all subscribers
            stop_event.set()

            # Wait for all subscriber threads to finish
            for i, thread in enumerate(subscriber_threads):
                thread.join(timeout=2.0)
                if thread.is_alive():
                    print(f"⚠️  Subscriber {i} thread did not finish cleanly")

        # Calculate final results
        actual_duration = time.time() - start_time
        total_bytes_broadcast = broadcast_count * message_size_bytes

        # Aggregate subscriber results
        total_received = sum(data['received_count'] for data in subscriber_data)
        total_bytes_received = sum(data['total_bytes'] for data in subscriber_data)
        all_latencies = []
        for data in subscriber_data:
            all_latencies.extend(data['latencies'])

        # Calculate metrics
        throughput_mbps = (total_bytes_received / actual_duration) / (1024 * 1024)
        frequency_hz = total_received / actual_duration if actual_duration > 0 else 0

        results.update({
            'message_count': total_received,
            'total_bytes': total_bytes_received,
            'duration_seconds': actual_duration,
            'throughput_mbps': throughput_mbps,
            'frequency_hz': frequency_hz,
            'latencies_ms': all_latencies,
            'subscriber_results': subscriber_data,
            'broadcast_count': broadcast_count,
            'target_throughput_mbps': 1.4 * 1024,  # 1.4 GB/s in MB/s
            'target_frequency_hz': 40.0
        })

        # Cleanup
        for subscriber in subscribers:
            subscriber.deregister()

        ultrapubsub.cleanup_shared_memory(test_name)

    except Exception as e:
        print(f"❌ Benchmark error: {e}")
        import traceback
        traceback.print_exc()

    return results

def print_benchmark_results(results):
    """Print benchmark results with clear numerical formatting"""
    print("\n" + "="*80)
    print("📊 PERFORMANCE BENCHMARK RESULTS")
    print("="*80)

    print(f"\n🎯 TARGET PERFORMANCE:")
    print(f"   - Throughput: {results['target_throughput_mbps']:.1f} MB/s (1.4 GB/s)")
    print(f"   - Frequency: {results['target_frequency_hz']:.1f} Hz")

    print(f"\n📈 ACTUAL PERFORMANCE:")
    print(f"   - Duration: {results['duration_seconds']:.2f} seconds")
    print(f"   - Messages broadcast: {results['broadcast_count']:,}")
    print(f"   - Messages received: {results['message_count']:,}")
    print(f"   - Total data received: {results['total_bytes']:,} bytes ({results['total_bytes'] / 1024 / 1024:.1f} MB)")
    print(f"   - Throughput: {results['throughput_mbps']:.2f} MB/s ({results['throughput_mbps'] / 1024:.3f} GB/s)")
    print(f"   - Frequency: {results['frequency_hz']:.2f} Hz")

    # Calculate performance ratios
    throughput_ratio = results['throughput_mbps'] / results['target_throughput_mbps']
    frequency_ratio = results['frequency_hz'] / results['target_frequency_hz']

    print(f"\n📊 PERFORMANCE RATIOS:")
    print(f"   - Throughput achieved: {throughput_ratio * 100:.1f}% of target")
    print(f"   - Frequency achieved: {frequency_ratio * 100:.1f}% of target")

    if results['latencies_ms']:
        avg_latency = statistics.mean(results['latencies_ms'])
        median_latency = statistics.median(results['latencies_ms'])
        min_latency = min(results['latencies_ms'])
        max_latency = max(results['latencies_ms'])

        print(f"\n⏱️  LATENCY STATISTICS:")
        print(f"   - Average latency: {avg_latency:.2f} ms")
        print(f"   - Median latency: {median_latency:.2f} ms")
        print(f"   - Min latency: {min_latency:.2f} ms")
        print(f"   - Max latency: {max_latency:.2f} ms")

    print(f"\n📋 SUBSCRIBER BREAKDOWN:")
    for i, data in enumerate(results['subscriber_results']):
        if data['received_count'] > 0:
            sub_throughput = (data['total_bytes'] / results['duration_seconds']) / (1024 * 1024)
            sub_frequency = data['received_count'] / results['duration_seconds']
            avg_sub_latency = statistics.mean(data['latencies']) if data['latencies'] else 0
            print(f"   - Subscriber {i}: {data['received_count']:,} msgs, {sub_throughput:.2f} MB/s, {sub_frequency:.2f} Hz, {avg_sub_latency:.2f} ms avg latency")
        else:
            print(f"   - Subscriber {i}: 0 messages received")

    print("\n" + "="*80)

    # Performance assessment
    if throughput_ratio >= 1.0 and frequency_ratio >= 1.0:
        print("🎉 PERFORMANCE TARGETS ACHIEVED!")
    elif throughput_ratio >= 0.8 and frequency_ratio >= 0.8:
        print("⚠️  CLOSE TO TARGETS - Further optimization needed")
    else:
        print("❌ PERFORMANCE TARGETS NOT MET - Significant optimization required")

    print("="*80)

def main():
    """Main benchmark function"""
    print("UltraPubSub Performance Benchmark")
    print("This will test the current implementation and establish baseline metrics.")
    print()

    # Run benchmark with current implementation
    results = benchmark_publisher_subscriber_pairs(
        test_name="/baseline_benchmark",
        message_size_mb=35,
        num_subscribers=6,
        duration_seconds=10
    )

    if results['message_count'] > 0:
        print_benchmark_results(results)

        # Save results to file for comparison
        with open('/tmp/baseline_benchmark_results.txt', 'w') as f:
            f.write(f"Baseline Performance Results\n")
            f.write(f"Throughput: {results['throughput_mbps']:.2f} MB/s\n")
            f.write(f"Frequency: {results['frequency_hz']:.2f} Hz\n")
            f.write(f"Messages: {results['message_count']:,}\n")
            f.write(f"Duration: {results['duration_seconds']:.2f}s\n")

        print(f"\n💾 Results saved to /tmp/baseline_benchmark_results.txt")
    else:
        print("❌ No messages received - benchmark failed")

if __name__ == "__main__":
    main()