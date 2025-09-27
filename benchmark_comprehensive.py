#!/usr/bin/env python3
"""
Comprehensive benchmark suite for UltraPubSub optimized implementation
"""
import time
import multiprocessing as mp
import threading
import queue
import ultrapubsub

def create_test_data(size_bytes):
    """Create test data of specified size"""
    return b'A' * size_bytes

def benchmark_message_sizes():
    """Benchmark throughput for different message sizes"""
    sizes = [
        (1024, "1KB"),
        (1024*1024, "1MB"),
        (10*1024*1024, "10MB"),
        (35*1024*1024, "35MB"),
    ]

    print("📊 Message Size Benchmark")
    print("=" * 60)

    for size, name in sizes:
        test_name = f"/benchmark_size_{size}"

        try:
            # Create publisher
            publisher = ultrapubsub.create_publisher(test_name)

            # Create subscriber
            subscriber = ultrapubsub.create_subscriber_with_id(test_name, 0)

            # Test data
            test_data = create_test_data(size)

            # Warm up
            for _ in range(3):
                publisher.broadcast(test_data)
                _ = subscriber.receive()

            # Benchmark
            messages = 0
            start_time = time.time()

            # Run for 5 seconds
            while time.time() - start_time < 5:
                publisher.broadcast(test_data)
                messages += 1
                # Small delay to prevent overwhelming
                time.sleep(0.001)

            duration = time.time() - start_time

            # Calculate metrics
            throughput_mb_s = (messages * size) / duration / (1024*1024)
            frequency = messages / duration

            print(f"{name:>8}: {throughput_mb_s:>8.2f} MB/s, {frequency:>6.2f} Hz, {messages:>4} msgs")

            # Cleanup
            ultrapubsub.cleanup_shared_memory(test_name)

        except Exception as e:
            print(f"{name:>8}: ERROR - {e}")

def benchmark_subscriber_scalability():
    """Benchmark performance with different subscriber counts"""
    subscriber_counts = [1, 2, 4, 6, 8]
    message_size = 1024*1024  # 1MB messages

    print("\n📊 Subscriber Scalability Benchmark")
    print("=" * 60)

    for num_subscribers in subscriber_counts:
        test_name = f"/benchmark_subscribers_{num_subscribers}"

        try:
            # Create publisher
            publisher = ultrapubsub.create_publisher(test_name)

            # Start subscriber threads
            results_queue = queue.Queue()
            subscriber_threads = []

            def subscriber_worker(sub_id):
                try:
                    subscriber = ultrapubsub.create_subscriber_with_id(test_name, sub_id)
                    received = 0
                    start_time = time.time()

                    while time.time() - start_time < 3:
                        msg = subscriber.receive()
                        if msg:
                            received += 1

                    results_queue.put(received)
                except Exception as e:
                    results_queue.put(f"ERROR: {e}")

            # Start subscribers
            for i in range(num_subscribers):
                thread = threading.Thread(target=subscriber_worker, args=(i,))
                subscriber_threads.append(thread)
                thread.start()

            # Wait for subscribers to be ready
            time.sleep(0.5)

            # Broadcast messages
            test_data = create_test_data(message_size)
            messages_sent = 0
            start_time = time.time()

            while time.time() - start_time < 3:
                publisher.broadcast(test_data)
                messages_sent += 1
                time.sleep(0.001)

            duration = time.time() - start_time

            # Wait for subscribers to finish
            for thread in subscriber_threads:
                thread.join(timeout=5)

            # Collect results
            total_received = 0
            for _ in range(num_subscribers):
                try:
                    result = results_queue.get(timeout=1)
                    if isinstance(result, int):
                        total_received += result
                except:
                    pass

            # Calculate metrics
            total_throughput_mb_s = (total_received * message_size) / duration / (1024*1024)
            per_subscriber = total_throughput_mb_s / num_subscribers if num_subscribers > 0 else 0

            print(f"{num_subscribers:>2} subs: {total_throughput_mb_s:>8.2f} MB/s total, {per_subscriber:>6.2f} MB/s/sub, {messages_sent:>4} sent, {total_received:>4} recv")

            # Cleanup
            ultrapubsub.cleanup_shared_memory(test_name)

        except Exception as e:
            print(f"{num_subscribers:>2} subs: ERROR - {e}")

def benchmark_latency():
    """Measure latency characteristics"""
    print("\n📊 Latency Benchmark")
    print("=" * 60)

    test_name = "/benchmark_latency"

    try:
        publisher = ultrapubsub.create_publisher(test_name)
        subscriber = ultrapubsub.create_subscriber_with_id(test_name, 0)

        # Small message for latency testing
        test_data = b"Hello World"

        latencies = []

        for _ in range(100):
            start_time = time.perf_counter()
            publisher.broadcast(test_data)
            _ = subscriber.receive()
            end_time = time.perf_counter()

            latencies.append((end_time - start_time) * 1000)  # Convert to ms

        # Calculate statistics
        latencies.sort()
        min_latency = latencies[0]
        max_latency = latencies[-1]
        avg_latency = sum(latencies) / len(latencies)
        p95_latency = latencies[int(len(latencies) * 0.95)]
        p99_latency = latencies[int(len(latencies) * 0.99)]

        print(f"Messages: {len(latencies)}")
        print(f"Min latency: {min_latency:.3f} ms")
        print(f"Max latency: {max_latency:.3f} ms")
        print(f"Avg latency: {avg_latency:.3f} ms")
        print(f"95th percentile: {p95_latency:.3f} ms")
        print(f"99th percentile: {p99_latency:.3f} ms")

        # Cleanup
        ultrapubsub.cleanup_shared_memory(test_name)

    except Exception as e:
        print(f"Latency benchmark failed: {e}")

def target_performance_test():
    """Test against project target requirements"""
    print("\n📊 Target Performance Test")
    print("=" * 60)
    print("Target: 0.8 GB/s throughput at 40 Hz with 6 subscribers")

    test_name = "/target_test"
    message_size = 35 * 1024 * 1024  # 35MB
    target_duration = 10  # seconds

    try:
        publisher = ultrapubsub.create_publisher(test_name)

        # Start 6 subscriber processes using threads for simplicity
        results_queue = queue.Queue()
        subscriber_threads = []

        def target_subscriber_worker(sub_id):
            try:
                subscriber = ultrapubsub.create_subscriber_with_id(test_name, sub_id)
                received = 0
                start_time = time.time()

                while time.time() - start_time < target_duration:
                    msg = subscriber.receive()
                    if msg:
                        received += 1

                results_queue.put(received)
            except Exception as e:
                results_queue.put(f"ERROR: {e}")

        # Start 6 subscribers
        for i in range(6):
            thread = threading.Thread(target=target_subscriber_worker, args=(i,))
            subscriber_threads.append(thread)
            thread.start()

        # Wait for subscribers
        time.sleep(1)

        # Start broadcasting
        test_data = create_test_data(message_size)
        messages_sent = 0
        start_time = time.time()

        while time.time() - start_time < target_duration:
            publisher.broadcast(test_data)
            messages_sent += 1
            time.sleep(0.001)  # Minimal delay

        actual_duration = time.time() - start_time

        # Wait for subscribers
        for thread in subscriber_threads:
            thread.join(timeout=5)

        # Collect results
        total_received = 0
        successful_subscribers = 0

        for _ in range(6):
            try:
                result = results_queue.get(timeout=1)
                if isinstance(result, int):
                    total_received += result
                    successful_subscribers += 1
            except:
                pass

        # Calculate metrics
        total_bytes = total_received * message_size
        throughput_gb_s = total_bytes / actual_duration / (1024**3)
        frequency = messages_sent / actual_duration

        print(f"\nResults:")
        print(f"  Messages sent: {messages_sent}")
        print(f"  Messages received: {total_received}")
        print(f"  Successful subscribers: {successful_subscribers}/6")
        print(f"  Duration: {actual_duration:.2f}s")
        print(f"  Frequency: {frequency:.2f} Hz")
        print(f"  Throughput: {throughput_gb_s:.3f} GB/s")

        # Target comparison
        target_throughput = 0.8  # GB/s
        target_freq = 40  # Hz

        print(f"\nTarget Comparison:")
        throughput_achieved = throughput_gb_s >= target_throughput
        frequency_achieved = frequency >= target_freq

        print(f"  Throughput: {'✅' if throughput_achieved else '❌'} {throughput_gb_s:.3f} GB/s (target: {target_throughput} GB/s)")
        print(f"  Frequency: {'✅' if frequency_achieved else '❌'} {frequency:.1f} Hz (target: {target_freq} Hz)")

        if throughput_achieved and frequency_achieved:
            print("  🎉 ALL TARGETS MET!")
        else:
            print("  ⚠️  Targets not fully met")

        # Cleanup
        ultrapubsub.cleanup_shared_memory(test_name)

    except Exception as e:
        print(f"Target test failed: {e}")

def main():
    """Run all benchmarks"""
    print("🚀 UltraPubSub Comprehensive Benchmark Suite")
    print("=" * 70)

    benchmark_message_sizes()
    benchmark_subscriber_scalability()
    benchmark_latency()
    target_performance_test()

    print("\n" + "=" * 70)
    print("✅ Benchmark suite completed")

if __name__ == "__main__":
    main()