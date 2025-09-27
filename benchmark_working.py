#!/usr/bin/env python3
"""
Simple working benchmark
"""
import ultrapubsub
import multiprocessing as mp
import time

def subscriber_worker(test_name, subscriber_id, result_queue, duration):
    try:
        subscriber = ultrapubsub.create_subscriber_with_id(test_name, subscriber_id)
        print(f"Subscriber {subscriber_id} ready")

        messages_received = 0
        total_bytes = 0
        start_time = time.time()

        while time.time() - start_time < duration:
            try:
                msg = subscriber.receive()
                messages_received += 1
                total_bytes += len(msg)
            except:
                break

        result_queue.put({
            'subscriber_id': subscriber_id,
            'messages_received': messages_received,
            'total_bytes': total_bytes,
            'success': True
        })

    except Exception as e:
        result_queue.put({
            'subscriber_id': subscriber_id,
            'error': str(e),
            'success': False
        })

def benchmark_test(message_size, test_duration, num_subscribers):
    test_name = f"/benchmark_{message_size}_{num_subscribers}sub"

    print(f"\n📊 Benchmark: {message_size:,} bytes, {num_subscribers} subscribers, {test_duration}s")
    print("=" * 60)

    try:
        # Clean up
        try:
            ultrapubsub.cleanup_shared_memory(test_name)
        except:
            pass

        # Create publisher
        publisher = ultrapubsub.create_publisher(test_name)

        # Register subscribers
        subscriber_ids = []
        for i in range(num_subscribers):
            sub_id = publisher.register_subscriber()
            subscriber_ids.append(sub_id)

        print(f"✅ Registered {num_subscribers} subscribers: {subscriber_ids}")

        # Start subscriber processes
        result_queue = mp.Queue()
        processes = []

        for sub_id in subscriber_ids:
            process = mp.Process(
                target=subscriber_worker,
                args=(test_name, sub_id, result_queue, test_duration)
            )
            processes.append(process)
            process.start()

        # Give subscribers time to start
        time.sleep(1.0)

        # Create test data
        test_data = b'A' * message_size

        # Start broadcasting
        print(f"📤 Starting broadcast of {len(test_data):,} bytes...")
        messages_sent = 0
        start_time = time.time()

        while time.time() - start_time < test_duration:
            publisher.broadcast(test_data)
            messages_sent += 1
            # Small delay to prevent overwhelming
            time.sleep(0.001)

        actual_duration = time.time() - start_time

        # Wait for all processes
        for process in processes:
            process.join(timeout=5.0)
            if process.is_alive():
                process.terminate()

        # Collect results
        results = []
        while not result_queue.empty():
            try:
                results.append(result_queue.get_nowait())
            except:
                break

        # Calculate metrics
        total_received = sum(r.get('messages_received', 0) for r in results if r.get('success'))
        total_bytes = sum(r.get('total_bytes', 0) for r in results if r.get('success'))

        publish_throughput = (messages_sent * message_size) / actual_duration / (1024*1024)
        receive_throughput = total_bytes / actual_duration / (1024*1024)
        frequency = messages_sent / actual_duration

        print(f"\n📈 Results:")
        print(f"  Messages sent: {messages_sent}")
        print(f"  Messages received: {total_received}")
        print(f"  Duration: {actual_duration:.2f}s")
        print(f"  Publish throughput: {publish_throughput:.2f} MB/s")
        print(f"  Receive throughput: {receive_throughput:.2f} MB/s")
        print(f"  Frequency: {frequency:.2f} Hz")

        # Individual subscriber results
        for result in results:
            if result.get('success'):
                print(f"  Subscriber {result['subscriber_id']}: {result['messages_received']} msgs, {result['total_bytes']:,} bytes")
            else:
                print(f"  Subscriber {result['subscriber_id']}: ERROR - {result.get('error')}")

        # Cleanup
        ultrapubsub.cleanup_shared_memory(test_name)

        return {
            'publish_throughput': publish_throughput,
            'receive_throughput': receive_throughput,
            'frequency': frequency,
            'messages_sent': messages_sent,
            'messages_received': total_received
        }

    except Exception as e:
        print(f"❌ Benchmark failed: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    print("🚀 UltraPubSub Working Benchmark")
    print("=" * 50)

    # Test different configurations
    configs = [
        (1024, 5, 1),      # 1KB, 1 subscriber
        (1024*1024, 5, 1), # 1MB, 1 subscriber
        (1024*1024, 5, 3), # 1MB, 3 subscribers
        (1024*1024, 5, 6), # 1MB, 6 subscribers (target)
    ]

    for msg_size, duration, num_subs in configs:
        result = benchmark_test(msg_size, duration, num_subs)
        if result:
            # Check against targets (0.8 GB/s = 800 MB/s at 40 Hz)
            if num_subs == 6 and msg_size >= 1024*1024:
                print(f"\n🎯 Target Analysis:")
                print(f"  Target: 800 MB/s at 40 Hz with 6 subscribers")
                print(f"  Achieved: {result['receive_throughput']:.1f} MB/s at {result['frequency']:.1f} Hz")
                if result['receive_throughput'] >= 800 and result['frequency'] >= 40:
                    print("  🎉 TARGETS MET!")
                else:
                    throughput_gap = max(0, 800 - result['receive_throughput'])
                    frequency_gap = max(0, 40 - result['frequency'])
                    if throughput_gap > 0:
                        print(f"  Throughput gap: {throughput_gap:.1f} MB/s")
                    if frequency_gap > 0:
                        print(f"  Frequency gap: {frequency_gap:.1f} Hz")

    print("\n" + "=" * 50)
    print("✅ Benchmark completed")

if __name__ == "__main__":
    main()