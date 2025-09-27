#!/usr/bin/env python3
"""
Simple benchmark that works with the current UltraPubSub implementation
"""
import time
import threading
import queue
import ultrapubsub

def test_basic_performance():
    """Test basic performance metrics"""
    print("🚀 UltraPubSub Performance Test")
    print("=" * 50)

    test_name = "/performance_test"
    message_sizes = [1024, 1024*1024, 10*1024*1024]  # 1KB, 1MB, 10MB

    try:
        for size in message_sizes:
            print(f"\n📊 Testing {size:,} byte messages:")

            # Create publisher and subscriber
            publisher = ultrapubsub.create_publisher(test_name + f"_{size}")
            subscriber = ultrapubsub.create_subscriber_with_id(test_name + f"_{size}", 0)

            # Create test data
            test_data = b'A' * size

            # Warm up
            for _ in range(3):
                publisher.broadcast(test_data)
                received = subscriber.receive()
                if len(received) != size:
                    print(f"❌ Message size mismatch: expected {size}, got {len(received)}")
                    break

            # Benchmark - send as many messages as possible in 3 seconds
            messages_sent = 0
            start_time = time.time()

            while time.time() - start_time < 3:
                publisher.broadcast(test_data)
                messages_sent += 1
                # Small delay to prevent overwhelming the system
                time.sleep(0.001)

            # Receive all messages
            messages_received = 0
            start_receive = time.time()

            while time.time() - start_receive < 5:  # 5 second timeout for receiving
                try:
                    msg = subscriber.receive()
                    if msg:
                        messages_received += 1
                        if len(msg) != size:
                            print(f"❌ Corrupted message: expected {size}, got {len(msg)}")
                            break
                except:
                    break

            duration = time.time() - start_time
            throughput_mb_s = (messages_received * size) / duration / (1024 * 1024)
            frequency = messages_sent / duration

            print(f"  Messages sent: {messages_sent}")
            print(f"  Messages received: {messages_received}")
            print(f"  Duration: {duration:.2f}s")
            print(f"  Throughput: {throughput_mb_s:.2f} MB/s")
            print(f"  Frequency: {frequency:.2f} Hz")
            print(f"  Success rate: {messages_received/messages_sent*100:.1f}%")

            # Get performance metrics if available
            try:
                metrics = publisher.get_performance_metrics()
                print(f"  Metrics: {metrics}")
            except:
                pass

            # Cleanup
            ultrapubsub.cleanup_shared_memory(test_name + f"_{size}")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

def test_6_subscriber_target():
    """Test the target requirement: 6 subscribers, 35MB messages"""
    print(f"\n🎯 Target Requirement Test")
    print("=" * 50)
    print("6 subscribers, 35MB messages, 10 seconds")

    test_name = "/target_test"
    message_size = 35 * 1024 * 1024  # 35MB
    test_duration = 10  # seconds

    try:
        # Create publisher
        publisher = ultrapubsub.create_publisher(test_name)

        # Start subscriber threads
        results_queue = queue.Queue()
        subscriber_threads = []

        def subscriber_worker(sub_id):
            try:
                subscriber = ultrapubsub.create_subscriber_with_id(test_name, sub_id)
                received_count = 0
                start_time = time.time()

                while time.time() - start_time < test_duration:
                    msg = subscriber.receive()
                    if msg:
                        received_count += 1
                        if len(msg) != message_size:
                            print(f"Subscriber {sub_id}: Message size error!")
                            break

                results_queue.put({
                    'subscriber_id': sub_id,
                    'messages_received': received_count,
                    'success': True
                })

            except Exception as e:
                results_queue.put({
                    'subscriber_id': sub_id,
                    'error': str(e),
                    'success': False
                })

        # Start 6 subscribers
        print("📡 Starting 6 subscribers...")
        for i in range(6):
            thread = threading.Thread(target=subscriber_worker, args=(i,))
            subscriber_threads.append(thread)
            thread.start()
            print(f"   Started subscriber {i}")

        # Wait for subscribers to be ready
        print("⏳ Waiting for subscribers to be ready...")
        time.sleep(1)

        # Start broadcasting
        test_data = b'A' * message_size
        messages_sent = 0
        start_time = time.time()

        print(f"📤 Starting {test_duration}s broadcast...")
        while time.time() - start_time < test_duration:
            publisher.broadcast(test_data)
            messages_sent += 1
            # Minimal delay for pacing
            time.sleep(0.001)

        actual_duration = time.time() - start_time

        # Wait for all subscribers to finish
        print("⏳ Waiting for subscribers to complete...")
        for thread in subscriber_threads:
            thread.join(timeout=5)

        # Collect results
        total_received = 0
        successful_subscribers = 0
        subscriber_results = []

        while not results_queue.empty():
            try:
                result = results_queue.get_nowait()
                subscriber_results.append(result)
                if result['success']:
                    total_received += result['messages_received']
                    successful_subscribers += 1
            except:
                break

        # Calculate metrics
        total_bytes = total_received * message_size
        throughput_gb_s = total_bytes / actual_duration / (1024**3)
        frequency = messages_sent / actual_duration

        print(f"\n📊 Results:")
        print(f"  Messages sent: {messages_sent}")
        print(f"  Messages received: {total_received}")
        print(f"  Successful subscribers: {successful_subscribers}/6")
        print(f"  Duration: {actual_duration:.2f}s")
        print(f"  Frequency: {frequency:.2f} Hz")
        print(f"  Throughput: {throughput_gb_s:.3f} GB/s")

        # Individual subscriber results
        print(f"\n📈 Subscriber Results:")
        for result in sorted(subscriber_results, key=lambda x: x['subscriber_id']):
            if result['success']:
                print(f"  Subscriber {result['subscriber_id']}: {result['messages_received']} messages")
            else:
                print(f"  Subscriber {result['subscriber_id']}: ERROR - {result['error']}")

        # Target comparison (0.8 GB/s at 40 Hz)
        target_throughput = 0.8  # GB/s
        target_frequency = 40   # Hz

        print(f"\n🎯 Target Analysis:")
        throughput_achieved = throughput_gb_s >= target_throughput
        frequency_achieved = frequency >= target_frequency

        print(f"  Throughput: {'✅' if throughput_achieved else '❌'} {throughput_gb_s:.3f} GB/s (target: {target_throughput} GB/s)")
        print(f"  Frequency: {'✅' if frequency_achieved else '❌'} {frequency:.1f} Hz (target: {target_frequency} Hz)")

        if throughput_achieved and frequency_achieved:
            print("  🎉 ALL TARGETS MET!")
        else:
            throughput_gap = target_throughput - throughput_gb_s if not throughput_achieved else 0
            frequency_gap = target_frequency - frequency if not frequency_achieved else 0
            if throughput_gap > 0:
                print(f"  Throughput gap: {throughput_gap:.3f} GB/s")
            if frequency_gap > 0:
                print(f"  Frequency gap: {frequency_gap:.1f} Hz")

        # Cleanup
        ultrapubsub.cleanup_shared_memory(test_name)

    except Exception as e:
        print(f"❌ Target test failed: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Run all performance tests"""
    test_basic_performance()
    test_6_subscriber_target()

if __name__ == "__main__":
    main()