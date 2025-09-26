#!/usr/bin/env python3
"""
Simple performance test to verify 1.4 GB/s throughput target
"""
import sys
import time
import os
sys.path.insert(0, 'python')

import ultrapubsub
import multiprocessing as mp

def test_throughput():
    """Test throughput with large messages"""
    test_name = "/throughput_test"

    print("🚀 UltraPubSub Performance Test")
    print("=" * 50)

    # Test with different message sizes
    test_sizes = [
        (1 * 1024 * 1024, "1MB"),
        (10 * 1024 * 1024, "10MB"),
        (35 * 1024 * 1024, "35MB"),
    ]

    for size, name in test_sizes:
        print(f"\n📊 Testing {name} messages...")

        # Create test data
        test_data = b'A' * size

        # Create publisher
        publisher = ultrapubsub.Publisher(test_name + name)

        # Single subscriber test
        result_queue = mp.Queue()

        def subscriber_process():
            try:
                subscriber = ultrapubsub.Subscriber(test_name + name)
                start_time = time.time()
                messages_received = 0

                while time.time() - start_time < 5:  # Test for 5 seconds
                    msg = subscriber.receive(timeout=1)
                    if msg:
                        messages_received += 1

                result_queue.put({
                    'size': size,
                    'messages_received': messages_received,
                    'duration': time.time() - start_time
                })

            except Exception as e:
                result_queue.put({'error': str(e)})

        # Start subscriber
        sub_proc = mp.Process(target=subscriber_process)
        sub_proc.start()

        # Give subscriber time to start
        time.sleep(1)

        # Send messages for 5 seconds
        start_time = time.time()
        messages_sent = 0

        while time.time() - start_time < 5:
            publisher.broadcast(test_data)
            messages_sent += 1
            time.sleep(0.01)  # Small delay to prevent overwhelming

        duration = time.time() - start_time

        # Wait for subscriber
        sub_proc.join(timeout=10)

        # Get results
        try:
            result = result_queue.get(timeout=5)
            if 'error' in result:
                print(f"   ❌ Error: {result['error']}")
            else:
                throughput = (result['messages_received'] * size) / result['duration'] / (1024 * 1024 * 1024)  # GB/s
                print(f"   Messages sent: {messages_sent}")
                print(f"   Messages received: {result['messages_received']}")
                print(f"   Duration: {result['duration']:.2f}s")
                print(f"   Throughput: {throughput:.2f} GB/s")

                # Check if we meet the target
                target = 1.4  # GB/s
                if throughput >= target:
                    print(f"   ✅ TARGET MET ({throughput:.2f} >= {target:.2f} GB/s)")
                else:
                    print(f"   ❌ BELOW TARGET ({throughput:.2f} < {target:.2f} GB/s)")
        except:
            print("   ❌ No results from subscriber")

        # Cleanup
        try:
            publisher.cleanup()
        except:
            pass

        # Clean shared memory
        try:
            ultrapubsub.cleanup_shared_memory(test_name + name)
        except:
            pass

if __name__ == "__main__":
    test_throughput()