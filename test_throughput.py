#!/usr/bin/env python3
"""
Direct throughput test to measure UltraPubSub performance.
This test focuses on measuring publish throughput to demonstrate performance capabilities.
"""

import sys
import time
import multiprocessing as mp

# Add the path to the UltraPubSub module
sys.path.append('/home/adam/Documents/src/ultrapubsub/python')
sys.path.append('/home/adam/Documents/src/ultrapubsub')

def throughput_test(message_size_mb, duration_seconds):
    """Test throughput by publishing messages as fast as possible."""
    try:
        from ultrapubsub import SharedMemory

        print(f"Starting throughput test:")
        print(f"  Message size: {message_size_mb}MB")
        print(f"  Duration: {duration_seconds}s")
        print(f"  Target: 800MB/s")

        # Create shared memory and publisher
        shm = SharedMemory("throughput_test")
        publisher = shm.create_publisher()

        # Generate test message
        message_size_bytes = int(message_size_mb * 1024 * 1024)
        test_message = b'X' * message_size_bytes

        print(f"  Actual message size: {message_size_bytes} bytes")

        # Start timing
        start_time = time.time()
        message_count = 0
        total_bytes = 0

        print("  Publishing messages...")
        last_report = start_time

        # Publish messages for the specified duration
        while time.time() - start_time < duration_seconds:
            publisher.publish(test_message)
            message_count += 1
            total_bytes += message_size_bytes

            # Report progress every second
            current_time = time.time()
            if current_time - last_report >= 1.0:
                elapsed = current_time - start_time
                throughput_mbps = (total_bytes / (1024 * 1024)) / elapsed
                print(f"    {elapsed:.1f}s: {message_count} messages, {throughput_mbps:.2f}MB/s")
                last_report = current_time

        # Calculate final results
        end_time = time.time()
        actual_duration = end_time - start_time
        throughput_mbps = (total_bytes / (1024 * 1024)) / actual_duration
        messages_per_second = message_count / actual_duration

        print(f"\nResults:")
        print(f"  Duration: {actual_duration:.2f}s")
        print(f"  Messages sent: {message_count}")
        print(f"  Total bytes: {total_bytes:,}")
        print(f"  Throughput: {throughput_mbps:.2f}MB/s")
        print(f"  Messages/sec: {messages_per_second:.1f}")
        print(f"  Target met: {'✓ YES' if throughput_mbps >= 800 else '✗ NO'}")

        return throughput_mbps, message_count

    except Exception as e:
        print(f"✗ Test failed: {e}")
        return 0.0, 0

def main():
    print("UltraPubSub Throughput Performance Test")
    print("=" * 50)

    # Test with different message sizes
    test_configs = [
        (1, 5),   # 1MB messages for 5 seconds
        (10, 5),  # 10MB messages for 5 seconds
        (20, 5),  # 20MB messages for 5 seconds
    ]

    results = []

    for msg_size_mb, duration in test_configs:
        print(f"\nTest {len(results) + 1}: {msg_size_mb}MB messages")
        throughput, messages = throughput_test(msg_size_mb, duration)
        results.append((msg_size_mb, throughput, messages))

    # Summary
    print(f"\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)

    max_throughput = 0
    best_config = None

    for msg_size, throughput, messages in results:
        print(f"  {msg_size}MB messages: {throughput:.2f}MB/s ({messages} messages)")
        if throughput > max_throughput:
            max_throughput = throughput
            best_config = msg_size

    print(f"\nBest performance: {max_throughput:.2f}MB/s with {best_config}MB messages")
    print(f"Target requirement (800MB/s): {'✓ MET' if max_throughput >= 800 else '✗ NOT MET'}")

    if max_throughput < 800:
        shortfall = 800 - max_throughput
        print(f"  Shortfall: {shortfall:.2f}MB/s ({shortfall/800*100:.1f}% below target)")

if __name__ == "__main__":
    main()