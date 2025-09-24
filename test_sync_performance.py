#!/usr/bin/env python3
"""
Synchronous performance test for UltraPubSub.

Uses independent processes with spawn() to test 20MB @ 40Hz with 1 publisher and 6 subscribers.
Each subscriber runs independently and synchronously receives messages.
"""

import sys
import os
import time
import multiprocessing as mp
from multiprocessing import Process, Queue
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Use installed version, no additional paths needed

def publisher_process(message_size_mb, frequency_hz, duration_seconds, result_queue):
    """Publisher process that sends messages at specified frequency."""
    try:
        from ultrapubsub import SharedMemory

        # Calculate message size and interval
        message_size_bytes = message_size_mb * 1024 * 1024
        interval_seconds = 1.0 / frequency_hz

        # Create shared memory and publisher
        shm = SharedMemory("sync_perf_test")
        publisher = shm.create_publisher()

        # Generate test message
        test_message = b'X' * message_size_bytes

        logger.info(f"Publisher starting: {message_size_mb}MB messages at {frequency_hz}Hz for {duration_seconds}s")

        message_count = 0
        start_time = time.time()
        total_bytes_sent = 0

        # Publish at precise frequency
        next_time = start_time

        while time.time() - start_time < duration_seconds:
            # Wait for next scheduled time
            current_time = time.time()
            if current_time < next_time:
                time.sleep(next_time - current_time)

            # Publish message
            publisher.publish(test_message)

            message_count += 1
            total_bytes_sent += message_size_bytes

            # Schedule next message
            next_time = start_time + (message_count * interval_seconds)

        # Calculate results
        actual_duration = time.time() - start_time
        throughput_mbps = (total_bytes_sent / (1024 * 1024)) / actual_duration
        actual_frequency = message_count / actual_duration

        results = {
            'process_type': 'publisher',
            'messages_sent': message_count,
            'total_bytes_sent': total_bytes_sent,
            'duration_seconds': actual_duration,
            'throughput_mbps': throughput_mbps,
            'actual_frequency_hz': actual_frequency,
            'target_frequency_hz': frequency_hz
        }

        result_queue.put(results)
        logger.info(f"Publisher completed: {message_count} messages, {throughput_mbps:.2f}MB/s")

    except Exception as e:
        logger.error(f"Publisher process failed: {e}")
        result_queue.put({'process_type': 'publisher', 'error': str(e)})

def subscriber_process(subscriber_id, duration_seconds, result_queue):
    """Subscriber process that receives messages synchronously."""
    try:
        from ultrapubsub import SharedMemory

        # Attach to existing shared memory and create subscriber
        shm = SharedMemory.attach("sync_perf_test")
        subscriber = shm.create_subscriber()

        logger.info(f"Subscriber {subscriber_id} starting")

        message_count = 0
        start_time = time.time()

        # Receive messages for specified duration
        while time.time() - start_time < duration_seconds:
            try:
                # Synchronous receive with timeout
                message = subscriber.receive(timeout=0.1)  # 100ms timeout
                if message:
                    message_count += 1
            except Exception as e:
                # Continue on timeout or error
                pass
            # Small sleep to avoid busy waiting
            time.sleep(0.001)

        # Calculate results
        actual_duration = time.time() - start_time

        results = {
            'process_type': 'subscriber',
            'subscriber_id': subscriber_id,
            'messages_received': message_count,
            'duration_seconds': actual_duration,
            'messages_per_second': message_count / actual_duration if actual_duration > 0 else 0
        }

        result_queue.put(results)
        logger.info(f"Subscriber {subscriber_id} completed: {message_count} messages")

    except Exception as e:
        logger.error(f"Subscriber {subscriber_id} process failed: {e}")
        result_queue.put({
            'process_type': 'subscriber',
            'subscriber_id': subscriber_id,
            'error': str(e)
        })

def run_performance_test():
    """Run the complete performance test with independent processes."""
    logger.info("Starting synchronous UltraPubSub performance test")

    # Test configuration
    message_size_mb = 20  # 20MB
    frequency_hz = 40     # 40Hz
    duration_seconds = 10 # 10 seconds
    num_subscribers = 6   # 6 subscribers

    logger.info(f"Configuration: {message_size_mb}MB @ {frequency_hz}Hz, {num_subscribers} subscribers, {duration_seconds}s")

    # Use spawn method for process creation
    mp.set_start_method('spawn')

    # Create result queue
    result_queue = Queue()

    # Create and start publisher process
    publisher = Process(
        target=publisher_process,
        args=(message_size_mb, frequency_hz, duration_seconds, result_queue)
    )

    # Create and start subscriber processes
    subscribers = []
    for i in range(num_subscribers):
        subscriber = Process(
            target=subscriber_process,
            args=(i, duration_seconds, result_queue)
        )
        subscribers.append(subscriber)

    # Start all processes
    logger.info("Starting all processes...")
    publisher.start()

    # Small delay to ensure publisher starts first
    time.sleep(0.1)

    for subscriber in subscribers:
        subscriber.start()

    # Wait for all processes to complete
    logger.info("Waiting for processes to complete...")
    publisher.join()

    for subscriber in subscribers:
        subscriber.join()

    # Collect results
    logger.info("Collecting results...")
    results = []

    # Get all results from queue
    while not result_queue.empty():
        results.append(result_queue.get())

    # Analyze and display results
    analyze_results(results, frequency_hz, message_size_mb)

    return results

def analyze_results(results, target_frequency, message_size_mb):
    """Analyze and display test results."""
    print("\n" + "="*60)
    print("SYNCHRONOUS PERFORMANCE TEST RESULTS")
    print("="*60)

    publisher_results = None
    subscriber_results = []

    # Separate publisher and subscriber results
    for result in results:
        if result.get('process_type') == 'publisher':
            publisher_results = result
        elif result.get('process_type') == 'subscriber':
            subscriber_results.append(result)

    # Display publisher results
    if publisher_results:
        print(f"\nPUBLISHER RESULTS:")
        print(f"  Messages sent: {publisher_results['messages_sent']}")
        print(f"  Total bytes sent: {publisher_results['total_bytes_sent']:,}")
        print(f"  Duration: {publisher_results['duration_seconds']:.2f}s")
        print(f"  Throughput: {publisher_results['throughput_mbps']:.2f} MB/s")
        print(f"  Target frequency: {publisher_results['target_frequency_hz']} Hz")
        print(f"  Actual frequency: {publisher_results['actual_frequency_hz']:.2f} Hz")

        # Calculate target throughput
        target_throughput = (message_size_mb * target_frequency)
        print(f"  Target throughput: {target_throughput:.2f} MB/s")

        # Check if target was met
        throughput_efficiency = (publisher_results['throughput_mbps'] / target_throughput) * 100
        frequency_efficiency = (publisher_results['actual_frequency_hz'] / target_frequency) * 100

        print(f"  Throughput efficiency: {throughput_efficiency:.1f}%")
        print(f"  Frequency efficiency: {frequency_efficiency:.1f}%")

    # Display subscriber results
    if subscriber_results:
        print(f"\nSUBSCRIBER RESULTS:")
        total_received = 0
        min_received = float('inf')
        max_received = 0

        for sub_result in subscriber_results:
            sub_id = sub_result['subscriber_id']
            messages_received = sub_result['messages_received']
            messages_per_second = sub_result['messages_per_second']

            print(f"  Subscriber {sub_id}: {messages_received} messages ({messages_per_second:.1f} msg/s)")

            total_received += messages_received
            min_received = min(min_received, messages_received)
            max_received = max(max_received, messages_received)

        # Calculate subscriber metrics
        avg_received = total_received / len(subscriber_results)
        loss_rate = ((publisher_results['messages_sent'] - avg_received) / publisher_results['messages_sent']) * 100 if publisher_results else 0

        print(f"\n  Average received: {avg_received:.1f} messages")
        print(f"  Min received: {min_received} messages")
        print(f"  Max received: {max_received} messages")
        print(f"  Loss rate: {loss_rate:.1f}%")

        # Check if all subscribers received similar counts
        if max_received > 0:
            fairness = (min_received / max_received) * 100
            print(f"  Fairness: {fairness:.1f}%")

    # Overall assessment
    print(f"\nOVERALL ASSESSMENT:")
    if publisher_results:
        target_throughput = message_size_mb * target_frequency
        if publisher_results['throughput_mbps'] >= target_throughput * 0.9:  # 90% threshold
            print("  ✅ THROUGHPUT TARGET MET")
        else:
            print(f"  ❌ THROUGHPUT TARGET NOT MET ({publisher_results['throughput_mbps']:.2f} < {target_throughput:.2f} MB/s)")

        if publisher_results['actual_frequency_hz'] >= target_frequency * 0.9:
            print("  ✅ FREQUENCY TARGET MET")
        else:
            print(f"  ❌ FREQUENCY TARGET NOT MET ({publisher_results['actual_frequency_hz']:.2f} < {target_frequency} Hz)")

    print("="*60)

if __name__ == "__main__":
    try:
        results = run_performance_test()
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Test failed: {e}")
        sys.exit(1)