#!/usr/bin/env python3
"""
Test subscriber polling efficiency and identify bottlenecks.
"""

import sys
import os
import time
import multiprocessing as mp
from multiprocessing import Process, Queue
import logging
import psutil

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_subscriber_cpu_usage():
    """Test subscriber CPU usage during polling."""
    try:
        from ultrapubsub import SharedMemory

        logger.info("=== Subscriber CPU Usage Test ===")

        # Create subscriber
        shm = SharedMemory("subscriber_cpu_test")
        subscriber = shm.create_subscriber()

        # Monitor CPU usage
        process = psutil.Process()
        start_time = time.time()
        start_cpu = process.cpu_times()

        # Poll for 10 seconds (no messages expected)
        duration = 10
        poll_count = 0

        while time.time() - start_time < duration:
            try:
                # Use very short timeout to test polling efficiency
                message = subscriber.receive(timeout=0.001)  # 1ms timeout
                if message:
                    logger.info(f"Received unexpected message: {len(message)} bytes")
                poll_count += 1
            except Exception as e:
                # On exception, just continue polling
                poll_count += 1

        end_time = time.time()
        end_cpu = process.cpu_times()

        # Calculate CPU usage
        cpu_time = end_cpu.user + end_cpu.system - (start_cpu.user + start_cpu.system)
        cpu_percent = (cpu_time / (end_time - start_time)) * 100

        logger.info(f"Polling completed:")
        logger.info(f"  Duration: {end_time - start_time:.1f}s")
        logger.info(f"  Poll count: {poll_count}")
        logger.info(f"  Poll rate: {poll_count / (end_time - start_time):.0f} polls/sec")
        logger.info(f"  CPU usage: {cpu_percent:.1f}%")
        logger.info(f"  Time per poll: {(end_time - start_time) * 1000000 / poll_count:.1f}μs")

        # Analysis
        if cpu_percent > 50:
            logger.warning("⚠️  High CPU usage indicates busy-wait polling")
            logger.warning("   Consider using longer timeouts or event-based polling")
        elif cpu_percent < 5:
            logger.info("✅ CPU usage is good")
        else:
            logger.info("ℹ️  CPU usage is moderate")

        return cpu_percent, poll_count

    except Exception as e:
        logger.error(f"Subscriber CPU test failed: {e}")
        return 0, 0

def test_timeout_behavior():
    """Test different timeout values and their effect on CPU usage."""
    try:
        from ultrapubsub import SharedMemory

        logger.info("=== Timeout Behavior Test ===")

        timeouts = [0.001, 0.01, 0.1, 0.5, 1.0]  # 1ms to 1s

        for timeout in timeouts:
            logger.info(f"\nTesting timeout: {timeout:.3f}s")

            shm = SharedMemory(f"timeout_test_{timeout}")
            subscriber = shm.create_subscriber()

            process = psutil.Process()
            start_time = time.time()
            start_cpu = process.cpu_times()

            # Test for 5 seconds
            test_duration = 5
            poll_count = 0

            while time.time() - start_time < test_duration:
                try:
                    message = subscriber.receive(timeout=timeout)
                    if message:
                        logger.info(f"Received message: {len(message)} bytes")
                    poll_count += 1
                except Exception:
                    # Continue on timeout
                    poll_count += 1

            end_time = time.time()
            end_cpu = process.cpu_times()

            cpu_time = end_cpu.user + end_cpu.system - (start_cpu.user + start_cpu.system)
            cpu_percent = (cpu_time / (end_time - start_time)) * 100

            logger.info(f"  CPU usage: {cpu_percent:.1f}%")
            logger.info(f"  Poll rate: {poll_count / (end_time - start_time):.0f} polls/sec")

    except Exception as e:
        logger.error(f"Timeout test failed: {e}")

def test_simple_ipc():
    """Test basic IPC functionality."""
    try:
        from ultrapubsub import SharedMemory

        logger.info("=== Simple IPC Test ===")

        # Publisher function
        def publisher_func(result_queue):
            try:
                shm = SharedMemory("simple_ipc_test")
                publisher = shm.create_publisher()

                # Send a few messages
                for i in range(5):
                    message = f"Message {i}".encode()
                    publisher.publish(message)
                    time.sleep(0.1)  # Small delay

                result_queue.put({"status": "success", "messages_sent": 5})
            except Exception as e:
                result_queue.put({"status": "error", "error": str(e)})

        # Subscriber function
        def subscriber_func(result_queue):
            try:
                shm = SharedMemory.attach("simple_ipc_test")
                subscriber = shm.create_subscriber()

                messages_received = 0
                start_time = time.time()

                # Wait for messages for 10 seconds
                while time.time() - start_time < 10:
                    try:
                        message = subscriber.receive(timeout=1.0)
                        if message:
                            messages_received += 1
                            logger.info(f"Received: {message.decode()}")
                    except Exception:
                        # Continue on timeout
                        pass

                result_queue.put({"status": "success", "messages_received": messages_received})
            except Exception as e:
                result_queue.put({"status": "error", "error": str(e)})

        # Run test with spawn
        mp.set_start_method('spawn')
        result_queue = Queue()

        # Start processes
        pub_proc = Process(target=publisher_func, args=(result_queue,))
        sub_proc = Process(target=subscriber_func, args=(result_queue,))

        pub_proc.start()
        time.sleep(0.5)  # Give publisher time to start
        sub_proc.start()

        # Wait for completion
        pub_proc.join()
        sub_proc.join()

        # Collect results
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        for result in results:
            logger.info(f"Result: {result}")

    except Exception as e:
        logger.error(f"Simple IPC test failed: {e}")

def main():
    """Run subscriber polling tests."""
    logger.info("Starting Subscriber Polling Analysis")

    # Test 1: Basic CPU usage
    cpu_percent, poll_count = test_subscriber_cpu_usage()

    # Test 2: Timeout behavior
    test_timeout_behavior()

    # Test 3: Simple IPC
    test_simple_ipc()

    logger.info("\n" + "="*60)
    logger.info("ANALYSIS SUMMARY")
    logger.info("="*60)

    if cpu_percent > 50:
        logger.warning("🚨 SUBSCRIBER POLLING IS THE MAIN BOTTLENECK")
        logger.warning("   CPU usage is too high due to busy-wait polling")
        logger.warning("   This is likely why overall performance is poor")
    else:
        logger.info("✅ Subscriber polling appears efficient")

    logger.info("\nRecommendations:")
    if cpu_percent > 50:
        logger.info("1. Implement event-based polling instead of busy-wait")
        logger.info("2. Use longer timeouts to reduce CPU usage")
        logger.info("3. Consider using epoll or similar mechanisms")
        logger.info("4. The subscriber polling loop needs immediate attention")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Test failed: {e}")
        sys.exit(1)