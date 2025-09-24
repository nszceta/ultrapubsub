#!/usr/bin/env python3
"""
Comprehensive performance profiler for UltraPubSub.

This tool systematically measures each component to identify bottlenecks.
"""

import sys
import os
import time
import multiprocessing as mp
from multiprocessing import Process, Queue
import logging
import psutil
import gc

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_memory_allocation_overhead(message_size_mb, num_allocations):
    """Test pure memory allocation overhead without IPC."""
    try:
        from ultrapubsub import SharedMemory

        logger.info(f"=== Memory Allocation Overhead Test ===")
        logger.info(f"Message size: {message_size_mb}MB, Allocations: {num_allocations}")

        shm = SharedMemory("profile_alloc_test")
        publisher = shm.create_publisher()

        message_size_bytes = message_size_mb * 1024 * 1024
        test_message = b'X' * message_size_bytes

        # Test allocation and deallocation speed
        start_time = time.time()

        for i in range(num_allocations):
            # Allocate memory and copy data
            ptr = publisher.allocate_and_write(message_size_bytes)
            # Free memory
            publisher.free_memory(ptr, message_size_bytes)

        alloc_duration = time.time() - start_time

        logger.info(f"Allocation overhead: {alloc_duration:.3f}s for {num_allocations} allocations")
        logger.info(f"Per allocation: {(alloc_duration * 1000000 / num_allocations):.1f}μs")
        logger.info(f"Allocation throughput: {num_allocations / alloc_duration:.0f} ops/sec")

        # Test data copying speed
        start_time = time.time()

        for i in range(num_allocations):
            ptr = publisher.allocate_and_write(message_size_bytes)
            # Simulate data copying by writing directly
            publisher.publish_allocation(ptr, message_size_bytes)

        copy_duration = time.time() - start_time

        logger.info(f"Data copying overhead: {copy_duration:.3f}s for {num_allocations} copies")
        logger.info(f"Per copy: {(copy_duration * 1000000 / num_allocations):.1f}μs")
        logger.info(f"Copy throughput: {(num_allocations * message_size_bytes / (1024*1024)) / copy_duration:.1f}MB/s")

        return alloc_duration, copy_duration

    except Exception as e:
        logger.error(f"Memory allocation test failed: {e}")
        return 0, 0

def test_io_uring_overhead(message_size_mb, num_messages):
    """Test io_uring submission overhead."""
    try:
        from ultrapubsub import SharedMemory

        logger.info(f"=== io_uring Overhead Test ===")
        logger.info(f"Message size: {message_size_mb}MB, Messages: {num_messages}")

        shm = SharedMemory("profile_io_uring_test")
        publisher = shm.create_publisher()

        message_size_bytes = message_size_mb * 1024 * 1024
        test_message = b'X' * message_size_bytes

        # Test batch publishing performance
        batch_sizes = [1, 4, 8, 16, 32]

        for batch_size in batch_sizes:
            messages = [test_message] * num_messages
            message_chunks = [messages[i:i+batch_size] for i in range(0, len(messages), batch_size)]

            start_time = time.time()

            for chunk in message_chunks:
                publisher.publish_batch(chunk)

            batch_duration = time.time() - start_time

            logger.info(f"Batch size {batch_size}: {batch_duration:.3f}s, "
                       f"{len(message_chunks)/batch_duration:.1f} batches/sec, "
                       f"{num_messages/batch_duration:.1f} messages/sec")

        # Test single message publishing
        start_time = time.time()

        for i in range(num_messages):
            publisher.publish(test_message)

        single_duration = time.time() - start_time

        logger.info(f"Single message: {single_duration:.3f}s, {num_messages/single_duration:.1f} messages/sec")

        return single_duration

    except Exception as e:
        logger.error(f"io_uring test failed: {e}")
        return 0

def test_subscriber_overhead(duration_seconds):
    """Test subscriber polling overhead."""
    try:
        from ultrapubsub import SharedMemory

        logger.info(f"=== Subscriber Overhead Test ===")
        logger.info(f"Duration: {duration_seconds}s")

        shm = SharedMemory("profile_subscriber_test")
        subscriber = shm.create_subscriber()

        # Measure CPU usage while polling
        process = psutil.Process()
        start_time = time.time()
        start_cpu = process.cpu_times()

        messages_received = 0

        while time.time() - start_time < duration_seconds:
            try:
                message = subscriber.receive(timeout=0.001)  # 1ms timeout
                if message:
                    messages_received += 1
            except Exception:
                # Continue on timeout
                pass

        end_time = time.time()
        end_cpu = process.cpu_times()

        cpu_time = end_cpu.user + end_cpu.system - (start_cpu.user + start_cpu.system)
        cpu_percent = (cpu_time / (end_time - start_time)) * 100

        logger.info(f"Received {messages_received} messages in {end_time - start_time:.1f}s")
        logger.info(f"CPU usage: {cpu_percent:.1f}%")
        logger.info(f"Polling efficiency: {messages_received / (end_time - start_time):.1f} msgs/sec")

        return cpu_percent, messages_received

    except Exception as e:
        logger.error(f"Subscriber test failed: {e}")
        return 0, 0

def test_ipc_round_trip(message_size_mb, num_messages):
    """Test complete IPC round trip performance."""
    try:
        from ultrapubsub import SharedMemory

        logger.info(f"=== IPC Round Trip Test ===")
        logger.info(f"Message size: {message_size_mb}MB, Messages: {num_messages}")

        # Publisher process
        def publisher_process(result_queue, message_size, count):
            try:
                shm = SharedMemory("profile_ipc_test")
                publisher = shm.create_publisher()

                test_message = b'X' * message_size

                start_time = time.time()

                for i in range(count):
                    publisher.publish(test_message)

                duration = time.time() - start_time

                result_queue.put({
                    'process_type': 'publisher',
                    'messages_sent': count,
                    'duration': duration,
                    'throughput_mbps': (count * message_size / (1024*1024)) / duration,
                    'messages_per_sec': count / duration
                })

            except Exception as e:
                result_queue.put({'process_type': 'publisher', 'error': str(e)})

        # Subscriber process
        def subscriber_process(result_queue, expected_messages):
            try:
                shm = SharedMemory.attach("profile_ipc_test")
                subscriber = shm.create_subscriber()

                messages_received = 0
                start_time = time.time()

                while messages_received < expected_messages:
                    try:
                        message = subscriber.receive(timeout=1.0)
                        if message:
                            messages_received += 1
                    except Exception:
                        if time.time() - start_time > 30:  # 30s timeout
                            break

                duration = time.time() - start_time

                result_queue.put({
                    'process_type': 'subscriber',
                    'messages_received': messages_received,
                    'duration': duration,
                    'messages_per_sec': messages_received / duration if duration > 0 else 0
                })

            except Exception as e:
                result_queue.put({'process_type': 'subscriber', 'error': str(e)})

        # Use spawn for independent processes
        mp.set_start_method('spawn')
        result_queue = Queue()

        # Start publisher
        pub_proc = Process(target=publisher_process, args=(result_queue, message_size_mb * 1024 * 1024, num_messages))

        # Start subscriber
        sub_proc = Process(target=subscriber_process, args=(result_queue, num_messages))

        # Run test
        pub_proc.start()
        time.sleep(0.1)  # Give publisher time to start
        sub_proc.start()

        # Wait for completion
        pub_proc.join()
        sub_proc.join()

        # Collect results
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        # Analyze results
        pub_result = None
        sub_result = None

        for result in results:
            if result.get('process_type') == 'publisher':
                pub_result = result
            elif result.get('process_type') == 'subscriber':
                sub_result = result

        if pub_result and sub_result:
            logger.info(f"Publisher: {pub_result['messages_per_sec']:.1f} msgs/sec, "
                       f"{pub_result['throughput_mbps']:.1f}MB/s")
            logger.info(f"Subscriber: {sub_result['messages_per_sec']:.1f} msgs/sec")
            logger.info(f"Efficiency: {(sub_result['messages_received'] / pub_result['messages_sent'] * 100):.1f}%")

            return pub_result['throughput_mbps'], sub_result['messages_per_sec']

        return 0, 0

    except Exception as e:
        logger.error(f"IPC test failed: {e}")
        return 0, 0

def profile_system_resources():
    """Profile system resource usage during test."""
    logger.info(f"=== System Resource Profile ===")

    # Memory info
    memory = psutil.virtual_memory()
    logger.info(f"Total memory: {memory.total / (1024**3):.1f}GB")
    logger.info(f"Available memory: {memory.available / (1024**3):.1f}GB")
    logger.info(f"Memory usage: {memory.percent:.1f}%")

    # CPU info
    cpu_count = psutil.cpu_count()
    cpu_freq = psutil.cpu_freq()
    logger.info(f"CPU cores: {cpu_count}")
    logger.info(f"CPU frequency: {cpu_freq.current:.0f}MHz")

    # Disk I/O (for shared memory)
    disk_io = psutil.disk_io_counters()
    logger.info(f"Disk read: {disk_io.read_bytes / (1024**2):.1f}MB")
    logger.info(f"Disk write: {disk_io.write_bytes / (1024**2):.1f}MB")

def main():
    """Run comprehensive performance profiling."""
    logger.info("Starting UltraPubSub Performance Profiler")

    # Test configuration
    message_size_mb = 20
    num_messages = 100
    duration_seconds = 10

    logger.info(f"Configuration: {message_size_mb}MB messages, {num_messages} messages, {duration_seconds}s duration")

    # Profile system resources
    profile_system_resources()

    # Test individual components
    logger.info("\n" + "="*60)

    # 1. Memory allocation overhead
    alloc_time, copy_time = test_memory_allocation_overhead(message_size_mb, num_messages)

    # 2. io_uring overhead
    io_uring_time = test_io_uring_overhead(message_size_mb, num_messages)

    # 3. Subscriber overhead
    cpu_percent, msg_count = test_subscriber_overhead(duration_seconds)

    # 4. Complete IPC round trip
    ipc_throughput, sub_rate = test_ipc_round_trip(message_size_mb, num_messages)

    # Analysis
    logger.info("\n" + "="*60)
    logger.info("BOTTLENECK ANALYSIS")
    logger.info("="*60)

    total_time = alloc_time + copy_time + io_uring_time
    if total_time > 0:
        logger.info(f"Memory allocation: {(alloc_time/total_time*100):.1f}% of time")
        logger.info(f"Data copying: {(copy_time/total_time*100):.1f}% of time")
        logger.info(f"io_uring operations: {(io_uring_time/total_time*100):.1f}% of time")

    logger.info(f"Subscriber CPU usage: {cpu_percent:.1f}%")
    logger.info(f"IPC throughput: {ipc_throughput:.1f}MB/s")
    logger.info(f"Target throughput: 800MB/s")
    logger.info(f"Efficiency: {(ipc_throughput/800*100):.1f}%")

    # Recommendations
    logger.info("\n" + "="*60)
    logger.info("PERFORMANCE RECOMMENDATIONS")
    logger.info("="*60)

    if alloc_time > copy_time:
        logger.info("⚠️  Memory allocation is a significant bottleneck")
        logger.info("   Consider using a memory pool or pre-allocation")

    if copy_time > alloc_time:
        logger.info("⚠️  Data copying is a significant bottleneck")
        logger.info("   Consider implementing true zero-copy semantics")

    if cpu_percent > 50:
        logger.info("⚠️  Subscriber CPU usage is high")
        logger.info("   Consider optimizing the polling loop")

    if ipc_throughput < 100:
        logger.info("🚨 Overall IPC performance is very poor")
        logger.info("   Check io_uring setup and shared memory mapping")

    logger.info("🔍 Next steps: Focus on the largest bottleneck identified above")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Profiling interrupted by user")
    except Exception as e:
        logger.error(f"Profiling failed: {e}")
        sys.exit(1)