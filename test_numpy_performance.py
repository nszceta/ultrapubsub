#!/usr/bin/env python3
"""
High-performance numpy array test with 6 subscribers
Generates 35MB numpy arrays (4096, 4096, 3) with timestamps and indices
Tests maximum frequency to 6 subscribers with detailed diagnostics
"""
import sys
import time
import multiprocessing
import ctypes
import numpy as np
from datetime import datetime
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

# Test configuration
ARRAY_SHAPE = (3480, 3480, 3)  # 3480*3480*3 = 36,331,200 bytes (~35MB exactly)
DTYPE = np.uint8
SUBSCRIBER_COUNT = 6
TEST_DURATION = 10  # seconds

def create_test_array(index):
    """Create a test numpy array with timestamp and index encoded"""
    # Create base array
    array = np.zeros(ARRAY_SHAPE, dtype=DTYPE)

    # Fill with test pattern
    array[:, :, 0] = (index * 10) % 256  # Red channel encodes index
    array[:, :, 1] = int(time.time() * 1000) % 256  # Green channel encodes ms timestamp
    array[:, :, 2] = 128  # Blue channel fixed

    # Encode actual timestamp and index in first few pixels
    timestamp_bytes = int(time.time() * 1000000).to_bytes(8, 'little')  # microseconds
    index_bytes = index.to_bytes(4, 'little')

    # Write timestamp to first 8 pixels of first row
    for i, byte in enumerate(timestamp_bytes):
        array[0, i, 0] = byte
        array[0, i, 1] = byte ^ 0x80  # Complement for verification

    # Write index to next 4 pixels
    for i, byte in enumerate(index_bytes):
        array[0, i+8, 0] = byte
        array[0, i+8, 1] = byte ^ 0x80

    return array

def decode_array_info(array):
    """Decode timestamp and index from numpy array"""
    # Read timestamp from first 8 pixels
    timestamp_bytes = bytes()
    for i in range(8):
        byte = array[0, i, 0]
        complement = array[0, i, 1]
        if byte != (complement ^ 0x80):
            return None, None, "Data corruption detected"
        timestamp_bytes += bytes([byte])

    # Read index from next 4 pixels
    index_bytes = bytes()
    for i in range(4):
        byte = array[0, i+8, 0]
        complement = array[0, i+8, 1]
        if byte != (complement ^ 0x80):
            return None, None, "Data corruption detected"
        index_bytes += bytes([byte])

    timestamp = int.from_bytes(timestamp_bytes, 'little') / 1000000.0
    index = int.from_bytes(index_bytes, 'little')

    return timestamp, index, "OK"

def numpy_pool_publisher(name, duration):
    """Publisher using zero-copy pool for numpy arrays"""
    shm = SharedMemory(name)
    publisher = shm.create_publisher()

    start_time = time.time()
    count = 0
    sequences = []

    print(f"📤 Numpy publisher started for {duration} seconds")
    print(f"📊 Array shape: {ARRAY_SHAPE}, dtype: {DTYPE}")
    print(f"💾 Array size: {ARRAY_SHAPE[0] * ARRAY_SHAPE[1] * ARRAY_SHAPE[2] * DTYPE().itemsize:,} bytes")

    last_report_time = start_time
    report_interval = 1.0  # Report every second

    try:
        while time.time() - start_time < duration:
            # Create test array with current timestamp
            test_array = create_test_array(count)
            array_size = test_array.nbytes

            # Allocate pool slot
            slot, ptr = publisher.allocate_pool_slot()

            # Copy numpy array directly to pool slot
            pool_mem = ctypes.cast(ptr, ctypes.POINTER(ctypes.c_char * array_size))
            array_ptr = test_array.ctypes.data_as(ctypes.POINTER(ctypes.c_char * array_size))
            ctypes.memmove(pool_mem, array_ptr, array_size)

            # Publish the pool slot
            seq = publisher.publish_pool_slot(slot, array_size)
            sequences.append((seq, count, time.time()))
            count += 1

            # Report progress
            current_time = time.time()
            if current_time - last_report_time >= report_interval:
                elapsed = current_time - start_time
                freq = count / elapsed
                throughput = (count * array_size) / elapsed / 1024 / 1024  # MB/s
                print(f"  📈 Progress: {count} arrays, {freq:.1f} Hz, {throughput:.1f} MB/s")
                last_report_time = current_time

    except Exception as e:
        print(f"❌ Publisher error: {e}")

    total_time = time.time() - start_time
    avg_frequency = count / total_time if total_time > 0 else 0
    throughput = (count * ARRAY_SHAPE[0] * ARRAY_SHAPE[1] * ARRAY_SHAPE[2] * DTYPE().itemsize) / total_time / 1024 / 1024

    print(f"📤 Publisher completed: {count} arrays in {total_time:.2f}s")
    print(f"📊 Average frequency: {avg_frequency:.1f} Hz")
    print(f"📊 Average throughput: {throughput:.1f} MB/s")

    return {
        'count': count,
        'total_time': total_time,
        'avg_frequency': avg_frequency,
        'throughput_mb_s': throughput,
        'sequences': sequences
    }

def numpy_subscriber_process(name, duration, sub_id, result_queue):
    """Subscriber process for numpy arrays with detailed diagnostics"""
    try:
        shm = SharedMemory(name)
        subscriber = shm.create_subscriber()

        start_time = time.time()
        count = 0
        latencies = []
        decode_errors = 0
        first_message_time = None
        last_message_time = None

        print(f"📡 Subscriber {sub_id} started...")

        while time.time() - start_time < duration:
            msg_start = time.time()

            try:
                data = subscriber.receive(timeout=1.0)
                msg_end = time.time()

                if data:
                    # Convert bytes back to numpy array
                    array = np.frombuffer(data, dtype=DTYPE).reshape(ARRAY_SHAPE)

                    # Decode embedded information
                    timestamp, index, status = decode_array_info(array)

                    latency = (msg_end - msg_start) * 1000  # ms
                    transport_latency = msg_end - timestamp if timestamp else 0

                    latencies.append(latency)

                    if first_message_time is None:
                        first_message_time = msg_end
                    last_message_time = msg_end

                    if status != "OK":
                        decode_errors += 1
                        print(f"  ❌ Sub {sub_id}: Decode error - {status}")

                    count += 1

                    # Log every 10th message
                    if count % 10 == 0:
                        avg_latency = sum(latencies) / len(latencies)
                        print(f"  📡 Sub {sub_id}: {count} arrays, avg latency: {avg_latency:.1f}ms")

            except Exception as e:
                print(f"  ❌ Sub {sub_id}: Receive error - {e}")

        # Calculate statistics
        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            min_latency = min(latencies)
            max_latency = max(latencies)
            p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
            p99_latency = sorted(latencies)[int(len(latencies) * 0.99)]
        else:
            avg_latency = min_latency = max_latency = p95_latency = p99_latency = 0

        # Calculate message rate
        if first_message_time and last_message_time:
            active_duration = last_message_time - first_message_time
            msg_rate = count / active_duration if active_duration > 0 else 0
        else:
            msg_rate = 0

        result = {
            'subscriber_id': sub_id,
            'arrays_received': count,
            'decode_errors': decode_errors,
            'avg_latency_ms': avg_latency,
            'min_latency_ms': min_latency,
            'max_latency_ms': max_latency,
            'p95_latency_ms': p95_latency,
            'p99_latency_ms': p99_latency,
            'message_rate_hz': msg_rate,
            'data_integrity_ok': decode_errors == 0
        }

        result_queue.put(result)

        print(f"📡 Subscriber {sub_id} completed:")
        print(f"   Arrays received: {count}")
        print(f"   Avg latency: {avg_latency:.1f}ms (min: {min_latency:.1f}ms, max: {max_latency:.1f}ms)")
        print(f"   P95 latency: {p95_latency:.1f}ms, P99: {p99_latency:.1f}ms")
        print(f"   Message rate: {msg_rate:.1f} Hz")
        print(f"   Decode errors: {decode_errors}")

    except Exception as e:
        print(f"❌ Subscriber {sub_id} failed: {e}")
        result_queue.put({
            'subscriber_id': sub_id,
            'arrays_received': 0,
            'decode_errors': 999,
            'avg_latency_ms': 0,
            'min_latency_ms': 0,
            'max_latency_ms': 0,
            'p95_latency_ms': 0,
            'p99_latency_ms': 0,
            'message_rate_hz': 0,
            'data_integrity_ok': False,
            'error': str(e)
        })

def run_numpy_performance_test():
    """Run comprehensive numpy array performance test"""
    print("🚀 UltraPubSub Numpy Array Performance Test")
    print("=" * 70)
    print(f"📊 Test Configuration:")
    print(f"   Array shape: {ARRAY_SHAPE}")
    print(f"   Data type: {DTYPE}")
    print(f"   Array size: {ARRAY_SHAPE[0] * ARRAY_SHAPE[1] * ARRAY_SHAPE[2] * DTYPE().itemsize:,} bytes")
    print(f"   Subscribers: {SUBSCRIBER_COUNT}")
    print(f"   Duration: {TEST_DURATION} seconds")
    print(f"   Method: Zero-copy pool")
    print("=" * 70)

    test_name = "numpy_test"

    # Start subscribers
    print(f"\n📡 Starting {SUBSCRIBER_COUNT} subscribers...")
    result_queue = multiprocessing.Queue()
    subscriber_processes = []

    for i in range(SUBSCRIBER_COUNT):
        p = multiprocessing.Process(
            target=numpy_subscriber_process,
            args=(test_name, TEST_DURATION + 2, i, result_queue)  # +2s for buffer
        )
        p.start()
        subscriber_processes.append(p)
        time.sleep(0.1)  # Stagger start

    # Give subscribers time to initialize
    time.sleep(0.5)

    # Start publisher
    print(f"\n📤 Starting publisher...")
    publisher_result = numpy_pool_publisher(test_name, TEST_DURATION)

    # Wait for subscribers to finish
    print(f"\n⏳ Waiting for subscribers to complete...")
    for p in subscriber_processes:
        p.join()

    # Collect subscriber results
    subscriber_results = []
    for _ in range(SUBSCRIBER_COUNT):
        try:
            result = result_queue.get(timeout=5)
            subscriber_results.append(result)
        except:
            print("❌ Timeout getting subscriber result")
            subscriber_results.append({
                'subscriber_id': len(subscriber_results),
                'arrays_received': 0,
                'decode_errors': 999,
                'avg_latency_ms': 0,
                'message_rate_hz': 0,
                'data_integrity_ok': False
            })

    # Analyze results
    print("\n" + "=" * 70)
    print("📊 PERFORMANCE ANALYSIS")
    print("=" * 70)

    # Publisher metrics
    print(f"\n📤 Publisher Performance:")
    print(f"   Arrays sent: {publisher_result['count']}")
    print(f"   Frequency: {publisher_result['avg_frequency']:.1f} Hz")
    print(f"   Throughput: {publisher_result['throughput_mb_s']:.1f} MB/s")
    print(f"   Duration: {publisher_result['total_time']:.2f}s")

    # Subscriber metrics
    total_received = sum(r['arrays_received'] for r in subscriber_results)
    total_errors = sum(r['decode_errors'] for r in subscriber_results)
    avg_subscriber_rate = sum(r['message_rate_hz'] for r in subscriber_results) / len(subscriber_results)

    print(f"\n📡 Subscriber Performance:")
    print(f"   Total arrays received: {total_received}")
    print(f"   Average per subscriber: {total_received / SUBSCRIBER_COUNT:.1f}")
    print(f"   Total decode errors: {total_errors}")
    print(f"   Average subscriber rate: {avg_subscriber_rate:.1f} Hz")

    # Latency analysis
    all_latencies = []
    for r in subscriber_results:
        if r['arrays_received'] > 0:
            all_latencies.extend([r['avg_latency_ms']] * r['arrays_received'])

    if all_latencies:
        overall_avg = sum(all_latencies) / len(all_latencies)
        overall_max = max(r['max_latency_ms'] for r in subscriber_results if r['arrays_received'] > 0)
        overall_p95 = np.percentile(all_latencies, 95)
        overall_p99 = np.percentile(all_latencies, 99)

        print(f"\n⏱️  Latency Analysis:")
        print(f"   Overall average: {overall_avg:.1f}ms")
        print(f"   Overall maximum: {overall_max:.1f}ms")
        print(f"   95th percentile: {overall_p95:.1f}ms")
        print(f"   99th percentile: {overall_p99:.1f}ms")

    # System efficiency
    if publisher_result['count'] > 0:
        delivery_rate = (total_received / (publisher_result['count'] * SUBSCRIBER_COUNT)) * 100
        print(f"\n🎯 System Efficiency:")
        print(f"   Delivery rate: {delivery_rate:.1f}%")
        print(f"   Data integrity: {'✅ PASS' if total_errors == 0 else '❌ FAIL'}")

        # Target analysis
        target_freq = 40.0
        achieved_freq = publisher_result['avg_frequency']
        print(f"   Target frequency: {target_freq} Hz")
        print(f"   Achieved frequency: {achieved_freq:.1f} Hz")
        print(f"   Target achievement: {(achieved_freq / target_freq * 100):.1f}%")

        if achieved_freq >= target_freq:
            print("   ✅ TARGET PERFORMANCE ACHIEVED!")
        else:
            gap = target_freq - achieved_freq
            print(f"   ❌ {gap:.1f} Hz below target")

    # Individual subscriber breakdown
    print(f"\n📋 Individual Subscriber Results:")
    for r in sorted(subscriber_results, key=lambda x: x['subscriber_id']):
        status = "✅" if r['data_integrity_ok'] else "❌"
        print(f"   Sub {r['subscriber_id']}: {r['arrays_received']} arrays, "
              f"{r['message_rate_hz']:.1f} Hz, {r['avg_latency_ms']:.1f}ms avg latency {status}")

    return {
        'publisher': publisher_result,
        'subscribers': subscriber_results,
        'total_received': total_received,
        'total_errors': total_errors
    }

if __name__ == "__main__":
    results = run_numpy_performance_test()

    print("\n" + "=" * 70)
    print("✅ NUMPY PERFORMANCE TEST COMPLETED")
    print("=" * 70)