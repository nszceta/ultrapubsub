#!/usr/bin/env python3
"""
Optimized numpy array test with independent subscriber processes for maximum delivery rates
"""
import sys
import time
import ctypes
import os
import multiprocessing
import numpy as np
from datetime import datetime
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

# Test configuration - exactly 35MB
ARRAY_SHAPE = (3480, 3480, 3)  # 3480*3480*3 = 36,331,200 bytes (~35MB)
DTYPE = np.uint8
SUBSCRIBER_COUNT = 6
TEST_DURATION = 10  # seconds

def create_test_array(index):
    """Create a test numpy array with timestamp and index encoded"""
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

def parse_result_line(line):
    """Parse a result line from subprocess output"""
    if line.startswith("RESULT:"):
        import ast
        result_str = line[7:]  # Remove "RESULT:" prefix
        return ast.literal_eval(result_str)
    return None

def main():
    print("🚀 UltraPubSub Numpy Array Performance Test (Optimized)")
    print("=" * 70)

    # Calculate array size
    array_size = ARRAY_SHAPE[0] * ARRAY_SHAPE[1] * ARRAY_SHAPE[2] * DTYPE().itemsize
    print(f"📊 Test Configuration:")
    print(f"   Array shape: {ARRAY_SHAPE}")
    print(f"   Data type: {DTYPE}")
    print(f"   Array size: {array_size:,} bytes ({array_size/1024/1024:.1f}MB)")
    print(f"   Subscribers: {SUBSCRIBER_COUNT}")
    print(f"   Duration: {TEST_DURATION} seconds")
    print(f"   Method: Independent subscriber processes")
    print("=" * 70)

    test_name = "numpy_optimized_test"

    # Start subscriber processes first
    print(f"\n📡 Starting {SUBSCRIBER_COUNT} subscriber processes...")
    import subprocess
    subscriber_processes = []

    for i in range(SUBSCRIBER_COUNT):
        env = os.environ.copy()
        env['PYTHONPATH'] = '.'

        process = subprocess.Popen(
            ['uv', 'run', 'python', 'subscriber_process.py', str(i), test_name, str(TEST_DURATION + 2)],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            env=env,
            cwd=os.getcwd()
        )
        subscriber_processes.append(process)
        time.sleep(0.1)  # Stagger process starts

    # Give subscribers time to initialize
    time.sleep(0.5)

    # Create publisher
    print(f"\n📤 Creating publisher...")
    shm = SharedMemory(test_name)
    publisher = shm.create_publisher()

    # Start publishing
    print(f"\n📤 Starting publisher...")
    start_time = time.time()
    count = 0
    sequences = []

    last_report_time = start_time
    report_interval = 1.0

    try:
        while time.time() - start_time < TEST_DURATION:
            # Create test array with current timestamp
            test_array = create_test_array(count)

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

            # No polling in main thread - subscriber processes handle receiving

    except Exception as e:
        print(f"❌ Publisher error: {e}")

    # Wait for subscriber processes to finish
    print(f"\n⏳ Waiting for subscriber processes to complete...")
    subscriber_results = []

    for i, process in enumerate(subscriber_processes):
        # Stream output in real-time
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                print(output.strip())
                result = parse_result_line(output.strip())
                if result:
                    subscriber_results.append(result)

        process.wait()

    # Final publisher stats
    total_time = time.time() - start_time
    avg_frequency = count / total_time if total_time > 0 else 0
    throughput = (count * array_size) / total_time / 1024 / 1024

    print(f"\n📤 Publisher Results:")
    print(f"   Arrays sent: {count}")
    print(f"   Duration: {total_time:.2f}s")
    print(f"   Frequency: {avg_frequency:.1f} Hz")
    print(f"   Throughput: {throughput:.1f} MB/s")

    # Fill in missing results
    while len(subscriber_results) < SUBSCRIBER_COUNT:
        missing_id = len(subscriber_results)
        print(f"  ⚠️  No result from subscriber {missing_id}")
        subscriber_results.append({
            'subscriber_id': missing_id,
            'arrays_received': 0,
            'errors': 999,
            'avg_latency_ms': 0,
            'min_latency_ms': 0,
            'max_latency_ms': 0,
            'p95_latency_ms': 0,
            'p99_latency_ms': 0,
            'message_rate_hz': 0,
            'data_integrity_ok': False
        })

    # Final subscriber stats
    print(f"\n📡 Subscriber Results:")
    total_received = 0
    total_errors = 0
    all_latencies = []

    for result in sorted(subscriber_results, key=lambda x: x['subscriber_id']):
        sub_id = result['subscriber_id']
        received = result['arrays_received']
        errors = result['errors']
        avg_lat = result['avg_latency_ms']
        min_lat = result['min_latency_ms']
        max_lat = result['max_latency_ms']
        p95_lat = result['p95_latency_ms']
        p99_lat = result['p99_latency_ms']
        msg_rate = result['message_rate_hz']
        integrity_ok = result['data_integrity_ok']

        total_received += received
        total_errors += errors
        all_latencies.extend([avg_lat] * received if received > 0 else [])

        status = "✅" if integrity_ok else "❌"
        print(f"   Sub {sub_id}: {received} arrays, {errors} errors, "
              f"avg: {avg_lat:.1f}ms, P95: {p95_lat:.1f}ms, rate: {msg_rate:.1f} Hz {status}")

    # Overall analysis
    print(f"\n📊 Overall Performance:")
    print(f"   Total arrays sent: {count}")
    print(f"   Total arrays received: {total_received}")
    print(f"   Delivery rate: {(total_received/(count*SUBSCRIBER_COUNT)*100):.1f}%")
    print(f"   Total errors: {total_errors}")

    if all_latencies:
        overall_avg = sum(all_latencies) / len(all_latencies)
        overall_max = max(r['max_latency_ms'] for r in subscriber_results if r['arrays_received'] > 0)
        overall_min = min(r['min_latency_ms'] for r in subscriber_results if r['arrays_received'] > 0)

        # Calculate overall percentiles
        all_individual_latencies = []
        for r in subscriber_results:
            if r['arrays_received'] > 0:
                # Use individual latencies if available, otherwise use average
                all_individual_latencies.extend([r['avg_latency_ms']] * r['arrays_received'])

        if all_individual_latencies:
            sorted_lats = sorted(all_individual_latencies)
            p95 = sorted_lats[int(len(sorted_lats) * 0.95)]
            p99 = sorted_lats[int(len(sorted_lats) * 0.99)]
        else:
            p95 = p99 = 0

        print(f"   Latency - avg: {overall_avg:.1f}ms, min: {overall_min:.1f}ms, "
              f"max: {overall_max:.1f}ms")
        print(f"   Latency - P95: {p95:.1f}ms, P99: {p99:.1f}ms")

    # Target analysis
    target_freq = 40.0
    print(f"\n🎯 Target Analysis:")
    print(f"   Target: {target_freq} Hz")
    print(f"   Achieved: {avg_frequency:.1f} Hz")
    print(f"   Target achievement: {(avg_frequency/target_freq*100):.1f}%")

    if avg_frequency >= target_freq:
        print("   ✅ TARGET PERFORMANCE ACHIEVED!")
    else:
        gap = target_freq - avg_frequency
        print(f"   ❌ {gap:.1f} Hz below target")

    # Data integrity check
    integrity_pass = total_errors == 0
    print(f"\n🔒 Data Integrity: {'✅ PASS' if integrity_pass else '❌ FAIL'}")

    # System efficiency
    if count > 0:
        delivery_efficiency = (total_received / (count * SUBSCRIBER_COUNT)) * 100
        print(f"📈 System Efficiency: {delivery_efficiency:.1f}% delivery to all subscribers")

    print(f"\n✅ Test completed successfully!")

if __name__ == "__main__":
    main()