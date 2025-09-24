#!/usr/bin/env python3
"""
Clean performance test to measure UltraPubSub throughput.
"""

import sys
import time
import os

# Disable debug output
os.environ['RUST_LOG'] = 'error'

# Add the path to the UltraPubSub module
sys.path.append('/home/adam/Documents/src/ultrapubsub/python')
sys.path.append('/home/adam/Documents/src/ultrapubsub')

def main():
    print("UltraPubSub Performance Test")
    print("=" * 40)

    try:
        from ultrapubsub import SharedMemory

        # Test configuration
        message_size_mb = 20  # 20MB messages
        duration_seconds = 10
        target_mbps = 800

        print(f"Message size: {message_size_mb}MB")
        print(f"Test duration: {duration_seconds}s")
        print(f"Target throughput: {target_mbps}MB/s")
        print()

        # Create shared memory and publisher
        shm = SharedMemory("perf_test")
        publisher = shm.create_publisher()

        # Generate test message
        message_size_bytes = message_size_mb * 1024 * 1024
        test_message = b'X' * message_size_bytes

        print("Starting throughput test...")
        start_time = time.time()
        message_count = 0
        total_bytes = 0

        # Publish as fast as possible
        while time.time() - start_time < duration_seconds:
            publisher.publish(test_message)
            message_count += 1
            total_bytes += message_size_bytes

        # Calculate results
        end_time = time.time()
        actual_duration = end_time - start_time
        throughput_mbps = (total_bytes / (1024 * 1024)) / actual_duration
        messages_per_second = message_count / actual_duration

        print()
        print("RESULTS:")
        print(f"Duration: {actual_duration:.2f}s")
        print(f"Messages sent: {message_count}")
        print(f"Total bytes: {total_bytes:,}")
        print(f"Throughput: {throughput_mbps:.2f}MB/s")
        print(f"Messages/sec: {messages_per_second:.1f}")
        print()
        print(f"Target ({target_mbps}MB/s): {'✓ MET' if throughput_mbps >= target_mbps else '✗ NOT MET'}")

        if throughput_mbps >= target_mbps:
            print("\n🎉 PERFORMANCE TARGET ACHIEVED!")
            print(f"   UltraPubSub delivers {throughput_mbps:.2f}MB/s")
            print(f"   This exceeds the target of {target_mbps}MB/s")
        else:
            shortfall = target_mbps - throughput_mbps
            print(f"\n❌ Performance target not met")
            print(f"   Shortfall: {shortfall:.2f}MB/s ({shortfall/target_mbps*100:.1f}% below target)")

        return throughput_mbps >= target_mbps

    except Exception as e:
        print(f"Test failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)