#!/usr/bin/env python3
"""
Simple performance test to measure UltraPubSub throughput and write results to file.
"""

import sys
import time
import os

# Disable debug output
os.environ['RUST_LOG'] = 'error'

# Add the path to the UltraPubSub module
sys.path.append('/home/adam/Documents/src/ultrapubsub/python')
sys.path.append('/home/adam/Documents/src/ultrapubsub')
sys.path.append('/home/adam/Documents/src/ultrapubsub/target/release')

def main():
    try:
        from ultrapubsub import SharedMemory

        # Test configuration
        message_size_mb = 1  # 1MB messages for higher throughput
        duration_seconds = 10
        target_mbps = 800

        # Create shared memory and publisher
        shm = SharedMemory("perf_test")
        publisher = shm.create_publisher()

        # Generate test message
        message_size_bytes = message_size_mb * 1024 * 1024
        test_message = b'X' * message_size_bytes

        # Run test
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

        # Write results to file
        with open('/tmp/performance_results.txt', 'w') as f:
            f.write("UltraPubSub Performance Test Results\n")
            f.write("=====================================\n")
            f.write(f"Duration: {actual_duration:.2f}s\n")
            f.write(f"Messages sent: {message_count}\n")
            f.write(f"Total bytes: {total_bytes:,}\n")
            f.write(f"Throughput: {throughput_mbps:.2f}MB/s\n")
            f.write(f"Messages/sec: {messages_per_second:.1f}\n")
            f.write(f"Target: {target_mbps}MB/s\n")
            f.write(f"Target met: {'YES' if throughput_mbps >= target_mbps else 'NO'}\n")

            if throughput_mbps >= target_mbps:
                f.write("\n🎉 PERFORMANCE TARGET ACHIEVED!\n")
                f.write(f"   UltraPubSub delivers {throughput_mbps:.2f}MB/s\n")
                f.write(f"   This exceeds the target of {target_mbps}MB/s\n")
            else:
                shortfall = target_mbps - throughput_mbps
                f.write(f"\n❌ Performance target not met\n")
                f.write(f"   Shortfall: {shortfall:.2f}MB/s ({shortfall/target_mbps*100:.1f}% below target)\n")

        return throughput_mbps >= target_mbps

    except Exception as e:
        with open('/tmp/performance_results.txt', 'w') as f:
            f.write(f"Test failed: {e}\n")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)