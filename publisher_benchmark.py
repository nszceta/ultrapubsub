#!/usr/bin/env python3
"""
Standalone publisher process for performance benchmarking
"""
import sys
import time
import ultrapubsub
import signal
import os

def run_publisher_process(test_name, message_size_mb=35, duration_seconds=10, target_hz=40):
    """Run publisher as a standalone process"""
    print(f"🚀 Publisher process started (PID: {os.getpid()})")
    print(f"📊 Configuration:")
    print(f"   - Test name: {test_name}")
    print(f"   - Message size: {message_size_mb} MB")
    print(f"   - Duration: {duration_seconds} seconds")
    print(f"   - Target frequency: {target_hz} Hz")

    try:
        # Create test message
        test_message = b'X' * (message_size_mb * 1024 * 1024)
        message_size_bytes = len(test_message)
        print(f"📦 Test message size: {message_size_bytes:,} bytes")

        # Create publisher
        publisher = ultrapubsub.create_publisher(test_name)
        print(f"✅ Publisher created, subscriber count: {publisher.subscriber_count()}")

        # Wait for subscribers to register (with timeout)
        wait_start = time.time()
        max_wait_time = 30.0  # 30 seconds max wait

        while publisher.subscriber_count() == 0 and (time.time() - wait_start) < max_wait_time:
            print(f"⏳ Waiting for subscribers... ({publisher.subscriber_count()} registered)")
            time.sleep(0.5)

        if publisher.subscriber_count() == 0:
            print("❌ No subscribers registered within timeout")
            return 1

        print(f"✅ {publisher.subscriber_count()} subscribers registered")

        # Start broadcasting
        print(f"📤 Starting broadcast for {duration_seconds} seconds...")
        start_time = time.time()
        end_time = start_time + duration_seconds
        broadcast_count = 0

        # Setup signal handler for graceful shutdown
        def signal_handler(signum, frame):
            print(f"⚠️  Received signal {signum}, shutting down...")
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            while time.time() < end_time:
                broadcast_start = time.time()

                # Broadcast the message
                publisher.broadcast(test_message)
                broadcast_count += 1

                # Calculate actual time used and adjust for next broadcast
                actual_broadcast_time = time.time() - broadcast_start

                # If we have time left in this cycle, sleep
                cycle_time = 1.0 / target_hz
                sleep_time = max(0, cycle_time - actual_broadcast_time)
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            print(f"⚠️  Broadcast interrupted")

        actual_duration = time.time() - start_time
        total_bytes = broadcast_count * message_size_bytes
        throughput_mbps = (total_bytes / actual_duration) / (1024 * 1024)
        actual_hz = broadcast_count / actual_duration

        print(f"\n📊 Publisher Results:")
        print(f"   - Duration: {actual_duration:.2f} seconds")
        print(f"   - Messages broadcast: {broadcast_count:,}")
        print(f"   - Total data: {total_bytes:,} bytes ({total_bytes / 1024 / 1024:.1f} MB)")
        print(f"   - Throughput: {throughput_mbps:.2f} MB/s ({throughput_mbps / 1024:.3f} GB/s)")
        print(f"   - Frequency: {actual_hz:.2f} Hz")

        # Cleanup
        ultrapubsub.cleanup_shared_memory(test_name)
        print("✅ Publisher completed successfully")
        return 0

    except Exception as e:
        print(f"❌ Publisher error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python publisher_benchmark.py <test_name> <message_size_mb> <duration_seconds> <target_hz>")
        sys.exit(1)

    test_name = sys.argv[1]
    message_size_mb = int(sys.argv[2])
    duration_seconds = int(sys.argv[3])
    target_hz = float(sys.argv[4])

    sys.exit(run_publisher_process(test_name, message_size_mb, duration_seconds, target_hz))