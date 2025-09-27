#!/usr/bin/env python3
"""
Standalone subscriber process for performance benchmarking
"""
import sys
import time
import ultrapubsub
import signal
import os

def run_subscriber_process(test_name, subscriber_id, duration_seconds=10):
    """Run subscriber as a standalone process"""
    print(f"👥 Subscriber process {subscriber_id} started (PID: {os.getpid()})")

    try:
        # Create subscriber
        subscriber = ultrapubsub.create_subscriber_with_id(test_name, subscriber_id)
        subscriber.register()
        print(f"✅ Subscriber {subscriber_id} registered")

        # Setup signal handler for graceful shutdown
        def signal_handler(signum, frame):
            print(f"⚠️  Subscriber {subscriber_id} received signal {signum}, shutting down...")
            subscriber.deregister()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Start receiving
        print(f"🔄 Subscriber {subscriber_id} starting to receive for {duration_seconds} seconds...")
        start_time = time.time()
        end_time = start_time + duration_seconds
        receive_count = 0
        total_bytes = 0
        latencies = []

        try:
            while time.time() < end_time:
                receive_start = time.time()

                # Receive message with timeout
                received = subscriber.receive(timeout=1.0)
                if received:
                    receive_end = time.time()
                    latency_ms = (receive_end - receive_start) * 1000

                    receive_count += 1
                    total_bytes += len(received)
                    latencies.append(latency_ms)

                    # Log progress every 10 messages
                    if receive_count % 10 == 0:
                        elapsed = time.time() - start_time
                        current_hz = receive_count / elapsed
                        current_mbps = (total_bytes / elapsed) / (1024 * 1024)
                        avg_latency = sum(latencies[-10:]) / min(10, len(latencies))
                        print(f"📊 Subscriber {subscriber_id}: {receive_count} msgs, {current_hz:.1f} Hz, {current_mbps:.1f} MB/s, {avg_latency:.1f}ms avg")

        except KeyboardInterrupt:
            print(f"⚠️  Subscriber {subscriber_id} interrupted")

        except Exception as e:
            print(f"❌ Subscriber {subscriber_id} error: {e}")

        # Calculate final stats
        actual_duration = time.time() - start_time
        if receive_count > 0:
            avg_latency = sum(latencies) / len(latencies)
            throughput_mbps = (total_bytes / actual_duration) / (1024 * 1024)
            actual_hz = receive_count / actual_duration

            print(f"\n📊 Subscriber {subscriber_id} Results:")
            print(f"   - Duration: {actual_duration:.2f} seconds")
            print(f"   - Messages received: {receive_count:,}")
            print(f"   - Total data: {total_bytes:,} bytes ({total_bytes / 1024 / 1024:.1f} MB)")
            print(f"   - Throughput: {throughput_mbps:.2f} MB/s ({throughput_mbps / 1024:.3f} GB/s)")
            print(f"   - Frequency: {actual_hz:.2f} Hz")
            print(f"   - Average latency: {avg_latency:.2f} ms")

            # Save results to file for orchestrator
            result = {
                'subscriber_id': subscriber_id,
                'receive_count': receive_count,
                'total_bytes': total_bytes,
                'duration': actual_duration,
                'throughput_mbps': throughput_mbps,
                'frequency_hz': actual_hz,
                'avg_latency_ms': avg_latency,
                'pid': os.getpid()
            }

            with open(f'/tmp/subscriber_{subscriber_id}_results.txt', 'w') as f:
                import json
                json.dump(result, f)

        else:
            print(f"❌ Subscriber {subscriber_id}: No messages received")

        # Cleanup
        subscriber.deregister()
        print(f"✅ Subscriber {subscriber_id} completed")
        return 0

    except Exception as e:
        print(f"❌ Subscriber {subscriber_id} error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python subscriber_benchmark.py <test_name> <subscriber_id> <duration_seconds>")
        sys.exit(1)

    test_name = sys.argv[1]
    subscriber_id = int(sys.argv[2])
    duration_seconds = int(sys.argv[3])

    sys.exit(run_subscriber_process(test_name, subscriber_id, duration_seconds))