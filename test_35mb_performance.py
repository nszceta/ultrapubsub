#!/usr/bin/env python3
"""
35MB broadcast performance envelope test
"""
import subprocess
import time
import json
import os

def run_35mb_performance_test():
    """Run 35MB broadcast performance test"""
    print("🚀 35MB Broadcast Performance Envelope Test")
    print("=" * 50)

    test_name = "/35mb_performance_test"
    message_size_mb = 35  # Original target size
    target_hz = 40  # Original target frequency
    duration_seconds = 10  # Test duration

    # Clean up any existing shared memory
    try:
        subprocess.run(['python', '-c', f'import ultrapubsub; ultrapubsub.cleanup_shared_memory("{test_name}")'],
                      timeout=5, capture_output=True)
    except:
        pass

    # Start subscriber in background
    subscriber_cmd = ['python', '-c', f'''
import ultrapubsub
import time
import json
import os

# Create subscriber
subscriber = ultrapubsub.create_subscriber_with_id("{test_name}", 0)
subscriber.register()

print("Subscriber ready for 35MB messages...")

# Receive messages
start_time = time.time()
messages_received = 0
total_bytes = 0
latencies = []

while time.time() - start_time < {duration_seconds}:
    msg_start = time.time()
    try:
        msg = subscriber.receive(timeout=1.0)
        if msg:
            msg_end = time.time()
            messages_received += 1
            total_bytes += len(msg)
            latencies.append((msg_end - msg_start) * 1000)

            # Log progress every 5 messages
            if messages_received % 5 == 0:
                elapsed = time.time() - start_time
                current_hz = messages_received / elapsed
                current_mbps = (total_bytes / elapsed) / (1024 * 1024)
                avg_latency = sum(latencies[-5:]) / min(5, len(latencies))
                print(f"Received {{messages_received}}: {{len(msg)/1024/1024:.1f}}MB, {{current_hz:.1f}} Hz, {{current_mbps:.1f}} MB/s, {{avg_latency:.1f}}ms")
    except:
        # Timeout expected
        pass

# Calculate final results
duration = time.time() - start_time
avg_latency = sum(latencies) / len(latencies) if latencies else 0
actual_hz = messages_received / duration
throughput_mbps = (total_bytes / duration) / (1024 * 1024)
throughput_gbps = throughput_mbps / 1024

result = {{
    "type": "subscriber",
    "messages_received": messages_received,
    "total_bytes": total_bytes,
    "total_mb": total_bytes / (1024 * 1024),
    "duration": duration,
    "actual_hz": actual_hz,
    "target_hz": {target_hz},
    "avg_latency_ms": avg_latency,
    "throughput_mbps": throughput_mbps,
    "throughput_gbps": throughput_gbps,
    "efficiency": (actual_hz / {target_hz}) * 100,
    "target_throughput_mbps": 1.4 * 1024,  # 1.4 GB/s target
    "target_efficiency": (throughput_mbps / (1.4 * 1024)) * 100
}}

print(json.dumps(result))
''']

    subscriber_proc = subprocess.Popen(subscriber_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Give subscriber time to start
    time.sleep(2)

    # Start publisher
    publisher_cmd = ['python', '-c', f'''
import ultrapubsub
import time
import json
import os

# Create publisher
publisher = ultrapubsub.create_publisher("{test_name}")

# Wait for subscriber
max_wait = 10.0
wait_start = time.time()
while publisher.subscriber_count() == 0 and (time.time() - wait_start) < max_wait:
    time.sleep(0.1)

if publisher.subscriber_count() == 0:
    print("No subscribers registered")
    exit(1)

print(f"Starting 35MB broadcasts to {{publisher.subscriber_count()}} subscribers...")

# Create 35MB test message
test_message = b'X' * ({message_size_mb} * 1024 * 1024)
print(f"Test message size: {{len(test_message)/1024/1024:.1f}} MB")

# Broadcast for specified duration
start_time = time.time()
end_time = start_time + {duration_seconds}
message_count = 0
broadcast_times = []

while time.time() < end_time:
    broadcast_start = time.time()

    # Broadcast the 35MB message
    publisher.broadcast(test_message)
    message_count += 1

    broadcast_end = time.time()
    broadcast_time = (broadcast_end - broadcast_start) * 1000
    broadcast_times.append(broadcast_time)

    # Log progress every 5 broadcasts
    if message_count % 5 == 0:
        elapsed = time.time() - start_time
        current_hz = message_count / elapsed
        avg_broadcast_time = sum(broadcast_times[-5:]) / min(5, len(broadcast_times))
        print(f"Broadcast {{message_count}}: {{broadcast_time:.1f}}ms avg, {{current_hz:.1f}} Hz")

# Calculate final results
duration = time.time() - start_time
actual_hz = message_count / duration
avg_broadcast_time = sum(broadcast_times) / len(broadcast_times) if broadcast_times else 0
total_bytes = message_count * len(test_message)
throughput_mbps = (total_bytes / duration) / (1024 * 1024)
throughput_gbps = throughput_mbps / 1024

result = {{
    "type": "publisher",
    "messages_sent": message_count,
    "message_size_mb": {message_size_mb},
    "total_bytes": total_bytes,
    "total_mb": total_bytes / (1024 * 1024),
    "duration": duration,
    "actual_hz": actual_hz,
    "target_hz": {target_hz},
    "avg_broadcast_time_ms": avg_broadcast_time,
    "throughput_mbps": throughput_mbps,
    "throughput_gbps": throughput_gbps,
    "efficiency": (actual_hz / {target_hz}) * 100,
    "target_throughput_mbps": 1.4 * 1024,  # 1.4 GB/s target
    "target_efficiency": (throughput_mbps / (1.4 * 1024)) * 100
}}

print(json.dumps(result))
''']

    publisher_proc = subprocess.Popen(publisher_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Wait for both processes
    try:
        publisher_proc.wait(timeout=duration_seconds + 10)
        subscriber_proc.wait(timeout=duration_seconds + 10)
    except subprocess.TimeoutExpired:
        publisher_proc.terminate()
        subscriber_proc.terminate()

    # Get results
    pub_stdout, pub_stderr = publisher_proc.communicate()
    sub_stdout, sub_stderr = subscriber_proc.communicate()

    print(f"\n📤 Publisher Output:")
    print(pub_stdout)
    if pub_stderr:
        print(f"Publisher stderr: {pub_stderr}")

    print(f"\n👥 Subscriber Output:")
    print(sub_stdout)
    if sub_stderr:
        print(f"Subscriber stderr: {sub_stderr}")

    # Parse results
    try:
        pub_result = json.loads(pub_stdout.strip())
        sub_result = json.loads(sub_stdout.strip())

        print("\n" + "=" * 70)
        print("📊 35MB BROADCAST PERFORMANCE ENVELOPE")
        print("=" * 70)

        print(f"\n🎯 TARGET PERFORMANCE:")
        print(f"   - Message size: {message_size_mb} MB")
        print(f"   - Target frequency: {target_hz} Hz")
        print(f"   - Target throughput: 1.4 GB/s (1,434 MB/s)")

        print(f"\n📈 PUBLISHER RESULTS:")
        print(f"   - Messages sent: {pub_result['messages_sent']:,}")
        print(f"   - Actual frequency: {pub_result['actual_hz']:.2f} Hz")
        print(f"   - Target efficiency: {pub_result['efficiency']:.1f}%")
        print(f"   - Avg broadcast time: {pub_result['avg_broadcast_time_ms']:.1f} ms")
        print(f"   - Throughput: {pub_result['throughput_mbps']:.0f} MB/s ({pub_result['throughput_gbps']:.3f} GB/s)")

        print(f"\n👥 SUBSCRIBER RESULTS:")
        print(f"   - Messages received: {sub_result['messages_received']:,}")
        print(f"   - Actual frequency: {sub_result['actual_hz']:.2f} Hz")
        print(f"   - Avg latency: {sub_result['avg_latency_ms']:.1f} ms")
        print(f"   - Throughput: {sub_result['throughput_mbps']:.0f} MB/s ({sub_result['throughput_gbps']:.3f} GB/s)")

        print(f"\n📊 PERFORMANCE ENVELOPE:")
        print(f"   - Target efficiency: {sub_result['target_efficiency']:.1f}%")
        print(f"   - Message loss: {((pub_result['messages_sent'] - sub_result['messages_received']) / pub_result['messages_sent'] * 100):.1f}%")

        # Performance assessment
        target_efficiency = sub_result['target_efficiency']
        if target_efficiency >= 100:
            print(f"\n🎉 OUTSTANDING! Exceeding 1.4 GB/s target!")
        elif target_efficiency >= 80:
            print(f"\n✅ EXCELLENT! Achieving {target_efficiency:.0f}% of 1.4 GB/s target!")
        elif target_efficiency >= 60:
            print(f"\n👍 GOOD! Achieving {target_efficiency:.0f}% of 1.4 GB/s target!")
        elif target_efficiency >= 40:
            print(f"\n⚠️  FAIR! Achieving {target_efficiency:.0f}% of 1.4 GB/s target - needs optimization")
        else:
            print(f"\n❌ POOR! Only {target_efficiency:.0f}% of 1.4 GB/s target - significant optimization needed")

        print("\n" + "=" * 70)
        return True

    except Exception as e:
        print(f"❌ Failed to parse results: {e}")
        return False

    finally:
        # Cleanup
        try:
            subprocess.run(['python', '-c', f'import ultrapubsub; ultrapubsub.cleanup_shared_memory("{test_name}")'],
                          timeout=5, capture_output=True)
        except:
            pass

if __name__ == "__main__":
    run_35mb_performance_test()