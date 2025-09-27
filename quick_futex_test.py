#!/usr/bin/env python3
"""
Quick futex performance demonstration
"""
import subprocess
import time
import json
import os

def run_quick_test():
    """Run a quick futex performance test"""
    print("🚀 Quick Futex Performance Test")
    print("=" * 40)

    test_name = "/quick_futex_test"
    message_size_mb = 1
    target_hz = 50
    message_count = 10

    # Clean up any existing shared memory
    try:
        subprocess.run(['python', '-c', f'import ultrapubsub; ultrapubsub.cleanup_shared_memory("{test_name}")'],
                      timeout=5, capture_output=True)
    except:
        pass

    # Start publisher first to create shared memory
    publisher_cmd = ['python', '-c', f'''
import ultrapubsub
import time
import json
import os

# Create publisher
publisher = ultrapubsub.create_publisher("{test_name}")

# Wait for subscriber
while publisher.subscriber_count() == 0:
    time.sleep(0.1)

print(f"Starting to broadcast {{publisher.subscriber_count()}} subscribers...")

# Broadcast messages
start_time = time.time()
test_message = b'X' * ({message_size_mb} * 1024 * 1024)

for i in range({message_count}):
    broadcast_start = time.time()
    publisher.broadcast(test_message)
    broadcast_time = (time.time() - broadcast_start) * 1000
    print(f"Broadcast {{i+1}} completed in {{broadcast_time:.2f}}ms")

duration = time.time() - start_time
actual_hz = {message_count} / duration

result = {{
    "type": "publisher",
    "messages_sent": {message_count},
    "duration": duration,
    "actual_hz": actual_hz,
    "target_hz": {target_hz},
    "efficiency": (actual_hz / {target_hz}) * 100
}}

print(json.dumps(result))
''']

    publisher_proc = subprocess.Popen(publisher_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Give publisher time to create shared memory
    time.sleep(1)

    # Start subscriber
    subscriber_cmd = ['python', '-c', f'''
import ultrapubsub
import time
import json
import os

# Create subscriber
subscriber = ultrapubsub.create_subscriber_with_id("{test_name}", 0)
subscriber.register()

print("Subscriber ready, waiting for messages...")

# Receive messages
start_time = time.time()
messages_received = 0
total_bytes = 0
latencies = []

for i in range({message_count}):
    msg_start = time.time()
    msg = subscriber.receive(timeout=5.0)
    if msg:
        msg_end = time.time()
        messages_received += 1
        total_bytes += len(msg)
        latencies.append((msg_end - msg_start) * 1000)
        print(f"Received message {{messages_received}}: {{len(msg)}} bytes")

# Calculate results
duration = time.time() - start_time
avg_latency = sum(latencies) / len(latencies) if latencies else 0
actual_hz = messages_received / duration

result = {{
    "type": "subscriber",
    "messages_received": messages_received,
    "total_bytes": total_bytes,
    "duration": duration,
    "actual_hz": actual_hz,
    "avg_latency_ms": avg_latency,
    "throughput_mbps": (total_bytes / duration) / (1024 * 1024)
}}

print(json.dumps(result))
''']

    subscriber_proc = subprocess.Popen(subscriber_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    # Give subscriber time to start
    time.sleep(1)

    # Wait for both processes
    try:
        publisher_proc.wait(timeout=10)
        subscriber_proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        publisher_proc.terminate()
        subscriber_proc.terminate()

    # Get results
    pub_stdout, pub_stderr = publisher_proc.communicate()
    sub_stdout, sub_stderr = subscriber_proc.communicate()

    print(f"\n📤 Publisher output:")
    print(pub_stdout)
    if pub_stderr:
        print(f"Publisher errors: {pub_stderr}")

    print(f"\n👥 Subscriber output:")
    print(sub_stdout)
    if sub_stderr:
        print(f"Subscriber errors: {sub_stderr}")

    # Parse results
    try:
        pub_result = json.loads(pub_stdout.strip())
        sub_result = json.loads(sub_stdout.strip())

        print("\n" + "=" * 50)
        print("📊 FUTEX PERFORMANCE RESULTS")
        print("=" * 50)

        print(f"\n🎯 TARGET: {target_hz} Hz with {message_size_mb}MB messages")
        print(f"📈 PUBLISHER: {pub_result['actual_hz']:.2f} Hz ({pub_result['efficiency']:.1f}% efficiency)")
        print(f"👥 SUBSCRIBER: {sub_result['actual_hz']:.2f} Hz, {sub_result['avg_latency_ms']:.2f}ms avg latency")
        print(f"💾 THROUGHPUT: {sub_result['throughput_mbps']:.2f} MB/s")

        if pub_result['efficiency'] >= 90:
            print("\n🎉 EXCELLENT! Futex optimization achieving high efficiency!")
        elif pub_result['efficiency'] >= 70:
            print("\n✅ GOOD! Futex optimization working well!")
        else:
            print("\n⚠️  Room for improvement in futex optimization")

        print("=" * 50)
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
    run_quick_test()