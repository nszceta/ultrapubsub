#!/usr/bin/env python3
"""
Longer running performance test for profiling
"""
import sys
import os
import time
import multiprocessing
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def publisher_process(name, duration):
    """Publisher process that runs for specified duration"""
    shm = SharedMemory(name)
    publisher = shm.create_publisher()

    # Create test payload
    payload = b'X' * (35 * 1024 * 1024)  # 35MB

    start_time = time.time()
    count = 0

    print(f"📤 Publisher started, will run for {duration} seconds...")

    while time.time() - start_time < duration:
        seq = publisher.publish(payload)
        count += 1

        # Target 40Hz = 25ms per message
        time.sleep(0.025)

    print(f"📤 Publisher completed: {count} messages in {duration}s")
    return count

def subscriber_process(name, duration, sub_id):
    """Subscriber process"""
    shm = SharedMemory(name)
    subscriber = shm.create_subscriber()

    start_time = time.time()
    count = 0
    total_latency = 0
    max_latency = 0

    print(f"📡 Subscriber {sub_id} started...")

    while time.time() - start_time < duration:
        msg_start = time.time()
        data = subscriber.receive(timeout=1.0)

        if data:
            msg_end = time.time()
            latency = (msg_end - msg_start) * 1000  # Convert to ms
            total_latency += latency
            max_latency = max(max_latency, latency)
            count += 1

            if count % 5 == 0:
                print(f"📡 Sub {sub_id}: {count} msgs, avg latency: {total_latency/count:.1f}ms")

    print(f"📡 Subscriber {sub_id} completed: {count} messages")
    if count > 0:
        print(f"   Avg latency: {total_latency/count:.1f}ms, Max: {max_latency:.1f}ms")

    return count

if __name__ == "__main__":
    name = "profile_test"
    duration = 30  # 30 seconds for better profiling

    print(f"🚀 UltraPubSub Profiling Test ({duration}s)")
    print("=" * 50)

    # Start subscribers
    subscribers = []
    for i in range(3):  # Fewer subscribers for cleaner profiling
        p = multiprocessing.Process(target=subscriber_process, args=(name, duration, i))
        p.start()
        subscribers.append(p)
        time.sleep(0.5)  # Stagger start

    # Start publisher
    pub_process = multiprocessing.Process(target=publisher_process, args=(name, duration))
    pub_process.start()

    # Wait for completion
    pub_process.join()
    for sub in subscribers:
        sub.join()

    print("✅ Profiling test completed")