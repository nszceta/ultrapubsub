#!/usr/bin/env python3
"""
Test the new zero-copy pool performance
"""
import sys
import time
import multiprocessing
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def publisher_process(name, duration):
    """Publisher using zero-copy pool"""
    import ctypes
    shm = SharedMemory(name)
    publisher = shm.create_publisher()

    # Pre-allocate 35MB payload
    payload = b'X' * (35 * 1024 * 1024)

    start_time = time.time()
    count = 0

    print(f"📤 Zero-copy publisher started, will run for {duration} seconds...")

    while time.time() - start_time < duration:
        try:
            # Allocate pool slot
            slot, ptr = publisher.allocate_pool_slot()

            # Copy data directly to pool slot
            pool_mem = ctypes.cast(ptr, ctypes.POINTER(ctypes.c_char * 35 * 1024 * 1024))
            ctypes.memmove(pool_mem, payload, len(payload))

            # Publish the pool slot
            seq = publisher.publish_pool_slot(slot, len(payload))
            count += 1

            if count % 5 == 0:
                elapsed = time.time() - start_time
                rate = count / elapsed
                print(f"  Published {count} messages, rate: {rate:.1f} Hz")

        except Exception as e:
            print(f"  Error publishing: {e}")
            break

    print(f"📤 Zero-copy publisher completed: {count} messages in {duration}s")
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
    name = "pool_test"
    duration = 10  # 10 seconds

    print(f"🚀 UltraPubSub Zero-Copy Pool Performance Test ({duration}s)")
    print("=" * 60)

    # Start subscribers
    subscribers = []
    for i in range(3):
        p = multiprocessing.Process(target=subscriber_process, args=(name, duration, i))
        p.start()
        subscribers.append(p)
        time.sleep(0.5)

    # Start publisher
    pub_process = multiprocessing.Process(target=publisher_process, args=(name, duration))
    pub_process.start()

    # Wait for completion
    pub_process.join()
    for sub in subscribers:
        sub.join()

    print("✅ Zero-copy pool test completed")