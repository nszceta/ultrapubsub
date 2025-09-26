#!/usr/bin/env python3
"""
Test performance with 6 subscribers to match project requirements
"""
import sys
import time
import os
sys.path.insert(0, 'python')

import ultrapubsub
import multiprocessing as mp

def subscriber_process(subscriber_id, test_name, results_queue):
    """Subscriber process for performance testing"""
    try:
        subscriber = ultrapubsub.Subscriber.with_id(test_name, subscriber_id)

        start_time = time.time()
        messages_received = 0

        # Test for 10 seconds
        while time.time() - start_time < 10:
            msg = subscriber.receive(timeout=1)
            if msg:
                messages_received += 1

        results_queue.put({
            'subscriber_id': subscriber_id,
            'messages_received': messages_received,
            'duration': time.time() - start_time
        })

    except Exception as e:
        results_queue.put({
            'subscriber_id': subscriber_id,
            'error': str(e)
        })

def test_target_performance():
    """Test performance with 6 subscribers (project requirement)"""
    test_name = "/target_performance_test"

    print("🚀 UltraPubSub Target Performance Test")
    print("=" * 60)
    print("📊 Configuration: 6 subscribers, 35MB messages, 10s duration")

    # Message size exactly as specified in project
    message_size = 35 * 1024 * 1024  # 35MB
    test_data = b'A' * message_size

    # Create publisher
    publisher = ultrapubsub.Publisher(test_name)

    # Start 6 subscriber processes
    subscriber_processes = []
    results_queue = mp.Queue()

    print(f"📡 Starting 6 subscriber processes...")
    for i in range(6):
        proc = mp.Process(target=subscriber_process, args=(i, test_name, results_queue))
        subscriber_processes.append(proc)
        proc.start()
        print(f"   Started subscriber {i}")

    # Wait for all subscribers to register
    print(f"⏳ Waiting for subscribers to register...")
    start_time = time.time()
    while publisher.subscriber_count() < 6:
        if time.time() - start_time > 30:
            print(f"❌ Timeout waiting for subscribers, got {publisher.subscriber_count()}")
            break
        time.sleep(0.1)

    print(f"✅ {publisher.subscriber_count()} subscribers registered")

    # Start broadcasting
    print(f"📤 Starting broadcast test...")
    start_time = time.time()
    messages_sent = 0

    # Broadcast for 10 seconds with minimal delay
    while time.time() - start_time < 10:
        publisher.broadcast(test_data)
        messages_sent += 1
        # Minimal delay - just enough to prevent overwhelming
        time.sleep(0.001)

    broadcast_duration = time.time() - start_time

    print(f"✅ Broadcast completed: {messages_sent} messages in {broadcast_duration:.2f}s")

    # Wait for all subscribers to finish
    print(f"⏳ Waiting for subscribers to complete...")
    for proc in subscriber_processes:
        proc.join(timeout=15)
        if proc.is_alive():
            proc.terminate()
            proc.join()

    # Collect results
    results = []
    while not results_queue.empty():
        try:
            result = results_queue.get_nowait()
            results.append(result)
        except:
            break

    # Analyze results
    print(f"\n📊 Results Analysis:")
    print(f"   Messages sent: {messages_sent}")
    print(f"   Broadcast duration: {broadcast_duration:.2f}s")
    print(f"   Broadcast rate: {messages_sent / broadcast_duration:.2f} Hz")

    # Calculate total throughput
    total_messages_received = 0
    for result in results:
        if 'messages_received' in result:
            total_messages_received += result['messages_received']
            print(f"   Subscriber {result['subscriber_id']}: {result['messages_received']} messages")
        else:
            print(f"   Subscriber {result['subscriber_id']}: ERROR - {result.get('error', 'Unknown error')}")

    # Calculate throughput in GB/s
    total_bytes = total_messages_received * message_size
    total_duration = max(r.get('duration', broadcast_duration) for r in results if 'duration' in r)
    throughput_gb_s = total_bytes / total_duration / (1024 * 1024 * 1024)

    print(f"\n📈 Performance Metrics:")
    print(f"   Total messages received: {total_messages_received}")
    print(f"   Average per subscriber: {total_messages_received / 6:.1f}")
    print(f"   Total throughput: {throughput_gb_s:.3f} GB/s")
    print(f"   Per subscriber throughput: {throughput_gb_s / 6:.3f} GB/s")

    # Target analysis
    target_throughput = 0.8  # 800 MB/s as specified in project plan
    target_frequency = 40   # 40 Hz as specified

    achieved_frequency = messages_sent / broadcast_duration

    print(f"\n🎯 Target Analysis:")
    print(f"   Target throughput: {target_throughput:.1f} GB/s")
    print(f"   Achieved throughput: {throughput_gb_s:.3f} GB/s")
    print(f"   Target frequency: {target_frequency} Hz")
    print(f"   Achieved frequency: {achieved_frequency:.1f} Hz")

    if throughput_gb_s >= target_throughput:
        print(f"   ✅ THROUGHPUT TARGET MET")
    else:
        gap = target_throughput - throughput_gb_s
        print(f"   ❌ {gap:.3f} GB/s below target")

    if achieved_frequency >= target_frequency:
        print(f"   ✅ FREQUENCY TARGET MET")
    else:
        gap = target_frequency - achieved_frequency
        print(f"   ❌ {gap:.1f} Hz below target")

    # Cleanup
    try:
        publisher.cleanup()
    except:
        pass

    try:
        ultrapubsub.cleanup_shared_memory(test_name)
    except:
        pass

if __name__ == "__main__":
    test_target_performance()