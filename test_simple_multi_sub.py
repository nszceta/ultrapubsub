#!/usr/bin/env python3
"""
Simple test for multi-subscriber communication
"""

import sys
import os
import time
import multiprocessing

# Add the current directory to Python path to import the module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import ultrapubsub
    print("✅ Successfully imported ultrapubsub")
except ImportError as e:
    print(f"❌ Failed to import ultrapubsub: {e}")
    sys.exit(1)


def simple_subscriber(subscriber_id: int, shm_name: str, result_queue: multiprocessing.Queue):
    """Simple subscriber process"""
    try:
        print(f"📡 Subscriber {subscriber_id}: Starting...")

        # Give publisher time to create shared memory
        time.sleep(2.0)

        # Attach to shared memory
        shm = ultrapubsub.SharedMemory.attach(shm_name)
        subscriber = shm.create_subscriber()

        print(f"✅ Subscriber {subscriber_id}: Attached successfully")

        # Try to receive one message
        message = subscriber.receive(timeout=5.0)
        if message:
            print(f"✅ Subscriber {subscriber_id}: Received message: {len(message)} bytes")
            result_queue.put({'subscriber_id': subscriber_id, 'status': 'success', 'size': len(message)})
        else:
            print(f"❌ Subscriber {subscriber_id}: No message received")
            result_queue.put({'subscriber_id': subscriber_id, 'status': 'timeout'})

    except Exception as e:
        print(f"❌ Subscriber {subscriber_id}: Error - {e}")
        result_queue.put({'subscriber_id': subscriber_id, 'status': 'error', 'error': str(e)})


def simple_publisher(shm_name: str):
    """Simple publisher process"""
    try:
        print(f"📤 Publisher: Starting...")

        # Create shared memory
        shm = ultrapubsub.SharedMemory(shm_name, entries=32)
        publisher = shm.create_publisher()

        print(f"✅ Publisher: Shared memory created")

        # Give subscribers time to attach
        time.sleep(3.0)

        # Send a simple message
        test_message = b"Hello from publisher to all subscribers!"
        publisher.publish(test_message)
        print(f"✅ Publisher: Sent message: {len(test_message)} bytes")

        # Wait a bit for subscribers to receive
        time.sleep(2.0)
        print(f"✅ Publisher: Done")

    except Exception as e:
        print(f"❌ Publisher: Error - {e}")


def test_simple_multi_subscriber():
    """Test simple multi-subscriber scenario"""
    print("\n🧪 Testing simple multi-subscriber communication...")

    num_subscribers = 3  # Start with 3 subscribers
    shm_name = "simple_test"
    result_queue = multiprocessing.Queue()

    # Start publisher
    publisher_process = multiprocessing.Process(
        target=simple_publisher,
        args=(shm_name,)
    )
    publisher_process.start()
    print(f"📤 Started publisher")

    # Start subscribers
    subscriber_processes = []
    for i in range(num_subscribers):
        process = multiprocessing.Process(
            target=simple_subscriber,
            args=(i, shm_name, result_queue)
        )
        process.start()
        subscriber_processes.append(process)
        print(f"📡 Started subscriber {i}")

    # Wait for all processes to finish
    publisher_process.join()
    for process in subscriber_processes:
        process.join()

    # Collect results
    results = []
    for _ in range(num_subscribers):
        try:
            result = result_queue.get(timeout=5.0)
            results.append(result)
        except:
            results.append({'subscriber_id': len(results), 'status': 'timeout'})

    # Print results
    print(f"\n📊 Results:")
    successful = 0
    for result in results:
        status_symbol = "✅" if result['status'] == 'success' else "❌"
        print(f"   {status_symbol} Subscriber {result['subscriber_id']}: {result['status']}")
        if result['status'] == 'success':
            successful += 1

    print(f"\n📊 Summary: {successful}/{num_subscribers} subscribers successful")

    return successful == num_subscribers


if __name__ == "__main__":
    success = test_simple_multi_subscriber()
    sys.exit(0 if success else 1)