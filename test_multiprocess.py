#!/usr/bin/env python3
"""
Test with multiprocessing instead of threading
"""
import ultrapubsub
import multiprocessing as mp
import time

def subscriber_process(test_name, subscriber_id, result_queue):
    try:
        # Create subscriber in child process
        subscriber = ultrapubsub.create_subscriber_with_id(test_name, subscriber_id)
        print(f"Subscriber {subscriber_id} started")

        # Receive message
        msg = subscriber.receive()
        print(f"Subscriber {subscriber_id} received: {len(msg)} bytes")

        # Send result back
        result_queue.put({
            'subscriber_id': subscriber_id,
            'success': True,
            'message_length': len(msg)
        })

    except Exception as e:
        print(f"Subscriber {subscriber_id} error: {e}")
        result_queue.put({
            'subscriber_id': subscriber_id,
            'success': False,
            'error': str(e)
        })

def test_multiprocess():
    test_name = "/mp_test"

    print("Testing with multiprocessing...")

    try:
        # Clean up
        try:
            ultrapubsub.cleanup_shared_memory(test_name)
        except:
            pass

        # Create publisher
        publisher = ultrapubsub.create_publisher(test_name)
        print("✅ Publisher created")

        # Register subscriber
        subscriber_id = publisher.register_subscriber()
        print(f"✅ Registered subscriber ID: {subscriber_id}")

        # Create result queue
        result_queue = mp.Queue()

        # Start subscriber process
        print("🚀 Starting subscriber process...")
        sub_process = mp.Process(
            target=subscriber_process,
            args=(test_name, subscriber_id, result_queue)
        )
        sub_process.start()

        # Give subscriber time to start and connect
        time.sleep(0.5)

        # Check subscriber count
        count = publisher.subscriber_count()
        print(f"✅ Subscriber count: {count}")

        # Broadcast message
        test_msg = b"Hello Multiprocess Subscriber!" * 1000  # Make it larger
        print(f"📤 Broadcasting {len(test_msg)} bytes...")

        start_time = time.time()
        sequence = publisher.broadcast(test_msg)
        broadcast_time = time.time() - start_time

        print(f"✅ Broadcast completed in {broadcast_time:.3f}s")

        # Wait for subscriber process
        sub_process.join(timeout=10.0)

        if sub_process.is_alive():
            print("❌ Subscriber process still running - terminating!")
            sub_process.terminate()
            sub_process.join()
            return

        # Get result
        try:
            result = result_queue.get(timeout=1.0)
            if result['success']:
                print(f"✅ SUCCESS: Subscriber {result['subscriber_id']} received {result['message_length']} bytes")
                if result['message_length'] == len(test_msg):
                    print("✅ Message length matches!")
                else:
                    print(f"❌ Message length mismatch: expected {len(test_msg)}, got {result['message_length']}")
            else:
                print(f"❌ Subscriber failed: {result['error']}")
        except:
            print("❌ No result from subscriber")

        # Cleanup
        ultrapubsub.cleanup_shared_memory(test_name)

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_multiprocess()