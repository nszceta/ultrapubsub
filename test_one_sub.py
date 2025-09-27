#!/usr/bin/env python3
"""
Test with one subscriber
"""
import ultrapubsub
import threading
import time

def subscriber_worker(subscriber, results):
    try:
        print("Subscriber waiting for message...")
        msg = subscriber.receive()
        print(f"Subscriber received: {msg}")
        results['received'] = msg
        results['success'] = True
    except Exception as e:
        print(f"Subscriber error: {e}")
        results['error'] = str(e)
        results['success'] = False

def test_one_subscriber():
    test_name = "/one_sub_test"

    print("Testing with one subscriber...")

    try:
        # Clean up
        try:
            ultrapubsub.cleanup_shared_memory(test_name)
        except:
            pass

        # Create publisher
        publisher = ultrapubsub.create_publisher(test_name)
        print("✅ Publisher created")

        # Register a subscriber through publisher
        subscriber_id = publisher.register_subscriber()
        print(f"✅ Registered subscriber with ID: {subscriber_id}")

        # Check subscriber count
        count = publisher.subscriber_count()
        print(f"✅ Subscriber count: {count}")

        # Create subscriber Python object
        subscriber = ultrapubsub.create_subscriber_with_id(test_name, subscriber_id)
        print("✅ Subscriber object created")

        # Start subscriber in background thread
        results = {}
        thread = threading.Thread(target=subscriber_worker, args=(subscriber, results))
        thread.start()

        # Give subscriber time to start
        time.sleep(0.1)

        # Broadcast message
        test_msg = b"Hello Subscriber!"
        print(f"📤 Broadcasting: {test_msg}")

        start_time = time.time()
        sequence = publisher.broadcast(test_msg)
        broadcast_time = time.time() - start_time

        print(f"✅ Broadcast completed in {broadcast_time:.3f}s, sequence: {sequence}")

        # Wait for subscriber to finish
        thread.join(timeout=5.0)

        if thread.is_alive():
            print("❌ Subscriber thread still running - hanging!")
            return

        # Check results
        if results.get('success'):
            received = results['received']
            if received == test_msg:
                print("✅ SUCCESS: Message transmitted correctly!")
            else:
                print(f"❌ FAIL: Message corrupted. Expected {test_msg}, got {received}")
        else:
            print(f"❌ FAIL: Subscriber failed with error: {results.get('error')}")

        # Cleanup
        ultrapubsub.cleanup_shared_memory(test_name)

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_one_subscriber()