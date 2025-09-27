#!/usr/bin/env python3
"""
Test the pthread mutex implementation with actual broadcast
"""
import time
import ultrapubsub

def test_broadcast():
    test_name = "/broadcast_test"

    print("Testing broadcast with pthread mutex...")

    try:
        # Clean up
        try:
            ultrapubsub.cleanup_shared_memory(test_name)
        except:
            pass

        # Create publisher
        publisher = ultrapubsub.create_publisher(test_name)
        print(f"✅ Publisher created, subscriber count: {publisher.subscriber_count()}")

        # Create and register subscribers
        subscribers = []
        for i in range(3):
            subscriber = ultrapubsub.create_subscriber_with_id(test_name, i)
            subscriber.register()
            subscribers.append(subscriber)
            print(f"✅ Subscriber {i} registered")

        print(f"✅ Publisher subscriber count: {publisher.subscriber_count()}")

        # Test broadcast
        test_message = b"Hello from pthread mutex test!"
        print(f"📤 Broadcasting: {test_message.decode()}")

        # Start receiving threads first
        import threading
        receive_results = [None] * len(subscribers)

        def receive_thread(subscriber, index):
            try:
                received = subscriber.receive()
                receive_results[index] = received
                print(f"✅ Subscriber {index} received: {received.decode()}")
            except Exception as e:
                print(f"❌ Subscriber {index} error: {e}")
                receive_results[index] = e

        # Start receive threads
        receive_threads = []
        for i, subscriber in enumerate(subscribers):
            thread = threading.Thread(target=receive_thread, args=(subscriber, i), daemon=True)
            thread.start()
            receive_threads.append(thread)

        # Small delay to let receivers start
        time.sleep(0.1)

        # Broadcast from main thread
        try:
            publisher.broadcast(test_message)
            print("✅ Broadcast completed")
        except Exception as e:
            print(f"❌ Broadcast error: {e}")

        # Wait for all receivers
        for i, thread in enumerate(receive_threads):
            thread.join(timeout=5.0)
            if thread.is_alive():
                print(f"❌ Subscriber {i} timed out")
            elif isinstance(receive_results[i], Exception):
                print(f"❌ Subscriber {i} failed: {receive_results[i]}")

        # Cleanup
        for subscriber in subscribers:
            subscriber.deregister()

        ultrapubsub.cleanup_shared_memory(test_name)
        print("✅ Test completed successfully")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_broadcast()