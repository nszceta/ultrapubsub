#!/usr/bin/env python3
"""
Simple single-threaded test for pthread mutex implementation
"""
import time
import ultrapubsub

def test_simple_broadcast():
    test_name = "/simple_broadcast_test"

    print("Testing simple broadcast with pthread mutex...")

    try:
        # Clean up
        try:
            ultrapubsub.cleanup_shared_memory(test_name)
        except:
            pass

        # Create publisher
        publisher = ultrapubsub.create_publisher(test_name)
        print(f"✅ Publisher created")

        # Create and register subscribers
        subscribers = []
        for i in range(2):
            subscriber = ultrapubsub.create_subscriber_with_id(test_name, i)
            subscriber.register()
            subscribers.append(subscriber)
            print(f"✅ Subscriber {i} registered")

        print(f"✅ Publisher subscriber count: {publisher.subscriber_count()}")

        # Test broadcast - subscribers will receive when we call receive()
        test_message = b"Hello from simple test!"
        print(f"📤 Broadcasting: {test_message.decode()}")

        # Broadcast
        publisher.broadcast(test_message)
        print("✅ Broadcast completed")

        # Now receive from each subscriber
        for i, subscriber in enumerate(subscribers):
            print(f"📥 Subscriber {i} receiving...")
            received = subscriber.receive()
            print(f"✅ Subscriber {i} received: {received.decode()}")

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
    test_simple_broadcast()