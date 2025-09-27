#!/usr/bin/env python3
"""
Debug test to see what's happening
"""
import ultrapubsub
import time

def test_debug():
    test_name = "/debug_test"

    print("Debug test...")

    try:
        # Clean up
        try:
            ultrapubsub.cleanup_shared_memory(test_name)
        except:
            pass

        # Create publisher
        publisher = ultrapubsub.create_publisher(test_name)
        print("✅ Publisher created")

        # Check subscriber count
        count = publisher.subscriber_count()
        print(f"Initial subscriber count: {count}")

        # Create subscriber
        subscriber = ultrapubsub.create_subscriber_with_id(test_name, 0)
        print("✅ Subscriber created")

        # Check subscriber count again
        count = publisher.subscriber_count()
        print(f"After subscriber creation: {count}")

        # Manually register subscriber
        print("Registering subscriber...")
        subscriber.register()
        print("✅ Subscriber registered")

        # Check subscriber count again
        count = publisher.subscriber_count()
        print(f"After registration: {count}")

        if count == 0:
            print("❌ Subscriber not being registered properly")
            return

        # Send a message
        test_msg = b"Hello World"
        print(f"📤 Sending message: {test_msg}")

        start_time = time.time()
        sequence = publisher.broadcast(test_msg)
        broadcast_time = time.time() - start_time

        print(f"✅ Message sent with sequence: {sequence}")
        print(f"⏱️  Broadcast took: {broadcast_time:.3f}s")

        # Receive message
        start_time = time.time()
        received = subscriber.receive()
        receive_time = time.time() - start_time

        print(f"📥 Received message: {received}")
        print(f"⏱️  Receive took: {receive_time:.3f}s")

        if received == test_msg:
            print("✅ SUCCESS: Message received correctly!")
        else:
            print(f"❌ FAIL: Expected {test_msg}, got {received}")

        # Cleanup
        ultrapubsub.cleanup_shared_memory(test_name)

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_debug()