#!/usr/bin/env python3
"""
Simple test to verify basic communication works
"""
import ultrapubsub
import time

def test_basic():
    test_name = "/simple_test"

    print("Testing basic communication...")

    try:
        # Clean up
        try:
            ultrapubsub.cleanup_shared_memory(test_name)
        except:
            pass

        # Create publisher
        publisher = ultrapubsub.create_publisher(test_name)
        print("✅ Publisher created")

        # Create subscriber
        subscriber = ultrapubsub.create_subscriber_with_id(test_name, 0)
        print("✅ Subscriber created")

        # Send a message
        test_msg = b"Hello World"
        print(f"📤 Sending message: {test_msg}")

        sequence = publisher.broadcast(test_msg)
        print(f"✅ Message sent with sequence: {sequence}")

        # Wait a moment for message to be available
        time.sleep(0.1)

        # Receive message
        received = subscriber.receive()
        print(f"📥 Received message: {received}")

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
    test_basic()