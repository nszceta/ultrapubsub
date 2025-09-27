#!/usr/bin/env python3
"""
Test using publisher's register_subscriber method
"""
import ultrapubsub
import time

def test_pub_reg():
    test_name = "/pub_reg_test"

    print("Testing publisher registration...")

    try:
        # Clean up
        try:
            ultrapubsub.cleanup_shared_memory(test_name)
        except:
            pass

        # Create publisher
        publisher = ultrapubsub.create_publisher(test_name)
        print("✅ Publisher created")

        # Register subscriber through publisher (Rust-level)
        print("Registering subscriber through publisher...")
        subscriber_id = publisher.register_subscriber()
        print(f"✅ Subscriber registered with ID: {subscriber_id}")

        # Create subscriber
        subscriber = ultrapubsub.create_subscriber_with_id(test_name, 0)
        print("✅ Subscriber created")

        # Check subscriber count
        count = publisher.subscriber_count()
        print(f"Subscriber count: {count}")

        if count > 0:
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
        else:
            print("❌ No subscribers registered, broadcast would hang")

        # Cleanup
        ultrapubsub.cleanup_shared_memory(test_name)

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_pub_reg()