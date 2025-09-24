#!/usr/bin/env python3
"""
Test message filtering functionality
"""
import sys
import time
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_message_filtering():
    """Test message filtering with prefix and size filters"""
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_filtering')

        print("Creating publisher and subscriber...")
        publisher = shm.create_publisher()
        subscriber = shm.create_subscriber()

        # Test messages
        messages = [
            b"IMPORTANT: System alert",
            b"DEBUG: Log message",
            b"INFO: Regular message",
            b"IMPORTANT: Another alert",
            b"ERROR: Critical error"
        ]

        print("Publishing test messages...")
        for msg in messages:
            publisher.publish(msg)
            print(f"Published: {msg.decode()}")

        print("\n--- Testing prefix filter ---")
        # Set prefix filter for "IMPORTANT" messages
        subscriber.set_prefix_filter(b"IMPORTANT")
        print("Set prefix filter for 'IMPORTANT' messages")

        received = []
        for _ in range(5):  # Try to receive up to 5 messages
            msg = subscriber.try_receive()
            if msg:
                received.append(msg)
                print(f"✅ Received filtered: {msg.decode()}")
            else:
                break

        # Should only receive IMPORTANT messages
        important_messages = [m for m in messages if m.startswith(b"IMPORTANT")]
        print(f"\nExpected {len(important_messages)} IMPORTANT messages, received {len(received)}")

        if len(received) == len(important_messages):
            print("✅ Prefix filter working correctly!")
        else:
            print("❌ Prefix filter not working correctly")
            return False

        # Clear filter and test size filter
        subscriber.clear_filter()
        print("\n--- Testing size filter ---")

        # First clear any remaining messages without filter
        print("Clearing remaining messages...")
        while subscriber.try_receive():
            pass

        # Set size filter for messages between 20 and 30 bytes
        subscriber.set_size_filter(20, 30)
        print("Set size filter for messages between 20-30 bytes")

        # Publish new test messages with specific sizes
        test_sizes = [
            b"short",  # 5 bytes
            b"this is a medium message",  # 24 bytes
            b"this is a very long message that should be filtered out",  # 50+ bytes
            b"another medium sized msg",  # 25 bytes
        ]

        print("Publishing size test messages...")
        for msg in test_sizes:
            publisher.publish(msg)
            print(f"Published ({len(msg)} bytes): {msg.decode()}")

        received_sizes = []
        for i in range(5):
            msg = subscriber.try_receive()
            if msg:
                received_sizes.append(len(msg))
                print(f"✅ Received ({len(msg)} bytes): {msg.decode()}")
            else:
                print(f"Attempt {i+1}: No message available")
            time.sleep(0.1)  # Small delay between attempts

        expected_sizes = [len(m) for m in test_sizes if 20 <= len(m) <= 30]
        print(f"\nExpected messages with sizes: {expected_sizes}")
        print(f"Received messages with sizes: {received_sizes}")

        if set(received_sizes) == set(expected_sizes):
            print("✅ Size filter working correctly!")
        else:
            print("❌ Size filter not working correctly")
            return False

        print("\n--- Testing filter clearing ---")
        subscriber.clear_filter()
        print("Cleared all filters")

        # Should now receive any remaining messages
        remaining = subscriber.try_receive()
        if remaining:
            print(f"✅ Received after filter clear: {remaining.decode()}")
        else:
            print("✅ No remaining messages (expected)")

        print("\n🎉 All filtering tests passed!")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Testing message filtering functionality...")
    success = test_message_filtering()
    if success:
        print("✅ Message filtering test completed successfully!")
    else:
        print("💥 Message filtering test FAILED!")