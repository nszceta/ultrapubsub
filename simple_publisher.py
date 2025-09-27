#!/usr/bin/env python3
"""
Simple publisher process for UltraPubSub testing
"""
import sys
import time
import ultrapubsub

def main():
    if len(sys.argv) != 3:
        print("Usage: python publisher_process.py <test_name> <message_count>")
        sys.exit(1)

    test_name = sys.argv[1]
    message_count = int(sys.argv[2])

    print(f"Publisher process starting for test: {test_name}")

    try:
        # Clean up any existing shared memory
        try:
            ultrapubsub.cleanup_shared_memory(test_name)
        except:
            pass

        # Create publisher
        publisher = ultrapubsub.create_publisher(test_name)
        print("✅ Publisher created")

        # Wait for subscribers to register
        print("Waiting for subscribers to register...")
        while publisher.subscriber_count() < 6:
            print(f"Current subscriber count: {publisher.subscriber_count()}")
            time.sleep(0.5)

        print(f"✅ All 6 subscribers registered!")

        # Send messages
        for i in range(message_count):
            message = f"Message {i+1} from publisher".encode()
            sequence = publisher.broadcast(message)
            print(f"📤 Sent message {i+1}/{message_count} with sequence {sequence}")
            time.sleep(0.1)  # Small delay between messages

        print("✅ All messages sent!")

        # Keep publisher alive for a bit longer
        time.sleep(2)

        # Cleanup
        ultrapubsub.cleanup_shared_memory(test_name)
        print("✅ Cleanup completed")

    except Exception as e:
        print(f"❌ Publisher error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()