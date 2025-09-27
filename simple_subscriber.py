#!/usr/bin/env python3
"""
Simple subscriber process for UltraPubSub testing
"""
import sys
import time
import ultrapubsub

def main():
    if len(sys.argv) != 3:
        print("Usage: python subscriber_process.py <test_name> <subscriber_id>")
        sys.exit(1)

    test_name = sys.argv[1]
    subscriber_id = int(sys.argv[2])

    print(f"Subscriber {subscriber_id} starting for test: {test_name}")

    try:
        # Create subscriber
        subscriber = ultrapubsub.create_subscriber_with_id(test_name, subscriber_id)
        print(f"✅ Subscriber {subscriber_id} created")

        # Register the subscriber
        subscriber.register()
        print(f"✅ Subscriber {subscriber_id} registered")

        # Verify registration worked
        retries = 0
        max_retries = 10
        while True:
            try:
                # Check if we have any new messages to verify connection
                if subscriber.has_messages():
                    print(f"✅ Subscriber {subscriber_id} connection verified")
                    break
                retries += 1
                if retries >= max_retries:
                    print(f"⚠️  Subscriber {subscriber_id} connection verification timeout")
                    break
                time.sleep(0.1)
            except:
                retries += 1
                if retries >= max_retries:
                    print(f"⚠️  Subscriber {subscriber_id} connection verification timeout")
                    break
                time.sleep(0.1)

        # Wait for and receive messages
        message_count = 0
        start_time = time.time()

        print(f"Subscriber {subscriber_id} waiting for messages...")

        while message_count < 10:  # Expect 10 messages
            try:
                received = subscriber.receive()
                message_count += 1
                elapsed = time.time() - start_time
                print(f"📥 Subscriber {subscriber_id} received message {message_count}: {received.decode()} "
                      f"(elapsed: {elapsed:.2f}s)")
            except Exception as e:
                print(f"❌ Subscriber {subscriber_id} error receiving: {e}")
                break

        print(f"✅ Subscriber {subscriber_id} completed! Received {message_count} messages")

    except Exception as e:
        print(f"❌ Subscriber {subscriber_id} error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()