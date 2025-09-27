#!/usr/bin/env python3
"""
Simple test to debug subscriber registration
"""
import time
import ultrapubsub

def test_registration():
    test_name = "/reg_test"

    print("Testing subscriber registration...")

    try:
        # Clean up
        try:
            ultrapubsub.cleanup_shared_memory(test_name)
        except:
            pass

        # Create publisher
        publisher = ultrapubsub.create_publisher(test_name)
        print(f"✅ Publisher created, initial subscriber count: {publisher.subscriber_count()}")

        # Create and register subscribers one by one
        for i in range(6):
            subscriber = ultrapubsub.create_subscriber_with_id(test_name, i)
            print(f"✅ Subscriber {i} created")

            subscriber.register()
            print(f"✅ Subscriber {i} registered")

            time.sleep(0.1)  # Small delay

            current_count = publisher.subscriber_count()
            print(f"   Current subscriber count: {current_count}")

        print(f"✅ Final subscriber count: {publisher.subscriber_count()}")

        # Cleanup
        ultrapubsub.cleanup_shared_memory(test_name)
        print("✅ Test completed successfully")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_registration()