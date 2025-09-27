#!/usr/bin/env python3
"""
Minimal test to verify basic functionality without hanging
"""
import ultrapubsub

def test_minimal():
    test_name = "/minimal_test"

    print("Minimal test...")

    try:
        # Clean up
        try:
            ultrapubsub.cleanup_shared_memory(test_name)
        except:
            pass

        # Just create publisher and check it doesn't crash
        publisher = ultrapubsub.create_publisher(test_name)
        print("✅ Publisher created")

        # Check subscriber count
        count = publisher.subscriber_count()
        print(f"✅ Subscriber count: {count}")

        # Try to broadcast with no subscribers (should not hang)
        print("Testing broadcast with no subscribers...")
        test_msg = b"Test message"
        sequence = publisher.broadcast(test_msg)
        print(f"✅ Broadcast succeeded with sequence: {sequence}")

        # Cleanup
        ultrapubsub.cleanup_shared_memory(test_name)
        print("✅ Test completed successfully!")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_minimal()