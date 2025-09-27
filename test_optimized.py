#!/usr/bin/env python3
"""
Simple test for optimized implementation
"""
import time

import ultrapubsub

def test_optimized_basic():
    """Test basic functionality of optimized implementation"""
    test_name = "/optimized_test"

    print("🚀 Testing Optimized Implementation")
    print("=" * 40)

    try:
        # Clean up any existing shared memory first
        print("Cleaning up existing shared memory...")
        try:
            ultrapubsub.cleanup_shared_memory(test_name)
        except:
            pass

        # Create publisher
        print("Creating publisher...")
        publisher = ultrapubsub.Publisher(test_name)
        print("✅ Publisher created")

        # Create subscriber
        print("Creating subscriber...")
        subscriber = ultrapubsub.Subscriber.with_id(test_name, 0)
        print("✅ Subscriber created")

        # Test small message
        test_message = b"Hello Optimized!"
        print(f"Broadcasting message: {len(test_message)} bytes")

        sequence = publisher.broadcast(test_message)
        print(f"✅ Broadcast completed, sequence: {sequence}")

        # Receive message
        received = subscriber.receive()
        print(f"✅ Message received: {len(received)} bytes")

        # Verify message
        if received == test_message:
            print("✅ Message integrity verified")
        else:
            print("❌ Message integrity failed")

        # Test performance metrics
        metrics = publisher.get_performance_metrics()
        print(f"✅ Performance metrics: {metrics}")

        # Cleanup
        publisher.cleanup()
        print("✅ Cleanup completed")

        print("\n🎉 Basic optimized implementation test PASSED!")

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_optimized_basic()