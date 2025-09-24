#!/usr/bin/env python3
"""
Test event-driven notification system
"""
import sys
import time
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory, PyEventLoop

def test_event_driven_notifications():
    """Test event-driven notifications"""
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_event')

        print("Creating publisher and subscriber...")
        publisher = shm.create_publisher()
        subscriber = shm.create_subscriber()

        print("Creating event loop for subscriber...")
        event_loop = PyEventLoop('test_subscriber')
        event_fd = event_loop.get_event_fd()
        print(f"Event FD: {event_fd}")

        # Test notification mechanism
        print("Testing notification mechanism...")
        event_loop.notify()
        print("Notified event loop")

        # Wait for notification
        result = event_loop.wait_for_notification(None)
        print(f"Notification received: {result}")

        # Test basic pub/sub with event notifications
        print("Testing pub/sub with event notifications...")
        test_message = b"Event-driven test message"
        publisher.publish(test_message)

        # Wait a bit and then check for notification
        time.sleep(0.1)
        notification_result = event_loop.wait_for_notification(None)
        print(f"Pub/sub notification received: {notification_result}")

        # Try to receive the message
        received = subscriber.receive(timeout=1.0)
        if received:
            print(f"✅ Received message: {received}")
            if received == test_message:
                print("✅ Event-driven test PASSED!")
                return True
            else:
                print("❌ Data corruption detected")
                return False
        else:
            print("❌ Failed to receive message")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Testing event-driven notification system...")
    success = test_event_driven_notifications()
    if success:
        print("✅ Event-driven notification test completed successfully!")
    else:
        print("💥 Event-driven notification test FAILED!")