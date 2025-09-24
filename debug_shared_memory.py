#!/usr/bin/env python3
"""
Debug script to understand shared memory attachment issues
"""

import sys
import os
import time

# Add the current directory to Python path to import the module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import ultrapubsub
    print("✅ Successfully imported ultrapubsub")
except ImportError as e:
    print(f"❌ Failed to import ultrapubsub: {e}")
    sys.exit(1)


def test_creation_and_attachment():
    """Test basic shared memory creation and attachment"""
    print("\n🧪 Testing shared memory creation and attachment...")

    shm_name = "debug_test"

    try:
        # Create shared memory
        print("📤 Creating shared memory...")
        shm = ultrapubsub.SharedMemory(shm_name, entries=32)
        publisher = shm.create_publisher()
        print("✅ Shared memory created successfully")

        # Send a test message
        test_message = b"Debug test message"
        publisher.publish(test_message)
        print(f"✅ Sent test message: {len(test_message)} bytes")

        # Now try to attach in the same process
        print("📡 Attempting to attach to existing shared memory...")
        attached_shm = ultrapubsub.SharedMemory.attach(shm_name)
        attached_subscriber = attached_shm.create_subscriber()
        print("✅ Attached successfully")

        # Try to receive the message
        received = attached_subscriber.receive(timeout=2.0)
        if received:
            print(f"✅ Received message: {len(received)} bytes")
            print(f"✅ Message content: {received}")
        else:
            print("❌ No message received")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_separate_process():
    """Test with separate processes"""
    print("\n🧪 Testing with separate processes...")

    import multiprocessing

    shm_name = "debug_separate_test"
    result_queue = multiprocessing.Queue()

    def subscriber_process(shm_name, result_queue):
        try:
            print("📡 Subscriber: Starting...")
            time.sleep(2.0)  # Give publisher time to create

            attached_shm = ultrapubsub.SharedMemory.attach(shm_name)
            subscriber = attached_shm.create_subscriber()

            received = subscriber.receive(timeout=5.0)
            if received:
                print(f"✅ Subscriber received: {len(received)} bytes")
                result_queue.put({"status": "success", "size": len(received)})
            else:
                print("❌ Subscriber: No message received")
                result_queue.put({"status": "timeout"})

        except Exception as e:
            print(f"❌ Subscriber error: {e}")
            result_queue.put({"status": "error", "error": str(e)})

    def publisher_process(shm_name):
        try:
            print("📤 Publisher: Starting...")
            shm = ultrapubsub.SharedMemory(shm_name, entries=32)
            publisher = shm.create_publisher()

            time.sleep(3.0)  # Give subscriber time to attach

            test_message = b"Hello from separate process!"
            publisher.publish(test_message)
            print(f"✅ Publisher sent: {len(test_message)} bytes")

            time.sleep(2.0)  # Give subscriber time to receive

        except Exception as e:
            print(f"❌ Publisher error: {e}")

    # Start processes
    pub_proc = multiprocessing.Process(target=publisher_process, args=(shm_name,))
    sub_proc = multiprocessing.Process(target=subscriber_process, args=(shm_name, result_queue))

    pub_proc.start()
    sub_proc.start()

    # Wait for completion
    pub_proc.join()
    sub_proc.join()

    # Get result
    try:
        result = result_queue.get(timeout=2.0)
        print(f"📊 Result: {result}")
        return result.get("status") == "success"
    except:
        print("❌ No result from subscriber")
        return False


def main():
    """Run debug tests"""
    print("🐛 Debug Shared Memory Attachment")

    # Test 1: Same process
    print("\n" + "="*50)
    print("Test 1: Same process creation and attachment")
    print("="*50)
    test1_success = test_creation_and_attachment()

    # Test 2: Separate processes
    print("\n" + "="*50)
    print("Test 2: Separate processes")
    print("="*50)
    test2_success = test_separate_process()

    print("\n" + "="*50)
    print("SUMMARY")
    print("="*50)
    print(f"Test 1 (same process): {'✅ PASS' if test1_success else '❌ FAIL'}")
    print(f"Test 2 (separate processes): {'✅ PASS' if test2_success else '❌ FAIL'}")

    return test1_success and test2_success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)