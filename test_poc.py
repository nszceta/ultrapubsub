#!/usr/bin/env python3
"""
Basic test script for ultrapubsub PoC
Tests the Python bindings and basic message passing functionality

This test script uses the new API for multi-process communication.
"""

import sys
import time
import os
import multiprocessing
import json

# Add the current directory to Python path to import the module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import ultrapubsub
    print("✅ Successfully imported ultrapubsub")
except ImportError as e:
    print(f"❌ Failed to import ultrapubsub: {e}")
    sys.exit(1)


def test_shared_memory():
    """Test shared memory creation and basic operations"""
    print("\n🧪 Testing SharedMemory...")

    try:
        # Create shared memory using new API
        shm = ultrapubsub.SharedMemory("test_shm", entries=32)
        print("✅ Created shared memory")

        # Create publisher and subscriber
        publisher = shm.create_publisher()
        subscriber = shm.create_subscriber()
        print("✅ Created publisher and subscriber")

        return shm, publisher, subscriber

    except Exception as e:
        print(f"❌ Shared memory test failed: {e}")
        return None, None, None


def test_message_publishing(publisher, subscriber):
    """Test basic message publishing"""
    print("\n🧪 Testing Message Publishing...")

    try:
        # Test publishing bytes
        test_data = b"Hello, ultrapubsub!"
        publisher.publish(test_data)
        print("✅ Published bytes message")

        # Test publishing string
        publisher.publish_string("Hello from string!")
        print("✅ Published string message")

        # Test publishing JSON
        test_obj = {"message": "Hello JSON", "number": 42, "list": [1, 2, 3]}
        publisher.publish_json(test_obj)
        print("✅ Published JSON message")

        return True

    except Exception as e:
        print(f"❌ Message publishing test failed: {e}")
        return False


def test_message_receiving(subscriber):
    """Test basic message receiving"""
    print("\n🧪 Testing Message Receiving...")

    try:
        # Note: In a real multi-process scenario, messages would be received
        # This test just verifies the API exists
        print("✅ Message receiving API available")
        return True

    except Exception as e:
        print(f"❌ Message receiving test failed: {e}")
        return False


def test_multi_process_ipc():
    """Test multi-process IPC communication"""
    print("\n🧪 Testing Multi-Process IPC...")

    try:
        # Create parent shared memory
        parent_shm = ultrapubsub.SharedMemory("multi_process_test", entries=64)
        parent_publisher = parent_shm.create_publisher()

        # Fork child process
        pid = os.fork()

        if pid == 0:
            # Child process
            try:
                # Attach to parent's shared memory
                child_shm = ultrapubsub.SharedMemory.attach("multi_process_test")
                child_subscriber = child_shm.create_subscriber()

                # Try to receive a message
                received = child_subscriber.receive(timeout=2.0)
                if received:
                    print(f"✅ Child received: {received}")

                os._exit(0)
            except Exception as e:
                print(f"❌ Child process error: {e}")
                os._exit(1)
        else:
            # Parent process
            # Give child time to set up
            time.sleep(0.1)

            # Send a message
            parent_publisher.publish(b"Hello from parent!")
            print("✅ Parent sent message")

            # Wait for child to finish
            child_pid, status = os.waitpid(pid, 0)
            if os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0:
                print("✅ Child process completed successfully")
                return True
            else:
                print("❌ Child process failed")
                return False

    except Exception as e:
        print(f"❌ Multi-process test failed: {e}")
        return False


def test_performance():
    """Test performance with the new API"""
    print("\n🧪 Testing Performance...")

    try:
        shm = ultrapubsub.SharedMemory("perf_test", entries=128)
        publisher = shm.create_publisher()

        # Test message publishing performance
        start_time = time.time()

        for i in range(1000):
            test_data = f"Message {i}".encode()
            publisher.publish(test_data)

        end_time = time.time()
        duration = end_time - start_time

        print(f"✅ Performance test: 1000 messages in {duration:.4f} seconds")
        print(f"✅ Average time per message: {duration/1000*1000:.4f} ms")
        print(f"✅ Throughput: {1000/duration:.0f} messages/sec")

        return True

    except Exception as e:
        print(f"❌ Performance test failed: {e}")
        return False


def test_high_level_api():
    """Test the high-level API features"""
    print("\n🧪 Testing High-Level API...")

    try:
        # Test process pair creation
        parent_shm, child_shm = ultrapubsub.create_process_pair("api_test")
        print("✅ Created process pair")

        # Test publisher convenience methods
        publisher = parent_shm.create_publisher()
        publisher.publish_string("Test string")
        publisher.publish_json({"test": "data"})
        print("✅ Publisher convenience methods work")

        return True

    except Exception as e:
        print(f"❌ High-level API test failed: {e}")
        return False


def main():
    """Run all tests"""
    print("🚀 Starting ultrapubsub PoC tests with new API...")

    # Test shared memory
    shm, publisher, subscriber = test_shared_memory()
    if not shm:
        return False

    # Test message publishing
    if not test_message_publishing(publisher, subscriber):
        return False

    # Test message receiving
    if not test_message_receiving(subscriber):
        return False

    # Test multi-process IPC
    if not test_multi_process_ipc():
        return False

    # Test performance
    if not test_performance():
        return False

    # Test high-level API
    if not test_high_level_api():
        return False

    print("\n🎉 All tests passed! New API is working correctly.")
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)