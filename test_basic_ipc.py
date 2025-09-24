#!/usr/bin/env python3
"""
Basic IPC test to establish working communication
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


def test_basic_publish():
    """Test basic publishing without subscribers"""
    print("\n🧪 Testing basic publishing...")

    try:
        shm = ultrapubsub.SharedMemory("basic_test", entries=16)
        publisher = shm.create_publisher()

        # Publish small messages
        messages = [b"Hello", b"World", b"Test"]
        for msg in messages:
            publisher.publish(msg)
            print(f"✅ Published: {msg}")

        print("✅ Basic publishing test passed")
        return True

    except Exception as e:
        print(f"❌ Basic publishing test failed: {e}")
        return False


def test_fork_ipc():
    """Test basic fork-based IPC"""
    print("\n🧪 Testing fork-based IPC...")

    try:
        shm_name = "fork_test"
        result = {"success": False}

        def child_process():
            try:
                # Child process
                time.sleep(1.0)  # Give parent time to publish
                child_shm = ultrapubsub.SharedMemory.attach(shm_name)
                child_subscriber = child_shm.create_subscriber()

                received = child_subscriber.receive(timeout=3.0)
                if received:
                    print(f"✅ Child received: {received}")
                    result["success"] = True
                else:
                    print("❌ Child received nothing")

                os._exit(0)
            except Exception as e:
                print(f"❌ Child error: {e}")
                os._exit(1)

        # Parent process
        parent_shm = ultrapubsub.SharedMemory(shm_name, entries=16)
        parent_publisher = parent_shm.create_publisher()

        # Fork child
        pid = os.fork()
        if pid == 0:
            child_process()
        else:
            # Parent - publish message
            time.sleep(0.5)  # Give child time to start
            test_msg = b"Hello from parent!"
            parent_publisher.publish(test_msg)
            print(f"✅ Parent sent: {test_msg}")

            # Wait for child
            child_pid, status = os.waitpid(pid, 0)
            if os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0:
                print("✅ Child process completed successfully")
                return result["success"]
            else:
                print("❌ Child process failed")
                return False

    except Exception as e:
        print(f"❌ Fork IPC test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run basic IPC tests"""
    print("🧪 Basic IPC Test Suite")

    # Test basic publishing
    test1_success = test_basic_publish()

    # Test fork IPC
    test2_success = test_fork_ipc()

    print(f"\n📊 Results:")
    print(f"  Basic publishing: {'✅ PASS' if test1_success else '❌ FAIL'}")
    print(f"  Fork IPC: {'✅ PASS' if test2_success else '❌ FAIL'}")

    return test1_success and test2_success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)