#!/usr/bin/env python3
"""
Simple test to verify the minimal Rust synchronization API works.
"""
import sys
import time

# Add the path to the built extension
sys.path.insert(0, '/root/ultrapubsub/python')

try:
    # Test basic imports
    from ultrapubsub.ultrapubsub import (
        create_coordinator, connect_coordinator, register_subscriber,
        wait_for_subscribers, notify_broadcast, wait_for_broadcast,
        acknowledge_broadcast, wait_for_acknowledgments, get_subscriber_count,
        cleanup_coordinator
    )
    print("✅ All Rust functions imported successfully")
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

def test_basic_functionality():
    """Test basic synchronization functionality."""
    print("\n=== Testing Basic Synchronization ===")

    try:
        # Test coordinator creation
        create_coordinator("test_basic")
        print("✅ Coordinator created")

        # Test subscriber registration
        sub_id = register_subscriber()
        print(f"✅ Subscriber registered with ID: {sub_id}")

        # Test subscriber count
        count = get_subscriber_count()
        print(f"✅ Subscriber count: {count}")

        # Test wait for subscribers (should succeed quickly)
        start = time.time()
        ready = wait_for_subscribers(1000)
        elapsed = time.time() - start
        print(f"✅ Wait for subscribers: {ready} (took {elapsed*1000:.1f}ms)")

        # Test broadcast notification
        notify_broadcast(42)
        print("✅ Broadcast notification sent")

        # Test wait for broadcast
        start = time.time()
        result = wait_for_broadcast(1000)
        elapsed = time.time() - start
        print(f"✅ Wait for broadcast: {result} (took {elapsed*1000:.1f}ms)")

        # Test acknowledgment
        acknowledge_broadcast()
        print("✅ Broadcast acknowledged")

        # Test wait for acknowledgments
        start = time.time()
        acked = wait_for_acknowledgments(1000)
        elapsed = time.time() - start
        print(f"✅ Wait for acknowledgments: {acked} (took {elapsed*1000:.1f}ms)")

        # Cleanup
        cleanup_coordinator()
        print("✅ Coordinator cleaned up")

        return True

    except Exception as e:
        print(f"❌ Test failed: {e}")
        cleanup_coordinator()
        return False

def subscriber_process():
    """Simple subscriber process."""
    try:
        # Import in the subprocess
        import sys
        sys.path.insert(0, '/root/ultrapubsub/python')
        from ultrapubsub.ultrapubsub import (
            connect_coordinator, register_subscriber, wait_for_broadcast,
            acknowledge_broadcast, cleanup_coordinator
        )

        connect_coordinator("test_multi")
        sub_id = register_subscriber()
        print(f"  Subscriber {sub_id} registered")

        # Wait for broadcast
        result = wait_for_broadcast(5000)
        print(f"  Subscriber {sub_id} received broadcast: {result}")

        if result is not None:
            acknowledge_broadcast()
            print(f"  Subscriber {sub_id} acknowledged")
            return True
        return False

    except Exception as e:
        print(f"  Subscriber error: {e}")
        return False
    finally:
        try:
            cleanup_coordinator()
        except:
            pass

def publisher_process():
    """Simple publisher process."""
    try:
        # Import in the subprocess
        import sys
        sys.path.insert(0, '/root/ultrapubsub/python')
        from ultrapubsub.ultrapubsub import (
            create_coordinator, wait_for_subscribers, notify_broadcast,
            wait_for_acknowledgments, cleanup_coordinator
        )

        create_coordinator("test_multi")
        print("  Publisher created coordinator")

        # Wait for subscriber
        if wait_for_subscribers(3000):
            print("  Publisher found subscribers")
            notify_broadcast(123)
            print("  Publisher sent broadcast")

            # Wait for acknowledgment
            if wait_for_acknowledgments(3000):
                print("  Publisher received acknowledgment")
                cleanup_coordinator()
                return True

        cleanup_coordinator()
        return False

    except Exception as e:
        print(f"  Publisher error: {e}")
        return False
    finally:
        try:
            cleanup_coordinator()
        except:
            pass

def test_multi_process():
    """Test basic multi-process functionality."""
    print("\n=== Testing Multi-Process Coordination ===")

    import multiprocessing as mp

    try:
        # Start subscriber
        ctx = mp.get_context('spawn')
        sub_proc = ctx.Process(target=subscriber_process)
        sub_proc.start()

        # Give subscriber time to start
        time.sleep(0.5)

        # Start publisher
        pub_proc = ctx.Process(target=publisher_process)
        pub_proc.start()

        # Wait for completion
        sub_proc.join(timeout=10)
        pub_proc.join(timeout=10)

        sub_success = sub_proc.exitcode == 0
        pub_success = pub_proc.exitcode == 0

        print(f"  Subscriber: {'✅' if sub_success else '❌'}")
        print(f"  Publisher: {'✅' if pub_success else '❌'}")

        return sub_success and pub_success

    except Exception as e:
        print(f"❌ Multi-process test failed: {e}")
        return False

def main():
    """Main test function."""
    print("=== UltraPubSub Minimal API Test ===")

    # Test basic functionality
    basic_success = test_basic_functionality()

    # Test multi-process
    multi_success = test_multi_process()

    print(f"\n=== RESULTS ===")
    print(f"Basic functionality: {'✅ PASS' if basic_success else '❌ FAIL'}")
    print(f"Multi-process: {'✅ PASS' if multi_success else '❌ FAIL'}")

    overall_success = basic_success and multi_success
    print(f"Overall: {'🎉 SUCCESS' if overall_success else '❌ FAILURE'}")

    return overall_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)