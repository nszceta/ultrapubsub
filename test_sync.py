#!/usr/bin/env python3
"""
Simple test to verify the fixed multi-process synchronization works.
"""
import sys
import time
import multiprocessing as mp

# Add the path to the built extension
sys.path.insert(0, '/root/ultrapubsub/python')

try:
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

def run_subscriber_process():
    """Simple subscriber process."""
    try:
        connect_coordinator("test_sync")
        print("  Subscriber connected to coordinator")

        # Wait for broadcast
        result = wait_for_broadcast(5000)
        if result is not None:
            print(f"  Subscriber received broadcast: {result}")
            acknowledge_broadcast(0)  # Use subscriber ID 0
            print("  Subscriber acknowledged broadcast")
            return True
        else:
            print("  Subscriber timed out waiting for broadcast")
            return False
    except Exception as e:
        print(f"  Subscriber error: {e}")
        return False
    finally:
        try:
            cleanup_coordinator()
        except:
            pass

def run_publisher_process():
    """Simple publisher process."""
    try:
        create_coordinator("test_sync")
        print("  Publisher created coordinator")

        # Register a subscriber
        sub_id = register_subscriber()
        print(f"  Publisher registered subscriber: {sub_id}")

        # Wait for subscriber
        if wait_for_subscribers(3000):
            print("  Publisher found subscriber")

            # Send broadcast
            notify_broadcast(42)
            print("  Publisher sent broadcast")

            # Wait for acknowledgment
            if wait_for_acknowledgments(3000):
                print("  Publisher received acknowledgment")
                return True
            else:
                print("  Publisher timed out waiting for acknowledgment")
                return False
        else:
            print("  Publisher timed out waiting for subscriber")
            return False
    except Exception as e:
        print(f"  Publisher error: {e}")
        return False
    finally:
        try:
            cleanup_coordinator()
        except:
            pass

def main():
    """Main test function."""
    print("=== Testing Fixed Multi-Process Synchronization ===")

    try:
        # Use multiprocessing for true process isolation
        ctx = mp.get_context('spawn')

        # Start subscriber process
        subscriber_proc = ctx.Process(target=run_subscriber_process)
        subscriber_proc.start()

        # Give subscriber time to start
        time.sleep(0.5)

        # Start publisher process
        publisher_proc = ctx.Process(target=run_publisher_process)
        publisher_proc.start()

        # Wait for completion
        subscriber_proc.join(timeout=10)
        publisher_proc.join(timeout=10)

        sub_success = subscriber_proc.exitcode == 0
        pub_success = publisher_proc.exitcode == 0

        print(f"\n=== RESULTS ===")
        print(f"Subscriber: {'✅ SUCCESS' if sub_success else '❌ FAILED'}")
        print(f"Publisher: {'✅ SUCCESS' if pub_success else '❌ FAILED'}")

        if sub_success and pub_success:
            print("🎉 MULTI-PROCESS SYNCHRONIZATION WORKS!")
            return True
        else:
            print("❌ MULTI-PROCESS SYNCHRONIZATION FAILED")
            return False

    except Exception as e:
        print(f"Test error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)