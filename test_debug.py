#!/usr/bin/env python3
"""
Debug test to understand acknowledgment timing issues.
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
    """Simple subscriber process with debug output."""
    try:
        connect_coordinator("test_debug")
        print(f"  [{time.time():.3f}] Subscriber connected to coordinator")

        # Wait for broadcast
        print(f"  [{time.time():.3f}] Subscriber waiting for broadcast...")
        result = wait_for_broadcast(5000)
        if result is not None:
            print(f"  [{time.time():.3f}] Subscriber received broadcast: {result}")

            # Small delay before acknowledgment
            time.sleep(0.1)

            print(f"  [{time.time():.3f}] Subscriber sending acknowledgment...")
            acknowledge_broadcast(0)  # Use subscriber ID 0
            print(f"  [{time.time():.3f}] Subscriber acknowledged broadcast")
            return True
        else:
            print(f"  [{time.time():.3f}] Subscriber timed out waiting for broadcast")
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
    """Simple publisher process with debug output."""
    try:
        create_coordinator("test_debug")
        print(f"  [{time.time():.3f}] Publisher created coordinator")

        # Register a subscriber
        sub_id = register_subscriber()
        print(f"  [{time.time():.3f}] Publisher registered subscriber: {sub_id}")

        # Wait for subscriber
        print(f"  [{time.time():.3f}] Publisher waiting for subscriber...")
        if wait_for_subscribers(3000):
            print(f"  [{time.time():.3f}] Publisher found subscriber")

            # Send broadcast
            print(f"  [{time.time():.3f}] Publisher sending broadcast...")
            notify_broadcast(42)
            print(f"  [{time.time():.3f}] Publisher sent broadcast")

            # Wait for acknowledgment
            print(f"  [{time.time():.3f}] Publisher waiting for acknowledgment...")
            if wait_for_acknowledgments(3000):
                print(f"  [{time.time():.3f}] Publisher received acknowledgment")
                return True
            else:
                print(f"  [{time.time():.3f}] Publisher timed out waiting for acknowledgment")
                return False
        else:
            print(f"  [{time.time():.3f}] Publisher timed out waiting for subscriber")
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
    print("=== Debug Test for Acknowledgment Timing ===")

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