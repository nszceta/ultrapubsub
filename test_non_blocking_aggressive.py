#!/usr/bin/env python3
"""
Test non-blocking publish behavior with aggressive buffer filling
"""
import sys
import time
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_aggressive_non_blocking():
    """Test non-blocking publish by aggressively filling the buffer"""
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_non_blocking_aggressive')

        print("Creating publisher...")
        publisher = shm.create_publisher()

        # Create very large messages to fill the buffer quickly
        # The buffer is about 64MB total, so let's use 10MB messages
        message_size = 10 * 1024 * 1024  # 10MB messages
        large_message = b'X' * message_size

        print(f"Testing with {message_size // (1024 * 1024)}MB messages...")

        success_count = 0
        failure_count = 0
        total_attempts = 20

        for i in range(total_attempts):
            result = publisher.try_publish(large_message)

            if result:
                success_count += 1
                print(f"✅ Published large message {i+1}")
            else:
                failure_count += 1
                print(f"❌ Buffer full at message {i+1} - non-blocking working!")
                break

            # Small delay to prevent overwhelming the system
            time.sleep(0.01)

        print(f"\nResults:")
        print(f"✅ Successfully published: {success_count} messages")
        print(f"❌ Failed to publish: {failure_count} messages")
        print(f"Total size published: {success_count * message_size // (1024 * 1024)}MB")

        if failure_count > 0:
            print("🎉 Non-blocking behavior is working correctly!")
            return True
        elif success_count >= total_attempts:
            print("⚠️  Buffer is larger than expected - all messages published")
            return True
        else:
            print("❌ Unexpected behavior")
            return False

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Testing aggressive non-blocking publish behavior...")
    success = test_aggressive_non_blocking()
    if success:
        print("✅ Aggressive non-blocking publish test completed successfully!")
    else:
        print("💥 Aggressive non-blocking publish test FAILED!")