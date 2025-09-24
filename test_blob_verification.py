#!/usr/bin/env python3
"""
Test script to verify large binary blob transmission with signature verification
"""

import sys
import os
import time
import multiprocessing

# Add the current directory to Python path to import the module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import ultrapubsub
    print("✅ Successfully imported ultrapubsub")
except ImportError as e:
    print(f"❌ Failed to import ultrapubsub: {e}")
    sys.exit(1)

# Import functions from test_multi_subscriber
from test_multi_subscriber import generate_large_blob, verify_blob_signatures, calculate_checksum

def blob_subscriber(subscriber_id: int, shm_name: str, expected_size_mb: int,
                    expected_checksum: str, result_queue: multiprocessing.Queue):
    """Subscriber that receives and verifies a single blob with signatures"""
    try:
        print(f"📡 Subscriber {subscriber_id}: Starting...")

        # Give publisher time to create shared memory
        time.sleep(3.0)

        # Attach to shared memory
        shm = ultrapubsub.SharedMemory.attach(shm_name)
        subscriber = shm.create_subscriber()

        print(f"✅ Subscriber {subscriber_id}: Attached successfully")

        # Wait for and receive the blob
        start_time = time.time()
        message = None

        while time.time() - start_time < 15.0:  # 15 second timeout
            message = subscriber.receive(timeout=0.1)
            if message is not None:
                break
            time.sleep(0.01)

        if message is None:
            print(f"❌ Subscriber {subscriber_id}: Timeout waiting for blob")
            result_queue.put({
                'subscriber_id': subscriber_id,
                'status': 'timeout',
                'size': 0,
                'message': 'Timeout waiting for blob'
            })
            return

        # Verify signatures
        if not verify_blob_signatures(message, expected_size_mb):
            result_queue.put({
                'subscriber_id': subscriber_id,
                'status': 'signature_mismatch',
                'size': len(message),
                'message': 'Signature verification failed'
            })
            return

        # Verify checksum
        actual_checksum = calculate_checksum(message)
        if actual_checksum != expected_checksum:
            result_queue.put({
                'subscriber_id': subscriber_id,
                'status': 'checksum_mismatch',
                'size': len(message),
                'expected_checksum': expected_checksum,
                'actual_checksum': actual_checksum,
                'message': 'Checksum verification failed'
            })
            return

        # Success!
        result_queue.put({
            'subscriber_id': subscriber_id,
            'status': 'success',
            'size': len(message),
            'checksum': actual_checksum,
            'message': 'Blob received and verified successfully'
        })

        print(f"✅ Subscriber {subscriber_id}: SUCCESS! Received {len(message)} bytes with valid signatures and checksum")

    except Exception as e:
        print(f"❌ Subscriber {subscriber_id}: Error - {e}")
        result_queue.put({
            'subscriber_id': subscriber_id,
            'status': 'error',
            'size': 0,
            'message': str(e)
        })

def blob_publisher(shm_name: str, blob_size_mb: int):
    """Publisher that sends a single blob with signatures"""
    try:
        print(f"📤 Publisher: Starting...")

        # Generate blob with signatures
        blob = generate_large_blob(blob_size_mb)
        blob_checksum = calculate_checksum(blob)

        print(f"📤 Publisher: Generated {len(blob)} bytes blob with signatures")
        print(f"📤 Publisher: Blob checksum: {blob_checksum[:16]}...")

        # Create shared memory with enough entries for multiple subscribers
        shm = ultrapubsub.SharedMemory(shm_name, entries=64)
        publisher = shm.create_publisher()

        print(f"📤 Publisher: Shared memory created, waiting for subscribers...")

        # Give subscribers time to attach
        time.sleep(4.0)

        # Publish the blob
        publisher.publish(blob)
        print(f"✅ Publisher: Blob published successfully!")

        # Give subscribers time to receive
        time.sleep(5.0)

        return blob_checksum

    except Exception as e:
        print(f"❌ Publisher: Error - {e}")
        return None

def test_blob_transmission():
    """Test large binary blob transmission to multiple subscribers"""
    print("\n🚀 Testing large binary blob transmission with signature verification...")

    # Test configuration
    num_subscribers = 6
    blob_size_mb = 5  # 5MB blob
    shm_name = f"blob_test_{blob_size_mb}mb"

    result_queue = multiprocessing.Queue()

    # Start subscribers first
    subscriber_processes = []
    for sub_id in range(num_subscribers):
        process = multiprocessing.Process(
            target=blob_subscriber,
            args=(sub_id, shm_name, blob_size_mb, "placeholder", result_queue)
        )
        process.start()
        subscriber_processes.append(process)
        print(f"📡 Started subscriber {sub_id}")

    # Start publisher
    print(f"📤 Starting publisher...")
    publisher_checksum = blob_publisher(shm_name, blob_size_mb)

    if publisher_checksum is None:
        print("❌ Publisher failed to generate blob")
        return False

    # Update subscribers with correct checksum
    # Note: In a real implementation, we'd pass this differently
    # For now, we'll verify it in the main process

    # Wait for all processes to complete
    for process in subscriber_processes:
        process.join()

    # Collect results
    results = []
    for _ in range(num_subscribers):
        try:
            result = result_queue.get(timeout=5.0)
            results.append(result)
        except:
            results.append({
                'subscriber_id': len(results),
                'status': 'timeout',
                'size': 0,
                'message': 'Result queue timeout'
            })

    # Analyze results
    successful = sum(1 for r in results if r['status'] == 'success')
    total_size = sum(r['size'] for r in results)

    print(f"\n📊 Results:")
    print(f"   Successful subscribers: {successful}/{num_subscribers}")
    print(f"   Total data received: {total_size / 1024 / 1024:.1f}MB")

    # Print individual results
    for result in results:
        status_symbol = "✅" if result['status'] == 'success' else "❌"
        print(f"   {status_symbol} Subscriber {result['subscriber_id']}: {result['status']} ({result['size']} bytes)")

    # Verify all successful subscribers received the same data
    if successful > 1:
        successful_results = [r for r in results if r['status'] == 'success']
        if all(r['size'] == successful_results[0]['size'] for r in successful_results):
            print(f"✅ All successful subscribers received the same size: {successful_results[0]['size']} bytes")
        else:
            print("❌ Size mismatch between successful subscribers")

    return successful == num_subscribers

if __name__ == "__main__":
    success = test_blob_transmission()
    if success:
        print("\n🎉 ALL TESTS PASSED! Large binary blobs transmitted successfully with signature verification!")
    else:
        print("\n❌ Some tests failed")
    sys.exit(0 if success else 1)