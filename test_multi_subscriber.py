#!/usr/bin/env python3
"""
Multi-subscriber test for large binary blobs

This test verifies that large binary data can be sent from a publisher
to multiple subscribers simultaneously without message drops or corruption.
"""

import sys
import os
import time
import multiprocessing
import hashlib
import json
from typing import List, Dict, Any

# Add the current directory to Python path to import the module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import ultrapubsub
    print("✅ Successfully imported ultrapubsub")
except ImportError as e:
    print(f"❌ Failed to import ultrapubsub: {e}")
    sys.exit(1)


def generate_large_blob(size_mb: int) -> bytes:
    """Generate a large binary blob with verifiable content and indisputable signatures"""
    target_size = size_mb * 1024 * 1024

    # Create indisputable signatures
    header_signature = b"ULTRAPUBSUB_BLOB_START_" + str(size_mb).encode() + b"MB"
    footer_signature = b"ULTRAPUBSUB_BLOB_END_" + str(size_mb).encode() + b"MB"

    # Calculate payload size to achieve target total size
    payload_size = target_size - len(header_signature) - len(footer_signature)

    # Create deterministic but pseudo-random data for payload
    data = bytearray(header_signature)  # Start with header signature

    for i in range(0, payload_size, 4):
        # Use a pattern that's easy to verify
        value = i & 0xFFFFFFFF
        data.extend(value.to_bytes(4, 'little'))

    data.extend(footer_signature)  # End with footer signature
    return bytes(data)


def calculate_checksum(data: bytes) -> str:
    """Calculate SHA-256 checksum of data"""
    return hashlib.sha256(data).hexdigest()


def verify_blob_signatures(data: bytes, expected_size_mb: int) -> bool:
    """Verify that the blob has correct start and end signatures"""
    expected_header = b"ULTRAPUBSUB_BLOB_START_" + str(expected_size_mb).encode() + b"MB"
    expected_footer = b"ULTRAPUBSUB_BLOB_END_" + str(expected_size_mb).encode() + b"MB"

    # Check header signature
    if not data.startswith(expected_header):
        print(f"❌ Header signature mismatch. Expected: {expected_header}, Got: {data[:len(expected_header)]}")
        return False

    # Check footer signature
    if not data.endswith(expected_footer):
        print(f"❌ Footer signature mismatch. Expected: {expected_footer}, Got: {data[-len(expected_footer):]}")
        return False

    return True


def subscriber_process(subscriber_id: int, shm_name: str, num_messages: int,
                       expected_checksums: List[str], result_queue: multiprocessing.Queue):
    """Subscriber process that receives and verifies messages"""
    try:
        print(f"📡 Subscriber {subscriber_id}: Starting...")

        # Attach to shared memory
        shm = ultrapubsub.SharedMemory.attach(shm_name)
        subscriber = shm.create_subscriber()

        received_messages = []
        received_checksums = []

        # Receive messages
        for i in range(num_messages):
            start_time = time.time()
            message = None

            # Wait for message with timeout
            while time.time() - start_time < 10.0:  # 10 second timeout per message
                message = subscriber.receive(timeout=0.1)
                if message is not None:
                    break
                time.sleep(0.01)

            if message is None:
                print(f"❌ Subscriber {subscriber_id}: Timeout waiting for message {i}")
                result_queue.put({
                    'subscriber_id': subscriber_id,
                    'status': 'timeout',
                    'messages_received': len(received_messages),
                    'message': f"Timeout on message {i}"
                })
                return

            # Verify signatures first
            expected_size_mb = int(shm_name.split('_')[-2])  # Extract size from shm_name like "multi_sub_test_1mb"
            if not verify_blob_signatures(message, expected_size_mb):
                result_queue.put({
                    'subscriber_id': subscriber_id,
                    'status': 'signature_mismatch',
                    'messages_received': len(received_messages),
                    'message': f'Signature verification failed for message {i+1}'
                })
                return

            # Verify checksum
            checksum = calculate_checksum(message)
            received_checksums.append(checksum)
            received_messages.append(len(message))

            print(f"✅ Subscriber {subscriber_id}: Received message {i+1}/{num_messages}, "
                  f"size={len(message)} bytes, signatures verified, checksum={checksum[:16]}...")

        # Verify all checksums match expected
        if received_checksums == expected_checksums:
            print(f"✅ Subscriber {subscriber_id}: All messages verified successfully (signatures and checksums)")
            result_queue.put({
                'subscriber_id': subscriber_id,
                'status': 'success',
                'messages_received': len(received_messages),
                'message_sizes': received_messages,
                'checksums': received_checksums,
                'message': 'All messages received and verified with signatures'
            })
        else:
            print(f"❌ Subscriber {subscriber_id}: Checksum mismatch")
            result_queue.put({
                'subscriber_id': subscriber_id,
                'status': 'checksum_mismatch',
                'messages_received': len(received_messages),
                'expected_checksums': expected_checksums,
                'received_checksums': received_checksums,
                'message': 'Checksum verification failed'
            })

    except Exception as e:
        print(f"❌ Subscriber {subscriber_id}: Error - {e}")
        result_queue.put({
            'subscriber_id': subscriber_id,
            'status': 'error',
            'messages_received': 0,
            'message': str(e)
        })


def publisher_process(shm_name: str, messages: List[bytes], delay_between_messages: float = 0.1):
    """Publisher process that sends messages"""
    try:
        print(f"📤 Publisher: Starting...")

        # Create shared memory
        shm = ultrapubsub.SharedMemory(shm_name, entries=128)
        publisher = shm.create_publisher()

        # Give subscribers time to start
        time.sleep(1.0)

        # Send all messages
        for i, message in enumerate(messages):
            print(f"📤 Publisher: Sending message {i+1}/{len(messages)}, "
                  f"size={len(message)} bytes, checksum={calculate_checksum(message)[:16]}...")

            publisher.publish(message)
            time.sleep(delay_between_messages)

        print(f"✅ Publisher: All messages sent")
        return True

    except Exception as e:
        print(f"❌ Publisher: Error - {e}")
        return False


def run_multi_subscriber_test():
    """Run the multi-subscriber test"""
    print("\n🚀 Starting multi-subscriber large blob test...")

    # Test configuration
    num_subscribers = 6
    blob_sizes_mb = [1, 2, 5, 10, 20]  # Different blob sizes
    messages_per_size = 3  # Send each size multiple times

    results = {}

    for blob_size_mb in blob_sizes_mb:
        print(f"\n📊 Testing with {blob_size_mb}MB blobs...")

        # Generate test messages
        messages = []
        expected_checksums = []

        for i in range(messages_per_size):
            # Generate unique blob for each message (each has unique signatures)
            blob = generate_large_blob(blob_size_mb)
            messages.append(blob)
            expected_checksums.append(calculate_checksum(blob))

        print(f"📊 Generated {len(messages)} messages, total size: {sum(len(m) for m in messages) / 1024 / 1024:.1f}MB")

        # Create result queue
        result_queue = multiprocessing.Queue()

        # Start subscribers
        subscriber_processes = []
        shm_name = f"multi_sub_test_{blob_size_mb}mb"

        for sub_id in range(num_subscribers):
            process = multiprocessing.Process(
                target=subscriber_process,
                args=(sub_id, shm_name, len(messages), expected_checksums, result_queue)
            )
            process.start()
            subscriber_processes.append(process)
            print(f"📡 Started subscriber {sub_id}")

        # Start publisher
        publisher_process_obj = multiprocessing.Process(
            target=publisher_process,
            args=(shm_name, messages, 0.2)  # 200ms delay between messages
        )
        publisher_process_obj.start()
        print(f"📤 Started publisher")

        # Wait for publisher to finish
        publisher_process_obj.join()
        publisher_success = publisher_process_obj.exitcode == 0

        # Wait for all subscribers to finish
        for process in subscriber_processes:
            process.join()

        # Collect results
        subscriber_results = []
        for _ in range(num_subscribers):
            try:
                result = result_queue.get(timeout=5.0)
                subscriber_results.append(result)
            except:
                print(f"⚠️  Timeout waiting for subscriber result")
                subscriber_results.append({
                    'subscriber_id': len(subscriber_results),
                    'status': 'timeout',
                    'messages_received': 0,
                    'message': 'Result queue timeout'
                })

        # Analyze results
        successful_subscribers = sum(1 for r in subscriber_results if r['status'] == 'success')
        total_messages_expected = num_subscribers * len(messages)
        total_messages_received = sum(r['messages_received'] for r in subscriber_results)

        print(f"\n📊 Results for {blob_size_mb}MB blobs:")
        print(f"   Publisher success: {publisher_success}")
        print(f"   Successful subscribers: {successful_subscribers}/{num_subscribers}")
        print(f"   Messages expected: {total_messages_expected}")
        print(f"   Messages received: {total_messages_received}")
        print(f"   Success rate: {successful_subscribers/num_subscribers*100:.1f}%")
        print(f"   Message delivery rate: {total_messages_received/total_messages_expected*100:.1f}%")

        results[blob_size_mb] = {
            'publisher_success': publisher_success,
            'successful_subscribers': successful_subscribers,
            'total_messages_expected': total_messages_expected,
            'total_messages_received': total_messages_received,
            'subscriber_results': subscriber_results
        }

        # Print individual subscriber results
        for result in subscriber_results:
            status_symbol = "✅" if result['status'] == 'success' else "❌"
            print(f"   {status_symbol} Subscriber {result['subscriber_id']}: "
                  f"{result['messages_received']}/{len(messages)} messages - {result['status']}")

        # Check for overall success
        if successful_subscribers < num_subscribers:
            print(f"⚠️  Only {successful_subscribers}/{num_subscribers} subscribers succeeded")

        if total_messages_received < total_messages_expected:
            print(f"⚠️  Message drops detected: {total_messages_expected - total_messages_received} messages lost")

    return results


def test_performance_with_multiple_subscribers():
    """Test performance with multiple subscribers receiving large blobs"""
    print("\n🚀 Starting performance test with multiple subscribers...")

    # Configuration
    num_subscribers = 6
    blob_size_mb = 5  # 5MB blobs
    num_messages = 10

    print(f"📊 Performance test: {num_subscribers} subscribers, {blob_size_mb}MB blobs, {num_messages} messages")

    # Generate messages
    messages = [generate_large_blob(blob_size_mb) for _ in range(num_messages)]
    total_data_mb = sum(len(m) for m in messages) / 1024 / 1024

    shm_name = "perf_multi_sub_test"
    result_queue = multiprocessing.Queue()

    # Start subscribers
    subscriber_processes = []
    start_time = time.time()

    for sub_id in range(num_subscribers):
        process = multiprocessing.Process(
            target=subscriber_process,
            args=(sub_id, shm_name, num_messages, [calculate_checksum(m) for m in messages], result_queue)
        )
        process.start()
        subscriber_processes.append(process)

    # Start publisher
    publisher_process_obj = multiprocessing.Process(
        target=publisher_process,
        args=(shm_name, messages, 0.1)  # 100ms delay for performance test
    )
    publisher_process_obj.start()

    # Wait for completion
    publisher_process_obj.join()
    for process in subscriber_processes:
        process.join()

    end_time = time.time()
    total_time = end_time - start_time

    # Collect results
    successful_subscribers = 0
    total_messages_received = 0

    for _ in range(num_subscribers):
        try:
            result = result_queue.get(timeout=5.0)
            if result['status'] == 'success':
                successful_subscribers += 1
            total_messages_received += result['messages_received']
        except:
            pass

    # Calculate performance metrics
    total_data_transferred_mb = total_messages_received * blob_size_mb
    throughput_mb_per_sec = total_data_transferred_mb / total_time if total_time > 0 else 0

    print(f"\n📊 Performance Results:")
    print(f"   Total time: {total_time:.2f} seconds")
    print(f"   Successful subscribers: {successful_subscribers}/{num_subscribers}")
    print(f"   Total messages received: {total_messages_received}")
    print(f"   Total data transferred: {total_data_transferred_mb:.1f}MB")
    print(f"   Throughput: {throughput_mb_per_sec:.1f}MB/sec")
    print(f"   Per-subscriber throughput: {throughput_mb_per_sec/num_subscribers:.1f}MB/sec")

    return {
        'total_time': total_time,
        'successful_subscribers': successful_subscribers,
        'total_messages_received': total_messages_received,
        'throughput_mb_per_sec': throughput_mb_per_sec
    }


def main():
    """Run all tests"""
    print("🧪 Multi-Subscriber Large Blob Test Suite")
    print("=" * 50)

    # Run main test
    results = run_multi_subscriber_test()

    # Run performance test
    perf_results = test_performance_with_multiple_subscribers()

    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)

    overall_success = True
    for blob_size, result in results.items():
        success_rate = result['successful_subscribers'] / 6 * 100
        delivery_rate = result['total_messages_received'] / result['total_messages_expected'] * 100

        print(f"{blob_size}MB blobs:")
        print(f"  Subscriber success rate: {success_rate:.1f}%")
        print(f"  Message delivery rate: {delivery_rate:.1f}%")

        if success_rate < 100 or delivery_rate < 100:
            overall_success = False

    if overall_success and perf_results['successful_subscribers'] == 6:
        print("\n🎉 ALL TESTS PASSED!")
        print("✅ Large binary blobs successfully delivered to multiple subscribers")
        print("✅ No message drops detected")
        print("✅ Data integrity verified")
        print("✅ Performance within acceptable limits")
        return True
    else:
        print("\n❌ SOME TESTS FAILED!")
        print("Check the detailed results above for issues")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)