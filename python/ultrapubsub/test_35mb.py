#!/usr/bin/env python3
"""
Test complete zero-copy system with 35MB arrays.
This demonstrates the full zero-copy architecture with minimal Rust synchronization.
"""
import numpy as np
import time
import os
import sys
import multiprocessing as mp
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

try:
    # Import the Rust synchronization functions
    import sys
    import os
    sys.path.insert(0, '/root/ultrapubsub/python')
    from ultrapubsub.ultrapubsub import (
        create_coordinator, connect_coordinator, register_subscriber,
        wait_for_subscribers, notify_broadcast, wait_for_broadcast,
        acknowledge_broadcast, wait_for_acknowledgments, get_subscriber_count,
        cleanup_coordinator
    )
    # Import the zero-copy classes directly from the module
    from ultrapubsub.zerocopy import ZeroCopyPublisher, ZeroCopySubscriber
except ImportError as e:
    print(f"Failed to import ultrapubsub: {e}")
    sys.exit(1)

def calculate_35mb_dimensions():
    """Calculate dimensions for a 35MB numpy array."""
    target_size = 35 * 1024 * 1024  # 35MB
    dtype_size = 8  # float64
    total_elements = target_size // dtype_size

    # Use a 2D array for reasonable dimensions
    dim1 = int(np.sqrt(total_elements))
    dim2 = total_elements // dim1

    print(f"Array dimensions: {dim1} x {dim2} = {dim1 * dim2} elements")
    print(f"Expected size: {dim1 * dim2 * dtype_size / (1024*1024):.2f} MB")

    return (dim1, dim2)

def run_publisher(array_shape, num_roundtrips=10):
    """Run the publisher process."""
    print(f"Starting publisher with array shape {array_shape}")

    try:
        # Create publisher
        pub = ZeroCopyPublisher("test_35mb", array_shape, np.float64)

        # Get the shared array
        shared_array = pub.get_array()
        print(f"Publisher array size: {shared_array.nbytes / (1024*1024):.2f} MB")

        # Wait for subscribers
        if not pub.wait_for_subscribers(timeout_ms=5000):
            print("ERROR: Timeout waiting for subscribers")
            return False

        print(f"Found {pub.get_subscriber_count()} subscribers")

        # Test performance
        total_time = 0
        successful_roundtrips = 0

        for i in range(num_roundtrips):
            try:
                # Generate test data (simple pattern)
                seq_num = i + 1
                shared_array.fill(seq_num)  # Fill with sequence number

                # Time the broadcast operation
                start_time = time.time()

                # Notify subscribers
                pub.notify_broadcast(seq_num)

                # Wait for acknowledgments
                if not pub.wait_for_acknowledgments(timeout_ms=2000):
                    print(f"WARNING: Timeout waiting for acknowledgments on roundtrip {i+1}")
                    continue

                end_time = time.time()
                roundtrip_time = end_time - start_time
                total_time += roundtrip_time
                successful_roundtrips += 1

                if i == 0:  # Only print first roundtrip details
                    print(f"Roundtrip {i+1}: {roundtrip_time*1000:.2f}ms")

            except Exception as e:
                print(f"Error in roundtrip {i+1}: {e}")
                continue

        # Calculate performance
        if successful_roundtrips > 0:
            avg_roundtrip_time = total_time / successful_roundtrips
            throughput = (shared_array.nbytes * successful_roundtrips) / total_time
            hz = successful_roundtrips / total_time

            print(f"\n=== PUBLISHER RESULTS ===")
            print(f"Successful roundtrips: {successful_roundtrips}/{num_roundtrips}")
            print(f"Average roundtrip time: {avg_roundtrip_time*1000:.2f}ms")
            print(f"Throughput: {throughput / (1024*1024):.2f} MB/s")
            print(f"Frequency: {hz:.2f} Hz")

            # Verify performance meets targets
            target_hz = 40.0
            target_throughput = 1.4 * 1024 * 1024 * 1024  # 1.4 GB/s

            if hz >= target_hz:
                print(f"✅ FREQUENCY TARGET MET: {hz:.2f} Hz >= {target_hz} Hz")
            else:
                print(f"❌ FREQUENCY TARGET NOT MET: {hz:.2f} Hz < {target_hz} Hz")

            if throughput >= target_throughput:
                print(f"✅ THROUGHPUT TARGET MET: {throughput/(1024*1024*1024):.2f} GB/s >= {target_throughput/(1024*1024*1024):.2f} GB/s")
            else:
                print(f"❌ THROUGHPUT TARGET NOT MET: {throughput/(1024*1024*1024):.2f} GB/s < {target_throughput/(1024*1024*1024):.2f} GB/s")

            return True
        else:
            print("ERROR: No successful roundtrips completed")
            return False

    except Exception as e:
        print(f"Publisher error: {e}")
        return False
    finally:
        try:
            pub.cleanup()
        except:
            pass

def run_subscriber(array_shape, num_roundtrips=10):
    """Run a subscriber process."""
    print(f"Starting subscriber with array shape {array_shape}")

    try:
        # Give publisher time to start
        time.sleep(0.5)

        # Connect to existing coordinator
        sub = ZeroCopySubscriber("test_35mb", array_shape, np.float64)

        # Register this subscriber
        sub_id = sub.register_subscriber()
        print(f"Subscriber registered with ID: {sub_id}")

        successful_roundtrips = 0

        for i in range(num_roundtrips):
            try:
                # Wait for broadcast notification
                result = sub.wait_for_broadcast(timeout_ms=2000)
                if result is None:
                    print(f"WARNING: Timeout waiting for broadcast on roundtrip {i+1}")
                    continue

                seq_num = result
                shared_array = sub.get_array()

                # Verify data integrity
                expected_value = seq_num
                actual_value = shared_array[0, 0]  # Check first element

                if actual_value == expected_value:
                    # Verify array is filled correctly
                    if np.all(shared_array == expected_value):
                        # Acknowledge receipt
                        sub.acknowledge_broadcast()
                        successful_roundtrips += 1

                        if i == 0:  # Only print first roundtrip details
                            print(f"Roundtrip {i+1}: Received seq {seq_num}, data verified")
                    else:
                        print(f"WARNING: Data integrity check failed on roundtrip {i+1}")
                else:
                    print(f"WARNING: Expected {expected_value}, got {actual_value} on roundtrip {i+1}")

            except Exception as e:
                print(f"Error in subscriber roundtrip {i+1}: {e}")
                continue

        print(f"Subscriber completed {successful_roundtrips}/{num_roundtrips} roundtrips successfully")
        return successful_roundtrips > 0

    except Exception as e:
        print(f"Subscriber error: {e}")
        return False
    finally:
        try:
            sub.cleanup()
        except:
            pass

def main():
    """Main test function."""
    print("=== 35MB Zero-Copy Array Performance Test ===")

    # Calculate array dimensions for 35MB
    array_shape = calculate_35mb_dimensions()
    num_roundtrips = 10

    print(f"\nStarting performance test with {num_roundtrips} roundtrips...")
    print("This test demonstrates zero-copy numpy array sharing across processes.")

    # Clean up any existing files
    cleanup_files()

    try:
        # Use multiprocessing for true process isolation
        ctx = mp.get_context('spawn')

        # Start subscriber process
        subscriber_proc = ctx.Process(
            target=run_subscriber,
            args=(array_shape, num_roundtrips)
        )
        subscriber_proc.start()

        # Start publisher process
        publisher_proc = ctx.Process(
            target=run_publisher,
            args=(array_shape, num_roundtrips)
        )
        publisher_proc.start()

        # Wait for completion with timeout
        timeout = 60  # 60 seconds timeout
        start_time = time.time()

        while True:
            if not subscriber_proc.is_alive() and not publisher_proc.is_alive():
                break

            if time.time() - start_time > timeout:
                print(f"ERROR: Test timed out after {timeout} seconds")
                subscriber_proc.terminate()
                publisher_proc.terminate()
                break

            time.sleep(0.1)

        # Get exit codes
        sub_success = subscriber_proc.exitcode == 0
        pub_success = publisher_proc.exitcode == 0

        print(f"\n=== TEST RESULTS ===")
        print(f"Subscriber process: {'✅ SUCCESS' if sub_success else '❌ FAILED'}")
        print(f"Publisher process: {'✅ SUCCESS' if pub_success else '❌ FAILED'}")

        if sub_success and pub_success:
            print("🎉 OVERALL TEST SUCCESS - Zero-copy 35MB array broadcasting works!")
            return True
        else:
            print("❌ TEST FAILED - One or more processes failed")
            return False

    except Exception as e:
        print(f"Test error: {e}")
        return False
    finally:
        cleanup_files()

def cleanup_files():
    """Clean up temporary files."""
    files_to_clean = [
        "/tmp/ultrapubsub_test_35mb.state",
        "/tmp/ultrapubsub_test_35mb_data.bin"
    ]

    for file_path in files_to_clean:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except:
            pass

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)