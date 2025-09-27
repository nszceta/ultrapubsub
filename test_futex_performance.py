#!/usr/bin/env python3
"""
Simple performance test to demonstrate futex optimization
"""
import subprocess
import time
import json
import os
import signal

def run_futex_performance_test():
    """Run a simple futex performance test"""
    print("🚀 Futex Performance Test")
    print("=" * 50)

    test_name = "/futex_performance_test"
    message_size_mb = 1  # Smaller for faster testing
    duration_seconds = 5
    target_hz = 100  # Higher target to show futex performance

    processes = []
    start_time = time.time()

    try:
        # Clean up any existing shared memory
        try:
            subprocess.run(['python', '-c', f'import ultrapubsub; ultrapubsub.cleanup_shared_memory("{test_name}")'],
                          timeout=5, capture_output=True)
        except:
            pass

        # Launch publisher first to create shared memory
        print("🚀 Launching publisher process...")
        publisher_cmd = ['python', 'simple_publisher.py', test_name, str(message_size_mb), str(duration_seconds), str(target_hz)]
        publisher_process = subprocess.Popen(publisher_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        processes.append({'type': 'publisher', 'process': publisher_process})

        # Give publisher time to create shared memory
        print("⏳ Waiting for publisher to create shared memory...")
        time.sleep(1)

        # Launch subscriber
        print("👥 Launching subscriber process...")
        subscriber_cmd = ['python', 'simple_subscriber.py', test_name, '0', str(duration_seconds)]
        subscriber_process = subprocess.Popen(subscriber_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        processes.append({'type': 'subscriber', 'process': subscriber_process})

        # Wait for completion with timeout
        max_wait_time = duration_seconds + 10
        end_time = time.time() + max_wait_time

        while time.time() < end_time:
            # Check if all processes completed
            all_completed = True
            for p in processes:
                if p['process'].poll() is None:
                    all_completed = False
                    break

            if all_completed:
                break

            time.sleep(0.1)

        # Force terminate any remaining processes
        for p in processes:
            if p['process'].poll() is None:
                print(f"🛑 Terminating {p['type']} process...")
                p['process'].terminate()
                try:
                    p['process'].wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p['process'].kill()

        # Collect results
        results = []
        for p in processes:
            stdout, stderr = p['process'].communicate()
            if stdout:
                print(f"📤 {p['type']} output:\n{stdout}")
                # Try to parse JSON results
                try:
                    result = json.loads(stdout.strip())
                    results.append(result)
                except:
                    pass
            if stderr:
                print(f"⚠️  {p['type']} errors:\n{stderr}")

        # Calculate and display performance
        if len(results) >= 2:  # Both publisher and subscriber results
            pub_result = next((r for r in results if r.get('type') == 'publisher'), None)
            sub_result = next((r for r in results if r.get('type') == 'subscriber'), None)

            if pub_result and sub_result:
                print("\n" + "=" * 60)
                print("📊 FUTEX PERFORMANCE RESULTS")
                print("=" * 60)

                print(f"\n🎯 TARGET:")
                print(f"   - Target frequency: {target_hz} Hz")
                print(f"   - Message size: {message_size_mb} MB")
                print(f"   - Duration: {duration_seconds} seconds")

                print(f"\n📈 PUBLISHER RESULTS:")
                print(f"   - Messages sent: {pub_result.get('message_count', 0):,}")
                print(f"   - Actual frequency: {pub_result.get('actual_hz', 0):.2f} Hz")
                print(f"   - Target achieved: {(pub_result.get('actual_hz', 0) / target_hz * 100):.1f}%")

                print(f"\n👥 SUBSCRIBER RESULTS:")
                print(f"   - Messages received: {sub_result.get('message_count', 0):,}")
                print(f"   - Actual frequency: {sub_result.get('actual_hz', 0):.2f} Hz")
                print(f"   - Average latency: {sub_result.get('avg_latency_ms', 0):.2f} ms")
                print(f"   - Message loss: {((pub_result.get('message_count', 0) - sub_result.get('message_count', 0)) / pub_result.get('message_count', 1) * 100):.1f}%")

                # Calculate throughput
                if sub_result.get('message_count', 0) > 0:
                    total_bytes = sub_result['message_count'] * message_size_mb * 1024 * 1024
                    throughput_mbps = total_bytes / duration_seconds / (1024 * 1024)
                    print(f"   - Throughput: {throughput_mbps:.2f} MB/s ({throughput_mbps / 1024:.3f} GB/s)")

                # Performance assessment
                pub_hz = pub_result.get('actual_hz', 0)
                if pub_hz >= target_hz * 0.9:
                    print(f"\n🎉 EXCELLENT! Futex optimization achieving {pub_hz:.1f} Hz ({pub_hz/target_hz*100:.1f}% of target)")
                elif pub_hz >= target_hz * 0.7:
                    print(f"\n✅ GOOD! Futex optimization achieving {pub_hz:.1f} Hz ({pub_hz/target_hz*100:.1f}% of target)")
                else:
                    print(f"\n⚠️  Futex optimization only achieving {pub_hz:.1f} Hz ({pub_hz/target_hz*100:.1f}% of target)")

                print("\n" + "=" * 60)
                return True
            else:
                print("❌ Could not parse results from both processes")
                return False
        else:
            print("❌ Did not receive results from both processes")
            return False

    except Exception as e:
        print(f"❌ Performance test error: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        # Clean up
        try:
            subprocess.run(['python', '-c', f'import ultrapubsub; ultrapubsub.cleanup_shared_memory("{test_name}")'],
                          timeout=5, capture_output=True)
        except:
            pass

if __name__ == "__main__":
    import sys
    success = run_futex_performance_test()
    if success:
        print("✅ Futex performance test completed successfully")
    else:
        print("❌ Futex performance test failed")
        sys.exit(1)