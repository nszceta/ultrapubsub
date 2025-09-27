#!/usr/bin/env python3
"""
Process orchestrator for performance benchmarking with proper timeouts
"""
import subprocess
import time
import json
import signal
import os
import sys
import glob
from pathlib import Path

def cleanup_process_tree(pid, timeout=10):
    """Clean up a process tree with timeout"""
    try:
        import psutil
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)

        # Send SIGTERM first
        parent.terminate()
        for child in children:
            child.terminate()

        # Wait for graceful termination
        gone, alive = psutil.wait_procs([parent] + children, timeout=timeout)

        # Force kill if still alive
        for proc in alive:
            proc.kill()

    except ImportError:
        # Fallback to basic signal handling
        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(2)
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

def run_benchmark_orchestrator(test_name="/benchmark_test", message_size_mb=35, num_subscribers=6, duration_seconds=10):
    """Run benchmark with separate processes and proper timeouts"""
    print(f"🎯 Benchmark Orchestrator Started")
    print(f"📊 Configuration:")
    print(f"   - Test name: {test_name}")
    print(f"   - Message size: {message_size_mb} MB")
    print(f"   - Subscribers: {num_subscribers}")
    print(f"   - Duration: {duration_seconds} seconds")
    print(f"   - Target: 1.4 GB/s at 40 Hz")
    print()

    processes = []
    start_time = time.time()
    max_total_time = 150  # 150 seconds total timeout

    try:
        # Clean up any existing shared memory
        try:
            subprocess.run(['python', '-c', f'import ultrapubsub; ultrapubsub.cleanup_shared_memory("{test_name}")'],
                          timeout=5, capture_output=True)
        except:
            pass

        # Launch publisher process first
        print(f"🚀 Launching publisher process...")
        publisher_cmd = ['python', 'publisher_benchmark.py', test_name, str(message_size_mb), str(duration_seconds), '40.0']
        publisher_process = subprocess.Popen(publisher_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        processes.append({'type': 'publisher', 'process': publisher_process})
        print(f"✅ Publisher launched (PID: {publisher_process.pid})")

        # Give publisher time to initialize and create shared memory
        print(f"⏳ Waiting for publisher to initialize...")
        time.sleep(3)

        # Launch subscriber processes
        print(f"🚀 Launching {num_subscribers} subscriber processes...")
        for i in range(num_subscribers):
            cmd = ['python', 'subscriber_benchmark.py', test_name, str(i), str(duration_seconds)]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            processes.append({'type': 'subscriber', 'id': i, 'process': process})
            print(f"✅ Subscriber {i} launched (PID: {process.pid})")

        # Give subscribers time to start and register
        time.sleep(2)

        # Wait for completion with timeout
        print(f"⏱️  Running benchmark for {duration_seconds} seconds...")
        end_time = start_time + max_total_time

        while time.time() < end_time:
            # Check if all processes are still running
            running_processes = []
            for p in processes:
                if p['process'].poll() is None:
                    running_processes.append(p)
                else:
                    # Process has finished, capture output
                    stdout, stderr = p['process'].communicate()
                    if stdout:
                        print(f"📤 {p['type']} {p.get('id', '')} output:\n{stdout}")
                    if stderr:
                        print(f"⚠️  {p['type']} {p.get('id', '')} errors:\n{stderr}")

            if not running_processes:
                print("✅ All processes completed")
                break

            # Check total time timeout
            if time.time() - start_time > max_total_time:
                print(f"⏰ Total time exceeded {max_total_time}s, terminating all processes")
                break

            time.sleep(1)

        # Cleanup remaining processes
        print(f"🧹 Cleaning up processes...")
        for p in processes:
            if p['process'].poll() is None:
                print(f"🛑 Terminating {p['type']} {p.get('id', '')} (PID: {p['process'].pid})")
                cleanup_process_tree(p['process'].pid, timeout=5)

        # Collect results
        print(f"📊 Collecting results...")
        results = {
            'test_name': test_name,
            'message_size_mb': message_size_mb,
            'num_subscribers': num_subscribers,
            'duration_seconds': duration_seconds,
            'total_test_time': time.time() - start_time,
            'subscriber_results': [],
            'publisher_result': None,
            'target_throughput_mbps': 1.4 * 1024,  # 1.4 GB/s in MB/s
            'target_frequency_hz': 40.0
        }

        # Collect subscriber results
        for i in range(num_subscribers):
            result_file = f'/tmp/subscriber_{i}_results.txt'
            if os.path.exists(result_file):
                try:
                    with open(result_file, 'r') as f:
                        subscriber_result = json.load(f)
                        results['subscriber_results'].append(subscriber_result)
                        os.remove(result_file)  # Clean up
                except Exception as e:
                    print(f"⚠️  Failed to read subscriber {i} results: {e}")

        # Calculate aggregate metrics
        if results['subscriber_results']:
            total_received = sum(r['receive_count'] for r in results['subscriber_results'])
            total_bytes = sum(r['total_bytes'] for r in results['subscriber_results'])
            avg_duration = sum(r['duration'] for r in results['subscriber_results']) / len(results['subscriber_results'])

            if total_received > 0 and avg_duration > 0:
                throughput_mbps = (total_bytes / avg_duration) / (1024 * 1024)
                frequency_hz = total_received / avg_duration
                avg_latency = sum(r['avg_latency_ms'] for r in results['subscriber_results']) / len(results['subscriber_results'])

                results['aggregated'] = {
                    'total_messages_received': total_received,
                    'total_bytes_received': total_bytes,
                    'throughput_mbps': throughput_mbps,
                    'frequency_hz': frequency_hz,
                    'avg_latency_ms': avg_latency,
                    'throughput_ratio': throughput_mbps / results['target_throughput_mbps'],
                    'frequency_ratio': frequency_hz / results['target_frequency_hz']
                }

        return results

    except Exception as e:
        print(f"❌ Orchestrator error: {e}")
        import traceback
        traceback.print_exc()
        return None

def print_benchmark_results(results):
    """Print comprehensive benchmark results"""
    if not results:
        print("❌ No results to display")
        return

    print("\n" + "="*80)
    print("📊 PERFORMANCE BENCHMARK RESULTS")
    print("="*80)

    print(f"\n🎯 TARGET PERFORMANCE:")
    print(f"   - Throughput: {results['target_throughput_mbps']:.1f} MB/s (1.4 GB/s)")
    print(f"   - Frequency: {results['target_frequency_hz']:.1f} Hz")

    print(f"\n⚙️  TEST CONFIGURATION:")
    print(f"   - Test name: {results['test_name']}")
    print(f"   - Message size: {results['message_size_mb']} MB")
    print(f"   - Subscribers: {results['num_subscribers']}")
    print(f"   - Duration: {results['duration_seconds']} seconds")
    print(f"   - Total test time: {results['total_test_time']:.1f} seconds")

    if 'aggregated' in results:
        agg = results['aggregated']
        print(f"\n📈 ACTUAL PERFORMANCE:")
        print(f"   - Messages received: {agg['total_messages_received']:,}")
        print(f"   - Total data: {agg['total_bytes_received']:,} bytes ({agg['total_bytes_received'] / 1024 / 1024:.1f} MB)")
        print(f"   - Throughput: {agg['throughput_mbps']:.2f} MB/s ({agg['throughput_mbps'] / 1024:.3f} GB/s)")
        print(f"   - Frequency: {agg['frequency_hz']:.2f} Hz")
        print(f"   - Average latency: {agg['avg_latency_ms']:.2f} ms")

        print(f"\n📊 PERFORMANCE RATIOS:")
        print(f"   - Throughput achieved: {agg['throughput_ratio'] * 100:.1f}% of target")
        print(f"   - Frequency achieved: {agg['frequency_ratio'] * 100:.1f}% of target")

        # Performance assessment
        if agg['throughput_ratio'] >= 1.0 and agg['frequency_ratio'] >= 1.0:
            print("\n🎉 PERFORMANCE TARGETS ACHIEVED!")
        elif agg['throughput_ratio'] >= 0.8 and agg['frequency_ratio'] >= 0.8:
            print("\n⚠️  CLOSE TO TARGETS - Further optimization needed")
        else:
            print("\n❌ PERFORMANCE TARGETS NOT MET - Significant optimization required")

    print(f"\n📋 SUBSCRIBER BREAKDOWN:")
    for result in results['subscriber_results']:
        print(f"   - Subscriber {result['subscriber_id']}: {result['receive_count']:,} msgs, {result['throughput_mbps']:.2f} MB/s, {result['frequency_hz']:.2f} Hz, {result['avg_latency_ms']:.2f} ms")

    print("\n" + "="*80)

def main():
    """Main orchestrator function"""
    print("UltraPubSub Process-Based Performance Benchmark")
    print("This will test the current implementation with separate processes.")
    print()

    # Run benchmark
    results = run_benchmark_orchestrator(
        test_name="/baseline_benchmark",
        message_size_mb=35,
        num_subscribers=6,
        duration_seconds=10
    )

    if results:
        print_benchmark_results(results)

        # Save results to file for comparison
        with open('/tmp/baseline_benchmark_results.json', 'w') as f:
            json.dump(results, f, indent=2)

        print(f"\n💾 Results saved to /tmp/baseline_benchmark_results.json")
    else:
        print("❌ Benchmark failed to complete")

if __name__ == "__main__":
    main()