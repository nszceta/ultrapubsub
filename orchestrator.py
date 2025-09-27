#!/usr/bin/env python3
"""
Orchestrator for UltraPubSub testing
Manages publisher and subscriber processes with proper timing
"""
import subprocess
import time
import sys
import signal
import os

def run_orchestrator():
    test_name = "/orchestrator_test"
    message_count = 10
    num_subscribers = 6

    print("🚀 Starting UltraPubSub Orchestrator Test")
    print(f"Test name: {test_name}")
    print(f"Message count: {message_count}")
    print(f"Number of subscribers: {num_subscribers}")

    processes = []

    try:
        # Start publisher process first to create shared memory
        print("\n📤 Starting publisher process...")
        publisher_cmd = [sys.executable, "simple_publisher.py", test_name, str(message_count)]
        publisher_process = subprocess.Popen(publisher_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        processes.append(('publisher', 0, publisher_process))
        print(f"  Started publisher (PID: {publisher_process.pid})")

        # Give publisher time to create shared memory
        print("\n⏳ Waiting for publisher to initialize...")
        time.sleep(1)

        # Start all subscriber processes
        print("\n👥 Starting subscriber processes...")
        for i in range(num_subscribers):
            cmd = [sys.executable, "simple_subscriber.py", test_name, str(i)]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            processes.append(('subscriber', i, process))
            print(f"  Started subscriber {i} (PID: {process.pid})")
            time.sleep(0.1)  # Small delay between starting subscribers

        # Give subscribers time to initialize and register
        print("\n⏳ Waiting for subscribers to initialize and register...")
        time.sleep(1)

        # Wait for all processes to complete
        print("\n⏳ Waiting for all processes to complete...")
        all_completed = False
        timeout = 30  # 30 second timeout
        start_time = time.time()

        while not all_completed and (time.time() - start_time) < timeout:
            all_completed = all(process[2].poll() is not None for process in processes)
            if not all_completed:
                time.sleep(0.5)

        # Check timeout
        if not all_completed:
            print(f"\n⚠️  Timeout reached after {timeout} seconds")
            print("Terminating remaining processes...")
            for name, id, process in processes:
                if process.poll() is None:
                    process.terminate()
                    print(f"  Terminated {name} {id}")

        # Collect and display results
        print("\n📊 Collecting results...")
        for name, id, process in processes:
            stdout, stderr = process.communicate()
            return_code = process.returncode

            print(f"\n{'='*50}")
            print(f"{name.upper()} {id} (PID: {process.pid})")
            print(f"Return code: {return_code}")
            if stdout:
                print("STDOUT:")
                for line in stdout.strip().split('\n'):
                    print(f"  {line}")
            if stderr:
                print("STDERR:")
                for line in stderr.strip().split('\n'):
                    print(f"  {line}")

        print(f"\n{'='*50}")
        print("🏁 Orchestrator test completed")

    except KeyboardInterrupt:
        print("\n🛑 Keyboard interrupt received")
        print("Terminating all processes...")
        for name, id, process in processes:
            if process.poll() is None:
                process.terminate()
                print(f"  Terminated {name} {id}")
    except Exception as e:
        print(f"\n❌ Orchestrator error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Final cleanup
        print("\n🧹 Final cleanup...")
        try:
            import ultrapubsub
            ultrapubsub.cleanup_shared_memory(test_name)
            print("✅ Shared memory cleaned up")
        except:
            pass

if __name__ == "__main__":
    run_orchestrator()