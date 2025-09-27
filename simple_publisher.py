#!/usr/bin/env python3
"""
Simple publisher for futex performance test
"""
import sys
import time
import ultrapubsub
import json
import os

def run_simple_publisher(test_name, message_size_mb, duration_seconds, target_hz):
    """Run simple publisher test"""
    try:
        # Create test message
        test_message = b'X' * (message_size_mb * 1024 * 1024)

        # Create publisher
        publisher = ultrapubsub.create_publisher(test_name)

        # Wait for subscriber
        max_wait = 10.0
        wait_start = time.time()
        while publisher.subscriber_count() == 0 and (time.time() - wait_start) < max_wait:
            time.sleep(0.1)

        if publisher.subscriber_count() == 0:
            result = {
                'type': 'publisher',
                'error': 'No subscribers registered',
                'message_count': 0,
                'actual_hz': 0
            }
            print(json.dumps(result))
            return 1

        # Start broadcasting
        start_time = time.time()
        end_time = start_time + duration_seconds
        message_count = 0

        try:
            while time.time() < end_time:
                broadcast_start = time.time()

                # Broadcast message
                publisher.broadcast(test_message)
                message_count += 1

                # Maintain target frequency
                actual_broadcast_time = time.time() - broadcast_start
                cycle_time = 1.0 / target_hz
                sleep_time = max(0, cycle_time - actual_broadcast_time)
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except Exception as e:
            result = {
                'type': 'publisher',
                'error': str(e),
                'message_count': message_count,
                'actual_hz': 0
            }
            print(json.dumps(result))
            return 1

        # Calculate results
        actual_duration = time.time() - start_time
        actual_hz = message_count / actual_duration if actual_duration > 0 else 0

        result = {
            'type': 'publisher',
            'message_count': message_count,
            'duration': actual_duration,
            'actual_hz': actual_hz,
            'target_hz': target_hz,
            'subscribers': publisher.subscriber_count(),
            'pid': os.getpid()
        }

        print(json.dumps(result))
        return 0

    except Exception as e:
        result = {
            'type': 'publisher',
            'error': str(e),
            'message_count': 0,
            'actual_hz': 0
        }
        print(json.dumps(result))
        return 1

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(json.dumps({'error': 'Usage: python simple_publisher.py <test_name> <message_size_mb> <duration_seconds> <target_hz>'}))
        sys.exit(1)

    test_name = sys.argv[1]
    message_size_mb = int(sys.argv[2])
    duration_seconds = int(sys.argv[3])
    target_hz = float(sys.argv[4])

    sys.exit(run_simple_publisher(test_name, message_size_mb, duration_seconds, target_hz))