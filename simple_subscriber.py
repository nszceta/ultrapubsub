#!/usr/bin/env python3
"""
Simple subscriber for futex performance test
"""
import sys
import time
import ultrapubsub
import json
import os

def run_simple_subscriber(test_name, subscriber_id, duration_seconds):
    """Run simple subscriber test"""
    try:
        # Create and register subscriber
        subscriber = ultrapubsub.create_subscriber_with_id(test_name, subscriber_id)
        subscriber.register()

        # Start receiving
        start_time = time.time()
        end_time = start_time + duration_seconds
        message_count = 0
        total_bytes = 0
        latencies = []

        print(f"👥 Subscriber {subscriber_id} waiting for messages...")

        try:
            while time.time() < end_time:
                receive_start = time.time()

                # Receive message with timeout
                received = subscriber.receive(timeout=1.0)
                if received:
                    receive_end = time.time()
                    latency_ms = (receive_end - receive_start) * 1000

                    message_count += 1
                    total_bytes += len(received)
                    latencies.append(latency_ms)

        except Exception as e:
            # Expected timeout when test ends
            pass

        # Calculate results
        actual_duration = time.time() - start_time
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        actual_hz = message_count / actual_duration if actual_duration > 0 else 0

        result = {
            'type': 'subscriber',
            'subscriber_id': subscriber_id,
            'message_count': message_count,
            'total_bytes': total_bytes,
            'duration': actual_duration,
            'actual_hz': actual_hz,
            'avg_latency_ms': avg_latency,
            'pid': os.getpid()
        }

        print(json.dumps(result))
        return 0

    except Exception as e:
        result = {
            'type': 'subscriber',
            'subscriber_id': subscriber_id,
            'error': str(e),
            'message_count': 0,
            'actual_hz': 0
        }
        print(json.dumps(result))
        return 1

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(json.dumps({'error': 'Usage: python simple_subscriber.py <test_name> <subscriber_id> <duration_seconds>'}))
        sys.exit(1)

    test_name = sys.argv[1]
    subscriber_id = int(sys.argv[2])
    duration_seconds = int(sys.argv[3])

    sys.exit(run_simple_subscriber(test_name, subscriber_id, duration_seconds))