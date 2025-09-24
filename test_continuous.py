#!/usr/bin/env python3
import sys
import time
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_continuous_publish():
    shm = SharedMemory('continuous_test')
    publisher = shm.create_publisher()

    msg = b'X' * (1024 * 1024)  # 1MB

    print("Starting continuous publish test...")
    count = 0
    start_time = time.time()

    while True:
        seq = publisher.publish(msg)
        count += 1

        if count % 10 == 0:
            elapsed = time.time() - start_time
            rate = count / elapsed
            print(f"Published {count} messages, rate: {rate:.1f} Hz")

        time.sleep(0.1)  # 10Hz target

if __name__ == "__main__":
    test_continuous_publish()