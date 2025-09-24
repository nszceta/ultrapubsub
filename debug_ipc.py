#!/usr/bin/env python3
"""
Debug IPC communication to understand why subscribers receive 0 messages.
"""

import time
import multiprocessing as mp
from multiprocessing import Process, Queue
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_simple_publisher_subscriber():
    """Test basic publisher-subscriber communication with debugging."""
    try:
        from ultrapubsub import SharedMemory

        logger.info("=== Simple Publisher-Subscriber Debug Test ===")

        # Publisher process
        def publisher_func(result_queue):
            try:
                shm = SharedMemory("debug_test")
                publisher = shm.create_publisher()

                logger.info("Publisher: Starting...")

                # Send a single message
                test_message = b"Hello from publisher!"
                logger.info(f"Publisher: Sending message: {test_message}")

                publisher.publish(test_message)
                logger.info("Publisher: Message sent")

                # Wait a bit to ensure subscriber has time to receive
                time.sleep(2)

                result_queue.put({"status": "success", "message": "Publisher completed"})
            except Exception as e:
                logger.error(f"Publisher error: {e}")
                result_queue.put({"status": "error", "error": str(e)})

        # Subscriber function
        def subscriber_func(result_queue):
            try:
                shm = SharedMemory.attach("debug_test")
                subscriber = shm.create_subscriber()

                logger.info("Subscriber: Starting...")

                # Try to receive a message with timeout
                start_time = time.time()
                timeout = 5  # 5 seconds

                while time.time() - start_time < timeout:
                    try:
                        message = subscriber.receive(timeout=1.0)
                        if message:
                            logger.info(f"Subscriber: Received message: {message}")
                            result_queue.put({"status": "success", "message": f"Received: {message}"})
                            return
                        else:
                            logger.info("Subscriber: No message, waiting...")
                    except Exception as e:
                        logger.info(f"Subscriber: Exception while receiving: {e}")

                result_queue.put({"status": "timeout", "message": "No message received within timeout"})
            except Exception as e:
                logger.error(f"Subscriber error: {e}")
                result_queue.put({"status": "error", "error": str(e)})

        # Use spawn for independent processes
        mp.set_start_method('spawn')
        result_queue = Queue()

        # Start publisher first
        logger.info("Starting publisher...")
        pub_proc = Process(target=publisher_func, args=(result_queue,))
        pub_proc.start()

        # Give publisher time to create shared memory
        time.sleep(1)

        # Start subscriber
        logger.info("Starting subscriber...")
        sub_proc = Process(target=subscriber_func, args=(result_queue,))
        sub_proc.start()

        # Wait for completion
        pub_proc.join()
        sub_proc.join()

        # Collect results
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        logger.info("=== Results ===")
        for result in results:
            logger.info(f"  {result}")

    except Exception as e:
        logger.error(f"Test failed: {e}")

if __name__ == "__main__":
    test_simple_publisher_subscriber()