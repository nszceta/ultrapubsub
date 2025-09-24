#!/usr/bin/env python3
"""
Simple debug test for IPC communication.
"""

import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_single_process_ipc():
    """Test IPC in a single process to isolate the issue."""
    try:
        from ultrapubsub import SharedMemory

        logger.info("=== Single Process IPC Debug Test ===")

        # Create shared memory and publisher
        shm = SharedMemory("simple_debug_test")
        publisher = shm.create_publisher()

        logger.info("Publisher created")

        # Create subscriber
        subscriber = shm.create_subscriber()

        logger.info("Subscriber created")

        # Test sending and receiving
        test_message = b"Hello IPC test!"
        logger.info(f"Sending message: {test_message}")

        try:
            publisher.publish(test_message)
            logger.info("Message sent successfully")
        except Exception as e:
            logger.error(f"Publisher failed: {e}")
            import traceback
            traceback.print_exc()
            return

        # Try to receive immediately
        time.sleep(0.1)  # Small delay

        received_message = subscriber.receive(timeout=1.0)
        if received_message:
            logger.info(f"Received message: {received_message}")
            logger.info("✅ IPC working in single process")
        else:
            logger.error("❌ No message received in single process")

    except Exception as e:
        logger.error(f"Single process test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_single_process_ipc()