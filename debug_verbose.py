#!/usr/bin/env python3
"""
More verbose test to see io_uring setup debug output
"""
import sys
import logging
import traceback
sys.path.insert(0, '.')

# Set up logging
logging.basicConfig(level=logging.DEBUG)

try:
    print("Importing...")
    from ultrapubsub.ultrapubsub import PyHring, PyPublisher, PySubscriber
    print("Import successful")

    print("Creating Hring with large entries...")
    hring = PyHring('debug_test', 128, 0, 0)
    print("Hring created successfully")

    print("Creating Publisher...")
    publisher = PyPublisher(hring)
    print("Publisher created successfully")

    print("Creating Subscriber...")
    subscriber = PySubscriber(hring)
    print("Subscriber created successfully")

    print("Test completed successfully")

except Exception as e:
    print(f"Error occurred: {e}")
    print("Traceback:")
    traceback.print_exc()