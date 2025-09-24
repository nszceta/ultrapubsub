#!/usr/bin/env python3
"""
Simple test to see io_uring setup debug output
"""
import sys
sys.path.insert(0, '.')

from ultrapubsub.ultrapubsub import PyHring, PyPublisher, PySubscriber

print("Creating Hring...")
hring = PyHring('debug_test', 32, 0, 0)
print("Hring created successfully")

print("Creating Publisher...")
publisher = PyPublisher(hring)
print("Publisher created successfully")

print("Creating Subscriber...")
subscriber = PySubscriber(hring)
print("Subscriber created successfully")

print("Test completed")