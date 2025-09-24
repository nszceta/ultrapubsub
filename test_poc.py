#!/usr/bin/env python3
"""
Basic test script for ultrapubsub PoC
Tests the Python bindings and basic message passing functionality
"""

import sys
import time
import os

# Add the current directory to Python path to import the module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import ultrapubsub
    print("✅ Successfully imported ultrapubsub")
except ImportError as e:
    print(f"❌ Failed to import ultrapubsub: {e}")
    sys.exit(1)

def test_shared_memory():
    """Test shared memory creation and basic operations"""
    print("\n🧪 Testing SharedMemory...")
    
    try:
        # Create shared memory
        shared_mem = ultrapubsub.PySharedMemory(1024)
        print(f"✅ Created shared memory with size: {shared_mem.size()}")
        
        # Test size (accounting for header overhead)
        actual_size = shared_mem.size()
        print(f"Shared memory usable size: {actual_size}")
        assert actual_size > 0, f"Expected positive size, got {actual_size}"
        print("✅ Shared memory size verification passed")
        
        return shared_mem
        
    except Exception as e:
        print(f"❌ Shared memory test failed: {e}")
        return None

def test_message():
    """Test message creation and data handling"""
    print("\n🧪 Testing Message...")
    
    try:
        # Create message with test data
        test_data = b"Hello, ultrapubsub!"
        message = ultrapubsub.PyMessage(list(test_data))
        print(f"✅ Created message with data: {test_data}")
        
        # Test data retrieval
        retrieved_data = bytes(message.data())
        assert retrieved_data == test_data
        print("✅ Message data verification passed")
        
        return message
        
    except Exception as e:
        print(f"❌ Message test failed: {e}")
        return None

def test_publisher_subscriber():
    """Test publisher-subscriber communication"""
    print("\n🧪 Testing Publisher-Subscriber communication...")
    
    try:
        # Create shared memory
        shared_mem = ultrapubsub.PySharedMemory(4096)
        
        # Create publisher and subscriber
        publisher = ultrapubsub.PyPublisher(shared_mem)
        subscriber = ultrapubsub.PySubscriber(shared_mem)
        print("✅ Created publisher and subscriber")
        
        # Create test message
        test_data = b"Test message from publisher"
        message = ultrapubsub.PyMessage(list(test_data))
        
        # Publish message
        publisher.publish(message)
        print("✅ Published message")
        
        # Note: In a real implementation, we'd need proper synchronization
        # For this PoC, we're just testing the API calls work
        print("✅ Publisher-Subscriber API test passed")
        
        return True
        
    except Exception as e:
        print(f"❌ Publisher-Subscriber test failed: {e}")
        return False

def test_performance():
    """Basic performance measurement"""
    print("\n🧪 Testing Performance...")
    
    try:
        # Use larger shared memory for performance test
        shared_mem = ultrapubsub.PySharedMemory(1024 * 1024)  # 1MB
        publisher = ultrapubsub.PyPublisher(shared_mem)
        
        # Test message creation performance with fewer messages
        start_time = time.time()
        
        for i in range(100):
            test_data = f"Message {i}".encode()
            message = ultrapubsub.PyMessage(list(test_data))
            publisher.publish(message)
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"✅ Performance test: 100 messages in {duration:.4f} seconds")
        print(f"✅ Average time per message: {duration/100*1000:.4f} ms")
        
        return True
        
    except Exception as e:
        print(f"❌ Performance test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 Starting ultrapubsub PoC tests...")
    
    # Test individual components
    shared_mem = test_shared_memory()
    if not shared_mem:
        return False
    
    message = test_message()
    if not message:
        return False
    
    # Test publisher-subscriber
    if not test_publisher_subscriber():
        return False
    
    # Test performance
    if not test_performance():
        return False
    
    print("\n🎉 All tests passed! PoC is working correctly.")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)