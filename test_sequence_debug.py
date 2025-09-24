#!/usr/bin/env python3
"""
Debug test to understand sequence number handling
"""
import sys
sys.path.insert(0, '.')

from ultrapubsub import SharedMemory

def test_sequence_debug():
    try:
        print("Creating shared memory...")
        shm = SharedMemory('test_sequence_debug')

        print("Creating publisher and subscriber...")
        publisher = shm.create_publisher()
        subscriber = shm.create_subscriber()

        # Clear any filter
        subscriber.clear_filter()

        print("Testing message sequence...")

        # Test message 1
        msg1 = b"message 1"
        print(f"Publishing message 1: {len(msg1)} bytes")
        seq1 = publisher.publish(msg1)
        print(f"Sequence 1: {seq1}")

        received1 = subscriber.receive(timeout=2.0)
        if received1:
            print(f"✅ Received message 1: {len(received1)} bytes")
        else:
            print("❌ Failed to receive message 1")

        # Test message 2
        msg2 = b"message 2"
        print(f"\nPublishing message 2: {len(msg2)} bytes")
        seq2 = publisher.publish(msg2)
        print(f"Sequence 2: {seq2}")

        received2 = subscriber.receive(timeout=2.0)
        if received2:
            print(f"✅ Received message 2: {len(received2)} bytes")
        else:
            print("❌ Failed to receive message 2")

        # Test large message
        msg_large = b'X' * (1024 * 1024)
        print(f"\nPublishing large message: {len(msg_large)} bytes")
        seq_large = publisher.publish(msg_large)
        print(f"Large sequence: {seq_large}")

        received_large = subscriber.receive(timeout=2.0)
        if received_large:
            print(f"✅ Received large message: {len(received_large)} bytes")
        else:
            print("❌ Failed to receive large message")

        # Test another small message after large
        msg3 = b"message 3"
        print(f"\nPublishing message 3: {len(msg3)} bytes")
        seq3 = publisher.publish(msg3)
        print(f"Sequence 3: {seq3}")

        received3 = subscriber.receive(timeout=2.0)
        if received3:
            print(f"✅ Received message 3: {len(received3)} bytes")
        else:
            print("❌ Failed to receive message 3")

        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("Sequence debugging...")
    success = test_sequence_debug()
    if success:
        print("✅ Sequence debug completed!")
    else:
        print("💥 Sequence debug FAILED!")