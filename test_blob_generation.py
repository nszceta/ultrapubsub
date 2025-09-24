#!/usr/bin/env python3
"""
Test script to verify large binary blob generation with indisputable signatures
"""

import sys
import os

# Add the current directory to Python path to import the module
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import ultrapubsub
    print("✅ Successfully imported ultrapubsub")
except ImportError as e:
    print(f"❌ Failed to import ultrapubsub: {e}")
    sys.exit(1)

# Import functions from test_multi_subscriber
from test_multi_subscriber import generate_large_blob, verify_blob_signatures, calculate_checksum

def test_blob_generation():
    """Test generation of large binary blobs with indisputable signatures"""
    print("\n🧪 Testing large binary blob generation with indisputable signatures...")

    test_sizes = [1, 5, 10, 20]  # MB

    for size_mb in test_sizes:
        print(f"\n📊 Testing {size_mb}MB blob generation...")

        # Generate blob with signatures
        print(f"   Generating {size_mb}MB blob...")
        blob = generate_large_blob(size_mb)

        # Verify size
        expected_min_size = size_mb * 1024 * 1024
        print(f"   Generated blob size: {len(blob)} bytes (expected min: {expected_min_size})")

        if len(blob) < expected_min_size:
            print(f"❌ Blob too small: got {len(blob)}, expected at least {expected_min_size}")
            return False

        # Verify signatures
        print(f"   Verifying signatures...")
        if verify_blob_signatures(blob, size_mb):
            print(f"   ✅ Signatures verified successfully")
        else:
            print(f"   ❌ Signature verification failed")
            return False

        # Calculate checksum
        checksum = calculate_checksum(blob)
        print(f"   Checksum: {checksum[:16]}...")

        # Show signature details
        header_signature = b"ULTRAPUBSUB_BLOB_START_" + str(size_mb).encode() + b"MB"
        footer_signature = b"ULTRAPUBSUB_BLOB_END_" + str(size_mb).encode() + b"MB"

        print(f"   Header signature: {header_signature}")
        print(f"   Footer signature: {footer_signature}")
        print(f"   Payload size: {len(blob) - len(header_signature) - len(footer_signature)} bytes")

        print(f"   ✅ {size_mb}MB blob generated successfully with indisputable signatures!")

    print("\n✅ All blob generation tests passed!")
    return True

def test_multiple_blob_generation():
    """Test generation of multiple unique blobs"""
    print("\n🧪 Testing multiple unique blob generation...")

    size_mb = 5
    num_blobs = 5

    blobs = []
    checksums = []

    for i in range(num_blobs):
        print(f"\n📊 Generating blob {i+1}/{num_blobs} ({size_mb}MB)...")

        # Each blob is unique due to the signature generation
        blob = generate_large_blob(size_mb)
        checksum = calculate_checksum(blob)

        # Verify signatures
        if not verify_blob_signatures(blob, size_mb):
            print(f"   ❌ Signature verification failed for blob {i+1}")
            return False

        blobs.append(blob)
        checksums.append(checksum)

        print(f"   ✅ Blob {i+1} generated, checksum: {checksum[:16]}...")

    # Verify all blobs are unique
    if len(set(checksums)) == num_blobs:
        print(f"\n✅ All {num_blobs} blobs are unique (different checksums)")
    else:
        print(f"\n❌ Blob uniqueness check failed - some blobs have identical checksums")
        return False

    print(f"\n✅ Successfully generated {num_blobs} unique {size_mb}MB blobs with indisputable signatures!")
    return True

if __name__ == "__main__":
    print("🧪 Large Binary Blob Generation Test Suite")
    print("=" * 50)

    # Test 1: Single blob generation
    test1_success = test_blob_generation()

    # Test 2: Multiple blob generation
    test2_success = test_multiple_blob_generation()

    print("\n" + "=" * 50)
    print("📊 SUMMARY")
    print("=" * 50)
    print(f"Single blob generation: {'✅ PASS' if test1_success else '❌ FAIL'}")
    print(f"Multiple blob generation: {'✅ PASS' if test2_success else '❌ FAIL'}")

    overall_success = test1_success and test2_success
    if overall_success:
        print("\n🎉 ALL TESTS PASSED!")
        print("✅ Large binary blobs can be generated with indisputable signatures")
        print("✅ Signatures are correctly verified")
        print("✅ Multiple blobs are unique")
        print("✅ Payload integrity is maintained")
    else:
        print("\n❌ SOME TESTS FAILED!")

    sys.exit(0 if overall_success else 1)