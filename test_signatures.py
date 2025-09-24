#!/usr/bin/env python3
"""
Test script to verify blob signature generation and verification
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
from test_multi_subscriber import generate_large_blob, verify_blob_signatures

def test_signature_verification():
    """Test that blob signatures are generated and verified correctly"""
    print("\n🧪 Testing blob signature generation and verification...")

    test_sizes = [1, 2, 5, 10, 20]  # MB

    for size_mb in test_sizes:
        print(f"\n📊 Testing {size_mb}MB blob...")

        # Generate blob with signatures
        blob = generate_large_blob(size_mb)
        base_size = size_mb * 1024 * 1024

        print(f"   Generated blob size: {len(blob)} bytes (base size: {base_size})")

        # Verify size is close to expected (signatures add some overhead)
        if len(blob) < base_size:
            print(f"❌ Blob too small: got {len(blob)}, expected at least {base_size}")
            return False

        # Verify signatures
        if verify_blob_signatures(blob, size_mb):
            print(f"✅ Signatures verified for {size_mb}MB blob")
        else:
            print(f"❌ Signature verification failed for {size_mb}MB blob")
            return False

        # Print first and last few bytes for visual verification
        print(f"   First 50 bytes: {blob[:50]}")
        print(f"   Last 50 bytes:  {blob[-50:]}")

    print("\n✅ All signature tests passed!")
    return True

if __name__ == "__main__":
    success = test_signature_verification()
    sys.exit(0 if success else 1)