#!/usr/bin/env python3
"""
Setup script for ultrapubsub Python extension
"""

import os
import subprocess
import sys

def build_rust_extension():
    """Build the Rust extension using maturin"""
    try:
        # Change to the ultrapubsub directory
        os.chdir("ultrapubsub")
        
        # Build using maturin
        result = subprocess.run([
            "maturin", "build", "--release", "--out", "../target/wheels"
        ], check=True, capture_output=True, text=True)
        
        print("✅ Rust extension built successfully")
        print(result.stdout)
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to build Rust extension: {e}")
        print(f"Error output: {e.stderr}")
        return False
    except FileNotFoundError:
        print("❌ maturin not found. Installing...")
        try:
            subprocess.run([sys.executable, "-m", "pip", "install", "maturin"], check=True)
            return build_rust_extension()
        except subprocess.CalledProcessError:
            print("❌ Failed to install maturin")
            return False

if __name__ == "__main__":
    if build_rust_extension():
        print("🎉 Extension built successfully!")
    else:
        sys.exit(1)