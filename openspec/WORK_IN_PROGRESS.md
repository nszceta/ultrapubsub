# Minimal Proof of Concept Implementation

## Overview
This document tracks the minimal proof of concept (PoC) implementation for ultrapubsub. The goal is to validate the core io_uring message passing approach with basic Python bindings.

## PoC Scope
- Single Rust project with io_uring dependency
- Basic publisher-subscriber message passing (single pair)
- Simple Python bindings using PyO3
- Small message validation (strings/binary data)
- No multi-subscriber, reference counting, or NumPy integration

## Current Status
- [ ] Create Rust project structure
- [ ] Implement basic io_uring setup
- [ ] Add simple shared memory allocation
- [ ] Create publisher-subscriber message passing
- [ ] Add PyO3 Python bindings
- [ ] Write basic test script
- [ ] Validate PoC functionality
- [ ] Measure baseline performance

## Success Criteria
- ✅ io_uring setup and message passing works
- ✅ Basic Python binding functional
- ✅ Single message round-trip successful
- ✅ No memory leaks in simple case
- ⏱️ Baseline performance measurement