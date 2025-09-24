"""
Payload generation and validation utilities for performance testing.

This module provides comprehensive payload generation capabilities including:
- 20MB payload generation with encoded timestamps
- Unique start/end signatures for integrity verification
- Checksum calculation and corruption detection
- Payload validation and integrity checking
"""

import hashlib
import time
import struct
from dataclasses import dataclass
from typing import Optional, Tuple, Dict, Any
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class PayloadPattern(Enum):
    """Payload generation patterns."""
    SEQUENTIAL = "sequential"  # Sequential bytes
    RANDOM = "random"  # Random bytes
    ZEROS = "zeros"  # All zeros
    REPEATING = "repeating"  # Repeating pattern


@dataclass
class PayloadMetadata:
    """Metadata embedded in payload."""
    start_signature: str
    end_signature: str
    timestamp_us: int
    sequence_number: int
    payload_size: int
    checksum: str
    payload_pattern: PayloadPattern


class PayloadGenerator:
    """Payload generation with integrity verification."""

    START_SIGNATURE_PREFIX = "ULTRAPUBSUB_PERF_START_"
    END_SIGNATURE_PREFIX = "ULTRAPUBSUB_PERF_END_"

    def __init__(self, payload_size: int = 20 * 1024 * 1024,
                 include_timestamp: bool = True,
                 include_checksum: bool = True,
                 pattern: PayloadPattern = PayloadPattern.SEQUENTIAL):
        self.payload_size = payload_size
        self.include_timestamp = include_timestamp
        self.include_checksum = include_checksum
        self.pattern = pattern
        self.sequence_counter = 0

    def generate_payload(self, sequence_number: Optional[int] = None) -> Tuple[bytes, PayloadMetadata]:
        """Generate a test payload with embedded metadata."""
        if sequence_number is None:
            sequence_number = self.sequence_counter
            self.sequence_counter += 1

        timestamp_us = int(time.time() * 1_000_000)

        # Create metadata
        start_signature = f"{self.START_SIGNATURE_PREFIX}{timestamp_us}"
        end_signature = f"{self.END_SIGNATURE_PREFIX}{timestamp_us}"

        metadata = PayloadMetadata(
            start_signature=start_signature,
            end_signature=end_signature,
            timestamp_us=timestamp_us,
            sequence_number=sequence_number,
            payload_size=self.payload_size,
            checksum="",  # Will be calculated
            payload_pattern=self.pattern
        )

        # Generate payload data
        payload_data = self._generate_payload_data(sequence_number, timestamp_us)

        # Embed metadata
        payload_with_metadata = self._embed_metadata(payload_data, metadata)

        # Calculate checksum if enabled
        if self.include_checksum:
            checksum = self._calculate_checksum(payload_with_metadata)
            metadata.checksum = checksum
            # Re-embed with checksum
            payload_with_metadata = self._embed_metadata(payload_data, metadata)

        return payload_with_metadata, metadata

    def _generate_payload_data(self, sequence_number: int, timestamp_us: int) -> bytes:
        """Generate the actual payload data."""
        if self.pattern == PayloadPattern.SEQUENTIAL:
            # Sequential pattern starting from sequence_number
            data = bytes((sequence_number + i) % 256 for i in range(self.payload_size))
        elif self.pattern == PayloadPattern.RANDOM:
            # Deterministic "random" based on sequence number
            import random
            random.seed(sequence_number + timestamp_us)
            data = bytes(random.getrandbits(8) for _ in range(self.payload_size))
        elif self.pattern == PayloadPattern.ZEROS:
            # All zeros
            data = bytes(self.payload_size)
        elif self.pattern == PayloadPattern.REPEATING:
            # Repeating pattern
            pattern = bytes([sequence_number % 256, (sequence_number + 1) % 256,
                           (sequence_number + 2) % 256, (sequence_number + 3) % 256])
            data = (pattern * ((self.payload_size // len(pattern)) + 1))[:self.payload_size]
        else:
            raise ValueError(f"Unsupported payload pattern: {self.pattern}")

        return data

    def _embed_metadata(self, payload_data: bytes, metadata: PayloadMetadata) -> bytes:
        """Embed metadata into the payload."""
        # Create metadata block
        metadata_parts = [
            metadata.start_signature.encode('utf-8'),
            struct.pack('<Q', metadata.timestamp_us),  # 8-byte timestamp
            struct.pack('<I', metadata.sequence_number),  # 4-byte sequence number
            struct.pack('<I', metadata.payload_size),  # 4-byte payload size
            metadata.checksum.encode('utf-8'),
            metadata.end_signature.encode('utf-8')
        ]

        metadata_block = b''.join(metadata_parts)

        # Calculate how much data we need to adjust
        metadata_size = len(metadata_block)
        if metadata_size > self.payload_size:
            raise ValueError(f"Metadata size ({metadata_size}) exceeds payload size ({self.payload_size})")

        # Replace the beginning and end of the payload with metadata
        start_size = len(metadata.start_signature.encode('utf-8')) + 16  # signature + timestamp + seq + size
        end_size = len(metadata.checksum.encode('utf-8')) + len(metadata.end_signature.encode('utf-8'))

        # Ensure we don't overlap
        if start_size + end_size > self.payload_size:
            raise ValueError("Metadata exceeds payload capacity")

        # Create final payload
        start_data = metadata_block[:start_size]
        end_data = metadata_block[start_size:]

        # Replace payload sections
        middle_start = start_size
        middle_end = self.payload_size - len(end_data)

        final_payload = (
            start_data +
            payload_data[middle_start:middle_end] +
            end_data
        )

        # Ensure exact size
        assert len(final_payload) == self.payload_size, f"Payload size mismatch: {len(final_payload)} != {self.payload_size}"

        return final_payload

    def _calculate_checksum(self, data: bytes) -> str:
        """Calculate SHA-256 checksum of data."""
        return hashlib.sha256(data).hexdigest()

    def parse_payload(self, payload_data: bytes) -> Optional[PayloadMetadata]:
        """Parse metadata from payload."""
        try:
            # Find start signature
            start_sig_bytes = self.START_SIGNATURE_PREFIX.encode('utf-8')
            start_pos = payload_data.find(start_sig_bytes)
            if start_pos == -1:
                return None

            # Extract metadata components
            pos = start_pos + len(start_sig_bytes)

            # Read timestamp (8 bytes)
            timestamp_us = struct.unpack_from('<Q', payload_data, pos)[0]
            pos += 8

            # Read sequence number (4 bytes)
            sequence_number = struct.unpack_from('<I', payload_data, pos)[0]
            pos += 4

            # Read payload size (4 bytes)
            payload_size = struct.unpack_from('<I', payload_data, pos)[0]
            pos += 4

            # Find end signature to extract checksum
            end_sig_bytes = self.END_SIGNATURE_PREFIX.encode('utf-8')
            end_pos = payload_data.rfind(end_sig_bytes)
            if end_pos == -1:
                return None

            # Extract checksum (everything between payload size and end signature)
            checksum_start = pos
            checksum_end = end_pos
            checksum = payload_data[checksum_start:checksum_end].decode('utf-8')

            # Reconstruct signatures with timestamp
            start_signature = f"{self.START_SIGNATURE_PREFIX}{timestamp_us}"
            end_signature = f"{self.END_SIGNATURE_PREFIX}{timestamp_us}"

            return PayloadMetadata(
                start_signature=start_signature,
                end_signature=end_signature,
                timestamp_us=timestamp_us,
                sequence_number=sequence_number,
                payload_size=payload_size,
                checksum=checksum,
                payload_pattern=self.pattern  # Assume same pattern
            )

        except Exception as e:
            logger.error(f"Error parsing payload metadata: {e}")
            return None


class PayloadValidator:
    """Payload validation and integrity checking."""

    def __init__(self, generator: PayloadGenerator):
        self.generator = generator

    def validate_payload(self, payload_data: bytes) -> Tuple[bool, Optional[PayloadMetadata], Dict[str, Any]]:
        """Validate payload integrity."""
        validation_results = {
            'valid': False,
            'errors': [],
            'warnings': [],
            'size_correct': False,
            'signatures_valid': False,
            'checksum_valid': False,
            'timestamp_recent': False
        }

        # Check size
        if len(payload_data) != self.generator.payload_size:
            validation_results['errors'].append(
                f"Size mismatch: expected {self.generator.payload_size}, got {len(payload_data)}"
            )
            return False, None, validation_results

        validation_results['size_correct'] = True

        # Parse metadata
        metadata = self.generator.parse_payload(payload_data)
        if metadata is None:
            validation_results['errors'].append("Cannot parse payload metadata")
            return False, None, validation_results

        # Validate signatures
        expected_start = f"{self.generator.START_SIGNATURE_PREFIX}{metadata.timestamp_us}"
        expected_end = f"{self.generator.END_SIGNATURE_PREFIX}{metadata.timestamp_us}"

        if metadata.start_signature != expected_start:
            validation_results['errors'].append("Start signature mismatch")

        if metadata.end_signature != expected_end:
            validation_results['errors'].append("End signature mismatch")

        if metadata.start_signature == expected_start and metadata.end_signature == expected_end:
            validation_results['signatures_valid'] = True

        # Validate checksum if enabled
        if self.generator.include_checksum and metadata.checksum:
            calculated_checksum = self.generator._calculate_checksum(payload_data)
            if calculated_checksum != metadata.checksum:
                validation_results['errors'].append(
                    f"Checksum mismatch: expected {metadata.checksum}, got {calculated_checksum}"
                )
            else:
                validation_results['checksum_valid'] = True
        else:
            validation_results['checksum_valid'] = True  # Not required

        # Check timestamp (within 1 minute)
        current_time_us = int(time.time() * 1_000_000)
        time_diff_us = current_time_us - metadata.timestamp_us
        if abs(time_diff_us) <= 60_000_000:  # 1 minute
            validation_results['timestamp_recent'] = True
        else:
            validation_results['warnings'].append(
                f"Timestamp not recent: {time_diff_us / 1_000_000:.1f}s ago"
            )

        # Overall validation
        validation_results['valid'] = (
            validation_results['size_correct'] and
            validation_results['signatures_valid'] and
            validation_results['checksum_valid']
        )

        return validation_results['valid'], metadata, validation_results

    def validate_payload_sequence(self, payloads: list) -> Dict[str, Any]:
        """Validate a sequence of payloads for ordering and completeness."""
        results = {
            'total_payloads': len(payloads),
            'valid_payloads': 0,
            'invalid_payloads': 0,
            'sequence_gaps': [],
            'duplicate_sequences': [],
            'ordering_issues': [],
            'validation_details': []
        }

        sequence_numbers = []
        valid_metadata = []

        for i, payload in enumerate(payloads):
            is_valid, metadata, validation_detail = self.validate_payload(payload)
            results['validation_details'].append({
                'index': i,
                'valid': is_valid,
                'errors': validation_detail['errors'],
                'warnings': validation_detail['warnings']
            })

            if is_valid and metadata:
                results['valid_payloads'] += 1
                sequence_numbers.append(metadata.sequence_number)
                valid_metadata.append(metadata)
            else:
                results['invalid_payloads'] += 1

        # Check sequence ordering
        if sequence_numbers:
            expected_sequence = sorted(sequence_numbers)
            actual_sequence = sequence_numbers

            # Check for duplicates
            seen = set()
            duplicates = set()
            for seq in actual_sequence:
                if seq in seen:
                    duplicates.add(seq)
                else:
                    seen.add(seq)

            if duplicates:
                results['duplicate_sequences'] = list(duplicates)

            # Check for gaps
            if expected_sequence == actual_sequence:  # No duplicates
                min_seq = min(expected_sequence)
                max_seq = max(expected_sequence)
                expected_range = set(range(min_seq, max_seq + 1))
                actual_set = set(expected_sequence)
                gaps = expected_range - actual_set
                if gaps:
                    results['sequence_gaps'] = sorted(gaps)

            # Check ordering
            for i in range(1, len(actual_sequence)):
                if actual_sequence[i] <= actual_sequence[i-1]:
                    results['ordering_issues'].append(
                        f"Out of order: {actual_sequence[i-1]} -> {actual_sequence[i]} at index {i}"
                    )

        return results