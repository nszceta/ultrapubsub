"""
Sequence tracking and message ordering validation for performance tests.

This module provides comprehensive sequence tracking capabilities including:
- Sequence number generation and tracking
- Message ordering verification
- Duplicate and missing message detection
- Payload integrity validation
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Any
from collections import defaultdict, deque
from enum import Enum

from ..utils import PayloadGenerator, PayloadValidator

logger = logging.getLogger(__name__)


class SequenceErrorType(Enum):
    """Types of sequence errors."""
    MISSING_MESSAGE = "missing_message"
    DUPLICATE_MESSAGE = "duplicate_message"
    OUT_OF_ORDER = "out_of_order"
    CORRUPTED_PAYLOAD = "corrupted_payload"
    SEQUENCE_GAP = "sequence_gap"


@dataclass
class SequenceError:
    """Sequence error information."""
    error_type: SequenceErrorType
    sequence_number: int
    expected_sequence: Optional[int] = None
    subscriber_id: Optional[int] = None
    timestamp: float = field(default_factory=time.time)
    details: str = ""


@dataclass
class SubscriberSequenceState:
    """Sequence state for a single subscriber."""
    subscriber_id: int
    expected_next_sequence: int = 0
    received_sequences: Set[int] = field(default_factory=set)
    out_of_order_sequences: List[int] = field(default_factory=list)
    duplicate_sequences: Set[int] = field(default_factory=set)
    missing_sequences: Set[int] = field(default_factory=set)
    last_received_time: Optional[float] = None
    total_received: int = 0
    total_errors: int = 0


class SequenceTracker:
    """Tracks message sequences and validates ordering."""

    def __init__(self, total_subscribers: int):
        self.total_subscribers = total_subscribers
        self.subscribers: Dict[int, SubscriberSequenceState] = {}
        self.global_sequence_counter = 0
        self.errors: List[SequenceError] = []
        self.start_time = time.time()

        # Initialize subscriber states
        for i in range(total_subscribers):
            self.subscribers[i] = SubscriberSequenceState(subscriber_id=i)

    def get_next_sequence(self) -> int:
        """Get the next global sequence number."""
        sequence = self.global_sequence_counter
        self.global_sequence_counter += 1
        return sequence

    def record_message_sent(self, sequence_number: int, message_size: int):
        """Record a message being sent."""
        logger.debug(f"Recording sent message: sequence={sequence_number}, size={message_size}")

    def record_message_received(self, subscriber_id: int, sequence_number: int,
                             message_size: int, receive_time: float) -> List[SequenceError]:
        """Record a message received by a subscriber."""
        errors = []

        if subscriber_id not in self.subscribers:
            error = SequenceError(
                error_type=SequenceErrorType.CORRUPTED_PAYLOAD,
                sequence_number=sequence_number,
                subscriber_id=subscriber_id,
                details=f"Unknown subscriber ID: {subscriber_id}"
            )
            errors.append(error)
            self.errors.append(error)
            return errors

        subscriber = self.subscribers[subscriber_id]
        subscriber.last_received_time = receive_time
        subscriber.total_received += 1

        # Check for duplicates
        if sequence_number in subscriber.received_sequences:
            error = SequenceError(
                error_type=SequenceErrorType.DUPLICATE_MESSAGE,
                sequence_number=sequence_number,
                subscriber_id=subscriber_id,
                expected_sequence=subscriber.expected_next_sequence,
                details=f"Duplicate sequence {sequence_number} for subscriber {subscriber_id}"
            )
            errors.append(error)
            subscriber.duplicate_sequences.add(sequence_number)
            subscriber.total_errors += 1
            self.errors.append(error)
            return errors

        # Check for out-of-order delivery
        if sequence_number < subscriber.expected_next_sequence:
            error = SequenceError(
                error_type=SequenceErrorType.OUT_OF_ORDER,
                sequence_number=sequence_number,
                subscriber_id=subscriber_id,
                expected_sequence=subscriber.expected_next_sequence,
                details=f"Out of order: received {sequence_number}, expected {subscriber.expected_next_sequence}"
            )
            errors.append(error)
            subscriber.out_of_order_sequences.append(sequence_number)
            subscriber.total_errors += 1
            self.errors.append(error)

        # Check for missing messages
        elif sequence_number > subscriber.expected_next_sequence:
            missing_range = range(subscriber.expected_next_sequence, sequence_number)
            for missing_seq in missing_range:
                if missing_seq not in subscriber.received_sequences:
                    error = SequenceError(
                        error_type=SequenceErrorType.MISSING_MESSAGE,
                        sequence_number=missing_seq,
                        subscriber_id=subscriber_id,
                        expected_sequence=subscriber.expected_next_sequence,
                        details=f"Missing sequence {missing_seq} for subscriber {subscriber_id}"
                    )
                    errors.append(error)
                    subscriber.missing_sequences.add(missing_seq)
                    subscriber.total_errors += 1
                    self.errors.append(error)

        # Record the received sequence
        subscriber.received_sequences.add(sequence_number)
        subscriber.expected_next_sequence = max(subscriber.expected_next_sequence, sequence_number + 1)

        return errors

    def get_subscriber_summary(self, subscriber_id: int) -> Dict[str, Any]:
        """Get summary for a specific subscriber."""
        if subscriber_id not in self.subscribers:
            return {}

        subscriber = self.subscribers[subscriber_id]
        return {
            "subscriber_id": subscriber_id,
            "expected_next_sequence": subscriber.expected_next_sequence,
            "total_received": subscriber.total_received,
            "total_errors": subscriber.total_errors,
            "unique_sequences_received": len(subscriber.received_sequences),
            "missing_sequences_count": len(subscriber.missing_sequences),
            "duplicate_sequences_count": len(subscriber.duplicate_sequences),
            "out_of_order_sequences_count": len(subscriber.out_of_order_sequences),
            "last_received_time": subscriber.last_received_time,
            "error_rate": subscriber.total_errors / subscriber.total_received if subscriber.total_received > 0 else 0.0
        }

    def get_global_summary(self) -> Dict[str, Any]:
        """Get global sequence summary."""
        total_errors = sum(sub.total_errors for sub in self.subscribers.values())
        total_received = sum(sub.total_received for sub in self.subscribers.values())

        # Calculate sequence completeness across all subscribers
        min_sequence = min(sub.expected_next_sequence for sub in self.subscribers.values())
        max_sequence = max(sub.expected_next_sequence for sub in self.subscribers.values())

        # Find common sequences (received by all subscribers)
        if self.subscribers:
            common_sequences = set.intersection(*[sub.received_sequences for sub in self.subscribers.values()])
        else:
            common_sequences = set()

        return {
            "global_sequence_counter": self.global_sequence_counter,
            "total_subscribers": self.total_subscribers,
            "total_messages_sent": self.global_sequence_counter,
            "total_messages_received": total_received,
            "total_errors": total_errors,
            "common_sequences_count": len(common_sequences),
            "min_expected_sequence": min_sequence,
            "max_expected_sequence": max_sequence,
            "sequence_range": max_sequence - min_sequence if self.subscribers else 0,
            "global_error_rate": total_errors / total_received if total_received > 0 else 0.0,
            "duration_seconds": time.time() - self.start_time
        }

    def get_error_summary(self) -> Dict[str, Any]:
        """Get error summary."""
        error_counts = defaultdict(int)
        error_by_subscriber = defaultdict(int)

        for error in self.errors:
            error_counts[error.error_type.value] += 1
            if error.subscriber_id is not None:
                error_by_subscriber[error.subscriber_id] += 1

        return {
            "total_errors": len(self.errors),
            "error_types": dict(error_counts),
            "errors_by_subscriber": dict(error_by_subscriber),
            "recent_errors": [
                {
                    "timestamp": error.timestamp,
                    "type": error.error_type.value,
                    "sequence_number": error.sequence_number,
                    "subscriber_id": error.subscriber_id,
                    "details": error.details
                }
                for error in self.errors[-10:]  # Last 10 errors
            ]
        }

    def validate_sequence_completeness(self) -> bool:
        """Validate that all subscribers have received all sequences."""
        if not self.subscribers:
            return False

        # Check if all subscribers have the same expected next sequence
        expected_sequences = [sub.expected_next_sequence for sub in self.subscribers.values()]
        return all(seq == expected_sequences[0] for seq in expected_sequences)

    def get_missing_sequences(self, subscriber_id: Optional[int] = None) -> Dict[int, List[int]]:
        """Get missing sequences for subscribers."""
        if subscriber_id is not None:
            if subscriber_id in self.subscribers:
                return {subscriber_id: sorted(self.subscribers[subscriber_id].missing_sequences)}
            return {}

        return {
            sub_id: sorted(sub.missing_sequences)
            for sub_id, sub in self.subscribers.items()
        }

    def reset(self):
        """Reset the sequence tracker."""
        self.global_sequence_counter = 0
        self.errors.clear()
        self.start_time = time.time()

        for subscriber in self.subscribers.values():
            subscriber.expected_next_sequence = 0
            subscriber.received_sequences.clear()
            subscriber.out_of_order_sequences.clear()
            subscriber.duplicate_sequences.clear()
            subscriber.missing_sequences.clear()
            subscriber.last_received_time = None
            subscriber.total_received = 0
            subscriber.total_errors = 0


class MessageOrderValidator:
    """Validates message ordering and integrity across multiple subscribers."""

    def __init__(self, payload_generator: PayloadGenerator, total_subscribers: int):
        self.payload_generator = payload_generator
        self.payload_validator = PayloadValidator(payload_generator)
        self.sequence_tracker = SequenceTracker(total_subscribers)
        self.total_subscribers = total_subscribers

    def validate_received_message(self, subscriber_id: int, payload_data: bytes,
                                 receive_time: float) -> Tuple[bool, List[str], Dict[str, Any]]:
        """Validate a received message for integrity and ordering."""
        validation_errors = []
        validation_details = {}

        # Validate payload integrity
        is_valid_payload, metadata, payload_validation = self.payload_validator.validate_payload(payload_data)

        if not is_valid_payload:
            validation_errors.extend(payload_validation.get('errors', []))
            validation_details['payload_validation'] = payload_validation
            return False, validation_errors, validation_details

        # Record sequence information
        sequence_errors = self.sequence_tracker.record_message_received(
            subscriber_id, metadata.sequence_number, len(payload_data), receive_time
        )

        for error in sequence_errors:
            validation_errors.append(f"{error.error_type.value}: {error.details}")

        validation_details['metadata'] = {
            'sequence_number': metadata.sequence_number,
            'timestamp_us': metadata.timestamp_us,
            'payload_size': metadata.payload_size,
            'payload_pattern': metadata.payload_pattern.value
        }

        validation_details['sequence_errors'] = [
            {
                'type': error.error_type.value,
                'sequence_number': error.sequence_number,
                'details': error.details
            }
            for error in sequence_errors
        ]

        return len(validation_errors) == 0, validation_errors, validation_details

    def get_validation_summary(self) -> Dict[str, Any]:
        """Get comprehensive validation summary."""
        return {
            "sequence_summary": self.sequence_tracker.get_global_summary(),
            "subscriber_summaries": {
                sub_id: self.sequence_tracker.get_subscriber_summary(sub_id)
                for sub_id in range(self.total_subscribers)
            },
            "error_summary": self.sequence_tracker.get_error_summary(),
            "missing_sequences": self.sequence_tracker.get_missing_sequences(),
            "is_sequence_complete": self.sequence_tracker.validate_sequence_completeness()
        }

    def reset(self):
        """Reset the validator state."""
        self.sequence_tracker.reset()