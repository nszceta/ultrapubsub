"""
High-resolution timing infrastructure for performance tests.

This module provides precise timing capabilities including:
- High-resolution timing utilities
- 40Hz message generation scheduling
- End-to-end latency measurement
- Timing jitter analysis
"""

import time
import threading
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any
from collections import deque
import statistics

logger = logging.getLogger(__name__)


@dataclass
class TimingMetrics:
    """Timing metrics collection."""
    timestamp: float
    duration_us: int  # microseconds
    jitter_us: int  # microseconds
    scheduled_time: float
    actual_time: float
    sequence_number: int


class HighResolutionTimer:
    """High-resolution timer for precise timing measurements."""

    def __init__(self):
        self.start_time = time.perf_counter()
        self基准_time = time.perf_counter_ns()

    def get_time_ns(self) -> int:
        """Get current time in nanoseconds."""
        return time.perf_counter_ns()

    def get_time_us(self) -> int:
        """Get current time in microseconds."""
        return self.get_time_ns() // 1000

    def get_time_ms(self) -> int:
        """Get current time in milliseconds."""
        return self.get_time_us() // 1000

    def get_elapsed_ns(self) -> int:
        """Get elapsed time since timer creation in nanoseconds."""
        return self.get_time_ns() - self.基准时间

    def get_elapsed_us(self) -> int:
        """Get elapsed time since timer creation in microseconds."""
        return self.get_elapsed_ns() // 1000

    def sleep_until_ns(self, target_time_ns: int):
        """High-precision sleep until target time in nanoseconds."""
        current_time = self.get_time_ns()

        if target_time_ns <= current_time:
            return

        # Calculate sleep duration
        sleep_duration_ns = target_time_ns - current_time

        # Use a combination of time.sleep and busy-wait for precision
        if sleep_duration_ns > 1_000_000:  # > 1ms
            # Use time.sleep for large durations
            sleep_duration_s = (sleep_duration_ns - 500_000) / 1_000_000_000  # Leave 0.5ms buffer
            time.sleep(max(0, sleep_duration_s))

        # Busy-wait for the remaining time
        while self.get_time_ns() < target_time_ns:
            pass


class FrequencyScheduler:
    """Scheduler for precise frequency-based message generation."""

    def __init__(self, frequency_hz: int, timer: Optional[HighResolutionTimer] = None):
        self.frequency_hz = frequency_hz
        self.timer = timer or HighResolutionTimer()
        self.interval_ns = 1_000_000_000 // frequency_hz  # nanoseconds per interval
        self.next_target_time = 0
        self.sequence_counter = 0
        self.running = False
        self.timing_metrics: deque = deque(maxlen=10000)

        # Statistics
        self.total_messages = 0
        self.jitter_samples: List[int] = []
        self.late_messages = 0
        self.early_messages = 0

    def start(self):
        """Start the scheduler."""
        self.running = True
        self.next_target_time = self.timer.get_time_ns() + self.interval_ns
        logger.info(f"Frequency scheduler started: {self.frequency_hz}Hz, interval={self.interval_ns}ns")

    def stop(self):
        """Stop the scheduler."""
        self.running = False
        logger.info(f"Frequency scheduler stopped. Total messages: {self.total_messages}")

    def wait_for_next_slot(self) -> Optional[TimingMetrics]:
        """Wait for the next scheduling slot and return timing metrics."""
        if not self.running:
            return None

        current_time = self.timer.get_time_ns()

        # Calculate target time for this message
        if self.next_target_time == 0:
            self.next_target_time = current_time + self.interval_ns
        elif current_time > self.next_target_time + self.interval_ns:
            # We've missed multiple intervals, resync
            missed_intervals = (current_time - self.next_target_time) // self.interval_ns
            self.next_target_time += missed_intervals * self.interval_ns
            self.late_messages += missed_intervals

        # Wait for the target time
        self.timer.sleep_until_ns(self.next_target_time)

        # Record timing metrics
        actual_time = self.timer.get_time_ns()
        scheduled_time = self.next_target_time
        timing_error_ns = actual_time - scheduled_time
        jitter_us = abs(timing_error_ns) // 1000

        timing_metrics = TimingMetrics(
            timestamp=time.time(),
            duration_us=int(timing_error_ns // 1000),
            jitter_us=jitter_us,
            scheduled_time=scheduled_time / 1_000_000_000,  # Convert to seconds
            actual_time=actual_time / 1_000_000_000,
            sequence_number=self.sequence_counter
        )

        # Update statistics
        self.timing_metrics.append(timing_metrics)
        self.jitter_samples.append(jitter_us)
        self.total_messages += 1

        if timing_error_ns > 0:
            self.late_messages += 1
        elif timing_error_ns < 0:
            self.early_messages += 1

        # Schedule next message
        self.next_target_time += self.interval_ns
        self.sequence_counter += 1

        return timing_metrics

    def get_timing_stats(self) -> Dict[str, Any]:
        """Get timing statistics."""
        if not self.jitter_samples:
            return {
                "total_messages": 0,
                "avg_jitter_us": 0,
                "max_jitter_us": 0,
                "min_jitter_us": 0,
                "std_jitter_us": 0,
                "late_messages": 0,
                "early_messages": 0,
                "on_time_messages": 0,
                "timing_accuracy_percent": 0
            }

        return {
            "total_messages": self.total_messages,
            "avg_jitter_us": statistics.mean(self.jitter_samples),
            "max_jitter_us": max(self.jitter_samples),
            "min_jitter_us": min(self.jitter_samples),
            "std_jitter_us": statistics.stdev(self.jitter_samples) if len(self.jitter_samples) > 1 else 0,
            "late_messages": self.late_messages,
            "early_messages": self.early_messages,
            "on_time_messages": self.total_messages - self.late_messages - self.early_messages,
            "timing_accuracy_percent": ((self.total_messages - self.late_messages - self.early_messages) / self.total_messages * 100) if self.total_messages > 0 else 0
        }

    def reset(self):
        """Reset the scheduler state."""
        self.sequence_counter = 0
        self.next_target_time = 0
        self.timing_metrics.clear()
        self.jitter_samples.clear()
        self.total_messages = 0
        self.late_messages = 0
        self.early_messages = 0


class LatencyTracker:
    """Tracks end-to-end latency measurements."""

    def __init__(self):
        self.send_times: Dict[int, float] = {}  # sequence_number -> send_time
        self.receive_times: Dict[int, Dict[int, float]] = {}  # sequence_number -> subscriber_id -> receive_time
        self.latency_samples: List[Dict[str, Any]] = []

    def record_send_time(self, sequence_number: int, send_time: float):
        """Record when a message was sent."""
        self.send_times[sequence_number] = send_time
        self.receive_times[sequence_number] = {}

    def record_receive_time(self, sequence_number: int, subscriber_id: int, receive_time: float):
        """Record when a message was received by a subscriber."""
        if sequence_number not in self.receive_times:
            self.receive_times[sequence_number] = {}

        self.receive_times[sequence_number][subscriber_id] = receive_time

        # Calculate latency if we have send time
        if sequence_number in self.send_times:
            send_time = self.send_times[sequence_number]
            latency_us = int((receive_time - send_time) * 1_000_000)  # Convert to microseconds

            latency_sample = {
                "sequence_number": sequence_number,
                "subscriber_id": subscriber_id,
                "send_time": send_time,
                "receive_time": receive_time,
                "latency_us": latency_us,
                "latency_ms": latency_us / 1000.0
            }

            self.latency_samples.append(latency_sample)

    def get_latency_stats(self) -> Dict[str, Any]:
        """Get latency statistics."""
        if not self.latency_samples:
            return {
                "total_samples": 0,
                "min_latency_us": 0,
                "max_latency_us": 0,
                "avg_latency_us": 0,
                "p50_latency_us": 0,
                "p95_latency_us": 0,
                "p99_latency_us": 0,
                "std_latency_us": 0
            }

        latencies = [sample["latency_us"] for sample in self.latency_samples]
        latencies.sort()

        count = len(latencies)
        p50_idx = int(count * 0.5)
        p95_idx = int(count * 0.95)
        p99_idx = int(count * 0.99)

        return {
            "total_samples": count,
            "min_latency_us": min(latencies),
            "max_latency_us": max(latencies),
            "avg_latency_us": statistics.mean(latencies),
            "p50_latency_us": latencies[p50_idx],
            "p95_latency_us": latencies[p95_idx] if p95_idx < count else latencies[-1],
            "p99_latency_us": latencies[p99_idx] if p99_idx < count else latencies[-1],
            "std_latency_us": statistics.stdev(latencies) if count > 1 else 0
        }

    def get_latency_by_subscriber(self) -> Dict[int, Dict[str, Any]]:
        """Get latency statistics broken down by subscriber."""
        subscriber_latencies = {}

        for sample in self.latency_samples:
            subscriber_id = sample["subscriber_id"]
            if subscriber_id not in subscriber_latencies:
                subscriber_latencies[subscriber_id] = []

            subscriber_latencies[subscriber_id].append(sample["latency_us"])

        result = {}
        for subscriber_id, latencies in subscriber_latencies.items():
            if latencies:
                latencies.sort()
                count = len(latencies)
                p50_idx = int(count * 0.5)
                p95_idx = int(count * 0.95)
                p99_idx = int(count * 0.99)

                result[subscriber_id] = {
                    "total_samples": count,
                    "min_latency_us": min(latencies),
                    "max_latency_us": max(latencies),
                    "avg_latency_us": statistics.mean(latencies),
                    "p50_latency_us": latencies[p50_idx],
                    "p95_latency_us": latencies[p95_idx] if p95_idx < count else latencies[-1],
                    "p99_latency_us": latencies[p99_idx] if p99_idx < count else latencies[-1],
                    "std_latency_us": statistics.stdev(latencies) if count > 1 else 0
                }

        return result

    def reset(self):
        """Reset the latency tracker."""
        self.send_times.clear()
        self.receive_times.clear()
        self.latency_samples.clear()