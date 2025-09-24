"""
Performance monitoring and metrics collection utilities.

This module provides comprehensive monitoring capabilities for performance tests,
including latency measurement, throughput tracking, and resource monitoring.
"""

import asyncio
import time
import psutil
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any
from collections import deque, defaultdict
import logging

logger = logging.getLogger(__name__)


@dataclass
class ResourceMetrics:
    """System resource metrics."""
    timestamp: float
    cpu_percent: float
    memory_percent: float
    memory_used_mb: float
    memory_available_mb: float
    disk_usage_percent: float
    network_bytes_sent: int
    network_bytes_recv: int
    process_count: int
    thread_count: int


@dataclass
class LatencyMetrics:
    """Latency measurement metrics."""
    timestamp: float
    latency_us: int  # microseconds
    sequence_number: int
    subscriber_id: int
    message_size_bytes: int


@dataclass
class ThroughputMetrics:
    """Throughput measurement metrics."""
    timestamp: float
    messages_sent: int
    messages_received: Dict[int, int]  # subscriber_id -> count
    bytes_sent: int
    bytes_received: Dict[int, int]  # subscriber_id -> bytes


@dataclass
class ErrorMetrics:
    """Error tracking metrics."""
    timestamp: float
    error_type: str
    error_message: str
    subscriber_id: Optional[int] = None
    sequence_number: Optional[int] = None


class PerformanceMonitor:
    """Comprehensive performance monitoring system."""

    def __init__(self, sampling_interval: float = 0.1):
        self.sampling_interval = sampling_interval
        self.running = False
        self.monitor_task = None

        # Metric storage
        self.resource_samples: deque = deque(maxlen=10000)
        self.latency_samples: deque = deque(maxlen=100000)
        self.throughput_samples: deque = deque(maxlen=10000)
        self.error_samples: deque = deque(maxlen=1000)

        # Aggregated counters
        self.messages_sent = 0
        self.messages_received: Dict[int, int] = defaultdict(int)
        self.bytes_sent = 0
        self.bytes_received: Dict[int, int] = defaultdict(int)

        # Callbacks
        self.sample_callbacks: List[Callable] = []

        # Performance tracking
        self._start_time = None
        self._last_sample_time = None

    async def start_monitoring(self):
        """Start the monitoring system."""
        if self.running:
            return

        self.running = True
        self._start_time = time.time()
        self._last_sample_time = self._start_time

        logger.info("Starting performance monitoring")
        self.monitor_task = asyncio.create_task(self._monitor_loop())

    async def stop_monitoring(self):
        """Stop the monitoring system."""
        if not self.running:
            return

        self.running = False
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass

        logger.info("Performance monitoring stopped")

    async def _monitor_loop(self):
        """Main monitoring loop."""
        while self.running:
            try:
                current_time = time.time()

                # Collect resource metrics
                resource_metrics = self._collect_resource_metrics(current_time)
                self.resource_samples.append(resource_metrics)

                # Collect throughput metrics
                throughput_metrics = self._collect_throughput_metrics(current_time)
                self.throughput_samples.append(throughput_metrics)

                # Call registered callbacks
                for callback in self.sample_callbacks:
                    try:
                        await callback(current_time, self)
                    except Exception as e:
                        logger.error(f"Error in monitor callback: {e}")

                self._last_sample_time = current_time

                # Sleep for next sampling interval
                await asyncio.sleep(self.sampling_interval)

            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(1.0)  # Back off on error

    def _collect_resource_metrics(self, timestamp: float) -> ResourceMetrics:
        """Collect system resource metrics."""
        try:
            process = psutil.Process()
            memory_info = process.memory_info()
            network_io = psutil.net_io_counters()

            return ResourceMetrics(
                timestamp=timestamp,
                cpu_percent=process.cpu_percent(),
                memory_percent=process.memory_percent(),
                memory_used_mb=memory_info.rss / 1024 / 1024,
                memory_available_mb=psutil.virtual_memory().available / 1024 / 1024,
                disk_usage_percent=psutil.disk_usage('/').percent,
                network_bytes_sent=network_io.bytes_sent,
                network_bytes_recv=network_io.bytes_recv,
                process_count=len(psutil.pids()),
                thread_count=process.num_threads()
            )
        except Exception as e:
            logger.error(f"Error collecting resource metrics: {e}")
            return ResourceMetrics(
                timestamp=timestamp,
                cpu_percent=0.0,
                memory_percent=0.0,
                memory_used_mb=0.0,
                memory_available_mb=0.0,
                disk_usage_percent=0.0,
                network_bytes_sent=0,
                network_bytes_recv=0,
                process_count=0,
                thread_count=0
            )

    def _collect_throughput_metrics(self, timestamp: float) -> ThroughputMetrics:
        """Collect throughput metrics."""
        return ThroughputMetrics(
            timestamp=timestamp,
            messages_sent=self.messages_sent,
            messages_received=dict(self.messages_received),
            bytes_sent=self.bytes_sent,
            bytes_received=dict(self.bytes_received)
        )

    def record_message_sent(self, message_size: int, sequence_number: int):
        """Record a sent message."""
        self.messages_sent += 1
        self.bytes_sent += message_size

    def record_message_received(self, subscriber_id: int, message_size: int,
                               sequence_number: int, latency_us: int):
        """Record a received message."""
        self.messages_received[subscriber_id] += 1
        self.bytes_received[subscriber_id] += message_size

        latency_metrics = LatencyMetrics(
            timestamp=time.time(),
            latency_us=latency_us,
            sequence_number=sequence_number,
            subscriber_id=subscriber_id,
            message_size_bytes=message_size
        )
        self.latency_samples.append(latency_metrics)

    def record_error(self, error_type: str, error_message: str,
                    subscriber_id: Optional[int] = None,
                    sequence_number: Optional[int] = None):
        """Record an error."""
        error_metrics = ErrorMetrics(
            timestamp=time.time(),
            error_type=error_type,
            error_message=error_message,
            subscriber_id=subscriber_id,
            sequence_number=sequence_number
        )
        self.error_samples.append(error_metrics)

    def add_sample_callback(self, callback: Callable):
        """Add a callback to be called on each sample."""
        self.sample_callbacks.append(callback)

    def get_latency_stats(self) -> Dict[str, float]:
        """Get latency statistics."""
        if not self.latency_samples:
            return {
                'min_us': 0.0,
                'max_us': 0.0,
                'avg_us': 0.0,
                'p50_us': 0.0,
                'p95_us': 0.0,
                'p99_us': 0.0,
                'std_us': 0.0,
                'count': 0
            }

        latencies = [sample.latency_us for sample in self.latency_samples]
        latencies.sort()

        count = len(latencies)
        p50_idx = int(count * 0.5)
        p95_idx = int(count * 0.95)
        p99_idx = int(count * 0.99)

        import statistics
        return {
            'min_us': min(latencies),
            'max_us': max(latencies),
            'avg_us': statistics.mean(latencies),
            'p50_us': latencies[p50_idx],
            'p95_us': latencies[p95_idx] if p95_idx < count else latencies[-1],
            'p99_us': latencies[p99_idx] if p99_idx < count else latencies[-1],
            'std_us': statistics.stdev(latencies) if count > 1 else 0.0,
            'count': count
        }

    def get_throughput_stats(self) -> Dict[str, float]:
        """Get throughput statistics."""
        if not self.throughput_samples:
            return {
                'messages_per_second': 0.0,
                'mb_per_second': 0.0,
                'subscriber_efficiency': 0.0
            }

        duration = time.time() - self._start_time
        if duration <= 0:
            return {
                'messages_per_second': 0.0,
                'mb_per_second': 0.0,
                'subscriber_efficiency': 0.0
            }

        messages_per_second = self.messages_sent / duration
        mb_per_second = (self.bytes_sent / 1024 / 1024) / duration

        # Calculate subscriber efficiency (how well messages are distributed)
        if self.messages_sent > 0:
            expected_per_subscriber = self.messages_sent / len(self.messages_received) if self.messages_received else self.messages_sent
            total_efficiency = sum(
                min(received / expected_per_subscriber, 1.0)
                for received in self.messages_received.values()
            )
            subscriber_efficiency = total_efficiency / len(self.messages_received) if self.messages_received else 0.0
        else:
            subscriber_efficiency = 0.0

        return {
            'messages_per_second': messages_per_second,
            'mb_per_second': mb_per_second,
            'subscriber_efficiency': subscriber_efficiency
        }

    def get_resource_stats(self) -> Dict[str, float]:
        """Get resource usage statistics."""
        if not self.resource_samples:
            return {
                'avg_cpu_percent': 0.0,
                'max_cpu_percent': 0.0,
                'avg_memory_percent': 0.0,
                'max_memory_percent': 0.0,
                'avg_memory_mb': 0.0,
                'max_memory_mb': 0.0
            }

        cpu_values = [sample.cpu_percent for sample in self.resource_samples]
        memory_values = [sample.memory_percent for sample in self.resource_samples]
        memory_mb_values = [sample.memory_used_mb for sample in self.resource_samples]

        import statistics
        return {
            'avg_cpu_percent': statistics.mean(cpu_values) if cpu_values else 0.0,
            'max_cpu_percent': max(cpu_values) if cpu_values else 0.0,
            'avg_memory_percent': statistics.mean(memory_values) if memory_values else 0.0,
            'max_memory_percent': max(memory_values) if memory_values else 0.0,
            'avg_memory_mb': statistics.mean(memory_mb_values) if memory_mb_values else 0.0,
            'max_memory_mb': max(memory_mb_values) if memory_mb_values else 0.0
        }

    def get_error_summary(self) -> Dict[str, Any]:
        """Get error summary statistics."""
        error_counts = defaultdict(int)
        error_by_subscriber = defaultdict(int)

        for error in self.error_samples:
            error_counts[error.error_type] += 1
            if error.subscriber_id is not None:
                error_by_subscriber[error.subscriber_id] += 1

        return {
            'total_errors': len(self.error_samples),
            'error_types': dict(error_counts),
            'errors_by_subscriber': dict(error_by_subscriber),
            'recent_errors': [
                {
                    'timestamp': error.timestamp,
                    'type': error.error_type,
                    'message': error.error_message,
                    'subscriber_id': error.subscriber_id
                }
                for error in list(self.error_samples)[-10:]  # Last 10 errors
            ]
        }

    def get_summary(self) -> Dict[str, Any]:
        """Get comprehensive monitoring summary."""
        return {
            'duration_seconds': time.time() - self._start_time if self._start_time else 0.0,
            'latency_stats': self.get_latency_stats(),
            'throughput_stats': self.get_throughput_stats(),
            'resource_stats': self.get_resource_stats(),
            'error_summary': self.get_error_summary(),
            'counters': {
                'messages_sent': self.messages_sent,
                'messages_received': dict(self.messages_received),
                'bytes_sent': self.bytes_sent,
                'bytes_received': dict(self.bytes_received)
            }
        }

    def reset_counters(self):
        """Reset all counters and metrics."""
        self.messages_sent = 0
        self.messages_received.clear()
        self.bytes_sent = 0
        self.bytes_received.clear()

        self.resource_samples.clear()
        self.latency_samples.clear()
        self.throughput_samples.clear()
        self.error_samples.clear()

        self._start_time = time.time()
        self._last_sample_time = self._start_time