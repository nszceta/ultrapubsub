"""
Performance testing framework for UltraPubSub Core IPC.

This module provides comprehensive performance testing capabilities to validate
design requirements including:
- 20MB message transmission at 40Hz with 6 subscribers
- Payload integrity verification
- Message ordering validation
- Latency measurement and reporting
- Sustained load testing
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from pathlib import Path
from datetime import datetime

from ..utils import PerformanceTestConfig, PerformanceMonitor
from .coordinator import TestCoordinator

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Performance metrics collection."""
    start_time: float = 0.0
    end_time: float = 0.0
    messages_sent: int = 0
    messages_received: Dict[int, int] = field(default_factory=dict)
    latency_samples: List[float] = field(default_factory=list)
    throughput_mbps: float = 0.0
    errors: List[str] = field(default_factory=list)
    resource_usage: Dict[str, Any] = field(default_factory=dict)
    monitoring_summary: Dict[str, Any] = field(default_factory=dict)


class PerformanceTestFramework:
    """Main performance testing framework."""

    def __init__(self, config: PerformanceTestConfig):
        self.config = config
        self.metrics = PerformanceMetrics()
        self.running = False
        self.test_tasks = []
        self.monitor = PerformanceMonitor(sampling_interval=config.sampling_interval_ms / 1000.0)
        self.coordinator = TestCoordinator(config)

    async def run_test(self) -> PerformanceMetrics:
        """Execute the complete performance test."""
        logger.info("Starting performance test")
        logger.info(f"Configuration: {self.config}")

        self.running = True
        self.metrics.start_time = time.time()

        try:
            # Create output directory
            output_path = Path(self.config.output_dir)
            output_path.mkdir(parents=True, exist_ok=True)

            # Start monitoring
            if self.config.enable_resource_monitoring:
                await self.monitor.start_monitoring()

            # Run the test
            await self._execute_test()

        except Exception as e:
            logger.error(f"Test execution failed: {e}")
            self.metrics.errors.append(f"Test execution failed: {e}")

        finally:
            # Stop monitoring
            if self.config.enable_resource_monitoring:
                await self.monitor.stop_monitoring()

            self.running = False
            self.metrics.end_time = time.time()

        # Calculate final metrics
        self._calculate_final_metrics()

        # Get monitoring summary
        self.metrics.monitoring_summary = self.monitor.get_summary()

        # Save results
        await self._save_results()

        logger.info("Performance test completed")
        return self.metrics

    async def _execute_test(self):
        """Execute the core test logic."""
        logger.info("Executing performance test with coordinator")

        # Run the coordinated test
        test_results = await self.coordinator.start_test()

        # Update metrics with test results
        self._update_metrics_from_results(test_results)

        logger.info("Test execution completed")

    def _update_metrics_from_results(self, test_results: Dict[str, Any]):
        """Update metrics from test results."""
        # Extract test info
        test_info = test_results.get("test_info", {})
        if test_info.get("start_time"):
            self.metrics.start_time = test_info["start_time"]
        if test_info.get("end_time"):
            self.metrics.end_time = test_info["end_time"]

        # Extract process info first (this has the correct message counts)
        process_info = test_results.get("process_info", {})
        total_messages_sent = 0
        total_bytes_sent = 0

        for pid, info in process_info.items():
            stats = info.get("stats", {})
            if "messages_sent" in stats:
                total_messages_sent += stats["messages_sent"]
            if "bytes_sent" in stats:
                total_bytes_sent += stats["bytes_sent"]

        # Update metrics with process info
        self.metrics.messages_sent = total_messages_sent

        # Calculate duration and throughput using the correct values
        duration = self.metrics.end_time - self.metrics.start_time if self.metrics.end_time and self.metrics.start_time else 0
        self.metrics.throughput_mbps = (total_bytes_sent / (1024 * 1024)) / duration if duration > 0 else 0

        logger.info(f"Processed process info: {total_messages_sent} messages, {total_bytes_sent} bytes")
        logger.info(f"Test duration: {duration:.2f}s")
        logger.info(f"Throughput: {self.metrics.throughput_mbps:.2f} MB/s")

        # Extract performance metrics
        perf_metrics = test_results.get("performance_metrics", {})
        counters = perf_metrics.get("counters", {})
        self.metrics.messages_received = counters.get("messages_received", {})

        # Extract latency samples
        latency_stats = perf_metrics.get("latency_stats", {})
        if latency_stats.get("count", 0) > 0:
            # Convert to list of samples for compatibility
            count = latency_stats["count"]
            avg_latency = latency_stats.get("avg_us", 0) / 1000.0
            self.metrics.latency_samples = [avg_latency] * count

        # Add resource usage
        if self.config.enable_resource_monitoring:
            resource_stats = perf_metrics.get("resource_stats", {})
            self.metrics.resource_usage = {
                'cpu_usage': resource_stats,
                'memory_usage': resource_stats,
                'duration_seconds': duration
            }

        # Add monitoring summary
        self.metrics.monitoring_summary = perf_metrics

        # Extract errors
        error_summary = perf_metrics.get("error_summary", {})
        if error_summary.get("total_errors", 0) > 0:
            for error in error_summary.get("recent_errors", []):
                self.metrics.errors.append(f"{error['type']}: {error['message']}")

        
        # Log final metrics
        final_duration = self.metrics.end_time - self.metrics.start_time if self.metrics.end_time and self.metrics.start_time else 0
        logger.info(f"Test duration: {final_duration:.2f}s")
        logger.info(f"Messages sent: {self.metrics.messages_sent}")
        logger.info(f"Messages received: {dict(self.metrics.messages_received)}")
        logger.info(f"Throughput: {self.metrics.throughput_mbps:.2f} MB/s")

    def _calculate_final_metrics(self):
        """Calculate final performance metrics."""
        duration = self.metrics.end_time - self.metrics.start_time

        # Calculate throughput if not already set
        if self.metrics.throughput_mbps == 0:
            total_bytes = self.monitor.bytes_sent
            self.metrics.throughput_mbps = (total_bytes / (1024 * 1024)) / duration if duration > 0 else 0

        # Ensure resource usage is set
        if self.config.enable_resource_monitoring and not self.metrics.resource_usage:
            resource_stats = self.monitor.get_resource_stats()
            self.metrics.resource_usage = {
                'cpu_usage': resource_stats,
                'memory_usage': resource_stats,
                'duration_seconds': duration
            }

        logger.info(f"Final metrics calculated: duration={duration:.2f}s, throughput={self.metrics.throughput_mbps:.2f}MB/s")

    async def _save_results(self):
        """Save test results to file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"performance_test_{timestamp}.json"
        filepath = Path(self.config.output_dir) / filename

        # Calculate latency stats from monitor
        latency_stats = self.monitor.get_latency_stats()

        results = {
            "config": {
                "message_size_bytes": self.config.message_size_bytes,
                "frequency_hz": self.config.frequency_hz,
                "subscriber_count": self.config.subscriber_count,
                "test_duration_seconds": self.config.test_duration_seconds,
                "warmup_duration_seconds": self.config.warmup_duration_seconds
            },
            "metrics": {
                "start_time": self.metrics.start_time,
                "end_time": self.metrics.end_time,
                "duration_seconds": self.metrics.end_time - self.metrics.start_time,
                "messages_sent": self.metrics.messages_sent,
                "messages_received": self.metrics.messages_received,
                "throughput_mbps": self.metrics.throughput_mbps,
                "latency_stats": {
                    "min_ms": latency_stats['min_us'] / 1000.0,
                    "max_ms": latency_stats['max_us'] / 1000.0,
                    "avg_ms": latency_stats['avg_us'] / 1000.0,
                    "p50_ms": latency_stats['p50_us'] / 1000.0,
                    "p95_ms": latency_stats['p95_us'] / 1000.0,
                    "p99_ms": latency_stats['p99_us'] / 1000.0,
                    "std_ms": latency_stats['std_us'] / 1000.0,
                    "count": latency_stats['count']
                },
                "throughput_stats": self.monitor.get_throughput_stats(),
                "resource_stats": self.monitor.get_resource_stats(),
                "error_summary": self.monitor.get_error_summary(),
                "errors": self.metrics.errors,
                "resource_usage": self.metrics.resource_usage,
                "monitoring_summary": self.metrics.monitoring_summary
            }
        }

        with open(filepath, 'w') as f:
            json.dump(results, f, indent=2)

        logger.info(f"Results saved to {filepath}")

    def stop_test(self):
        """Stop the currently running test."""
        logger.info("Stopping performance test")
        self.running = False

        # Cancel all tasks
        for task in self.test_tasks:
            task.cancel()