"""
Stability validation for extended performance test periods.

This module provides stability validation capabilities including:
- Extended period stability monitoring
- System resource usage tracking
- Performance degradation detection
- Memory leak detection
- Long-term reliability validation
"""

import time
import logging
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any, Tuple
from collections import deque, defaultdict
from enum import Enum
import statistics

logger = logging.getLogger(__name__)


class StabilityStatus(Enum):
    """Stability status indicators."""
    STABLE = "stable"
    DEGRADING = "degrading"
    UNSTABLE = "unstable"
    CRITICAL = "critical"


@dataclass
class StabilityThresholds:
    """Thresholds for stability validation."""
    max_memory_growth_mb_per_hour: float = 100.0
    max_cpu_degradation_percent: float = 10.0
    max_throughput_degradation_percent: float = 5.0
    max_latency_increase_percent: float = 20.0
    max_error_rate_increase_percent: float = 50.0
    max_jitter_increase_percent: float = 30.0


@dataclass
class StabilityMetrics:
    """Stability metrics for a time window."""
    timestamp: float
    duration_seconds: float
    memory_mb: float
    cpu_percent: float
    throughput_mbps: float
    avg_latency_ms: float
    jitter_us: float
    error_rate_percent: float
    messages_sent: int
    messages_received: int


class StabilityValidator:
    """Validates system stability over extended periods."""

    def __init__(self, thresholds: Optional[StabilityThresholds] = None):
        self.thresholds = thresholds or StabilityThresholds()
        self.metrics_history: deque = deque(maxlen=1000)  # Store last 1000 metrics
        self.current_metrics: Optional[StabilityMetrics] = None
        self.baseline_metrics: Optional[StabilityMetrics] = None
        self.start_time = time.time()
        self.monitoring_active = False

        # Stability analysis
        self.stability_trends: Dict[str, List[float]] = defaultdict(list)
        self.stability_alerts: List[Dict[str, Any]] = []

        # Degradation detection
        self.memory_baseline_mb: Optional[float] = None
        self.cpu_baseline_percent: Optional[float] = None
        self.throughput_baseline_mbps: Optional[float] = None
        self.latency_baseline_ms: Optional[float] = None

    def start_monitoring(self):
        """Start stability monitoring."""
        self.monitoring_active = True
        self.start_time = time.time()
        logger.info("Stability monitoring started")

    def stop_monitoring(self):
        """Stop stability monitoring."""
        self.monitoring_active = False
        logger.info("Stability monitoring stopped")

    def record_metrics(self, memory_mb: float, cpu_percent: float, throughput_mbps: float,
                      avg_latency_ms: float, jitter_us: float, error_rate_percent: float,
                      messages_sent: int, messages_received: int):
        """Record stability metrics for current time window."""
        if not self.monitoring_active:
            return

        current_time = time.time()
        duration = current_time - self.start_time

        metrics = StabilityMetrics(
            timestamp=current_time,
            duration_seconds=duration,
            memory_mb=memory_mb,
            cpu_percent=cpu_percent,
            throughput_mbps=throughput_mbps,
            avg_latency_ms=avg_latency_ms,
            jitter_us=jitter_us,
            error_rate_percent=error_rate_percent,
            messages_sent=messages_sent,
            messages_received=messages_received
        )

        self.current_metrics = metrics
        self.metrics_history.append(metrics)

        # Set baseline if this is the first measurement
        if self.baseline_metrics is None:
            self.baseline_metrics = metrics
            self.memory_baseline_mb = memory_mb
            self.cpu_baseline_percent = cpu_percent
            self.throughput_baseline_mbps = throughput_mbps
            self.latency_baseline_ms = avg_latency_ms
            logger.info(f"Stability baseline established: memory={memory_mb:.1f}MB, "
                       f"cpu={cpu_percent:.1f}%, throughput={throughput_mbps:.1f}MB/s")

        # Analyze stability
        self._analyze_stability()

    def _analyze_stability(self):
        """Analyze current stability status."""
        if self.baseline_metrics is None or self.current_metrics is None:
            return

        # Calculate changes from baseline
        memory_growth_mb = self.current_metrics.memory_mb - self.memory_baseline_mb
        cpu_change_percent = self.current_metrics.cpu_percent - self.cpu_baseline_percent
        throughput_change_percent = ((self.current_metrics.throughput_mbps - self.throughput_baseline_mbps) /
                                    self.throughput_baseline_mbps * 100) if self.throughput_baseline_mbps > 0 else 0
        latency_change_percent = ((self.current_metrics.avg_latency_ms - self.latency_baseline_ms) /
                                  self.latency_baseline_ms * 100) if self.latency_baseline_ms > 0 else 0

        # Calculate hourly rates
        duration_hours = self.current_metrics.duration_seconds / 3600
        memory_growth_mb_per_hour = memory_growth_mb / duration_hours if duration_hours > 0 else 0

        # Check for stability issues
        stability_issues = []

        # Memory growth check
        if memory_growth_mb_per_hour > self.thresholds.max_memory_growth_mb_per_hour:
            stability_issues.append({
                "type": "memory_growth",
                "severity": "warning",
                "current_value": memory_growth_mb_per_hour,
                "threshold": self.thresholds.max_memory_growth_mb_per_hour,
                "message": f"Memory growing at {memory_growth_mb_per_hour:.1f}MB/hour"
            })

        # CPU degradation check
        if cpu_change_percent > self.thresholds.max_cpu_degradation_percent:
            stability_issues.append({
                "type": "cpu_degradation",
                "severity": "warning",
                "current_value": cpu_change_percent,
                "threshold": self.thresholds.max_cpu_degradation_percent,
                "message": f"CPU usage increased by {cpu_change_percent:.1f}%"
            })

        # Throughput degradation check
        if throughput_change_percent < -self.thresholds.max_throughput_degradation_percent:
            stability_issues.append({
                "type": "throughput_degradation",
                "severity": "warning",
                "current_value": throughput_change_percent,
                "threshold": -self.thresholds.max_throughput_degradation_percent,
                "message": f"Throughput degraded by {abs(throughput_change_percent):.1f}%"
            })

        # Latency increase check
        if latency_change_percent > self.thresholds.max_latency_increase_percent:
            stability_issues.append({
                "type": "latency_increase",
                "severity": "warning",
                "current_value": latency_change_percent,
                "threshold": self.thresholds.max_latency_increase_percent,
                "message": f"Latency increased by {latency_change_percent:.1f}%"
            })

        # Record stability issues
        for issue in stability_issues:
            self.stability_alerts.append({
                "timestamp": time.time(),
                "duration_seconds": self.current_metrics.duration_seconds,
                **issue
            })

        # Update trends
        self.stability_trends["memory_mb"].append(self.current_metrics.memory_mb)
        self.stability_trends["cpu_percent"].append(self.current_metrics.cpu_percent)
        self.stability_trends["throughput_mbps"].append(self.current_metrics.throughput_mbps)
        self.stability_trends["latency_ms"].append(self.current_metrics.avg_latency_ms)
        self.stability_trends["jitter_us"].append(self.current_metrics.jitter_us)
        self.stability_trends["error_rate"].append(self.current_metrics.error_rate_percent)

    def get_stability_status(self) -> Tuple[StabilityStatus, List[Dict[str, Any]]]:
        """Get current stability status and issues."""
        if not self.current_metrics:
            return StabilityStatus.STABLE, []

        issues = []
        status = StabilityStatus.STABLE

        # Check recent alerts
        recent_alerts = [alert for alert in self.stability_alerts
                         if alert["timestamp"] > time.time() - 300]  # Last 5 minutes

        if recent_alerts:
            # Check severity of recent alerts
            critical_issues = [alert for alert in recent_alerts if alert.get("severity") == "critical"]
            if critical_issues:
                status = StabilityStatus.CRITICAL
                issues.extend(critical_issues)
            else:
                status = StabilityStatus.UNSTABLE
                issues.extend(recent_alerts)

        # Check for long-term trends
        if len(self.stability_trends["memory_mb"]) > 10:  # Need at least 10 samples
            # Check for memory leak pattern
            memory_samples = list(self.stability_trends["memory_mb"])[-20:]  # Last 20 samples
            if len(memory_samples) >= 10:
                # Simple linear regression to detect trend
                x_values = list(range(len(memory_samples)))
                y_values = memory_samples

                # Calculate slope
                n = len(x_values)
                sum_x = sum(x_values)
                sum_y = sum(y_values)
                sum_xy = sum(x * y for x, y in zip(x_values, y_values))
                sum_x2 = sum(x * x for x in x_values)

                if n > 1 and (n * sum_x2 - sum_x * sum_x) != 0:
                    slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
                    if slope > 0.5:  # Memory growing by more than 0.5MB per sample
                        issues.append({
                            "type": "memory_leak_detected",
                            "severity": "warning",
                            "message": f"Potential memory leak detected (slope: {slope:.2f}MB/sample)"
                        })
                        if status == StabilityStatus.STABLE:
                            status = StabilityStatus.DEGRADING

        return status, issues

    def get_stability_summary(self) -> Dict[str, Any]:
        """Get comprehensive stability summary."""
        status, issues = self.get_stability_status()

        # Calculate summary statistics
        summary_stats = {}
        if self.metrics_history:
            memory_values = [m.memory_mb for m in self.metrics_history]
            cpu_values = [m.cpu_percent for m in self.metrics_history]
            throughput_values = [m.throughput_mbps for m in self.metrics_history]

            summary_stats = {
                "memory": {
                    "current_mb": self.current_metrics.memory_mb if self.current_metrics else 0,
                    "baseline_mb": self.memory_baseline_mb or 0,
                    "min_mb": min(memory_values),
                    "max_mb": max(memory_values),
                    "avg_mb": statistics.mean(memory_values),
                    "growth_mb": (self.current_metrics.memory_mb - self.memory_baseline_mb) if self.current_metrics and self.memory_baseline_mb else 0
                },
                "cpu": {
                    "current_percent": self.current_metrics.cpu_percent if self.current_metrics else 0,
                    "baseline_percent": self.cpu_baseline_percent or 0,
                    "min_percent": min(cpu_values),
                    "max_percent": max(cpu_values),
                    "avg_percent": statistics.mean(cpu_values)
                },
                "throughput": {
                    "current_mbps": self.current_metrics.throughput_mbps if self.current_metrics else 0,
                    "baseline_mbps": self.throughput_baseline_mbps or 0,
                    "min_mbps": min(throughput_values),
                    "max_mbps": max(throughput_values),
                    "avg_mbps": statistics.mean(throughput_values)
                }
            }

        return {
            "status": status.value,
            "duration_seconds": self.current_metrics.duration_seconds if self.current_metrics else 0,
            "current_metrics": {
                "memory_mb": self.current_metrics.memory_mb if self.current_metrics else 0,
                "cpu_percent": self.current_metrics.cpu_percent if self.current_metrics else 0,
                "throughput_mbps": self.current_metrics.throughput_mbps if self.current_metrics else 0,
                "avg_latency_ms": self.current_metrics.avg_latency_ms if self.current_metrics else 0,
                "jitter_us": self.current_metrics.jitter_us if self.current_metrics else 0,
                "error_rate_percent": self.current_metrics.error_rate_percent if self.current_metrics else 0
            },
            "baseline_metrics": {
                "memory_mb": self.memory_baseline_mb or 0,
                "cpu_percent": self.cpu_baseline_percent or 0,
                "throughput_mbps": self.throughput_baseline_mbps or 0,
                "avg_latency_ms": self.latency_baseline_ms or 0
            },
            "summary_statistics": summary_stats,
            "active_issues": issues,
            "total_alerts": len(self.stability_alerts),
            "recent_alerts": self.stability_alerts[-10:]  # Last 10 alerts
        }

    def reset(self):
        """Reset the stability validator."""
        self.metrics_history.clear()
        self.current_metrics = None
        self.baseline_metrics = None
        self.start_time = time.time()
        self.stability_trends.clear()
        self.stability_alerts.clear()
        self.memory_baseline_mb = None
        self.cpu_baseline_percent = None
        self.throughput_baseline_mbps = None
        self.latency_baseline_ms = None


class MemoryLeakDetector:
    """Specialized detector for memory leaks."""

    def __init__(self, sampling_interval: float = 10.0):
        self.sampling_interval = sampling_interval
        self.memory_samples: deque = deque(maxlen=1000)
        self.start_time = time.time()
        self.monitoring_active = False

    def start_monitoring(self):
        """Start memory leak monitoring."""
        self.monitoring_active = True
        self.start_time = time.time()
        logger.info("Memory leak monitoring started")

    def stop_monitoring(self):
        """Stop memory leak monitoring."""
        self.monitoring_active = False
        logger.info("Memory leak monitoring stopped")

    def record_memory_usage(self, memory_mb: float):
        """Record current memory usage."""
        if not self.monitoring_active:
            return

        self.memory_samples.append({
            "timestamp": time.time(),
            "memory_mb": memory_mb,
            "duration_seconds": time.time() - self.start_time
        })

    def analyze_memory_trend(self) -> Dict[str, Any]:
        """Analyze memory usage trends for leaks."""
        if len(self.memory_samples) < 10:
            return {"status": "insufficient_data", "samples": len(self.memory_samples)}

        # Extract memory values and timestamps
        timestamps = [sample["timestamp"] for sample in self.memory_samples]
        memory_values = [sample["memory_mb"] for sample in self.memory_samples]

        # Calculate time differences from start
        time_offsets = [(t - self.start_time) / 3600 for t in timestamps]  # Convert to hours

        # Linear regression to find slope (MB/hour)
        n = len(time_offsets)
        sum_x = sum(time_offsets)
        sum_y = sum(memory_values)
        sum_xy = sum(x * y for x, y in zip(time_offsets, memory_values))
        sum_x2 = sum(x * x for x in time_offsets)

        if n > 1 and (n * sum_x2 - sum_x * sum_x) != 0:
            slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
            # Calculate correlation coefficient
            sum_y2 = sum(y * y for y in memory_values)
            correlation = (n * sum_xy - sum_x * sum_y) / (
                (n * sum_x2 - sum_x * sum_x) * (n * sum_y2 - sum_y * sum_y)
            ) ** 0.5 if (n * sum_y2 - sum_y * sum_y) > 0 else 0
        else:
            slope = 0
            correlation = 0

        # Determine leak status
        total_growth = memory_values[-1] - memory_values[0]
        duration_hours = (timestamps[-1] - timestamps[0]) / 3600 if timestamps else 1
        growth_rate_mb_per_hour = total_growth / duration_hours if duration_hours > 0 else 0

        leak_detected = False
        leak_severity = "none"

        if slope > 1.0 and correlation > 0.7:  # Strong positive correlation with growth
            leak_detected = True
            if slope > 10.0:
                leak_severity = "critical"
            elif slope > 5.0:
                leak_severity = "high"
            elif slope > 2.0:
                leak_severity = "medium"
            else:
                leak_severity = "low"

        return {
            "status": "leak_detected" if leak_detected else "no_leak",
            "severity": leak_severity,
            "slope_mb_per_hour": slope,
            "correlation": correlation,
            "total_growth_mb": total_growth,
            "growth_rate_mb_per_hour": growth_rate_mb_per_hour,
            "duration_hours": duration_hours,
            "start_memory_mb": memory_values[0],
            "end_memory_mb": memory_values[-1],
            "samples_analyzed": len(self.memory_samples)
        }

    def reset(self):
        """Reset the memory leak detector."""
        self.memory_samples.clear()
        self.start_time = time.time()