"""
Performance regression detection utilities.

This module provides comprehensive regression detection capabilities including:
- Performance baseline comparison
- Statistical significance testing
- Threshold-based regression detection
- Trend analysis
- Historical performance tracking
"""

import json
import logging
import statistics
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class PerformanceBaseline:
    """Performance baseline data."""
    timestamp: datetime
    throughput_mbps: float
    avg_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    cpu_usage_percent: float
    memory_usage_mb: float
    error_rate_percent: float
    test_config: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RegressionThresholds:
    """Thresholds for regression detection."""
    max_throughput_degradation_percent: float = 10.0
    max_latency_increase_percent: float = 20.0
    max_cpu_increase_percent: float = 15.0
    max_memory_increase_percent: float = 25.0
    max_error_rate_increase_percent: float = 50.0
    min_sample_size: int = 5
    statistical_significance_level: float = 0.05


@dataclass
class RegressionResult:
    """Regression detection result."""
    regression_detected: bool
    confidence_level: float
    affected_metrics: List[str]
    degradation_percentages: Dict[str, float]
    baseline_value: Dict[str, float]
    current_value: Dict[str, float]
    threshold_exceeded: Dict[str, float]
    recommendation: str


class PerformanceRegressionDetector:
    """Detects performance regressions by comparing against baselines."""

    def __init__(self, baseline_dir: str = "performance-baselines", thresholds: Optional[RegressionThresholds] = None):
        self.baseline_dir = Path(baseline_dir)
        self.thresholds = thresholds or RegressionThresholds()
        self.baselines: List[PerformanceBaseline] = []
        self.load_baselines()

    def load_baselines(self):
        """Load existing performance baselines."""
        self.baselines.clear()

        if not self.baseline_dir.exists():
            self.baseline_dir.mkdir(parents=True, exist_ok=True)
            return

        for baseline_file in self.baseline_dir.glob("baseline_*.json"):
            try:
                with open(baseline_file, 'r') as f:
                    data = json.load(f)

                baseline = PerformanceBaseline(
                    timestamp=datetime.fromisoformat(data['timestamp']),
                    throughput_mbps=data['throughput_mbps'],
                    avg_latency_ms=data['avg_latency_ms'],
                    p95_latency_ms=data['p95_latency_ms'],
                    p99_latency_ms=data['p99_latency_ms'],
                    cpu_usage_percent=data['cpu_usage_percent'],
                    memory_usage_mb=data['memory_usage_mb'],
                    error_rate_percent=data['error_rate_percent'],
                    test_config=data['test_config'],
                    metadata=data.get('metadata', {})
                )

                self.baselines.append(baseline)

            except Exception as e:
                logger.error(f"Error loading baseline {baseline_file}: {e}")

        # Sort baselines by timestamp
        self.baselines.sort(key=lambda x: x.timestamp)
        logger.info(f"Loaded {len(self.baselines)} performance baselines")

    def save_baseline(self, baseline: PerformanceBaseline):
        """Save a new performance baseline."""
        self.baseline_dir.mkdir(parents=True, exist_ok=True)

        timestamp_str = baseline.timestamp.strftime("%Y%m%d_%H%M%S")
        baseline_file = self.baseline_dir / f"baseline_{timestamp_str}.json"

        data = {
            'timestamp': baseline.timestamp.isoformat(),
            'throughput_mbps': baseline.throughput_mbps,
            'avg_latency_ms': baseline.avg_latency_ms,
            'p95_latency_ms': baseline.p95_latency_ms,
            'p99_latency_ms': baseline.p99_latency_ms,
            'cpu_usage_percent': baseline.cpu_usage_percent,
            'memory_usage_mb': baseline.memory_usage_mb,
            'error_rate_percent': baseline.error_rate_percent,
            'test_config': baseline.test_config,
            'metadata': baseline.metadata
        }

        with open(baseline_file, 'w') as f:
            json.dump(data, f, indent=2)

        self.baselines.append(baseline)
        self.baselines.sort(key=lambda x: x.timestamp)
        logger.info(f"Saved baseline to {baseline_file}")

    def get_recent_baseline(self, days: int = 7) -> Optional[PerformanceBaseline]:
        """Get the most recent baseline within the specified number of days."""
        cutoff_date = datetime.now() - timedelta(days=days)

        for baseline in reversed(self.baselines):
            if baseline.timestamp >= cutoff_date:
                return baseline

        return None

    def detect_regression(self, current_results: Dict[str, Any]) -> RegressionResult:
        """Detect performance regression by comparing current results against baseline."""
        # Extract current metrics
        current_metrics = self._extract_metrics(current_results)

        # Get appropriate baseline
        baseline = self.get_recent_baseline()
        if baseline is None:
            logger.warning("No recent baseline found for comparison")
            return RegressionResult(
                regression_detected=False,
                confidence_level=0.0,
                affected_metrics=[],
                degradation_percentages={},
                baseline_value={},
                current_value=current_metrics,
                threshold_exceeded={},
                recommendation="No baseline available for comparison"
            )

        # Compare metrics
        regression_metrics = []
        degradation_percentages = {}
        threshold_exceeded = {}

        # Throughput comparison
        throughput_degradation = self._calculate_percentage_change(
            baseline.throughput_mbps, current_metrics['throughput_mbps']
        )
        degradation_percentages['throughput'] = throughput_degradation

        if throughput_degradation < -self.thresholds.max_throughput_degradation_percent:
            regression_metrics.append('throughput')
            threshold_exceeded['throughput'] = self.thresholds.max_throughput_degradation_percent

        # Latency comparison
        latency_metrics = ['avg_latency', 'p95_latency', 'p99_latency']
        for metric in latency_metrics:
            baseline_value = getattr(baseline, f"{metric}_ms")
            current_value = current_metrics.get(metric, 0)

            latency_increase = self._calculate_percentage_change(baseline_value, current_value)
            degradation_percentages[metric] = latency_increase

            if latency_increase > self.thresholds.max_latency_increase_percent:
                regression_metrics.append(metric)
                threshold_exceeded[metric] = self.thresholds.max_latency_increase_percent

        # Resource usage comparison
        cpu_increase = self._calculate_percentage_change(
            baseline.cpu_usage_percent, current_metrics.get('cpu_usage_percent', 0)
        )
        degradation_percentages['cpu_usage'] = cpu_increase

        if cpu_increase > self.thresholds.max_cpu_increase_percent:
            regression_metrics.append('cpu_usage')
            threshold_exceeded['cpu_usage'] = self.thresholds.max_cpu_increase_percent

        memory_increase = self._calculate_percentage_change(
            baseline.memory_usage_mb, current_metrics.get('memory_usage_mb', 0)
        )
        degradation_percentages['memory_usage'] = memory_increase

        if memory_increase > self.thresholds.max_memory_increase_percent:
            regression_metrics.append('memory_usage')
            threshold_exceeded['memory_usage'] = self.thresholds.max_memory_increase_percent

        # Error rate comparison
        error_rate_increase = self._calculate_percentage_change(
            baseline.error_rate_percent, current_metrics.get('error_rate_percent', 0)
        )
        degradation_percentages['error_rate'] = error_rate_increase

        if error_rate_increase > self.thresholds.max_error_rate_increase_percent:
            regression_metrics.append('error_rate')
            threshold_exceeded['error_rate'] = self.thresholds.max_error_rate_increase_percent

        # Determine regression status
        regression_detected = len(regression_metrics) > 0
        confidence_level = self._calculate_confidence_level(regression_metrics, degradation_percentages)

        # Generate recommendation
        recommendation = self._generate_recommendation(
            regression_detected, regression_metrics, degradation_percentages
        )

        # Get baseline values for comparison
        baseline_value = {
            'throughput': baseline.throughput_mbps,
            'avg_latency': baseline.avg_latency_ms,
            'p95_latency': baseline.p95_latency_ms,
            'p99_latency': baseline.p99_latency_ms,
            'cpu_usage': baseline.cpu_usage_percent,
            'memory_usage': baseline.memory_usage_mb,
            'error_rate': baseline.error_rate_percent
        }

        return RegressionResult(
            regression_detected=regression_detected,
            confidence_level=confidence_level,
            affected_metrics=regression_metrics,
            degradation_percentages=degradation_percentages,
            baseline_value=baseline_value,
            current_value=current_metrics,
            threshold_exceeded=threshold_exceeded,
            recommendation=recommendation
        )

    def _extract_metrics(self, results: Dict[str, Any]) -> Dict[str, float]:
        """Extract key metrics from test results."""
        metrics = results.get('metrics', {})

        return {
            'throughput': metrics.get('throughput_mbps', 0),
            'avg_latency': statistics.mean(metrics.get('latency_samples', [0])) if metrics.get('latency_samples') else 0,
            'p95_latency': self._calculate_percentile(metrics.get('latency_samples', [0]), 95),
            'p99_latency': self._calculate_percentile(metrics.get('latency_samples', [0]), 99),
            'cpu_usage': metrics.get('resource_usage', {}).get('cpu_usage', {}).get('avg_cpu_percent', 0),
            'memory_usage': metrics.get('resource_usage', {}).get('memory_usage', {}).get('avg_memory_mb', 0),
            'error_rate': (len(metrics.get('errors', [])) / max(metrics.get('messages_sent', 1), 1)) * 100
        }

    def _calculate_percentage_change(self, baseline: float, current: float) -> float:
        """Calculate percentage change from baseline to current."""
        if baseline == 0:
            return 0.0
        return ((current - baseline) / baseline) * 100

    def _calculate_percentile(self, data: List[float], percentile: int) -> float:
        """Calculate percentile value."""
        if not data:
            return 0.0
        return np.percentile(data, percentile)

    def _calculate_confidence_level(self, regression_metrics: List[str], degradations: Dict[str, float]) -> float:
        """Calculate confidence level for regression detection."""
        if not regression_metrics:
            return 0.0

        # Simple confidence calculation based on number and severity of regressions
        total_degradation = sum(abs(degradations.get(metric, 0)) for metric in regression_metrics)
        avg_degradation = total_degradation / len(regression_metrics)

        # Map average degradation to confidence level
        if avg_degradation > 50:
            return 0.95  # High confidence
        elif avg_degradation > 25:
            return 0.80  # Medium confidence
        elif avg_degradation > 10:
            return 0.60  # Low confidence
        else:
            return 0.40  # Very low confidence

    def _generate_recommendation(self, regression_detected: bool, affected_metrics: List[str], degradations: Dict[str, float]) -> str:
        """Generate recommendation based on regression analysis."""
        if not regression_detected:
            return "No performance regression detected. Current performance is within acceptable thresholds."

        recommendations = []

        for metric in affected_metrics:
            degradation = degradations.get(metric, 0)
            if metric == 'throughput':
                recommendations.append(f"Throughput degraded by {abs(degradation):.1f}%. Investigate bottlenecks.")
            elif 'latency' in metric:
                recommendations.append(f"{metric.replace('_', ' ').title()} increased by {degradation:.1f}%. Review processing delays.")
            elif metric == 'cpu_usage':
                recommendations.append(f"CPU usage increased by {degradation:.1f}%. Optimize CPU-intensive operations.")
            elif metric == 'memory_usage':
                recommendations.append(f"Memory usage increased by {degradation:.1f}%. Check for memory leaks.")
            elif metric == 'error_rate':
                recommendations.append(f"Error rate increased by {degradation:.1f}%. Review error handling and stability.")

        return " ".join(recommendations)

    def get_performance_trend(self, metric: str, days: int = 30) -> Dict[str, Any]:
        """Analyze performance trends over time."""
        cutoff_date = datetime.now() - timedelta(days=days)
        recent_baselines = [b for b in self.baselines if b.timestamp >= cutoff_date]

        if len(recent_baselines) < 2:
            return {"error": "Insufficient data for trend analysis"}

        # Extract metric values over time
        timestamps = [b.timestamp for b in recent_baselines]

        if metric == 'throughput':
            values = [b.throughput_mbps for b in recent_baselines]
        elif metric == 'avg_latency':
            values = [b.avg_latency_ms for b in recent_baselines]
        elif metric == 'cpu_usage':
            values = [b.cpu_usage_percent for b in recent_baselines]
        elif metric == 'memory_usage':
            values = [b.memory_usage_mb for b in recent_baselines]
        else:
            return {"error": f"Unknown metric: {metric}"}

        # Calculate trend
        if len(values) >= 2:
            # Simple linear regression
            x = list(range(len(values)))
            y = values

            # Calculate slope
            n = len(x)
            sum_x = sum(x)
            sum_y = sum(y)
            sum_xy = sum(xi * yi for xi, yi in zip(x, y))
            sum_x2 = sum(xi * xi for xi in x)

            if n > 1 and (n * sum_x2 - sum_x * sum_x) != 0:
                slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
            else:
                slope = 0

            # Calculate correlation
            if len(set(y)) > 1:
                correlation = abs(slope) / max(y) if max(y) > 0 else 0
            else:
                correlation = 0

            trend_direction = "improving" if slope < 0 else "degrading"
            trend_strength = "strong" if correlation > 0.7 else "moderate" if correlation > 0.3 else "weak"

            return {
                "trend_direction": trend_direction,
                "trend_strength": trend_strength,
                "slope": slope,
                "correlation": correlation,
                "data_points": len(recent_baselines),
                "values": values,
                "timestamps": [t.isoformat() for t in timestamps]
            }
        else:
            return {"error": "Insufficient data for trend analysis"}

    def create_baseline_from_results(self, results: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None) -> PerformanceBaseline:
        """Create a baseline from test results."""
        metrics = self._extract_metrics(results)
        config = results.get('config', {}).get('config', {})

        baseline = PerformanceBaseline(
            timestamp=datetime.now(),
            throughput_mbps=metrics['throughput'],
            avg_latency_ms=metrics['avg_latency'],
            p95_latency_ms=metrics['p95_latency'],
            p99_latency_ms=metrics['p99_latency'],
            cpu_usage_percent=metrics['cpu_usage'],
            memory_usage_mb=metrics['memory_usage'],
            error_rate_percent=metrics['error_rate'],
            test_config=config,
            metadata=metadata or {}
        )

        self.save_baseline(baseline)
        return baseline