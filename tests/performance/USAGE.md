# Performance Testing Usage Guide

## Quick Start

### Prerequisites

- Python 3.11+
- Sufficient system resources (CPU, memory, disk I/O)
- Network capabilities for IPC testing

### Installation

```bash
# Install dependencies
pip install -e .

# Install test dependencies
pip install pytest pytest-benchmark psutil numpy
```

### Running Your First Test

```bash
# Basic performance test
python tests/performance/run_performance_test.py

# View results
ls performance-results/
cat performance-results/performance_test_*.json
```

## Detailed Usage

### Configuration Examples

#### Basic Configuration
```python
from tests.performance.utils import PerformanceTestConfig

# Minimal configuration
config = PerformanceTestConfig()

# Custom configuration
config = PerformanceTestConfig(
    message_size_bytes=1048576,  # 1MB messages
    frequency_hz=20,             # 20Hz frequency
    subscriber_count=3,          # 3 subscribers
    test_duration_seconds=30,   # 30 seconds
    output_dir="my-results"
)
```

#### Advanced Configuration
```python
# Production-grade configuration
config = PerformanceTestConfig(
    message_size_bytes=20971520,     # 20MB - design requirement
    frequency_hz=40,                 # 40Hz - design requirement
    subscriber_count=6,              # 6 subscribers - design requirement
    test_duration_seconds=300,       # 5 minutes for stability
    warmup_duration_seconds=10,      # Extended warmup
    output_dir="production-results",
    enable_resource_monitoring=True,
    sampling_interval_ms=50,         # High-frequency monitoring
    max_memory_mb=4096,              # Memory limit
    max_cpu_percent=80.0             # CPU limit
)
```

### Programmatic Usage

#### Simple Test Execution
```python
import asyncio
from tests.performance import PerformanceTestFramework, PerformanceTestConfig

async def run_test():
    # Configure test
    config = PerformanceTestConfig(
        message_size_bytes=20971520,
        frequency_hz=40,
        subscriber_count=6,
        test_duration_seconds=60
    )

    # Create and run test
    framework = PerformanceTestFramework(config)
    metrics = await framework.run_test()

    # Analyze results
    print(f"Test completed successfully!")
    print(f"Throughput: {metrics.throughput_mbps:.2f} MB/s")
    print(f"Messages sent: {metrics.messages_sent}")
    print(f"Duration: {metrics.end_time - metrics.start_time:.2f}s")

    return metrics

# Run the test
results = asyncio.run(run_test())
```

#### Advanced Test with Custom Validation
```python
import asyncio
from tests.performance import PerformanceTestFramework, PerformanceTestConfig
from tests.performance.regression import PerformanceRegressionDetector

async def run_validated_test():
    # Configure test
    config = PerformanceTestConfig(
        message_size_bytes=20971520,
        frequency_hz=40,
        subscriber_count=6,
        test_duration_seconds=120,
        enable_resource_monitoring=True
    )

    # Run test
    framework = PerformanceTestFramework(config)
    metrics = await framework.run_test()

    # Validate against requirements
    target_throughput = (20 * 40)  # 20MB * 40Hz = 800 MB/s
    actual_throughput = metrics.throughput_mbps

    print(f"Target throughput: {target_throughput} MB/s")
    print(f"Actual throughput: {actual_throughput:.2f} MB/s")
    print(f"Efficiency: {(actual_throughput/target_throughput)*100:.1f}%")

    # Check for regressions
    detector = PerformanceRegressionDetector()
    regression_result = detector.detect_regression({"metrics": vars(metrics)})

    if regression_result.regression_detected:
        print(f"⚠️  Regression detected: {regression_result.recommendation}")
    else:
        print("✅ No performance regression detected")

    return metrics, regression_result

results, regression = asyncio.run(run_validated_test())
```

### Command Line Usage

#### Basic Commands
```bash
# Run with default settings
python tests/performance/run_performance_test.py

# Specify test duration
python tests/performance/run_performance_test.py --duration 30

# Configure all parameters
python tests/performance/run_performance_test.py \
    --duration 60 \
    --frequency 40 \
    --subscribers 6 \
    --message-size 20971520 \
    --output-dir custom-results \
    --save-config my-config.json
```

#### Batch Testing
```bash
# Run multiple test configurations
for duration in 30 60 120; do
    python tests/performance/run_performance_test.py \
        --duration $duration \
        --output-dir results/duration_${duration}s
done

# Run scaling tests
for subscribers in 1 3 6 10; do
    python tests/performance/run_performance_test.py \
        --subscribers $subscribers \
        --output-dir results/subscribers_${subscribers}
done
```

### CI/CD Integration

#### GitHub Actions

The framework includes pre-configured GitHub Actions workflows:

```yaml
# .github/workflows/performance-tests.yml
name: Performance Tests

on:
  push:
    branches: [ main, dev ]
  pull_request:
    branches: [ main, dev ]

jobs:
  performance-test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        test-config:
          - name: "Quick Test"
            duration: 10
            frequency: 20
            subscribers: 3
            message-size: 1048576
```

#### Local CI Simulation

```bash
# Run all CI test configurations
python -c "
import asyncio
from tests.performance import PerformanceTestFramework, PerformanceTestConfig

configs = [
    {'name': 'Quick', 'duration': 10, 'frequency': 20, 'subscribers': 3, 'size': 1048576},
    {'name': 'Standard', 'duration': 30, 'frequency': 40, 'subscribers': 6, 'size': 20971520},
    {'name': 'Stress', 'duration': 60, 'frequency': 60, 'subscribers': 10, 'size': 20971520}
]

async def run_all_tests():
    for config_data in configs:
        print(f'Running {config_data[\"name\"]} test...')
        config = PerformanceTestConfig(
            test_duration_seconds=config_data['duration'],
            frequency_hz=config_data['frequency'],
            subscriber_count=config_data['subscribers'],
            message_size_bytes=config_data['size']
        )
        framework = PerformanceTestFramework(config)
        metrics = await framework.run_test()
        print(f'  Result: {metrics.throughput_mbps:.2f} MB/s')

asyncio.run(run_all_tests())
"
```

### Performance Regression Testing

#### Setting Up Baselines

```python
from tests.performance.regression import PerformanceRegressionDetector

# Initialize detector
detector = PerformanceRegressionDetector()

# Run baseline test
config = PerformanceTestConfig(test_duration_seconds=60)
framework = PerformanceTestFramework(config)
baseline_metrics = await framework.run_test()

# Save as baseline
baseline = detector.create_baseline_from_results(
    {"metrics": vars(baseline_metrics)},
    metadata={"environment": "production", "version": "1.0.0"}
)

print(f"Baseline created: {baseline.throughput_mbps:.2f} MB/s")
```

#### Regression Detection

```python
# Run current test
current_metrics = await framework.run_test()

# Check for regressions
regression_result = detector.detect_regression({
    "metrics": vars(current_metrics)
})

# Analyze results
if regression_result.regression_detected:
    print("🚨 PERFORMANCE REGRESSION DETECTED!")
    print(f"Confidence: {regression_result.confidence_level*100:.1f}%")
    print(f"Affected metrics: {', '.join(regression_result.affected_metrics)}")
    print(f"Recommendation: {regression_result.recommendation}")

    # Detailed breakdown
    for metric, degradation in regression_result.degradation_percentages.items():
        print(f"  {metric}: {degradation:+.1f}%")
else:
    print("✅ Performance is within acceptable limits")
```

### Advanced Monitoring

#### Real-time Monitoring

```python
import asyncio
from tests.performance.utils import PerformanceMonitor

async def monitor_test():
    monitor = PerformanceMonitor(sampling_interval_ms=100)
    await monitor.start_monitoring()

    try:
        # Run your test here
        await asyncio.sleep(10)  # Example test

        # Get real-time stats
        stats = monitor.get_resource_stats()
        print(f"CPU: {stats['cpu_usage']['avg_cpu_percent']:.1f}%")
        print(f"Memory: {stats['memory_usage']['avg_memory_mb']:.1f}MB")

    finally:
        await monitor.stop_monitoring()

asyncio.run(monitor_test())
```

#### Custom Metrics Collection

```python
class CustomMetrics:
    def __init__(self):
        self.custom_metrics = {}

    def record_metric(self, name: str, value: float):
        if name not in self.custom_metrics:
            self.custom_metrics[name] = []
        self.custom_metrics[name].append(value)

    def get_stats(self, name: str) -> dict:
        values = self.custom_metrics.get(name, [])
        if not values:
            return {}

        return {
            'count': len(values),
            'min': min(values),
            'max': max(values),
            'avg': sum(values) / len(values)
        }

# Usage in test
custom_metrics = CustomMetrics()
custom_metrics.record_metric('custom_latency', 1.5)
stats = custom_metrics.get_stats('custom_latency')
```

### Test Result Analysis

#### Loading and Analyzing Results

```python
import json
from pathlib import Path

def analyze_results(results_dir: str):
    """Analyze all test results in a directory."""
    results_path = Path(results_dir)

    for result_file in results_path.glob("performance_test_*.json"):
        with open(result_file, 'r') as f:
            data = json.load(f)

        metrics = data['metrics']
        config = data['config']

        print(f"\nTest: {result_file.stem}")
        print(f"  Duration: {config['test_duration_seconds']}s")
        print(f"  Throughput: {metrics['throughput_mbps']:.2f} MB/s")
        print(f"  Messages: {metrics['messages_sent']}")
        print(f"  Latency: {metrics['latency_stats']['avg_ms']:.2f}ms (avg)")

# Analyze all results
analyze_results("performance-results")
```

#### Generating Reports

```python
def generate_report(results_dir: str, output_file: str):
    """Generate a markdown report from test results."""
    results_path = Path(results_dir)
    report = ["# Performance Test Report\n"]

    for result_file in sorted(results_path.glob("performance_test_*.json")):
        with open(result_file, 'r') as f:
            data = json.load(f)

        metrics = data['metrics']
        config = data['config']

        report.append(f"## Test {result_file.stem}")
        report.append(f"- **Duration**: {config['test_duration_seconds']}s")
        report.append(f"- **Message Size**: {config['message_size_bytes'] / 1024 / 1024:.1f}MB")
        report.append(f"- **Frequency**: {config['frequency_hz']}Hz")
        report.append(f"- **Subscribers**: {config['subscriber_count']}")
        report.append(f"- **Throughput**: {metrics['throughput_mbps']:.2f} MB/s")
        report.append(f"- **Messages**: {metrics['messages_sent']}")
        report.append(f"- **Avg Latency**: {metrics['latency_stats']['avg_ms']:.2f}ms")
        report.append("")

    with open(output_file, 'w') as f:
        f.write('\n'.join(report))

    print(f"Report generated: {output_file}")

generate_report("performance-results", "performance-report.md")
```

### Troubleshooting Common Issues

#### Performance Issues

```python
# Debug performance problems
async def debug_performance():
    config = PerformanceTestConfig(
        test_duration_seconds=30,
        enable_resource_monitoring=True,
        sampling_interval_ms=50  # High-frequency monitoring
    )

    framework = PerformanceTestFramework(config)
    metrics = await framework.run_test()

    # Check resource usage
    resource_usage = metrics.resource_usage
    cpu_usage = resource_usage.get('cpu_usage', {}).get('avg_cpu_percent', 0)
    memory_usage = resource_usage.get('memory_usage', {}).get('avg_memory_mb', 0)

    print(f"CPU Usage: {cpu_usage:.1f}%")
    print(f"Memory Usage: {memory_usage:.1f}MB")

    # Check for resource bottlenecks
    if cpu_usage > 90:
        print("⚠️  High CPU usage - consider reducing frequency or message size")
    if memory_usage > 1024:
        print("⚠️  High memory usage - check for memory leaks")

    # Check throughput efficiency
    target_throughput = (config.message_size_bytes * config.frequency_hz) / (1024 * 1024)
    efficiency = (metrics.throughput_mbps / target_throughput) * 100

    print(f"Efficiency: {efficiency:.1f}%")
    if efficiency < 80:
        print("⚠️  Low efficiency - check for bottlenecks")

asyncio.run(debug_performance())
```

#### Memory Leak Detection

```python
from tests.performance.core.stability import MemoryLeakDetector

async def check_memory_leaks():
    detector = MemoryLeakDetector()

    # Run multiple iterations to detect leaks
    for i in range(5):
        config = PerformanceTestConfig(test_duration_seconds=30)
        framework = PerformanceTestFramework(config)
        metrics = await framework.run_test()

        detector.record_memory_usage(metrics.end_time,
                                    metrics.resource_usage.get('memory_usage', {}).get('avg_memory_mb', 0))

        print(f"Iteration {i+1}: Memory recorded")

    # Analyze for leaks
    leak_analysis = detector.analyze_memory_trend()

    if leak_analysis.get('leak_detected', False):
        print("🚨 Memory leak detected!")
        print(f"Leak rate: {leak_analysis.get('leak_rate_mb_per_hour', 0):.2f} MB/hour")
    else:
        print("✅ No memory leak detected")

asyncio.run(check_memory_leaks())
```

This usage guide provides comprehensive information for using the performance testing framework effectively, from basic usage to advanced monitoring and troubleshooting.