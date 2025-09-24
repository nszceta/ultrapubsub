# Performance Testing Documentation

## Overview

The UltraPubSub performance testing framework provides comprehensive capabilities to validate the Core IPC design requirements, specifically targeting 20MB message transmission at 40Hz with 6 concurrent subscribers.

## Architecture

### Core Components

1. **PerformanceTestFramework** (`tests/performance/core/framework.py`)
   - Main orchestrator for performance tests
   - Manages test lifecycle and metrics collection
   - Coordinates with test coordinator and monitoring

2. **TestCoordinator** (`tests/performance/core/coordinator.py`)
   - Manages producer and subscriber processes
   - Handles synchronization and error recovery
   - Coordinates multi-process test execution

3. **SequenceTracker** (`tests/performance/core/sequencing.py`)
   - Tracks message ordering and sequence numbers
   - Detects duplicates, missing messages, and out-of-order delivery
   - Validates payload integrity

4. **HighResolutionTimer** (`tests/performance/core/timing.py`)
   - Provides precise timing for 40Hz message generation
   - Measures end-to-end latency
   - Schedules message publication

5. **StabilityValidator** (`tests/performance/core/stability.py`)
   - Monitors system stability during extended tests
   - Detects memory leaks and performance degradation
   - Validates resource usage patterns

6. **PerformanceRegressionDetector** (`tests/performance/regression.py`)
   - Compares current performance against historical baselines
   - Detects performance regressions statistically
   - Provides trend analysis and reporting

## Configuration

### Test Configuration

```python
from tests.performance.utils import PerformanceTestConfig

config = PerformanceTestConfig(
    message_size_bytes=20971520,  # 20MB
    frequency_hz=40,              # 40Hz
    subscriber_count=6,           # 6 subscribers
    test_duration_seconds=60,     # 1 minute test
    warmup_duration_seconds=5,    # 5 second warmup
    output_dir="performance-results"
)
```

### Available Parameters

- `message_size_bytes`: Size of each message (default: 20MB)
- `frequency_hz`: Message publication frequency (default: 40Hz)
- `subscriber_count`: Number of concurrent subscribers (default: 6)
- `test_duration_seconds`: Total test duration (default: 60s)
- `warmup_duration_seconds`: Warm-up period (default: 5s)
- `output_dir`: Directory for results (default: "performance-results")
- `enable_resource_monitoring`: Enable system monitoring (default: True)
- `sampling_interval_ms`: Monitoring sampling interval (default: 100ms)

## Running Tests

### Basic Usage

```python
import asyncio
from tests.performance import PerformanceTestFramework, PerformanceTestConfig

async def run_performance_test():
    config = PerformanceTestConfig(
        message_size_bytes=20971520,
        frequency_hz=40,
        subscriber_count=6,
        test_duration_seconds=30
    )

    framework = PerformanceTestFramework(config)
    metrics = await framework.run_test()

    print(f"Throughput: {metrics.throughput_mbps:.2f} MB/s")
    print(f"Messages sent: {metrics.messages_sent}")
    print(f"Test duration: {metrics.end_time - metrics.start_time:.2f}s")

asyncio.run(run_performance_test())
```

### Command Line Interface

```bash
# Run performance test with default settings
python tests/performance/run_performance_test.py

# Custom configuration
python tests/performance/run_performance_test.py \
    --duration 60 \
    --frequency 40 \
    --subscribers 6 \
    --message-size 20971520 \
    --output-dir results
```

### CI/CD Integration

The framework includes GitHub Actions workflows for automated testing:

- **Quick Test**: 10s duration, 20Hz, 3 subscribers, 1MB messages
- **Standard Test**: 30s duration, 40Hz, 6 subscribers, 20MB messages
- **Stress Test**: 60s duration, 60Hz, 10 subscribers, 20MB messages

## Metrics and Reporting

### Performance Metrics

- **Throughput**: Data transfer rate in MB/s
- **Latency**: End-to-end message delivery time
- **Message Count**: Total messages sent/received
- **Resource Usage**: CPU, memory, and system resources
- **Error Rate**: Failed transmissions and errors

### Output Format

Results are saved as JSON files with timestamp:
```json
{
  "config": {
    "message_size_bytes": 20971520,
    "frequency_hz": 40,
    "subscriber_count": 6,
    "test_duration_seconds": 60
  },
  "metrics": {
    "start_time": 1634567890.123,
    "end_time": 1634567950.123,
    "messages_sent": 2400,
    "throughput_mbps": 800.0,
    "latency_stats": {
      "avg_ms": 2.5,
      "p95_ms": 5.2,
      "p99_ms": 8.1
    },
    "resource_usage": {
      "cpu_usage": {"avg_cpu_percent": 45.2},
      "memory_usage": {"avg_memory_mb": 512.3}
    }
  }
}
```

## Validation Requirements

The framework validates the Core IPC design requirements:

### 1. Throughput Requirement
- **Target**: 800 MB/s (20MB × 40Hz)
- **Validation**: Sustained throughput over test duration
- **Metrics**: `throughput_mbps` in results

### 2. Frequency Requirement
- **Target**: 40Hz message generation
- **Validation**: Precise scheduling with high-resolution timer
- **Metrics**: Timing accuracy and jitter analysis

### 3. Subscriber Scale
- **Target**: 6 concurrent subscribers
- **Validation**: All subscribers receive all messages
- **Metrics**: Message delivery rate per subscriber

### 4. Payload Integrity
- **Target**: Zero data corruption
- **Validation**: Checksum verification and timestamp validation
- **Metrics**: Corruption detection and error rates

### 5. Message Ordering
- **Target**: Strict message ordering
- **Validation**: Sequence number tracking
- **Metrics**: Out-of-order and duplicate message detection

## Performance Regression Detection

### Baseline Management

```python
from tests.performance.regression import PerformanceRegressionDetector

detector = PerformanceRegressionDetector()

# Create baseline from good performance
baseline = detector.create_baseline_from_results(test_results)

# Compare current results against baseline
regression_result = detector.detect_regression(current_results)

if regression_result.regression_detected:
    print(f"Regression detected: {regression_result.recommendation}")
```

### Regression Thresholds

- **Throughput**: Max 10% degradation
- **Latency**: Max 20% increase
- **CPU Usage**: Max 15% increase
- **Memory Usage**: Max 25% increase
- **Error Rate**: Max 50% increase

## Troubleshooting

### Common Issues

1. **Low Throughput**
   - Check system resource availability
   - Verify network bandwidth
   - Monitor CPU and memory usage

2. **High Latency**
   - Check for system contention
   - Verify timer resolution
   - Monitor process scheduling

3. **Message Loss**
   - Check subscriber connectivity
   - Verify buffer sizes
   - Monitor error rates

4. **Resource Exhaustion**
   - Adjust test duration
   - Monitor memory usage
   - Check for leaks

### Debug Mode

Enable detailed logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Performance Profiling

Use the built-in monitoring:
```python
config = PerformanceTestConfig(
    enable_resource_monitoring=True,
    sampling_interval_ms=50  # Higher resolution
)
```

## Best Practices

### Test Design

1. **Warm-up Period**: Always include warm-up to allow system stabilization
2. **Multiple Runs**: Execute tests multiple times for consistent results
3. **Resource Monitoring**: Enable monitoring to identify bottlenecks
4. **Baseline Comparison**: Use regression detection for performance tracking

### Environment Setup

1. **System Requirements**: Ensure sufficient CPU, memory, and network resources
2. **Process Priority**: Consider adjusting process priorities for timing-critical tests
3. **Background Processes**: Minimize background processes during testing
4. **System Configuration**: Optimize system settings for high-performance networking

### Result Analysis

1. **Trend Analysis**: Monitor performance over time
2. **Statistical Significance**: Use appropriate sample sizes
3. **Correlation Analysis**: Relate performance to system conditions
4. **Comparative Analysis**: Compare against baselines and previous results

## Extending the Framework

### Custom Metrics

```python
class CustomMetrics(PerformanceMetrics):
    def __init__(self):
        super().__init__()
        self.custom_metric = 0.0

class CustomTestFramework(PerformanceTestFramework):
    async def _execute_test(self):
        # Run custom test logic
        self.metrics.custom_metric = calculate_custom_metric()
```

### New Validation Rules

```python
class CustomValidator:
    def validate_custom_requirement(self, metrics: PerformanceMetrics) -> bool:
        # Implement custom validation logic
        return metrics.custom_metric > threshold
```

### Integration Points

- **Metrics Export**: Integrate with monitoring systems
- **Alerting**: Add performance degradation alerts
- **Reporting**: Generate custom reports and dashboards
- **CI/CD**: Extend pipeline workflows

## Contributing

### Adding New Tests

1. Create test configuration
2. Implement test logic in appropriate module
3. Add validation rules
4. Update documentation
5. Add CI/CD workflow if needed

### Code Standards

- Follow existing code patterns
- Include comprehensive error handling
- Add logging for debugging
- Document public interfaces
- Include unit tests for new functionality

## References

- **Core IPC Design Requirements**: [link to specs]
- **OpenSpec Methodology**: [link to openspec docs]
- **Performance Testing Best Practices**: [link to guidelines]
- **GitHub Actions Configuration**: [link to workflows]