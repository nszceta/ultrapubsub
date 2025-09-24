# Performance Testing API Reference

## Core Classes

### PerformanceTestFramework

Main orchestrator for performance tests.

```python
class PerformanceTestFramework:
    """Main performance testing framework."""

    def __init__(self, config: PerformanceTestConfig):
        """
        Initialize the performance test framework.

        Args:
            config: Test configuration parameters
        """

    async def run_test(self) -> PerformanceMetrics:
        """
        Execute the complete performance test.

        Returns:
            PerformanceMetrics: Test results and metrics
        """

    def stop_test(self):
        """Stop the currently running test."""
```

#### Usage Example

```python
from tests.performance import PerformanceTestFramework, PerformanceTestConfig

config = PerformanceTestConfig(
    message_size_bytes=20971520,
    frequency_hz=40,
    subscriber_count=6,
    test_duration_seconds=60
)

framework = PerformanceTestFramework(config)
metrics = await framework.run_test()
```

### PerformanceTestConfig

Configuration class for performance tests.

```python
@dataclass
class PerformanceTestConfig:
    """Configuration for performance tests."""

    message_size_bytes: int = 20971520
    frequency_hz: int = 40
    subscriber_count: int = 6
    test_duration_seconds: int = 60
    warmup_duration_seconds: int = 5
    output_dir: str = "performance-results"
    enable_resource_monitoring: bool = True
    sampling_interval_ms: int = 100
    max_memory_mb: Optional[int] = None
    max_cpu_percent: Optional[float] = None
```

### PerformanceMetrics

Container for performance test results.

```python
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
```

## Test Coordination

### TestCoordinator

Manages producer and subscriber processes.

```python
class TestCoordinator:
    """Coordinates test execution across multiple processes."""

    def __init__(self, config: PerformanceTestConfig):
        """
        Initialize the test coordinator.

        Args:
            config: Test configuration parameters
        """

    async def start_test(self) -> Dict[str, Any]:
        """
        Start and coordinate the performance test.

        Returns:
            Dict containing test results and metrics
        """

    async def stop_test(self):
        """Stop all test processes."""
```

### ProcessState

Process state enumeration.

```python
class ProcessState(Enum):
    """Process state enumeration."""
    CREATED = "created"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"
```

### ProcessInfo

Process information container.

```python
@dataclass
class ProcessInfo:
    """Information about a test process."""
    pid: int
    role: str  # "producer" or "subscriber"
    state: ProcessState
    start_time: float
    end_time: Optional[float] = None
    stats: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
```

## Sequence Tracking

### SequenceTracker

Tracks message ordering and sequence numbers.

```python
class SequenceTracker:
    """Tracks message sequence numbers and ordering."""

    def __init__(self, expected_subscribers: int):
        """
        Initialize sequence tracker.

        Args:
            expected_subscribers: Number of expected subscribers
        """

    def record_message_received(self, subscriber_id: int, sequence_number: int,
                              message_size: int, receive_time: float) -> List[SequenceError]:
        """
        Record a message received by a subscriber.

        Args:
            subscriber_id: ID of the subscriber
            sequence_number: Sequence number of the message
            message_size: Size of the received message
            receive_time: Timestamp when message was received

        Returns:
            List of sequence errors detected
        """

    def get_sequence_summary(self) -> Dict[str, Any]:
        """
        Get summary of sequence tracking data.

        Returns:
            Dictionary containing sequence statistics
        """
```

### MessageOrderValidator

Validates message ordering and integrity.

```python
class MessageOrderValidator:
    """Validates message ordering and payload integrity."""

    def __init__(self, expected_sequence_count: int):
        """
        Initialize message order validator.

        Args:
            expected_sequence_count: Expected number of messages
        """

    def validate_message_order(self, messages: List[Dict[str, Any]]) -> ValidationResult:
        """
        Validate message ordering for a subscriber.

        Args:
            messages: List of received messages

        Returns:
            ValidationResult with validation results
        """
```

### SequenceError

Sequence error information.

```python
@dataclass
class SequenceError:
    """Sequence error information."""
    error_type: SequenceErrorType
    subscriber_id: int
    expected_sequence: Optional[int] = None
    actual_sequence: Optional[int] = None
    timestamp: float = 0.0
    details: str = ""
```

### SequenceErrorType

Sequence error type enumeration.

```python
class SequenceErrorType(Enum):
    """Types of sequence errors."""
    MISSING_MESSAGE = "missing_message"
    DUPLICATE_MESSAGE = "duplicate_message"
    OUT_OF_ORDER = "out_of_order"
    PAYLOAD_MISMATCH = "payload_mismatch"
    CORRUPTED_PAYLOAD = "corrupted_payload"
```

## Timing and Scheduling

### HighResolutionTimer

Provides high-resolution timing capabilities.

```python
class HighResolutionTimer:
    """High-resolution timer for precise timing measurements."""

    def __init__(self):
        """Initialize high-resolution timer."""

    def get_time_ns(self) -> int:
        """
        Get current time in nanoseconds.

        Returns:
            Current time in nanoseconds
        """

    def get_time_us(self) -> float:
        """
        Get current time in microseconds.

        Returns:
            Current time in microseconds
        """

    def sleep_until(self, target_time_ns: int) -> bool:
        """
        Sleep until a specific target time.

        Args:
            target_time_ns: Target time in nanoseconds

        Returns:
            True if sleep was successful, False if overshot
        """
```

### FrequencyScheduler

Schedules message generation at specific frequencies.

```python
class FrequencyScheduler:
    """Schedules message generation at precise frequencies."""

    def __init__(self, frequency_hz: int):
        """
        Initialize frequency scheduler.

        Args:
            frequency_hz: Target frequency in Hz
        """

    def wait_for_next_slot(self) -> Optional[TimingMetrics]:
        """
        Wait for the next scheduling slot.

        Returns:
            TimingMetrics if slot was reached, None if stopped
        """

    def get_timing_stats(self) -> Dict[str, Any]:
        """
        Get timing statistics.

        Returns:
            Dictionary containing timing statistics
        """
```

### LatencyTracker

Tracks and measures message latency.

```python
class LatencyTracker:
    """Tracks message latency statistics."""

    def __init__(self):
        """Initialize latency tracker."""

    def record_latency(self, sequence_number: int, latency_us: int):
        """
        Record a latency measurement.

        Args:
            sequence_number: Message sequence number
            latency_us: Latency in microseconds
        """

    def get_latency_stats(self) -> Dict[str, Any]:
        """
        Get latency statistics.

        Returns:
            Dictionary containing latency statistics
        """
```

### TimingMetrics

Timing metrics container.

```python
@dataclass
class TimingMetrics:
    """Timing metrics for scheduling operations."""
    scheduled_time_ns: int
    actual_time_ns: int
    jitter_ns: int
    overshot_ns: int
    sequence_number: int
```

## Stability and Monitoring

### StabilityValidator

Validates system stability during tests.

```python
class StabilityValidator:
    """Validates system stability during extended tests."""

    def __init__(self, thresholds: Optional[StabilityThresholds] = None):
        """
        Initialize stability validator.

        Args:
            thresholds: Stability thresholds
        """

    def record_metrics(self, timestamp: float, metrics: Dict[str, Any]):
        """
        Record metrics for stability analysis.

        Args:
            timestamp: Measurement timestamp
            metrics: Performance metrics
        """

    def validate_stability(self) -> StabilityStatus:
        """
        Validate current stability state.

        Returns:
            StabilityStatus with validation results
        """
```

### MemoryLeakDetector

Detects memory leaks during testing.

```python
class MemoryLeakDetector:
    """Detects memory leaks during extended testing."""

    def __init__(self):
        """Initialize memory leak detector."""

    def record_memory_usage(self, timestamp: float, memory_mb: float):
        """
        Record memory usage measurement.

        Args:
            timestamp: Measurement timestamp
            memory_mb: Memory usage in MB
        """

    def analyze_memory_trend(self) -> Dict[str, Any]:
        """
        Analyze memory usage trends for leaks.

        Returns:
            Dictionary containing leak analysis results
        """
```

### PerformanceMonitor

Monitors system performance during tests.

```python
class PerformanceMonitor:
    """Monitors system performance during tests."""

    def __init__(self, sampling_interval: float = 0.1):
        """
        Initialize performance monitor.

        Args:
            sampling_interval: Sampling interval in seconds
        """

    async def start_monitoring(self):
        """Start performance monitoring."""

    async def stop_monitoring(self):
        """Stop performance monitoring."""

    def get_resource_stats(self) -> Dict[str, Any]:
        """
        Get current resource statistics.

        Returns:
            Dictionary containing resource statistics
        """

    def get_summary(self) -> Dict[str, Any]:
        """
        Get monitoring summary.

        Returns:
            Dictionary containing monitoring summary
        """
```

## Regression Detection

### PerformanceRegressionDetector

Detects performance regressions by comparing against baselines.

```python
class PerformanceRegressionDetector:
    """Detects performance regressions by comparing against baselines."""

    def __init__(self, baseline_dir: str = "performance-baselines",
                 thresholds: Optional[RegressionThresholds] = None):
        """
        Initialize regression detector.

        Args:
            baseline_dir: Directory containing baseline files
            thresholds: Regression detection thresholds
        """

    def detect_regression(self, current_results: Dict[str, Any]) -> RegressionResult:
        """
        Detect performance regression by comparing current results against baseline.

        Args:
            current_results: Current test results

        Returns:
            RegressionResult with detection results
        """

    def create_baseline_from_results(self, results: Dict[str, Any],
                                   metadata: Optional[Dict[str, Any]] = None) -> PerformanceBaseline:
        """
        Create a baseline from test results.

        Args:
            results: Test results to create baseline from
            metadata: Additional metadata for the baseline

        Returns:
            PerformanceBaseline that was created
        """

    def get_performance_trend(self, metric: str, days: int = 30) -> Dict[str, Any]:
        """
        Analyze performance trends over time.

        Args:
            metric: Metric to analyze
            days: Number of days to analyze

        Returns:
            Dictionary containing trend analysis results
        """
```

### PerformanceBaseline

Performance baseline data.

```python
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
```

### RegressionResult

Regression detection result.

```python
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
```

### RegressionThresholds

Thresholds for regression detection.

```python
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
```

## Utility Classes

### StabilityStatus

Stability validation status.

```python
@dataclass
class StabilityStatus:
    """Stability validation status."""
    is_stable: bool
    issues: List[str]
    metrics: StabilityMetrics
    timestamp: float
```

### StabilityMetrics

Stability metrics container.

```python
@dataclass
class StabilityMetrics:
    """Stability metrics."""
    cpu_trend: float
    memory_trend: float
    throughput_trend: float
    latency_trend: float
    error_rate_trend: float
    duration_seconds: float
    sample_count: int
```

### StabilityThresholds

Stability validation thresholds.

```python
@dataclass
class StabilityThresholds:
    """Thresholds for stability validation."""
    max_cpu_increase_mb_per_hour: float = 10.0
    max_memory_increase_mb_per_hour: float = 50.0
    max_throughput_degradation_percent_per_hour: float = 5.0
    max_latency_increase_percent_per_hour: float = 10.0
    max_error_rate_percent: float = 1.0
    min_samples: int = 10
```

## Exceptions

### SequenceError

Raised when sequence validation fails.

```python
class SequenceError(Exception):
    """Exception raised for sequence validation errors."""
    pass
```

### StabilityError

Raised when stability validation fails.

```python
class StabilityError(Exception):
    """Exception raised for stability validation errors."""
    pass
```

### PerformanceTestError

Raised when performance test execution fails.

```python
class PerformanceTestError(Exception):
    """Exception raised for performance test execution errors."""
    pass
```

## Module Structure

```
tests/performance/
├── __init__.py                 # Main exports
├── README.md                   # Documentation
├── USAGE.md                    # Usage guide
├── API.md                      # API reference
├── regression.py               # Regression detection
├── utils.py                    # Utility classes
├── core/                       # Core framework
│   ├── __init__.py
│   ├── framework.py           # Main framework
│   ├── coordinator.py         # Test coordination
│   ├── sequencing.py          # Sequence tracking
│   ├── timing.py              # Timing and scheduling
│   └── stability.py           # Stability validation
└── run_performance_test.py     # CLI entry point
```

## Usage Patterns

### Basic Test Execution

```python
from tests.performance import PerformanceTestFramework, PerformanceTestConfig

config = PerformanceTestConfig(
    message_size_bytes=20971520,
    frequency_hz=40,
    subscriber_count=6,
    test_duration_seconds=60
)

framework = PerformanceTestFramework(config)
metrics = await framework.run_test()
```

### Regression Detection

```python
from tests.performance.regression import PerformanceRegressionDetector

detector = PerformanceRegressionDetector()
regression_result = detector.detect_regression(current_results)

if regression_result.regression_detected:
    print(f"Regression detected: {regression_result.recommendation}")
```

### Custom Monitoring

```python
from tests.performance.utils import PerformanceMonitor

monitor = PerformanceMonitor(sampling_interval=0.05)
await monitor.start_monitoring()

# Run test...

stats = monitor.get_resource_stats()
await monitor.stop_monitoring()
```

This API reference provides comprehensive documentation for all classes and methods in the performance testing framework.