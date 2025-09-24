"""
Configuration management for performance tests.

This module provides comprehensive configuration management for performance tests,
including validation, defaults, and environment variable support.
"""

import os
import json
import logging
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class PerformanceTestConfig:
    """Comprehensive configuration for performance tests."""

    # Core test parameters
    message_size_bytes: int = 20 * 1024 * 1024  # 20MB
    frequency_hz: int = 40  # 40Hz
    subscriber_count: int = 6
    test_duration_seconds: int = 30
    warmup_duration_seconds: int = 5

    # Payload configuration
    enable_payload_verification: bool = True
    payload_pattern: str = "sequential"  # sequential, random, zeros
    include_timestamp: bool = True
    include_checksum: bool = True

    # Performance monitoring
    enable_latency_measurement: bool = True
    enable_resource_monitoring: bool = True
    enable_throughput_measurement: bool = True
    sampling_interval_ms: int = 100

    # Test orchestration
    producer_process_count: int = 1
    subscriber_process_count: int = 6
    coordinator_enabled: bool = True
    sync_timeout_seconds: int = 30

    # Output and reporting
    output_dir: str = "tests/performance/reports"
    report_format: str = "json"  # json, csv, both
    enable_realtime_reporting: bool = True
    report_compression: bool = False

    # Error handling
    max_errors_allowed: int = 10
    fail_fast: bool = True
    enable_retry: bool = False
    max_retries: int = 3

    # Resource limits
    max_memory_mb: int = 8192  # 8GB
    max_cpu_percent: int = 90
    max_disk_usage_percent: int = 80

    # Validation thresholds
    min_throughput_mbps: float = 700.0  # Minimum acceptable throughput
    max_latency_ms: float = 100.0  # Maximum acceptable latency
    max_jitter_ms: float = 10.0  # Maximum acceptable timing jitter
    max_error_rate_percent: float = 1.0  # Maximum acceptable error rate

    def __post_init__(self):
        """Validate configuration after initialization."""
        self._validate_config()

    def _validate_config(self):
        """Validate configuration parameters."""
        errors = []

        # Validate core parameters
        if self.message_size_bytes <= 0:
            errors.append("message_size_bytes must be positive")

        if self.message_size_bytes > 100 * 1024 * 1024:  # 100MB limit
            errors.append("message_size_bytes cannot exceed 100MB")

        if self.frequency_hz <= 0 or self.frequency_hz > 1000:
            errors.append("frequency_hz must be between 1 and 1000")

        if self.subscriber_count < 1 or self.subscriber_count > 20:
            errors.append("subscriber_count must be between 1 and 20")

        if self.test_duration_seconds <= 0:
            errors.append("test_duration_seconds must be positive")

        if self.warmup_duration_seconds < 0:
            errors.append("warmup_duration_seconds must be non-negative")

        if self.warmup_duration_seconds >= self.test_duration_seconds:
            errors.append("warmup_duration_seconds must be less than test_duration_seconds")

        # Validate resource limits
        if self.max_memory_mb <= 0:
            errors.append("max_memory_mb must be positive")

        if self.max_cpu_percent <= 0 or self.max_cpu_percent > 100:
            errors.append("max_cpu_percent must be between 1 and 100")

        # Validate thresholds
        if self.min_throughput_mbps <= 0:
            errors.append("min_throughput_mbps must be positive")

        if self.max_latency_ms <= 0:
            errors.append("max_latency_ms must be positive")

        if self.max_error_rate_percent < 0 or self.max_error_rate_percent > 100:
            errors.append("max_error_rate_percent must be between 0 and 100")

        if errors:
            raise ValueError(f"Configuration validation failed: {'; '.join(errors)}")

    @classmethod
    def from_env(cls) -> 'PerformanceTestConfig':
        """Create configuration from environment variables."""
        config = cls()

        # Map environment variables to config attributes
        env_mappings = {
            'PERF_MESSAGE_SIZE': 'message_size_bytes',
            'PERF_FREQUENCY': 'frequency_hz',
            'PERF_SUBSCRIBERS': 'subscriber_count',
            'PERF_DURATION': 'test_duration_seconds',
            'PERF_WARMUP': 'warmup_duration_seconds',
            'PERF_OUTPUT_DIR': 'output_dir',
            'PERF_MAX_ERRORS': 'max_errors_allowed',
            'PERF_FAIL_FAST': 'fail_fast',
            'PERF_MIN_THROUGHPUT': 'min_throughput_mbps',
            'PERF_MAX_LATENCY': 'max_latency_ms',
            'PERF_MAX_ERROR_RATE': 'max_error_rate_percent'
        }

        for env_var, config_attr in env_mappings.items():
            value = os.getenv(env_var)
            if value is not None:
                try:
                    # Convert string to appropriate type
                    if config_attr in ['message_size_bytes', 'frequency_hz', 'subscriber_count',
                                     'test_duration_seconds', 'warmup_duration_seconds',
                                     'max_errors_allowed', 'max_memory_mb', 'max_cpu_percent',
                                     'sampling_interval_ms']:
                        setattr(config, config_attr, int(value))
                    elif config_attr in ['min_throughput_mbps', 'max_latency_ms',
                                       'max_jitter_ms', 'max_error_rate_percent']:
                        setattr(config, config_attr, float(value))
                    elif config_attr in ['fail_fast', 'enable_payload_verification',
                                       'enable_latency_measurement', 'enable_resource_monitoring',
                                       'enable_throughput_measurement', 'coordinator_enabled',
                                       'enable_realtime_reporting', 'report_compression',
                                       'enable_retry']:
                        setattr(config, config_attr, value.lower() in ['true', '1', 'yes'])
                    else:
                        setattr(config, config_attr, value)
                except (ValueError, AttributeError) as e:
                    logger.warning(f"Failed to parse {env_var}={value}: {e}")

        return config

    @classmethod
    def from_file(cls, config_path: str) -> 'PerformanceTestConfig':
        """Load configuration from JSON file."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(path, 'r') as f:
            data = json.load(f)

        return cls(**data)

    def to_file(self, config_path: str):
        """Save configuration to JSON file."""
        path = Path(config_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, 'w') as f:
            json.dump(asdict(self), f, indent=2)

        logger.info(f"Configuration saved to {config_path}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return asdict(self)

    def get_target_throughput_mbps(self) -> float:
        """Calculate target throughput in MB/s."""
        return (self.message_size_bytes * self.frequency_hz) / (1024 * 1024)

    def get_target_total_messages(self) -> int:
        """Calculate target total message count."""
        return self.frequency_hz * self.test_duration_seconds

    def get_warmup_message_count(self) -> int:
        """Calculate warmup message count."""
        return self.frequency_hz * self.warmup_duration_seconds

    def __str__(self) -> str:
        """String representation of configuration."""
        return (f"PerformanceTestConfig("
                f"message_size={self.message_size_bytes/1024/1024:.1f}MB, "
                f"frequency={self.frequency_hz}Hz, "
                f"subscribers={self.subscriber_count}, "
                f"duration={self.test_duration_seconds}s, "
                f"target_throughput={self.get_target_throughput_mbps():.1f}MB/s)")


class ConfigManager:
    """Configuration manager for performance tests."""

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path
        self._config: Optional[PerformanceTestConfig] = None

    def load_config(self, config_path: Optional[str] = None) -> PerformanceTestConfig:
        """Load configuration from file or environment."""
        path = config_path or self.config_path

        if path and Path(path).exists():
            logger.info(f"Loading configuration from {path}")
            self._config = PerformanceTestConfig.from_file(path)
        else:
            logger.info("Loading configuration from environment variables")
            self._config = PerformanceTestConfig.from_env()

        logger.info(f"Configuration loaded: {self._config}")
        return self._config

    def get_config(self) -> PerformanceTestConfig:
        """Get current configuration."""
        if self._config is None:
            self._config = self.load_config()
        return self._config

    def save_config(self, config: PerformanceTestConfig, config_path: Optional[str] = None):
        """Save configuration to file."""
        path = config_path or self.config_path
        if path:
            config.to_file(path)

    def create_default_config(self, config_path: str) -> PerformanceTestConfig:
        """Create and save default configuration."""
        config = PerformanceTestConfig()
        self.save_config(config, config_path)
        return config

    def validate_config(self, config: PerformanceTestConfig) -> bool:
        """Validate configuration parameters."""
        try:
            config._validate_config()
            return True
        except ValueError as e:
            logger.error(f"Configuration validation failed: {e}")
            return False