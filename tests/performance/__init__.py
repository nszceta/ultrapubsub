"""
Performance testing suite for UltraPubSub.

This package provides comprehensive performance testing capabilities to validate
the Core IPC design requirements.
"""

from .core import PerformanceTestFramework, PerformanceMetrics
from .utils import PerformanceTestConfig

__all__ = ['PerformanceTestFramework', 'PerformanceMetrics', 'PerformanceTestConfig']