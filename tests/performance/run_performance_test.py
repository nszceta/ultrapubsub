#!/usr/bin/env python3
"""
Performance test runner script.

This script runs the complete performance test suite and generates reports.
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from tests.performance.core import PerformanceTestFramework
from tests.performance.utils import ConfigManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run UltraPubSub performance tests')

    parser.add_argument('--config', '-c', type=str,
                       help='Configuration file path (JSON)')
    parser.add_argument('--message-size', type=int,
                       help='Message size in bytes (overrides config)')
    parser.add_argument('--frequency', type=int,
                       help='Message frequency in Hz (overrides config)')
    parser.add_argument('--subscribers', type=int,
                       help='Number of subscribers (overrides config)')
    parser.add_argument('--duration', type=int,
                       help='Test duration in seconds (overrides config)')
    parser.add_argument('--warmup', type=int,
                       help='Warmup duration in seconds (overrides config)')
    parser.add_argument('--output-dir', type=str,
                       help='Output directory for reports (overrides config)')
    parser.add_argument('--save-config', type=str,
                       help='Save configuration to file and exit')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')

    return parser.parse_args()


async def main():
    """Main entry point."""
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load configuration
    config_manager = ConfigManager(args.config)
    try:
        config = config_manager.load_config()
    except FileNotFoundError:
        logger.info("No configuration file found, using defaults")
        config = config_manager.get_config()

    # Override with command line arguments
    if args.message_size is not None:
        config.message_size_bytes = args.message_size
    if args.frequency is not None:
        config.frequency_hz = args.frequency
    if args.subscribers is not None:
        config.subscriber_count = args.subscribers
    if args.duration is not None:
        config.test_duration_seconds = args.duration
    if args.warmup is not None:
        config.warmup_duration_seconds = args.warmup
    if args.output_dir is not None:
        config.output_dir = args.output_dir

    # Save configuration if requested
    if args.save_config:
        config_manager.save_config(config, args.save_config)
        logger.info(f"Configuration saved to {args.save_config}")
        return

    logger.info("Starting UltraPubSub performance test")
    logger.info(f"Configuration: {config}")

    # Create and run test
    framework = PerformanceTestFramework(config)

    try:
        metrics = await framework.run_test()

        # Print summary
        print("\n" + "="*60)
        print("PERFORMANCE TEST SUMMARY")
        print("="*60)
        print(f"Messages sent: {metrics.messages_sent}")
        print(f"Throughput: {metrics.throughput_mbps:.2f} MB/s")
        print(f"Target throughput: {config.get_target_throughput_mbps():.2f} MB/s")
        print(f"Test duration: {metrics.end_time - metrics.start_time:.2f}s")
        print(f"Errors: {len(metrics.errors)}")

        if metrics.errors:
            print("\nErrors encountered:")
            for error in metrics.errors:
                print(f"  - {error}")

        # Validate against thresholds
        print("\nValidation Results:")
        print(f"Throughput requirement: {'PASS' if metrics.throughput_mbps >= config.min_throughput_mbps else 'FAIL'} "
              f"({metrics.throughput_mbps:.1f} >= {config.min_throughput_mbps:.1f} MB/s)")

        if metrics.latency_samples:
            avg_latency = sum(metrics.latency_samples) / len(metrics.latency_samples)
            print(f"Latency requirement: {'PASS' if avg_latency <= config.max_latency_ms else 'FAIL'} "
                  f"({avg_latency:.1f} <= {config.max_latency_ms:.1f} ms)")

        print("="*60)

        # Exit with appropriate code
        success = (not metrics.errors and
                  metrics.throughput_mbps >= config.min_throughput_mbps and
                  (not metrics.latency_samples or
                   sum(metrics.latency_samples) / len(metrics.latency_samples) <= config.max_latency_ms))

        sys.exit(0 if success else 1)

    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        framework.stop_test()
        sys.exit(1)
    except Exception as e:
        logger.error(f"Test failed with exception: {e}")
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())