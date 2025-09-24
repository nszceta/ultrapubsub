"""
Test coordinator for managing performance test processes.

This module provides the test coordinator responsible for managing one producer
and six subscriber processes, including synchronization and lifecycle management.
"""

import asyncio
import multiprocessing
import signal
import time
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable
from enum import Enum
import json
import os
from pathlib import Path

from ..utils import PerformanceTestConfig, PayloadGenerator, PayloadValidator, PerformanceMonitor
from .sequencing import MessageOrderValidator
from .timing import FrequencyScheduler, LatencyTracker, HighResolutionTimer
from .stability import StabilityValidator, MemoryLeakDetector

logger = logging.getLogger(__name__)


class ProcessState(Enum):
    """Process states for lifecycle management."""
    INITIALIZING = "initializing"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TERMINATED = "terminated"


@dataclass
class ProcessInfo:
    """Information about a test process."""
    process_id: int
    process_type: str  # "producer" or "subscriber"
    state: ProcessState
    start_time: float
    end_time: Optional[float] = None
    exit_code: Optional[int] = None
    error_message: Optional[str] = None
    stats: Dict[str, Any] = field(default_factory=dict)


class TestCoordinator:
    """Coordinates performance test execution across multiple processes."""

    def __init__(self, config: PerformanceTestConfig):
        self.config = config
        self.processes: Dict[int, ProcessInfo] = {}
        self.running = False
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None

        # Synchronization primitives
        self.start_event = multiprocessing.Event()
        self.stop_event = multiprocessing.Event()
        self.ready_count = multiprocessing.Value('i', 0)
        self.completed_count = multiprocessing.Value('i', 0)

        # Communication queues
        self.command_queue = multiprocessing.Queue()
        self.result_queue = multiprocessing.Queue()

        # Payload generation
        self.payload_generator = PayloadGenerator(
            payload_size=config.message_size_bytes,
            include_timestamp=config.include_timestamp,
            include_checksum=config.include_checksum
        )

        # Performance monitoring
        self.monitor = PerformanceMonitor(sampling_interval=config.sampling_interval_ms / 1000.0)

        # Message ordering validation
        self.message_validator = MessageOrderValidator(self.payload_generator, config.subscriber_count)

        # Timing infrastructure
        self.timer = HighResolutionTimer()
        self.frequency_scheduler = FrequencyScheduler(config.frequency_hz, self.timer)
        self.latency_tracker = LatencyTracker()

        # Stability validation
        self.stability_validator = StabilityValidator()
        self.memory_leak_detector = MemoryLeakDetector()

        # Test results
        self.test_results: Dict[str, Any] = field(default_factory=dict)

    async def start_test(self) -> Dict[str, Any]:
        """Start the performance test."""
        logger.info("Starting performance test coordination")
        self.running = True
        self.start_time = time.time()

        try:
            # Start monitoring
            await self.monitor.start_monitoring()

            # Start timing scheduler
            self.frequency_scheduler.start()

            # Start stability monitoring
            self.stability_validator.start_monitoring()
            self.memory_leak_detector.start_monitoring()

            # Launch producer process
            producer_process = await self._launch_producer()

            # Launch subscriber processes
            subscriber_processes = []
            for i in range(self.config.subscriber_count):
                subscriber = await self._launch_subscriber(i)
                subscriber_processes.append(subscriber)

            # Wait for all processes to be ready
            await self._wait_for_ready_state()

            # Start the test
            self.start_event.set()

            # Monitor test execution
            await self._monitor_test_execution()

            # Wait for completion or timeout
            await self._wait_for_completion()

            # Set end time before collecting results
            self.end_time = time.time()

            # Collect results
            results = await self._collect_results()

            # Add validation results
            results["validation_results"] = self.message_validator.get_validation_summary()

            # Add timing results
            results["timing_results"] = {
                "scheduler_stats": self.frequency_scheduler.get_timing_stats(),
                "latency_stats": self.latency_tracker.get_latency_stats(),
                "latency_by_subscriber": self.latency_tracker.get_latency_by_subscriber()
            }

            # Stop timing scheduler
            self.frequency_scheduler.stop()

            # Stop stability monitoring
            self.stability_validator.stop_monitoring()
            self.memory_leak_detector.stop_monitoring()

            # Add stability results
            results["stability_results"] = {
                "stability_summary": self.stability_validator.get_stability_summary(),
                "memory_leak_analysis": self.memory_leak_detector.analyze_memory_trend()
            }

            return results

        except Exception as e:
            logger.error(f"Test coordination failed: {e}")
            await self._emergency_shutdown()
            raise
        finally:
            self.running = False
            self.end_time = time.time()
            await self.monitor.stop_monitoring()
            if hasattr(self, 'frequency_scheduler'):
                self.frequency_scheduler.stop()

    async def _launch_producer(self) -> multiprocessing.Process:
        """Launch the producer process."""
        logger.info("Launching producer process")

        process = multiprocessing.Process(
            target=self._producer_process_main,
            args=(self.config, self.start_event, self.stop_event,
                  self.ready_count, self.completed_count, self.result_queue)
        )

        process.start()
        self.processes[process.pid] = ProcessInfo(
            process_id=process.pid,
            process_type="producer",
            state=ProcessState.INITIALIZING,
            start_time=time.time()
        )

        return process

    async def _launch_subscriber(self, subscriber_id: int) -> multiprocessing.Process:
        """Launch a subscriber process."""
        logger.info(f"Launching subscriber process {subscriber_id}")

        process = multiprocessing.Process(
            target=self._subscriber_process_main,
            args=(subscriber_id, self.config, self.start_event, self.stop_event,
                  self.ready_count, self.completed_count, self.result_queue)
        )

        process.start()
        self.processes[process.pid] = ProcessInfo(
            process_id=process.pid,
            process_type="subscriber",
            state=ProcessState.INITIALIZING,
            start_time=time.time()
        )

        return process

    async def _wait_for_ready_state(self):
        """Wait for all processes to be ready."""
        logger.info("Waiting for processes to be ready")

        timeout = self.config.sync_timeout_seconds
        start_time = time.time()

        while self.ready_count.value < (self.config.subscriber_count + 1):
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Timeout waiting for processes to be ready. {self.ready_count.value}/{self.config.subscriber_count + 1} ready")
            await asyncio.sleep(0.1)

        logger.info("All processes are ready")

    async def _monitor_test_execution(self):
        """Monitor test execution and handle events."""
        logger.info("Monitoring test execution")

        while self.running and not self.stop_event.is_set():
            try:
                # Check for results from processes
                try:
                    result = self.result_queue.get_nowait()
                    logger.debug(f"Got result from queue: {result}")
                    await self._handle_process_result(result)
                except Exception as e:
                    # Queue is empty, continue
                    pass

                # Check process status
                await self._check_process_status()

                # Check for test duration timeout
                if self.start_time and time.time() - self.start_time > self.config.test_duration_seconds:
                    logger.info("Test duration reached, stopping test")
                    self.stop_event.set()
                    break

                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error in test monitoring: {e}")

    async def _wait_for_completion(self):
        """Wait for all processes to complete."""
        logger.info("Waiting for processes to complete")

        timeout = 30  # 30 seconds grace period
        start_time = time.time()

        while self.completed_count.value < (self.config.subscriber_count + 1):
            if time.time() - start_time > timeout:
                logger.warning("Timeout waiting for graceful completion, forcing shutdown")
                break

            # Process any remaining messages in the queue
            try:
                result = self.result_queue.get_nowait()
                logger.debug(f"Got result from queue during completion: {result}")
                await self._handle_process_result(result)
            except Exception as e:
                # Queue is empty, continue waiting
                pass

            await asyncio.sleep(0.1)

        # Process any final messages before shutdown
        try:
            while True:
                result = self.result_queue.get_nowait()
                logger.debug(f"Got final result from queue: {result}")
                await self._handle_process_result(result)
        except Exception as e:
            # Queue is empty
            pass

        # Ensure all processes are terminated
        await self._shutdown_processes()

    async def _collect_results(self) -> Dict[str, Any]:
        """Collect and aggregate test results."""
        logger.info("Collecting test results")
        logger.info(f"Processes in dictionary: {list(self.processes.keys())}")

        # Get monitoring summary
        monitoring_summary = self.monitor.get_summary()

        # Build process info dictionary
        process_info_dict = {}
        for pid, info in self.processes.items():
            logger.info(f"Process {pid}: {info.process_type}, stats: {info.stats}")
            process_info_dict[pid] = {
                "process_type": info.process_type,
                "state": info.state.value,
                "start_time": info.start_time,
                "end_time": info.end_time,
                "exit_code": info.exit_code,
                "error_message": info.error_message,
                "stats": info.stats
            }

        results = {
            "test_info": {
                "start_time": self.start_time,
                "end_time": self.end_time,
                "duration_seconds": self.end_time - self.start_time if self.end_time else 0,
                "config": self.config.to_dict()
            },
            "process_info": process_info_dict,
            "performance_metrics": monitoring_summary,
            "test_results": self.test_results
        }

        return results

    async def _handle_process_result(self, result: Dict[str, Any]):
        """Handle a result message from a process."""
        process_id = result.get("process_id")
        result_type = result.get("type")

        if result_type == "ready":
            self.processes[process_id].state = ProcessState.READY
            logger.debug(f"Process {process_id} is ready")

        elif result_type == "stats":
            self.processes[process_id].stats.update(result.get("stats", {}))

        elif result_type == "error":
            error_msg = result.get("error", "Unknown error")
            self.processes[process_id].error_message = error_msg
            self.monitor.record_error("process_error", error_msg, process_id)

        elif result_type == "completed":
            self.processes[process_id].state = ProcessState.COMPLETED
            self.processes[process_id].end_time = time.time()
            # Store the final stats from the completed message
            final_stats = result.get("stats", {})
            if final_stats:
                self.processes[process_id].stats.update(final_stats)

    async def _check_process_status(self):
        """Check the status of all processes."""
        for pid, process_info in self.processes.items():
            if process_info.state in [ProcessState.RUNNING, ProcessState.READY]:
                # Check if process is still alive
                try:
                    process = multiprocessing.Process(pid=pid)
                    if not process.is_alive():
                        process_info.state = ProcessState.TERMINATED
                        process_info.end_time = time.time()
                        logger.warning(f"Process {pid} terminated unexpectedly")
                except:
                    pass

    async def _shutdown_processes(self):
        """Gracefully shutdown all processes."""
        logger.info("Shutting down processes")

        self.stop_event.set()

        # Give processes time to shutdown gracefully
        await asyncio.sleep(2.0)

        # Force terminate any remaining processes
        for pid, process_info in self.processes.items():
            if process_info.state not in [ProcessState.COMPLETED, ProcessState.TERMINATED]:
                try:
                    process = multiprocessing.Process(pid=pid)
                    process.terminate()
                    process_info.state = ProcessState.TERMINATED
                    process_info.end_time = time.time()
                except:
                    pass

        # Join all processes
        for pid, process_info in self.processes.items():
            try:
                process = multiprocessing.Process(pid=pid)
                process.join(timeout=5.0)
            except:
                pass

    async def _emergency_shutdown(self):
        """Emergency shutdown in case of errors."""
        logger.error("Performing emergency shutdown")

        self.stop_event.set()

        for pid in list(self.processes.keys()):
            try:
                process = multiprocessing.Process(pid=pid)
                process.kill()
                self.processes[pid].state = ProcessState.TERMINATED
                self.processes[pid].end_time = time.time()
            except:
                pass

    def _producer_process_main(self, config: PerformanceTestConfig, start_event: multiprocessing.Event,
                              stop_event: multiprocessing.Event, ready_count: multiprocessing.Value,
                              completed_count: multiprocessing.Value, result_queue: multiprocessing.Queue):
        """Main function for producer process."""
        try:
            # Signal ready
            ready_count.value += 1
            result_queue.put({
                "process_id": os.getpid(),
                "type": "ready"
            })

            # Wait for start signal
            start_event.wait()

            # Run producer logic
            stats = self._run_producer_logic(config, stop_event, result_queue)

            # Signal completion
            completed_count.value += 1
            result_queue.put({
                "process_id": os.getpid(),
                "type": "completed",
                "stats": stats
            })

        except Exception as e:
            result_queue.put({
                "process_id": os.getpid(),
                "type": "error",
                "error": str(e)
            })

    def _subscriber_process_main(self, subscriber_id: int, config: PerformanceTestConfig,
                                 start_event: multiprocessing.Event, stop_event: multiprocessing.Event,
                                 ready_count: multiprocessing.Value, completed_count: multiprocessing.Value,
                                 result_queue: multiprocessing.Queue):
        """Main function for subscriber process."""
        try:
            # Signal ready
            ready_count.value += 1
            result_queue.put({
                "process_id": os.getpid(),
                "type": "ready"
            })

            # Wait for start signal
            start_event.wait()

            # Run subscriber logic
            stats = self._run_subscriber_logic(subscriber_id, config, stop_event, result_queue)

            # Signal completion
            completed_count.value += 1
            result_queue.put({
                "process_id": os.getpid(),
                "type": "completed",
                "stats": stats
            })

        except Exception as e:
            result_queue.put({
                "process_id": os.getpid(),
                "type": "error",
                "error": str(e)
            })

    def _run_producer_logic(self, config: PerformanceTestConfig, stop_event: multiprocessing.Event,
                           result_queue: multiprocessing.Queue) -> Dict[str, Any]:
        """Run the producer logic with precise timing."""
        logger.info("Producer logic starting")

        # Import the real UltraPubSub API
        import sys
        sys.path.append('/home/adam/Documents/src/ultrapubsub/python')
        from ultrapubsub import SharedMemory

        # Initialize timing infrastructure
        timer = HighResolutionTimer()
        frequency_scheduler = FrequencyScheduler(config.frequency_hz, timer)
        payload_generator = PayloadGenerator(
            payload_size=config.message_size_bytes,
            include_timestamp=config.include_timestamp,
            include_checksum=config.include_checksum
        )

        # Create shared memory and publisher
        shm = SharedMemory("performance_test")
        publisher = shm.create_publisher()
        logger.info("Successfully created shared memory and publisher")

        message_count = 0
        start_time = time.time()
        total_bytes_sent = 0

        frequency_scheduler.start()

        try:
            logger.info("Producer main loop starting")
            while not stop_event.is_set() and (time.time() - start_time) < config.test_duration_seconds:
                # Wait for next scheduling slot
                timing_metrics = frequency_scheduler.wait_for_next_slot()
                if timing_metrics is None:
                    logger.info("Producer received None from scheduler, breaking loop")
                    break

                # Generate payload
                payload, metadata = payload_generator.generate_payload(message_count)

                # Record send time for latency tracking
                send_time = time.time()

                # Actually send the message via UltraPubSub IPC
                if publisher:
                    publisher.publish(payload)
                # If no publisher, just simulate (for testing)
                else:
                    pass

                message_count += 1
                total_bytes_sent += len(payload)

                # Record timing metrics (would normally be sent to monitoring system)
                if message_count % 100 == 0:  # Log every 100 messages
                    logger.debug(f"Sent message {message_count}, jitter: {timing_metrics.jitter_us}us")

                # Send stats to result queue periodically
                if message_count % 1000 == 0:
                    result_queue.put({
                        "process_id": os.getpid(),
                        "type": "stats",
                        "stats": {
                            "messages_sent": message_count,
                            "bytes_sent": total_bytes_sent,
                            "current_jitter_us": timing_metrics.jitter_us,
                            "timing_accuracy": frequency_scheduler.get_timing_stats()
                        }
                    })

            logger.info(f"Producer loop completed. Messages sent: {message_count}, Duration: {time.time() - start_time:.2f}s")

        finally:
            frequency_scheduler.stop()

        # Send final stats to result queue
        final_stats = {
            "messages_sent": message_count,
            "bytes_sent": total_bytes_sent,
            "duration_seconds": time.time() - start_time,
            "timing_stats": frequency_scheduler.get_timing_stats(),
            "avg_frequency_hz": message_count / (time.time() - start_time) if (time.time() - start_time) > 0 else 0
        }

        logger.info(f"Producer sending final stats: {final_stats}")
        result_queue.put({
            "process_id": os.getpid(),
            "type": "stats",
            "stats": final_stats
        })

        return final_stats

    def _run_subscriber_logic(self, subscriber_id: int, config: PerformanceTestConfig,
                             stop_event: multiprocessing.Event, result_queue: multiprocessing.Queue) -> Dict[str, Any]:
        """Run the subscriber logic using real UltraPubSub IPC."""
        logger.info(f"Subscriber {subscriber_id} logic starting")

        # Import the real UltraPubSub API
        import sys
        sys.path.append('/home/adam/Documents/src/ultrapubsub/python')
        from ultrapubsub import SharedMemory

        # Attach to existing shared memory and create subscriber
        shm = SharedMemory.attach("performance_test")
        subscriber = shm.create_subscriber()
        logger.info("Successfully attached to shared memory and created subscriber")

        message_count = 0
        start_time = time.time()

        while not stop_event.is_set() and (time.time() - start_time) < config.test_duration_seconds:
            # Actually receive messages via UltraPubSub IPC
            if subscriber:
                try:
                    message = subscriber.receive(timeout=0.1)  # 100ms timeout
                    if message:
                        message_count += 1
                except Exception:
                    # Continue on timeout or error
                    pass
            else:
                # Simulate receiving messages
                time.sleep(0.01)  # Simulate processing time
                message_count += 1

        # Send final stats to result queue
        final_stats = {
            "subscriber_id": subscriber_id,
            "messages_received": message_count,
            "duration_seconds": time.time() - start_time
        }

        result_queue.put({
            "process_id": os.getpid(),
            "type": "stats",
            "stats": final_stats
        })

        return final_stats