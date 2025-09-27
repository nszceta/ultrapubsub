// UltraPubSub - High Performance Shared Memory Broadcast Buffer Implementation
//
// This module implements an optimized shared memory broadcast buffer with atomic operations
// for synchronous 1:N messaging, featuring zero-copy, pipelining, and cache optimization.

mod broadcast_buffer;
mod event_loop;
use broadcast_buffer::{SharedBroadcastBuffer, BroadcastPublisher, BroadcastSubscriber};
use event_loop::{PyEventLoop, add_event_loop_to_module};

use pyo3::prelude::*;
use nix::unistd::{fork, ForkResult, Pid};
use nix::sys::wait::{waitpid, WaitStatus};

// Publisher using Shared Memory Broadcast Buffer
pub struct Publisher {
    inner: BroadcastPublisher,
    _buffer: *mut SharedBroadcastBuffer,
    _name: String,
}

impl Publisher {
    /// Create a new publisher with a shared memory broadcast buffer
    pub fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // Create shared memory broadcast buffer
        let buffer = SharedBroadcastBuffer::create(name)?;
        let inner = BroadcastPublisher::new(buffer);

        Ok(Self {
            inner,
            _buffer: buffer,
            _name: name.to_string(),
        })
    }

    /// Broadcast a message to all subscribers synchronously
    pub fn broadcast(&mut self, data: &[u8]) -> Result<u64, Box<dyn std::error::Error>> {
        self.inner.broadcast(data)
    }

    /// Get the number of registered subscribers
    pub fn subscriber_count(&self) -> usize {
        self.inner.subscriber_count()
    }

    /// Wait for all subscribers to be ready
    pub fn wait_for_subscribers(&self, expected_count: u32) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.wait_for_subscribers(expected_count)
    }

    /// Get performance metrics (placeholder for original implementation)
    pub fn get_performance_metrics(&self) -> (u64, u64, u64, u64) {
        (0, 0, 0, 0) // Placeholder - return zeros for original implementation
    }

    /// Register a new subscriber and return the subscriber ID
    pub fn register_subscriber(&mut self) -> usize {
        self.inner.register_subscriber()
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        // Cleanup shared memory when publisher is dropped
        let _ = broadcast_buffer::cleanup_shared_memory(&self._name);
    }
}

// Subscriber using Shared Memory Broadcast Buffer
pub struct Subscriber {
    inner: BroadcastSubscriber,
    _buffer: *mut SharedBroadcastBuffer,
    _name: String,
    _subscriber_id: usize,
}

impl Subscriber {
    /// Create a new subscriber attaching to existing optimized shared memory
    pub fn new(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // For broadcast, use subscriber ID 0 as default
        let subscriber_id = 0;
        Self::with_id(name, subscriber_id)
    }

    /// Create a new subscriber with a specific ID
    pub fn with_id(name: &str, subscriber_id: usize) -> Result<Self, Box<dyn std::error::Error>> {
        // Attach to existing shared memory broadcast buffer
        let buffer = SharedBroadcastBuffer::connect(name)?;
        let inner = BroadcastSubscriber::new(buffer, subscriber_id);

        Ok(Self {
            inner,
            _buffer: buffer,
            _name: name.to_string(),
            _subscriber_id: subscriber_id,
        })
    }

    /// Receive the next available broadcast message (blocking)
    pub fn receive(&mut self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        self.inner.receive()
    }

    /// Try to receive a message without blocking
    pub fn try_receive(&mut self) -> Option<Vec<u8>> {
        if self.inner.has_new_message() {
            self.inner.receive().ok()
        } else {
            None
        }
    }

    /// Check if there are messages available
    pub fn has_messages(&self) -> bool {
        self.inner.has_new_message()
    }

    /// Check if a new message is available
    pub fn has_new_message(&self) -> bool {
        self.inner.has_new_message()
    }

    /// Get the last processed message sequence number
    pub fn last_processed(&self) -> u64 {
        self.inner.last_processed()
    }

    /// Get the subscriber ID
    pub fn subscriber_id(&self) -> usize {
        self._subscriber_id
    }

    // Removed filter methods - not applicable for synchronous broadcast

    /// Register this subscriber with the broadcast system
    pub fn register(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.inner.register()
    }
}

impl Drop for Subscriber {
    fn drop(&mut self) {
        // Subscriber doesn't cleanup shared memory - only publisher does
        // This prevents conflicts in multi-process scenarios
    }
}

// Process management utilities for multi-process scenarios
pub fn create_subscriber_process<F>(_name: &str, mut subscriber_func: F) -> Result<Pid, Box<dyn std::error::Error>>
where
    F: FnMut() + Send + 'static,
{
    match unsafe { fork() } {
        Ok(ForkResult::Parent { child, .. }) => Ok(child),
        Ok(ForkResult::Child) => {
            // Child process runs the subscriber function
            subscriber_func();
            std::process::exit(0);
        }
        Err(e) => Err(format!("Failed to fork process: {}", e).into()),
    }
}

pub fn wait_for_process(pid: Pid) -> Result<WaitStatus, Box<dyn std::error::Error>> {
    waitpid(pid, None).map_err(|e| e.into())
}

// Python bindings using PyO3
#[pyclass(unsendable)]
pub struct PyPublisher {
    inner: Option<Publisher>,
    name: String,
}

#[pymethods]
impl PyPublisher {
    #[new]
    pub fn new(name: String) -> PyResult<Self> {
        Ok(Self {
            inner: None,
            name,
        })
    }

    pub fn initialize(&mut self) -> PyResult<()> {
        match Publisher::new(&self.name) {
            Ok(publisher) => {
                self.inner = Some(publisher);
                Ok(())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())),
        }
    }

    pub fn publish(&mut self, data: Vec<u8>) -> PyResult<u64> {
        match &mut self.inner {
            Some(publisher) => publisher.broadcast(&data)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())),
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Publisher not initialized")),
        }
    }

    pub fn broadcast(&mut self, data: Vec<u8>) -> PyResult<u64> {
        match &mut self.inner {
            Some(publisher) => publisher.broadcast(&data)
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())),
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Publisher not initialized")),
        }
    }

    // Removed try_publish - not applicable for synchronous broadcast
    // Removed publish_batch - broadcast sends single message to all subscribers

    pub fn subscriber_count(&self) -> PyResult<usize> {
        match &self.inner {
            Some(publisher) => Ok(publisher.subscriber_count()),
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Publisher not initialized")),
        }
    }

    pub fn get_performance_metrics(&self) -> PyResult<(u64, u64, u64, u64)> {
        match &self.inner {
            Some(publisher) => Ok(publisher.get_performance_metrics()),
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Publisher not initialized")),
        }
    }

    pub fn register_subscriber(&mut self) -> PyResult<usize> {
        match &mut self.inner {
            Some(publisher) => Ok(publisher.register_subscriber()),
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Publisher not initialized")),
        }
    }

    pub fn cleanup(&mut self) -> PyResult<()> {
        match &mut self.inner {
            Some(_publisher) => {
                // For now, just drop the publisher which will clean up resources
                self.inner = None;
                Ok(())
            }
            None => Ok(()), // Already cleaned up
        }
    }

    // Removed allocate_and_publish - not applicable for synchronous broadcast
    // Removed allocate_pool_slot - not applicable for synchronous broadcast
    // Removed publish_pool_slot - not applicable for synchronous broadcast
}

#[pyclass(unsendable)]
pub struct PySubscriber {
    inner: Option<Subscriber>,
    name: String,
}

#[pymethods]
impl PySubscriber {
    #[new]
    pub fn new(name: String) -> PyResult<Self> {
        Ok(Self {
            inner: None,
            name,
        })
    }

    pub fn initialize(&mut self) -> PyResult<()> {
        match Subscriber::new(&self.name) {
            Ok(subscriber) => {
                self.inner = Some(subscriber);
                Ok(())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())),
        }
    }

    pub fn initialize_with_id(&mut self, subscriber_id: usize) -> PyResult<()> {
        match Subscriber::with_id(&self.name, subscriber_id) {
            Ok(subscriber) => {
                self.inner = Some(subscriber);
                Ok(())
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())),
        }
    }

    pub fn register(&mut self) -> PyResult<()> {
        match &mut self.inner {
            Some(subscriber) => {
                subscriber.register()
                    .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
            }
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Subscriber not initialized")),
        }
    }

    pub fn deregister(&mut self) -> PyResult<()> {
        match &mut self.inner {
            Some(_subscriber) => {
                // Deregistration is handled automatically in Drop
                Ok(())
            }
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Subscriber not initialized")),
        }
    }

    pub fn receive(&mut self) -> PyResult<Vec<u8>> {
        match &mut self.inner {
            Some(subscriber) => subscriber.receive()
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())),
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Subscriber not initialized")),
        }
    }

    pub fn try_receive(&mut self) -> PyResult<Option<Vec<u8>>> {
        match &mut self.inner {
            Some(subscriber) => Ok(if subscriber.has_new_message() { Some(subscriber.receive().unwrap()) } else { None }),
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Subscriber not initialized")),
        }
    }

    pub fn has_messages(&self) -> PyResult<bool> {
        match &self.inner {
            Some(subscriber) => Ok(subscriber.has_new_message()),
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Subscriber not initialized")),
        }
    }

    pub fn last_processed(&self) -> PyResult<u64> {
        match &self.inner {
            Some(subscriber) => Ok(subscriber.last_processed()),
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Subscriber not initialized")),
        }
    }

    pub fn subscriber_id(&self) -> PyResult<usize> {
        match &self.inner {
            Some(subscriber) => Ok(subscriber.subscriber_id()),
            None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Subscriber not initialized")),
        }
    }

    // Removed filter methods - not applicable for synchronous broadcast
}

// Utility functions for Python
#[pyfunction]
pub fn create_subscriber(name: String) -> PyResult<PySubscriber> {
    PySubscriber::new(name)
}

#[pyfunction]
pub fn create_subscriber_with_id(name: String, subscriber_id: usize) -> PyResult<PySubscriber> {
    let mut subscriber = PySubscriber::new(name)?;
    subscriber.initialize_with_id(subscriber_id)?;
    Ok(subscriber)
}

#[pyfunction]
pub fn create_publisher(name: String) -> PyResult<PyPublisher> {
    let mut publisher = PyPublisher::new(name)?;
    publisher.initialize()?;
    Ok(publisher)
}

#[pyfunction]
pub fn cleanup_shared_memory(name: String) -> PyResult<()> {
    // Attempt to clean up shared memory object
    broadcast_buffer::cleanup_shared_memory(&name)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
    Ok(())
}

// Python module definition
#[pymodule]
fn ultrapubsub(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyPublisher>()?;
    m.add_class::<PySubscriber>()?;
    m.add_class::<PyEventLoop>()?;
    m.add_function(wrap_pyfunction!(create_subscriber, m)?)?;
    m.add_function(wrap_pyfunction!(create_subscriber_with_id, m)?)?;
    m.add_function(wrap_pyfunction!(create_publisher, m)?)?;
    m.add_function(wrap_pyfunction!(cleanup_shared_memory, m)?)?;
    add_event_loop_to_module(m)?;
    Ok(())
}

// Performance benchmarking utilities
pub mod benchmark {
    use super::*;
    use std::time::Instant;

    /// Run a benchmark test with specified parameters
    pub fn run_benchmark(
        name: &str,
        message_size: usize,
        message_count: usize,
        subscriber_count: usize,
    ) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
        let mut publisher = Publisher::new(name)?;

        // Create subscribers
        let mut subscribers = Vec::new();
        for i in 0..subscriber_count {
            let subscriber = Subscriber::new(&format!("{}_sub_{}", name, i))?;
            subscribers.push(subscriber);
        }

        // Generate test data
        let test_data: Vec<u8> = (0..message_size)
            .map(|i| (i % 256) as u8)
            .collect();

        // Benchmark publish performance
        let start_time = Instant::now();
        let mut sequences = Vec::new();

        for _ in 0..message_count {
            let seq = publisher.broadcast(&test_data)?;
            sequences.push(seq);
        }

        let publish_duration = start_time.elapsed();
        let publish_throughput = (message_count as f64 * message_size as f64) / publish_duration.as_secs_f64();

        // Give subscribers time to process
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Benchmark receive performance
        let mut receive_results = Vec::new();
        for (i, subscriber) in subscribers.iter_mut().enumerate() {
            let start_time = Instant::now();
            let mut messages_received = 0;

            while messages_received < message_count {
                if let Some(_) = if subscriber.has_new_message() { Some(subscriber.receive().unwrap()) } else { None } {
                    messages_received += 1;
                } else {
                    // Small delay to prevent busy-waiting
                    std::thread::sleep(std::time::Duration::from_micros(10));
                }
            }

            let receive_duration = start_time.elapsed();
            let receive_throughput = (messages_received as f64 * message_size as f64) / receive_duration.as_secs_f64();

            receive_results.push(SubscriberResult {
                subscriber_id: i,
                messages_received,
                receive_duration,
                throughput: receive_throughput,
            });
        }

        Ok(BenchmarkResult {
            name: name.to_string(),
            message_size,
            message_count,
            subscriber_count,
            publish_duration,
            publish_throughput,
            subscriber_results: receive_results,
        })
    }

    pub struct BenchmarkResult {
        pub name: String,
        pub message_size: usize,
        pub message_count: usize,
        pub subscriber_count: usize,
        pub publish_duration: std::time::Duration,
        pub publish_throughput: f64,  // bytes per second
        pub subscriber_results: Vec<SubscriberResult>,
    }

    pub struct SubscriberResult {
        pub subscriber_id: usize,
        pub messages_received: usize,
        pub receive_duration: std::time::Duration,
        pub throughput: f64,  // bytes per second
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Instant;

    #[test]
    fn test_basic_pub_sub() {
        let name = "test_basic_pub_sub";

        // Clean up any existing shared memory
        let _ = broadcast_buffer::cleanup_shared_memory(name);

        // Test with direct SharedBroadcastBuffer
        let buffer = SharedBroadcastBuffer::create(name).expect("Failed to create buffer");
        let mut publisher = BroadcastPublisher::new(buffer);
        let subscriber_id = publisher.register_subscriber();
        let mut subscriber = BroadcastSubscriber::new(buffer, subscriber_id);
        subscriber.register().expect("Failed to register subscriber");

        // Test message
        let test_message = b"Hello, UltraPubSub!";
        let sequence = publisher.broadcast(test_message).expect("Failed to broadcast");
        println!("Published message with sequence: {}", sequence);

        // Receive message
        let received = subscriber.receive().expect("Failed to receive");
        assert_eq!(received, test_message);
        println!("Successfully received message!");

        assert_eq!(subscriber.last_processed(), sequence);

        println!("Test completed successfully!");

        // Cleanup
        let _ = broadcast_buffer::cleanup_shared_memory(name);
    }

    #[test]
    fn test_large_message() {
        let name = "test_large_message";

        // Clean up any existing shared memory
        let _ = broadcast_buffer::cleanup_shared_memory(name);

        let buffer = SharedBroadcastBuffer::create(name).expect("Failed to create buffer");
        let mut publisher = BroadcastPublisher::new(buffer);
        let subscriber_id = publisher.register_subscriber();
        let mut subscriber = BroadcastSubscriber::new(buffer, subscriber_id);
        subscriber.register().expect("Failed to register subscriber");

        // Test with 1MB message (smaller than 35MB but still large)
        let large_message = vec![0xAB; 1024 * 1024];
        let sequence = publisher.broadcast(&large_message).expect("Failed to broadcast large message");

        let received = subscriber.receive().expect("Failed to receive large message");
        assert_eq!(received, large_message);
        assert_eq!(subscriber.last_processed(), sequence);
    }

    #[test]
    fn test_multiple_messages() {
        let name = "test_multiple_messages";

        // Clean up any existing shared memory
        let _ = broadcast_buffer::cleanup_shared_memory(name);

        let buffer = SharedBroadcastBuffer::create(name).expect("Failed to create buffer");
        let mut publisher = BroadcastPublisher::new(buffer);
        let subscriber_id = publisher.register_subscriber();
        let mut subscriber = BroadcastSubscriber::new(buffer, subscriber_id);
        subscriber.register().expect("Failed to register subscriber");

        // Publish multiple messages
        let messages = vec
![b"Message 1".to_vec(), b"Message 2".to_vec(), b"Message 3".to_vec()];
        let mut expected_sequences = Vec::new();

        for message in &messages {
            let seq = publisher.broadcast(message).expect("Failed to publish");
            expected_sequences.push(seq);
        }

        // Receive all messages
        let mut received_messages = Vec::new();
        for _ in 0..messages.len() {
            let msg = subscriber.receive().expect("Failed to receive message");
            received_messages.push(msg);
        }

        assert_eq!(received_messages, messages);
        assert_eq!(subscriber.last_processed(), expected_sequences.last().copied().unwrap_or(0));
    }

    #[test]
    fn test_non_blocking_receive() {
        let name = "test_non_blocking";

        // Clean up any existing shared memory
        let _ = broadcast_buffer::cleanup_shared_memory(name);

        let buffer = SharedBroadcastBuffer::create(name).expect("Failed to create buffer");
        let mut publisher = BroadcastPublisher::new(buffer);
        let subscriber_id = publisher.register_subscriber();
        let mut subscriber = BroadcastSubscriber::new(buffer, subscriber_id);
        subscriber.register().expect("Failed to register subscriber");

        // Initially no messages
        assert!(!subscriber.has_new_message());

        // Publish a message
        publisher.broadcast(b"Test message").expect("Failed to publish");

        // Give message time to be available
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Now should have a message
        assert!(subscriber.has_new_message());
        let received = subscriber.receive().expect("Should receive message");
        assert_eq!(received, b"Test message");
    }

    
    #[test]
    fn test_performance_benchmark() {
        let name = "test_performance";

        // Clean up any existing shared memory
        let _ = broadcast_buffer::cleanup_shared_memory(name);

        let buffer = SharedBroadcastBuffer::create(name).expect("Failed to create buffer");
        let mut publisher = BroadcastPublisher::new(buffer);
        let subscriber_id = publisher.register_subscriber();
        let mut subscriber = BroadcastSubscriber::new(buffer, subscriber_id);
        subscriber.register().expect("Failed to register subscriber");

        // Benchmark with smaller messages for testing
        let message_size = 1024; // 1KB
        let message_count = 1000;
        let test_data = vec![0xAB; message_size];

        let start_time = Instant::now();
        for _ in 0..message_count {
            publisher.broadcast(&test_data).expect("Failed to publish");
        }
        let duration = start_time.elapsed();

        let throughput = (message_count * message_size) as f64 / duration.as_secs_f64();
        println!("Publish throughput: {:.2} bytes/sec", throughput);

        // Should achieve reasonable performance for 1KB messages
        assert!(throughput > 1_000_000.0, "Throughput should be at least 1MB/sec");

        // Verify subscriber can receive all messages
        let mut received_count = 0;
        while received_count < message_count {
            if if subscriber.has_new_message() { Some(subscriber.receive().unwrap()) } else { None }.is_some() {
                received_count += 1;
            } else {
                thread::sleep(Duration::from_micros(10));
            }
        }

        assert_eq!(received_count, message_count);
    }

    #[test]
    fn test_35mb_payload() {
        let name = "test_35mb";

        // Clean up any existing shared memory
        let _ = broadcast_buffer::cleanup_shared_memory(name);

        let buffer = SharedBroadcastBuffer::create(name).expect("Failed to create buffer");
        let mut publisher = BroadcastPublisher::new(buffer);
        let subscriber_id = publisher.register_subscriber();
        let mut subscriber = BroadcastSubscriber::new(buffer, subscriber_id);
        subscriber.register().expect("Failed to register subscriber");

        // Test with exactly 35MB payload (target requirement)
        let large_payload = vec![0xAB; 35 * 1024 * 1024];
        let sequence = publisher.broadcast(&large_payload).expect("Failed to publish 35MB payload");

        let received = subscriber.receive().expect("Failed to receive 35MB payload");
        assert_eq!(received.len(), 35 * 1024 * 1024);
        assert_eq!(received, large_payload);
        assert_eq!(subscriber.last_processed(), sequence);
    }

    #[test]
    fn test_40hz_timing() {
        let name = "test_40hz";

        // Clean up any existing shared memory
        let _ = broadcast_buffer::cleanup_shared_memory(name);

        let buffer = SharedBroadcastBuffer::create(name).expect("Failed to create buffer");
        let mut publisher = BroadcastPublisher::new(buffer);
        let subscriber_id = publisher.register_subscriber();
        let mut subscriber = BroadcastSubscriber::new(buffer, subscriber_id);
        subscriber.register().expect("Failed to register subscriber");

        // Test 40 Hz messaging (25ms intervals)
        let message_count = 10; // 10 messages = 250ms total
        let message_size = 1024;

        let start_time = Instant::now();
        let mut sequences = Vec::new();

        for i in 0..message_count {
            let message = format!("Message {} at 40Hz", i).into_bytes();
            let seq = publisher.broadcast(&message).expect("Failed to publish");
            sequences.push(seq);

            // Wait for next 25ms interval (40 Hz = 25ms per message)
            let elapsed = start_time.elapsed();
            let target_elapsed = Duration::from_millis((i + 1) as u64 * 25);
            if elapsed < target_elapsed {
                thread::sleep(target_elapsed - elapsed);
            }
        }

        let total_duration = start_time.elapsed();
        println!("40Hz test - published {} messages in {:?}", message_count, total_duration);

        // Should be close to 250ms (10 * 25ms)
        assert!(total_duration >= Duration::from_millis(240));
        assert!(total_duration <= Duration::from_millis(300));

        // Verify all messages received
        for _ in 0..message_count {
            let _ = subscriber.receive().expect("Failed to receive message");
        }
    }

    #[test]
    fn test_zero_copy_pattern() {
        let name = "test_zero_copy";

        // Clean up any existing shared memory
        let _ = broadcast_buffer::cleanup_shared_memory(name);

        let buffer = SharedBroadcastBuffer::create(name).expect("Failed to create buffer");
        let mut publisher = BroadcastPublisher::new(buffer);
        let subscriber_id = publisher.register_subscriber();
        let mut subscriber = BroadcastSubscriber::new(buffer, subscriber_id);
        subscriber.register().expect("Failed to register subscriber");

        // Use allocate_and_publish pattern
        let size = 1024;
        let sequence = publisher.allocate_and_publish(size, |buffer| {
            for i in 0..size {
                buffer[i] = (i % 256) as u8;
            }
        }).expect("Failed to allocate and publish");

        // Verify the data
        let received = subscriber.receive().expect("Failed to receive");
        assert_eq!(received.len(), size);

        for i in 0..size {
            assert_eq!(received[i], (i % 256) as u8);
        }

        assert_eq!(subscriber.last_processed(), sequence);
    }
}