// UltraPubSub - Minimal Rust Synchronization Layer
// This module provides only the essential synchronization primitives
// for coordinating zero-copy numpy array broadcasting across processes.

mod sync;
use sync::SyncCoordinator;

use pyo3::prelude::*;
use std::sync::Mutex;

// Thread-safe coordinator storage
static COORDINATOR: Mutex<Option<SyncCoordinator>> = Mutex::new(None);

/// Create a synchronization coordinator for broadcasting
#[pyfunction]
fn create_coordinator(name: &str) -> PyResult<()> {
    let mut coord = COORDINATOR.lock().unwrap();
    *coord = Some(SyncCoordinator::create(name).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?);
    Ok(())
}

/// Connect to existing synchronization coordinator
#[pyfunction]
fn connect_coordinator(name: &str) -> PyResult<()> {
    let mut coord = COORDINATOR.lock().unwrap();
    *coord = Some(SyncCoordinator::connect(name).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?);
    Ok(())
}

/// Register a new subscriber and return subscriber ID
#[pyfunction]
fn register_subscriber() -> PyResult<u32> {
    let mut coord = COORDINATOR.lock().unwrap();
    match coord.as_mut() {
        Some(coord) => coord.register_subscriber().map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())),
        None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No coordinator created")),
    }
}

/// Wait for all subscribers to be ready (publisher side)
#[pyfunction]
fn wait_for_subscribers(timeout_ms: u32) -> PyResult<bool> {
    let mut coord = COORDINATOR.lock().unwrap();
    match coord.as_mut() {
        Some(coord) => Ok(coord.wait_for_subscribers(timeout_ms)),
        None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No coordinator created")),
    }
}

/// Notify subscribers that new data is ready (publisher side)
#[pyfunction]
fn notify_broadcast(sequence: u32) -> PyResult<()> {
    let mut coord = COORDINATOR.lock().unwrap();
    match coord.as_mut() {
        Some(coord) => {
            coord.notify_broadcast(sequence);
            Ok(())
        }
        None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No coordinator created")),
    }
}

/// Wait for broadcast notification (subscriber side)
#[pyfunction]
fn wait_for_broadcast(timeout_ms: u32) -> PyResult<Option<u32>> {
    let mut coord = COORDINATOR.lock().unwrap();
    match coord.as_mut() {
        Some(coord) => Ok(coord.wait_for_broadcast(timeout_ms)),
        None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No coordinator created")),
    }
}

/// Acknowledge broadcast receipt (subscriber side)
#[pyfunction]
fn acknowledge_broadcast() -> PyResult<()> {
    let mut coord = COORDINATOR.lock().unwrap();
    match coord.as_mut() {
        Some(coord) => {
            // Note: In a real implementation, we'd need to track which subscriber is acknowledging
            // For now, this is a simplified version
            coord.acknowledge(0); // Using subscriber ID 0 for simplicity
            Ok(())
        }
        None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No coordinator created")),
    }
}

/// Wait for all acknowledgments (publisher side)
#[pyfunction]
fn wait_for_acknowledgments(timeout_ms: u32) -> PyResult<bool> {
    let mut coord = COORDINATOR.lock().unwrap();
    match coord.as_mut() {
        Some(coord) => Ok(coord.wait_for_acknowledgments(timeout_ms)),
        None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No coordinator created")),
    }
}

/// Get current subscriber count
#[pyfunction]
fn get_subscriber_count() -> PyResult<u32> {
    let mut coord = COORDINATOR.lock().unwrap();
    match coord.as_mut() {
        Some(coord) => Ok(coord.subscriber_count()),
        None => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("No coordinator created")),
    }
}

/// Clean up synchronization resources
#[pyfunction]
fn cleanup_coordinator() -> PyResult<()> {
    let mut coord = COORDINATOR.lock().unwrap();
    *coord = None;
    Ok(())
}

/// Python module for UltraPubSub synchronization
#[pymodule]
fn ultrapubsub(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(create_coordinator, m)?)?;
    m.add_function(wrap_pyfunction!(connect_coordinator, m)?)?;
    m.add_function(wrap_pyfunction!(register_subscriber, m)?)?;
    m.add_function(wrap_pyfunction!(wait_for_subscribers, m)?)?;
    m.add_function(wrap_pyfunction!(notify_broadcast, m)?)?;
    m.add_function(wrap_pyfunction!(wait_for_broadcast, m)?)?;
    m.add_function(wrap_pyfunction!(acknowledge_broadcast, m)?)?;
    m.add_function(wrap_pyfunction!(wait_for_acknowledgments, m)?)?;
    m.add_function(wrap_pyfunction!(get_subscriber_count, m)?)?;
    m.add_function(wrap_pyfunction!(cleanup_coordinator, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_functions() {
        // Test basic functionality
        create_coordinator("test").unwrap();
        let sub_id = register_subscriber().unwrap();
        assert_eq!(sub_id, 0);

        let count = get_subscriber_count().unwrap();
        assert_eq!(count, 1);

        cleanup_coordinator().unwrap();
    }
}