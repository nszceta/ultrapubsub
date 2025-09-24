// Event-driven notification system using eventfd
// This module eliminates polling by using event-driven notifications

use pyo3::prelude::*;
use std::os::fd::AsRawFd;
use nix::sys::eventfd::{EventFd, EfdFlags};

/// Simple event-driven notification system using eventfd
#[pyclass]
pub struct PyEventLoop {
    subscriber_id: String,
    event_fd: EventFd,
}

#[pymethods]
impl PyEventLoop {
    #[new]
    pub fn new(subscriber_id: String) -> PyResult<Self> {
        // Create eventfd for notifications
        let flags = EfdFlags::EFD_CLOEXEC | EfdFlags::EFD_NONBLOCK;
        let event_fd = EventFd::from_value_and_flags(0, flags)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(Self {
            subscriber_id,
            event_fd,
        })
    }

    pub fn get_event_fd(&self) -> i32 {
        self.event_fd.as_raw_fd()
    }

    pub fn wait_for_notification(&self, _timeout_ms: Option<u64>) -> PyResult<bool> {
        // Try to read from eventfd (non-blocking)
        match self.event_fd.read() {
            Ok(_) => Ok(true),
            Err(nix::errno::Errno::EAGAIN) => {
                // No data available, would block
                Ok(false)
            }
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())),
        }
    }

    pub fn notify(&self) -> PyResult<()> {
        // Write to eventfd to trigger notification
        let _ = self.event_fd.write(1)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(())
    }
}

impl Drop for PyEventLoop {
    fn drop(&mut self) {
        // EventFd will be closed automatically when dropped
    }
}

/// Add the event loop to the Python module
pub fn add_event_loop_to_module(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyEventLoop>()?;
    Ok(())
}