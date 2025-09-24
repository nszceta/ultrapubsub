use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use memmap2::MmapMut;
use io_uring::{opcode, types, IoUring};
use pyo3::prelude::*;

#[repr(C)]
pub struct MessageQueueHeader {
    write_pos: AtomicUsize,
    read_pos: AtomicUsize,
    message_count: AtomicUsize,
}

pub struct SharedMemory {
    mmap: Arc<MmapMut>,
    size: usize,
    header: *mut MessageQueueHeader,
}

impl Clone for SharedMemory {
    fn clone(&self) -> Self {
        Self {
            mmap: Arc::clone(&self.mmap),
            size: self.size,
            header: self.header,
        }
    }
}

impl SharedMemory {
    pub fn new(size: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let mmap = MmapMut::map_anon(size)?;
        let ptr = mmap.as_ptr() as *mut u8;
        
        // Initialize message queue header at the beginning
        let header = unsafe {
            let header_ptr = ptr as *mut MessageQueueHeader;
            ptr::write_volatile(&mut (*header_ptr).write_pos, AtomicUsize::new(0));
            ptr::write_volatile(&mut (*header_ptr).read_pos, AtomicUsize::new(0));
            ptr::write_volatile(&mut (*header_ptr).message_count, AtomicUsize::new(0));
            header_ptr
        };
        
        Ok(Self { 
            mmap: Arc::new(mmap), 
            size, 
            header 
        })
    }

    pub fn as_mut_ptr(&self) -> *mut u8 {
        unsafe { self.header.add(1) as *mut u8 }
    }

    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.header.add(1) as *const u8 }
    }

    pub fn size(&self) -> usize {
        self.size - std::mem::size_of::<MessageQueueHeader>()
    }

    pub fn header(&self) -> &MessageQueueHeader {
        unsafe { &*self.header }
    }
}

#[derive(Clone)]
pub struct Message {
    data: Vec<u8>,
}

impl Message {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_data(self) -> Vec<u8> {
        self.data
    }
}

pub struct Publisher {
    ring: IoUring,
    shared_mem: SharedMemory,
}

impl Publisher {
    pub fn new(shared_mem: SharedMemory) -> Result<Self, Box<dyn std::error::Error>> {
        let ring = IoUring::new(8)?;
        Ok(Self { ring, shared_mem })
    }

    pub fn publish(&mut self, message: Message) -> Result<(), Box<dyn std::error::Error>> {
        let data = message.data();
        let ptr = self.shared_mem.as_mut_ptr();
        let header = self.shared_mem.header();
        let write_pos = header.write_pos.load(Ordering::SeqCst);
        let available_space = self.shared_mem.size() - write_pos;
        
        // Check if we have enough space (including null terminator)
        if data.len() + 1 > available_space {
            return Err("Not enough space in shared memory".into());
        }
        
        // Copy message to shared memory with null terminator
        unsafe {
            let message_ptr = ptr.add(write_pos);
            ptr::copy_nonoverlapping(data.as_ptr(), message_ptr, data.len());
            *message_ptr.add(data.len()) = 0; // Null terminator
        }

        // Update write position atomically
        let total_size = data.len() + 1; // Include null terminator
        header.write_pos.store(write_pos + total_size, Ordering::SeqCst);
        header.message_count.fetch_add(1, Ordering::SeqCst);

        // Create io_uring write operation to stdout for demo
        let write_e = unsafe {
            opcode::Write::new(types::Fd(1), ptr.add(write_pos), total_size as u32)
                .build()
                .user_data(0x01)
        };

        // Submit the operation
        unsafe {
            self.ring.submission()
                .push(&write_e)
                .expect("submission queue is full");
        }

        self.ring.submit_and_wait(1)?;

        // Check completion
        let cqe = self.ring.completion().next().ok_or("No completion")?;
        if cqe.result() < 0 {
            return Err(format!("Write error: {}", cqe.result()).into());
        }

        println!("Published message of {} bytes at position {}", data.len(), write_pos);
        Ok(())
    }
}

pub struct Subscriber {
    shared_mem: SharedMemory,
}

impl Subscriber {
    pub fn new(shared_mem: SharedMemory) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self { shared_mem })
    }

    pub fn receive(&mut self) -> Result<Option<Message>, Box<dyn std::error::Error>> {
        let ptr = self.shared_mem.as_ptr();
        let header = self.shared_mem.header();
        
        // Check if there are messages available
        let read_pos = header.read_pos.load(Ordering::SeqCst);
        let write_pos = header.write_pos.load(Ordering::SeqCst);
        let message_count = header.message_count.load(Ordering::SeqCst);
        
        if message_count == 0 || read_pos >= write_pos {
            return Ok(None);
        }
        
        // For demo, assume fixed message size or read until next null terminator
        // In a real implementation, we'd have proper message framing
        let mut message_size = 0;
        unsafe {
            let data_ptr = ptr.add(read_pos);
            let max_size = write_pos - read_pos;
            
            // Find message size (simple approach: look for null terminator or use max 1KB)
            for i in 0..std::cmp::min(max_size, 1024) {
                if *data_ptr.add(i) == 0 {
                    message_size = i;
                    break;
                }
            }
            if message_size == 0 {
                message_size = std::cmp::min(max_size, 1024);
            }
            
            if message_size > 0 {
                let data = slice::from_raw_parts(data_ptr, message_size).to_vec();
                
                // Update read position and message count (include null terminator)
                let new_read_pos = read_pos + message_size + 1; // +1 for null terminator
                header.read_pos.store(new_read_pos, Ordering::SeqCst);
                header.message_count.fetch_sub(1, Ordering::SeqCst);
                
                println!("Received message of {} bytes", message_size);
                Ok(Some(Message::new(data)))
            } else {
                Ok(None)
            }
        }
    }
}

#[pyclass(unsendable)]
pub struct PySharedMemory {
    inner: SharedMemory,
}

#[pymethods]
impl PySharedMemory {
    #[new]
    pub fn new(size: usize) -> PyResult<Self> {
        let inner = SharedMemory::new(size).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(Self { inner })
    }

    pub fn size(&self) -> usize {
        self.inner.size()
    }
}

#[pyclass]
pub struct PyMessage {
    inner: Message,
}

#[pymethods]
impl PyMessage {
    #[new]
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            inner: Message::new(data),
        }
    }

    pub fn data(&self) -> Vec<u8> {
        self.inner.data().to_vec()
    }
}

#[pyclass(unsendable)]
pub struct PyPublisher {
    inner: Publisher,
}

#[pymethods]
impl PyPublisher {
    #[new]
    pub fn new(py_shared_mem: &Bound<'_, PySharedMemory>) -> PyResult<Self> {
        let shared_mem = py_shared_mem.borrow().inner.clone();
        let inner = Publisher::new(shared_mem).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(Self { inner })
    }

    pub fn publish(&mut self, py_message: &Bound<'_, PyMessage>) -> PyResult<()> {
        let message = py_message.borrow().inner.clone();
        self.inner.publish(message).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }
}

#[pyclass(unsendable)]
pub struct PySubscriber {
    inner: Subscriber,
}

#[pymethods]
impl PySubscriber {
    #[new]
    pub fn new(py_shared_mem: &Bound<'_, PySharedMemory>) -> PyResult<Self> {
        let shared_mem = py_shared_mem.borrow().inner.clone();
        let inner = Subscriber::new(shared_mem).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        Ok(Self { inner })
    }

    pub fn receive(&mut self) -> PyResult<Option<PyMessage>> {
        match self.inner.receive() {
            Ok(Some(message)) => Ok(Some(PyMessage { inner: message })),
            Ok(None) => Ok(None),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())),
        }
    }
}

#[pymodule]
fn ultrapubsub(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySharedMemory>()?;
    m.add_class::<PyMessage>()?;
    m.add_class::<PyPublisher>()?;
    m.add_class::<PySubscriber>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_memory_creation() {
        let shared_mem = SharedMemory::new(1024).unwrap();
        assert_eq!(shared_mem.size(), 1024 - std::mem::size_of::<MessageQueueHeader>());
    }

    #[test]
    fn test_message_creation() {
        let data = vec![1, 2, 3, 4];
        let message = Message::new(data.clone());
        assert_eq!(message.data(), &data);
    }

    #[test]
    fn test_publisher_subscriber_round_trip() {
        // Create shared memory
        let shared_mem = SharedMemory::new(4096).unwrap();
        
        // Create publisher and subscriber
        let mut publisher = Publisher::new(shared_mem.clone()).unwrap();
        let mut subscriber = Subscriber::new(shared_mem).unwrap();
        
        // Create and publish a message
        let test_data = b"Hello, ultrapubsub!";
        let message = Message::new(test_data.to_vec());
        publisher.publish(message).unwrap();
        
        // Receive the message
        let received_message = subscriber.receive().unwrap().unwrap();
        assert_eq!(received_message.data(), test_data);
    }

    #[test]
    fn test_multiple_messages() {
        let shared_mem = SharedMemory::new(4096).unwrap();
        let mut publisher = Publisher::new(shared_mem.clone()).unwrap();
        let mut subscriber = Subscriber::new(shared_mem).unwrap();
        
        // Publish multiple messages
        for i in 0..5 {
            let data = format!("Message {}", i).into_bytes();
            let message = Message::new(data);
            publisher.publish(message).unwrap();
        }
        
        // Receive all messages
        for i in 0..5 {
            let received = subscriber.receive().unwrap().unwrap();
            let expected = format!("Message {}", i).into_bytes();
            assert_eq!(received.data(), &expected);
        }
        
        // No more messages should be available
        assert!(subscriber.receive().unwrap().is_none());
    }

    #[test]
    fn test_empty_queue() {
        let shared_mem = SharedMemory::new(1024).unwrap();
        let mut subscriber = Subscriber::new(shared_mem).unwrap();
        
        // No messages published, should return None
        assert!(subscriber.receive().unwrap().is_none());
    }
}
