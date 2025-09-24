// Shared Memory Ring Buffer Implementation
// This module implements the high-performance ring buffer architecture as specified
// in the OpenSpec change proposal to replace the flawed io_uring approach.

use std::ptr;
use std::sync::atomic::{AtomicU64, AtomicI32, Ordering};
use std::os::fd::AsRawFd;
use nix::sys::mman::{shm_open, shm_unlink};
use nix::fcntl::OFlag;
use nix::sys::stat::Mode;
use nix::unistd::{ftruncate};
use libc::{c_void, mmap, munmap, MAP_FAILED, PROT_READ, PROT_WRITE, MAP_SHARED};

// Constants for the ring buffer implementation
pub const MAX_SUBSCRIBERS: usize = 16;  // Support up to 16 concurrent subscribers
pub const BUFFER_SIZE: usize = 128 * 1024 * 1024;  // 128MB circular buffer (sufficient for 3+ 35MB messages)
pub const MAX_MESSAGES: usize = 1024;  // Maximum number of messages in flight
pub const MESSAGE_HEADER_SIZE: usize = 16;  // 16 bytes per message header (offset + length)

// Memory layout constants
pub const HEAD_OFFSET: usize = 0;
pub const PUBLISHED_COUNT_OFFSET: usize = 8;
pub const TAILS_OFFSET: usize = 16;
pub const LAST_SEEN_OFFSET: usize = TAILS_OFFSET + (MAX_SUBSCRIBERS * 8);
pub const DATA_OFFSET: usize = LAST_SEEN_OFFSET + (MAX_SUBSCRIBERS * 8);
pub const OFFSETS_OFFSET: usize = DATA_OFFSET + BUFFER_SIZE;
pub const LENGTHS_OFFSET: usize = OFFSETS_OFFSET + (MAX_MESSAGES * 4);
pub const AVAILABLE_OFFSET: usize = LENGTHS_OFFSET + (MAX_MESSAGES * 2);
pub const NOTIFICATION_FD_OFFSET: usize = AVAILABLE_OFFSET + 8;

/// Shared memory ring buffer for 1:N pub/sub messaging
///
/// This struct represents the shared memory layout for the ring buffer.
/// It is designed to be shared between publisher and subscriber processes
/// using atomic operations for lock-free synchronization.
///
/// Memory Layout:
/// ```text
/// +------------------+
/// | head: AtomicU64  |  Publisher write position
/// | published_count: AtomicU64 | Total messages published
/// +------------------+
/// | tails[16]: AtomicU64 | Per-subscriber read positions
/// | last_seen[16]: AtomicU64 | Last sequence seen by each subscriber
/// +------------------+
/// | data[128MB]     | Circular message buffer
/// +------------------+
/// | offsets[1024]: u32 | Message offset table
/// | lengths[1024]: u16 | Message length table
/// | available: AtomicU64 | Message availability bitmap
/// | notification_fd: AtomicI32 | Eventfd for notifications
/// +------------------+
/// ```
#[repr(C)]
pub struct SharedRingBuffer {
    // Publisher state
    head: AtomicU64,                    // Current write position in circular buffer
    published_count: AtomicU64,         // Monotonically increasing message sequence number

    // Subscriber state (arrays for MAX_SUBSCRIBERS)
    tails: [AtomicU64; MAX_SUBSCRIBERS], // Per-subscriber read position in buffer
    last_seen: [AtomicU64; MAX_SUBSCRIBERS], // Last sequence number seen by each subscriber

    // Message storage
    data: [u8; BUFFER_SIZE],           // Circular buffer for message data
    offsets: [u32; MAX_MESSAGES],       // Message start offsets within data buffer
    lengths: [u16; MAX_MESSAGES],       // Message lengths

    // Synchronization and notification
    available: AtomicU64,               // Bitmap tracking which messages are available to which subscribers
    notification_fd: AtomicI32,         // Event file descriptor for subscriber notifications
}

impl SharedRingBuffer {
    /// Create a new shared memory ring buffer
    ///
    /// This function allocates and initializes a new shared memory region
    /// containing the ring buffer structure. It should be called by the publisher process.
    pub fn create(name: &str) -> Result<*mut SharedRingBuffer, Box<dyn std::error::Error>> {
        // Calculate total shared memory size
        let total_size = std::mem::size_of::<SharedRingBuffer>();

        // Create shared memory object
        let fd = shm_open(
            name.as_bytes(),
            OFlag::O_CREAT | OFlag::O_RDWR | OFlag::O_EXCL,
            Mode::S_IRUSR | Mode::S_IWUSR,
        )?;

        // Set the size of the shared memory
        ftruncate(&fd, total_size as i64)?;

        // Map the shared memory
        let ptr = unsafe {
            mmap(
                ptr::null_mut(),
                total_size,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        } as *mut SharedRingBuffer;

        if ptr as *mut c_void == MAP_FAILED {
            return Err("Failed to map shared memory".into());
        }

        // Initialize the ring buffer
        unsafe {
            ptr::write_volatile(&mut (*ptr).head, AtomicU64::new(0));
            ptr::write_volatile(&mut (*ptr).published_count, AtomicU64::new(0));

            // Initialize subscriber arrays
            for i in 0..MAX_SUBSCRIBERS {
                ptr::write_volatile(&mut (*ptr).tails[i], AtomicU64::new(0));
                ptr::write_volatile(&mut (*ptr).last_seen[i], AtomicU64::new(0));
            }

            // Initialize data buffer to zeros
            ptr::write_bytes((*ptr).data.as_mut_ptr(), 0, BUFFER_SIZE);

            // Initialize message metadata tables
            for i in 0..MAX_MESSAGES {
                (*ptr).offsets[i] = 0;
                (*ptr).lengths[i] = 0;
            }

            // Initialize availability bitmap (all bits 0 = no messages available)
            ptr::write_volatile(&mut (*ptr).available, AtomicU64::new(0));

            // Initialize notification fd (-1 = none)
            ptr::write_volatile(&mut (*ptr).notification_fd, AtomicI32::new(-1));
        }

        Ok(ptr)
    }

    /// Attach to an existing shared memory ring buffer
    ///
    /// This function maps an existing shared memory region containing
    /// a ring buffer created by another process. It should be called
    /// by subscriber processes.
    pub fn attach(name: &str) -> Result<*mut SharedRingBuffer, Box<dyn std::error::Error>> {
        let shm_name = format!("/{}", name);

        // Open existing shared memory object
        let fd = shm_open(
            shm_name.as_bytes(),
            OFlag::O_RDWR,
            Mode::S_IRUSR | Mode::S_IWUSR,
        )?;

        // Map the shared memory
        let ptr = unsafe {
            mmap(
                ptr::null_mut(),
                std::mem::size_of::<SharedRingBuffer>(),
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        } as *mut SharedRingBuffer;

        if ptr as *mut c_void == MAP_FAILED {
            return Err("Failed to map shared memory".into());
        }

        Ok(ptr)
    }

    /// Get the current head position (write position)
    pub fn get_head(&self) -> u64 {
        self.head.load(Ordering::Acquire)
    }

    /// Get the current tail position for a specific subscriber
    pub fn get_tail(&self, subscriber_id: usize) -> Result<u64, Box<dyn std::error::Error>> {
        if subscriber_id >= MAX_SUBSCRIBERS {
            return Err("Invalid subscriber ID".into());
        }
        Ok(self.tails[subscriber_id].load(Ordering::Acquire))
    }

    /// Get the published message count
    pub fn get_published_count(&self) -> u64 {
        self.published_count.load(Ordering::Acquire)
    }

    /// Get the last seen sequence number for a subscriber
    pub fn get_last_seen(&self, subscriber_id: usize) -> Result<u64, Box<dyn std::error::Error>> {
        if subscriber_id >= MAX_SUBSCRIBERS {
            return Err("Invalid subscriber ID".into());
        }
        Ok(self.last_seen[subscriber_id].load(Ordering::Acquire))
    }

    /// Calculate available space in the circular buffer
    pub fn available_space(&self) -> usize {
        let head = self.get_head();
        // Simple calculation - in practice, we'd need to consider subscriber positions
        BUFFER_SIZE.saturating_sub(head as usize)
    }

    /// Check if a specific message is available to a subscriber
    pub fn is_message_available(&self, message_id: usize, subscriber_id: usize) -> bool {
        if message_id >= 64 || subscriber_id >= MAX_SUBSCRIBERS {
            return false;
        }

        let bitmap = self.available.load(Ordering::Acquire);
        let bit_position = (message_id * MAX_SUBSCRIBERS) + subscriber_id;
        (bitmap & (1 << bit_position)) != 0
    }

    /// Mark a message as available to a specific subscriber
    pub fn mark_message_available(&self, message_id: usize, subscriber_id: usize) {
        if message_id >= 64 || subscriber_id >= MAX_SUBSCRIBERS {
            return;
        }

        let bit_position = (message_id * MAX_SUBSCRIBERS) + subscriber_id;
        let mask = 1 << bit_position;
        self.available.fetch_or(mask, Ordering::Release);
    }

    /// Clear message availability for a subscriber
    pub fn clear_message_availability(&self, message_id: usize, subscriber_id: usize) {
        if message_id >= 64 || subscriber_id >= MAX_SUBSCRIBERS {
            return;
        }

        let bit_position = (message_id * MAX_SUBSCRIBERS) + subscriber_id;
        let mask = !(1 << bit_position);
        self.available.fetch_and(mask, Ordering::Release);
    }

    /// Set the notification file descriptor
    pub fn set_notification_fd(&self, fd: i32) {
        self.notification_fd.store(fd, Ordering::Release);
    }

    /// Get the notification file descriptor
    pub fn get_notification_fd(&self) -> i32 {
        self.notification_fd.load(Ordering::Acquire)
    }

    /// Unmap and cleanup shared memory
    pub unsafe fn destroy(ptr: *mut SharedRingBuffer, name: &str) {
        if !ptr.is_null() {
            munmap(ptr as *mut c_void, std::mem::size_of::<SharedRingBuffer>());
        }
        let _ = shm_unlink(name.as_bytes());
    }
}

/// Publisher for the shared memory ring buffer
pub struct RingBufferPublisher {
    buffer: *mut SharedRingBuffer,
    subscriber_count: usize,
}

impl RingBufferPublisher {
    /// Create a new publisher for the given ring buffer
    pub fn new(buffer: *mut SharedRingBuffer) -> Self {
        Self {
            buffer,
            subscriber_count: 0,
        }
    }

    /// Publish a message to the ring buffer
    pub fn publish(&mut self, data: &[u8]) -> Result<u64, Box<dyn std::error::Error>> {
        if data.is_empty() {
            return Err("Message data cannot be empty".into());
        }

        if data.len() > BUFFER_SIZE {
            return Err("Message too large for buffer".into());
        }

        unsafe {
            let buffer = &*self.buffer;

            // Get current head and calculate next position
            let head = buffer.get_head();
            let next_head = head + data.len() as u64;

            // Check if we have enough space (simple wrap-around check)
            if next_head > BUFFER_SIZE as u64 {
                // For now, reject if we can't fit without wrapping
                // TODO: Implement proper circular buffer wrap-around
                return Err("Insufficient space in buffer".into());
            }

            // Get the next message sequence number
            let sequence = buffer.get_published_count();
            let message_slot = (sequence % MAX_MESSAGES as u64) as usize;

            // Copy message data to the buffer
            let data_ptr = buffer.data.as_ptr().add(head as usize);
            ptr::copy_nonoverlapping(data.as_ptr(), data_ptr as *mut u8, data.len());

            // Update message metadata (need to cast to mutable pointer)
            let buffer_mut = buffer as *const SharedRingBuffer as *mut SharedRingBuffer;
            unsafe {
                (*buffer_mut).offsets[message_slot] = head as u32;
                (*buffer_mut).lengths[message_slot] = data.len() as u16;
            }

            // Mark message as available for all subscribers
            for subscriber_id in 0..self.subscriber_count.max(1) {
                buffer.mark_message_available(message_slot, subscriber_id);
            }

            // Update publisher state
            buffer.head.store(next_head, Ordering::Release);
            buffer.published_count.store(sequence + 1, Ordering::Release);

            // Notify subscribers if notification fd is set
            let notify_fd = buffer.get_notification_fd();
            if notify_fd != -1 {
                // Simple notification - write 1 byte to eventfd
                let notification_data = 1u64;
                libc::write(notify_fd, &notification_data as *const u64 as *const c_void, 8);
            }

            Ok(sequence)
        }
    }

    /// Register a new subscriber
    pub fn register_subscriber(&mut self) -> usize {
        let subscriber_id = self.subscriber_count;
        if subscriber_id < MAX_SUBSCRIBERS {
            self.subscriber_count += 1;
            subscriber_id
        } else {
            panic!("Maximum number of subscribers reached");
        }
    }

    /// Get the current number of registered subscribers
    pub fn subscriber_count(&self) -> usize {
        self.subscriber_count
    }
}

/// Subscriber for the shared memory ring buffer
pub struct RingBufferSubscriber {
    buffer: *mut SharedRingBuffer,
    subscriber_id: usize,
    last_processed: u64,
}

impl RingBufferSubscriber {
    /// Create a new subscriber with the given ID
    pub fn new(buffer: *mut SharedRingBuffer, subscriber_id: usize) -> Self {
        Self {
            buffer,
            subscriber_id,
            last_processed: 0,
        }
    }

    /// Receive the next available message
    pub fn receive(&mut self) -> Option<Vec<u8>> {
        unsafe {
            let buffer = &*self.buffer;

            // Get the last sequence we've seen
            let last_seen = buffer.get_last_seen(self.subscriber_id).unwrap_or(0);
            let published_count = buffer.get_published_count();

            // Check if there are new messages
            if last_seen >= published_count {
                return None;
            }

            // Look for the next available message
            for sequence in (last_seen + 1)..=published_count {
                let message_slot = (sequence % MAX_MESSAGES as u64) as usize;

                if buffer.is_message_available(message_slot, self.subscriber_id) {
                    // Get message metadata
                    let offset = buffer.offsets[message_slot] as usize;
                    let length = buffer.lengths[message_slot] as usize;

                    // Copy message data
                    let data_ptr = buffer.data.as_ptr().add(offset);
                    let mut message_data = Vec::with_capacity(length);
                    ptr::copy_nonoverlapping(data_ptr, message_data.as_mut_ptr(), length);
                    message_data.set_len(length);

                    // Update our state
                    buffer.last_seen[self.subscriber_id].store(sequence, Ordering::Release);
                    self.last_processed = sequence;

                    // Clear availability for this message
                    buffer.clear_message_availability(message_slot, self.subscriber_id);

                    return Some(message_data);
                }
            }
        }

        None
    }

    /// Get the last processed sequence number
    pub fn last_processed(&self) -> u64 {
        self.last_processed
    }

    /// Check if there are messages available
    pub fn has_messages(&self) -> bool {
        unsafe {
            let buffer = &*self.buffer;
            let last_seen = buffer.get_last_seen(self.subscriber_id).unwrap_or(0);
            let published_count = buffer.get_published_count();

            published_count > last_seen
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer_creation() {
        let name = "test_creation";

        // Clean up any existing shared memory
        let _ = unsafe { SharedRingBuffer::destroy(std::ptr::null_mut(), name) };

        // Create a new ring buffer
        let buffer = SharedRingBuffer::create(name).expect("Failed to create ring buffer");
        assert!(!buffer.is_null());

        // Test basic properties
        unsafe {
            let buf_ref = &*buffer;
            assert_eq!(buf_ref.get_head(), 0);
            assert_eq!(buf_ref.get_published_count(), 0);
            assert_eq!(buf_ref.available_space(), BUFFER_SIZE);
        }

        // Cleanup
        unsafe { SharedRingBuffer::destroy(buffer, name) };
    }

    #[test]
    fn test_ring_buffer_attach() {
        let name = "test_attach";

        // Clean up any existing shared memory
        let _ = unsafe { SharedRingBuffer::destroy(std::ptr::null_mut(), name) };

        // Create a new ring buffer
        let buffer = SharedRingBuffer::create(name).expect("Failed to create ring buffer");

        // Attach to the existing buffer
        let attached = SharedRingBuffer::attach(name).expect("Failed to attach to ring buffer");
        assert!(!attached.is_null());

        // Both should point to the same memory
        unsafe {
            assert_eq!((*buffer).get_head(), (*attached).get_head());
        }

        // Cleanup
        unsafe {
            SharedRingBuffer::destroy(buffer, name);
            SharedRingBuffer::destroy(attached, name);
        }
    }

    #[test]
    fn test_publisher_subscriber_basic() {
        let name = "test_pub_sub";

        // Clean up any existing shared memory
        let _ = unsafe { SharedRingBuffer::destroy(std::ptr::null_mut(), name) };

        // Create ring buffer
        let buffer = SharedRingBuffer::create(name).expect("Failed to create ring buffer");

        // Create publisher and subscriber
        let mut publisher = RingBufferPublisher::new(buffer);
        let subscriber_id = publisher.register_subscriber();
        let mut subscriber = RingBufferSubscriber::new(buffer, subscriber_id);

        // Test publishing and receiving
        let test_message = b"Hello, Ring Buffer!";
        let sequence = publisher.publish(test_message).expect("Failed to publish message");

        // Receive the message
        let received = subscriber.receive().expect("Failed to receive message");
        assert_eq!(received, test_message);
        assert_eq!(subscriber.last_processed(), sequence);

        // Cleanup
        unsafe { SharedRingBuffer::destroy(buffer, name) };
    }

    #[test]
    fn test_multiple_messages() {
        let name = "test_multiple";

        // Clean up any existing shared memory
        let _ = unsafe { SharedRingBuffer::destroy(std::ptr::null_mut(), name) };

        // Create ring buffer
        let buffer = SharedRingBuffer::create(name).expect("Failed to create ring buffer");

        let mut publisher = RingBufferPublisher::new(buffer);
        let subscriber_id = publisher.register_subscriber();
        let mut subscriber = RingBufferSubscriber::new(buffer, subscriber_id);

        // Publish multiple messages
        let messages = vec![
            b"Message 1".to_vec(),
            b"Message 2".to_vec(),
            b"Message 3".to_vec(),
        ];

        let mut sequences = Vec::new();
        for msg in &messages {
            let seq = publisher.publish(msg).expect("Failed to publish");
            sequences.push(seq);
        }

        // Receive all messages
        let mut received_messages = Vec::new();
        while let Some(msg) = subscriber.receive() {
            received_messages.push(msg);
        }

        assert_eq!(received_messages, messages);
        assert_eq!(subscriber.last_processed(), sequences.last().copied().unwrap_or(0));

        // Cleanup
        unsafe { SharedRingBuffer::destroy(buffer, name) };
    }

    #[test]
    fn test_large_message() {
        let name = "test_large";

        // Clean up any existing shared memory
        let _ = unsafe { SharedRingBuffer::destroy(std::ptr::null_mut(), name) };

        // Create ring buffer
        let buffer = SharedRingBuffer::create(name).expect("Failed to create ring buffer");

        let mut publisher = RingBufferPublisher::new(buffer);
        let subscriber_id = publisher.register_subscriber();
        let mut subscriber = RingBufferSubscriber::new(buffer, subscriber_id);

        // Create a large message (but still within buffer size)
        let large_message = vec![0xAB; 1024 * 1024]; // 1MB message
        let sequence = publisher.publish(&large_message).expect("Failed to publish large message");

        // Receive the large message
        let received = subscriber.receive().expect("Failed to receive large message");
        assert_eq!(received, large_message);
        assert_eq!(subscriber.last_processed(), sequence);

        // Cleanup
        unsafe { SharedRingBuffer::destroy(buffer, name) };
    }
}