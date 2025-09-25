// Shared Memory Broadcast Buffer Implementation
// This module implements the synchronous broadcast architecture as specified
// in the OpenSpec change proposal for synchronous 1:N messaging.

use std::ptr;
use std::sync::atomic::{AtomicU64, AtomicI32, AtomicU32, Ordering};
use std::os::fd::AsRawFd;
use std::io::Write;
use nix::sys::mman::{shm_open, shm_unlink};
use nix::fcntl::OFlag;
use nix::sys::stat::Mode;
use nix::unistd::{ftruncate};
use libc::{c_void, mmap, munmap, MAP_FAILED, PROT_READ, PROT_WRITE, MAP_SHARED};

// Constants for the synchronous broadcast implementation
pub const MAX_SUBSCRIBERS: usize = 6;  // Exactly 6 concurrent subscribers for broadcast
pub const BROADCAST_SLOT_SIZE: usize = 35 * 1024 * 1024;  // 35MB single broadcast slot
pub const BROADCAST_STATES: usize = 3;  // PUBLISHING, WAITING, COMPLETED

// Broadcast states
pub const STATE_PUBLISHING: u32 = 0;
pub const STATE_WAITING: u32 = 1;
pub const STATE_COMPLETED: u32 = 2;

// Memory layout constants for synchronous broadcast
pub const SUBSCRIBER_COUNT_OFFSET: usize = 0;
pub const BROADCAST_STATE_OFFSET: usize = 4;
pub const CURRENT_SEQUENCE_OFFSET: usize = 8;
pub const BROADCAST_LENGTH_OFFSET: usize = 16;
pub const SUBSCRIBER_ACK_OFFSET: usize = 20;  // 6 bits for subscriber acknowledgments
pub const SUBSCRIBER_REGISTERED_OFFSET: usize = 24;  // 6 bits for registration status
pub const BROADCAST_DATA_OFFSET: usize = 32;  // Start of 35MB broadcast data

/// Shared memory broadcast buffer for synchronous 1:N messaging
///
/// This struct implements a synchronous broadcast mechanism where:
/// - All subscribers receive the SAME message from a single broadcast slot
/// - Publisher waits for ALL subscribers to complete before continuing
/// - Exactly 6 subscribers are supported with explicit registration/deregistration
///
/// Memory Layout:
/// ```text
/// +--------------------------+
/// | subscriber_count: u32     | Number of registered subscribers (0-6)
/// | broadcast_state: u32      | Current broadcast state (PUBLISHING/WAITING/COMPLETED)
/// | sequence_number: u64      | Monotonically increasing sequence
/// | broadcast_length: u32     | Length of current broadcast data
/// | subscriber_ack_bits: u32  | 6 bits for subscriber acknowledgments
/// | subscriber_reg_bits: u32  | 6 bits for subscriber registration status
/// +--------------------------+
/// | broadcast_data[35MB]     | Single broadcast slot accessible to all
/// +--------------------------+
/// ```
#[repr(C)]
pub struct SharedBroadcastBuffer {
    // Synchronous broadcast state
    subscriber_count: AtomicU32,        // Number of registered subscribers (0-6)
    broadcast_state: AtomicU32,         // Current broadcast state
    sequence_number: AtomicU64,         // Monotonically increasing sequence number
    broadcast_length: AtomicU32,        // Length of data in broadcast slot
    subscriber_ack_bits: AtomicU32,     // 6 bits: which subscribers have acknowledged
    subscriber_reg_bits: AtomicU32,     // 6 bits: which subscribers are registered

    // Single broadcast slot accessible to all subscribers
    broadcast_data: [u8; BROADCAST_SLOT_SIZE],  // 35MB single broadcast slot
}

impl SharedBroadcastBuffer {
    /// Create a new shared memory broadcast buffer
    ///
    /// This function allocates and initializes a new shared memory region
    /// containing the broadcast buffer structure. It should be called by the publisher process.
    pub fn create(name: &str) -> Result<*mut SharedBroadcastBuffer, Box<dyn std::error::Error>> {
        // Calculate total shared memory size
        let total_size = std::mem::size_of::<SharedBroadcastBuffer>();

        // Create shared memory object
        let shm_name = if name.starts_with("/") {
            name.to_string()
        } else {
            format!("/{}", name)
        };

        let fd = shm_open(
            shm_name.as_bytes(),
            OFlag::O_CREAT | OFlag::O_RDWR | OFlag::O_EXCL,
            Mode::S_IRUSR | Mode::S_IWUSR,
        )?;

        // Set the size of the shared memory object
        ftruncate(fd, total_size as i64)?;

        // Map the shared memory object into process address space
        let ptr = unsafe {
            mmap(
                ptr::null_mut(),
                total_size,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                fd,
                0,
            )
        };

        if ptr == MAP_FAILED {
            shm_unlink(&shm_name.as_bytes())?;
            return Err("Failed to mmap shared memory".into());
        }

        // Initialize the broadcast buffer
        let buffer = ptr as *mut SharedBroadcastBuffer;
        unsafe {
            ptr::write_volatile(&mut (*buffer).subscriber_count as *mut AtomicU32, AtomicU32::new(0));
            ptr::write_volatile(&mut (*buffer).broadcast_state as *mut AtomicU32, AtomicU32::new(STATE_COMPLETED));
            ptr::write_volatile(&mut (*buffer).sequence_number as *mut AtomicU64, AtomicU64::new(0));
            ptr::write_volatile(&mut (*buffer).broadcast_length as *mut AtomicU32, AtomicU32::new(0));
            ptr::write_volatile(&mut (*buffer).subscriber_ack_bits as *mut AtomicU32, AtomicU32::new(0));
            ptr::write_volatile(&mut (*buffer).subscriber_reg_bits as *mut AtomicU32, AtomicU32::new(0));
        }

        Ok(buffer)
    }

    /// Connect to an existing shared memory broadcast buffer
    ///
    /// This function attaches to an existing shared memory region created by the publisher.
    pub fn connect(name: &str) -> Result<*mut SharedBroadcastBuffer, Box<dyn std::error::Error>> {
        let total_size = std::mem::size_of::<SharedBroadcastBuffer>();

        let shm_name = if name.starts_with("/") {
            name.to_string()
        } else {
            format!("/{}", name)
        };

        let fd = shm_open(
            shm_name.as_bytes(),
            OFlag::O_RDWR,
            Mode::S_IRUSR | Mode::S_IWUSR,
        )?;

        // Map the shared memory object into process address space
        let ptr = unsafe {
            mmap(
                ptr::null_mut(),
                total_size,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                fd,
                0,
            )
        };

        if ptr == MAP_FAILED {
            return Err("Failed to mmap shared memory".into());
        }

        Ok(ptr as *mut SharedBroadcastBuffer)
    }

    /// Register a new subscriber with the broadcast buffer
    pub fn register_subscriber(&self, subscriber_id: usize) -> Result<(), Box<dyn std::error::Error>> {
        if subscriber_id >= MAX_SUBSCRIBERS {
            return Err("Subscriber ID must be 0-5".into());
        }

        let mask = 1 << subscriber_id;
        let mut reg_bits = self.subscriber_reg_bits.load(Ordering::Acquire);

        // Check if already registered
        if reg_bits & mask != 0 {
            return Ok(());
        }

        // Set the registration bit
        loop {
            let new_reg_bits = reg_bits | mask;
            match self.subscriber_reg_bits.compare_exchange_weak(
                reg_bits,
                new_reg_bits,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => reg_bits = current,
            }
        }

        // Increment subscriber count
        self.subscriber_count.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Deregister a subscriber from the broadcast buffer
    pub fn deregister_subscriber(&self, subscriber_id: usize) -> Result<(), Box<dyn std::error::Error>> {
        if subscriber_id >= MAX_SUBSCRIBERS {
            return Err("Subscriber ID must be 0-5".into());
        }

        let mask = !(1 << subscriber_id);
        let mut reg_bits = self.subscriber_reg_bits.load(Ordering::Acquire);

        // Check if already deregistered
        if reg_bits & (1 << subscriber_id) == 0 {
            return Ok(());
        }

        // Clear the registration bit
        loop {
            let new_reg_bits = reg_bits & mask;
            match self.subscriber_reg_bits.compare_exchange_weak(
                reg_bits,
                new_reg_bits,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => reg_bits = current,
            }
        }

        // Clear acknowledgment bit
        let mut ack_bits = self.subscriber_ack_bits.load(Ordering::Acquire);
        loop {
            let new_ack_bits = ack_bits & mask;
            match self.subscriber_ack_bits.compare_exchange_weak(
                ack_bits,
                new_ack_bits,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(current) => ack_bits = current,
            }
        }

        // Decrement subscriber count
        self.subscriber_count.fetch_sub(1, Ordering::AcqRel);
        Ok(())
    }

    /// Get the current number of registered subscribers
    pub fn get_subscriber_count(&self) -> u32 {
        self.subscriber_count.load(Ordering::Acquire)
    }

    /// Get the current broadcast state
    pub fn get_broadcast_state(&self) -> u32 {
        self.broadcast_state.load(Ordering::Acquire)
    }

    /// Get the current sequence number
    pub fn get_sequence_number(&self) -> u64 {
        self.sequence_number.load(Ordering::Acquire)
    }
}

/// Publisher for the shared memory broadcast buffer
pub struct BroadcastPublisher {
    buffer: *mut SharedBroadcastBuffer,
}

impl BroadcastPublisher {
    /// Create a new publisher for the given broadcast buffer
    pub fn new(buffer: *mut SharedBroadcastBuffer) -> Self {
        Self { buffer }
    }

    /// Broadcast a message to all subscribers synchronously
    ///
    /// This method will wait until ALL subscribers have acknowledged receipt
    /// of the message before returning.
    pub fn broadcast(&mut self, data: &[u8]) -> Result<u64, Box<dyn std::error::Error>> {
        if data.len() > BROADCAST_SLOT_SIZE {
            return Err("Message too large for broadcast slot".into());
        }

        let buffer = unsafe { &*self.buffer };

        // Wait for any previous broadcast to complete
        while buffer.get_broadcast_state() != STATE_COMPLETED {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        let sequence = buffer.get_sequence_number() + 1;
        let subscriber_count = buffer.get_subscriber_count();

        // If no subscribers, just update sequence and return
        if subscriber_count == 0 {
            unsafe {
                (*self.buffer).sequence_number.store(sequence, Ordering::Release);
            }
            return Ok(sequence);
        }

        // Set state to PUBLISHING
        unsafe {
            (*self.buffer).broadcast_state.store(STATE_PUBLISHING, Ordering::Release);
            (*self.buffer).broadcast_length.store(data.len() as u32, Ordering::Release);
            (*self.buffer).subscriber_ack_bits.store(0, Ordering::Release);
        }

        // Copy data to broadcast slot
        unsafe {
            ptr::copy_nonoverlapping(
                data.as_ptr(),
                (*self.buffer).broadcast_data.as_mut_ptr(),
                data.len()
            );
        }

        // Set state to WAITING and update sequence
        unsafe {
            (*self.buffer).sequence_number.store(sequence, Ordering::Release);
            (*self.buffer).broadcast_state.store(STATE_WAITING, Ordering::Release);
        }

        // Wait for all subscribers to acknowledge
        let expected_ack_bits = (1 << subscriber_count) - 1;
        loop {
            let ack_bits = unsafe { (*self.buffer).subscriber_ack_bits.load(Ordering::Acquire) };
            if ack_bits == expected_ack_bits {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        // Set state to COMPLETED
        unsafe {
            (*self.buffer).broadcast_state.store(STATE_COMPLETED, Ordering::Release);
        }

        Ok(sequence)
    }

    /// Wait for all subscribers to be ready
    pub fn wait_for_subscribers(&self, expected_count: u32) -> Result<(), Box<dyn std::error::Error>> {
        let buffer = unsafe { &*self.buffer };

        while buffer.get_subscriber_count() < expected_count {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        Ok(())
    }
}

/// Subscriber for the shared memory broadcast buffer
pub struct BroadcastSubscriber {
    buffer: *mut SharedBroadcastBuffer,
    subscriber_id: usize,
    last_sequence: u64,
}

impl BroadcastSubscriber {
    /// Create a new subscriber for the given broadcast buffer
    pub fn new(buffer: *mut SharedBroadcastBuffer, subscriber_id: usize) -> Self {
        Self {
            buffer,
            subscriber_id,
            last_sequence: 0,
        }
    }

    /// Register this subscriber with the broadcast system
    pub fn register(&self) -> Result<(), Box<dyn std::error::Error>> {
        let buffer = unsafe { &*self.buffer };
        buffer.register_subscriber(self.subscriber_id)
    }

    /// Deregister this subscriber from the broadcast system
    pub fn deregister(&self) -> Result<(), Box<dyn std::error::Error>> {
        let buffer = unsafe { &*self.buffer };
        buffer.deregister_subscriber(self.subscriber_id)
    }

    /// Wait for and receive the next broadcast message
    pub fn receive(&mut self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let buffer = unsafe { &*self.buffer };

        // Wait for next broadcast to start
        loop {
            let state = buffer.get_broadcast_state();
            let sequence = buffer.get_sequence_number();

            if state == STATE_WAITING && sequence > self.last_sequence {
                // New broadcast available
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        // Read the broadcast data
        let length = unsafe { (*self.buffer).broadcast_length.load(Ordering::Acquire) };
        let mut data = vec![0u8; length as usize];

        unsafe {
            ptr::copy_nonoverlapping(
                (*self.buffer).broadcast_data.as_ptr(),
                data.as_mut_ptr(),
                length as usize
            );
        }

        // Acknowledge receipt
        let mask = 1 << self.subscriber_id;
        let mut ack_bits = unsafe { (*self.buffer).subscriber_ack_bits.load(Ordering::Acquire) };
        loop {
            let new_ack_bits = ack_bits | mask;
            match unsafe { (*self.buffer).subscriber_ack_bits.compare_exchange_weak(
                ack_bits,
                new_ack_bits,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) } {
                Ok(_) => break,
                Err(current) => ack_bits = current,
            }
        }

        self.last_sequence = buffer.get_sequence_number();
        Ok(data)
    }

    /// Get the current sequence number
    pub fn get_current_sequence(&self) -> u64 {
        let buffer = unsafe { &*self.buffer };
        buffer.get_sequence_number()
    }

    /// Check if a new message is available
    pub fn has_new_message(&self) -> bool {
        let buffer = unsafe { &*self.buffer };
        let state = buffer.get_broadcast_state();
        let sequence = buffer.get_sequence_number();

        state == STATE_WAITING && sequence > self.last_sequence
    }
}

impl Drop for BroadcastSubscriber {
    fn drop(&mut self) {
        // Auto-deregister on drop
        let _ = self.deregister();
    }
}

/// Clean up shared memory resources
pub fn cleanup_shared_memory(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let shm_name = if name.starts_with("/") {
        name.to_string()
    } else {
        format!("/{}", name)
    };

    shm_unlink(&shm_name.as_bytes())?;
    Ok(())
}