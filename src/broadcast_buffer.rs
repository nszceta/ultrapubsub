// Shared Memory Broadcast Buffer Implementation
// This module implements the synchronous broadcast architecture as specified
// in the OpenSpec change proposal for synchronous 1:N messaging.

use std::ptr;
use std::sync::atomic::{AtomicU64, AtomicU32, Ordering};
use std::os::fd::AsRawFd;
use nix::sys::mman::{shm_open, shm_unlink};
use nix::fcntl::OFlag;
use nix::sys::stat::Mode;
use nix::unistd::{ftruncate};
use libc::{mmap, MAP_FAILED, PROT_READ, PROT_WRITE, MAP_SHARED};

// Constants for the synchronous broadcast implementation
pub const BROADCAST_SLOT_SIZE: usize = 35 * 1024 * 1024;  // 35MB single broadcast slot
// Broadcast states
pub const MAX_DYNAMIC_SUBSCRIBERS: usize = 32;  // Maximum supported dynamic subscribers

// Broadcast states
pub const STATE_PUBLISHING: u32 = 0;
pub const STATE_WAITING: u32 = 1;
pub const STATE_COMPLETED: u32 = 2;


/// Shared memory broadcast buffer for synchronous 1:N messaging
///
/// This struct implements a synchronous broadcast mechanism where:
/// - All subscribers receive the SAME message from a single broadcast slot
/// - Publisher waits for ALL subscribers to complete before continuing
/// - Dynamic number of subscribers supported (up to 32) with explicit registration/deregistration
///
/// Memory Layout:
/// ```text
/// +--------------------------+
/// | subscriber_count: u32     | Number of registered subscribers (0-32)
/// | broadcast_state: u32      | Current broadcast state (PUBLISHING/WAITING/COMPLETED)
/// | sequence_number: u64      | Monotonically increasing sequence
/// | broadcast_length: u32     | Length of current broadcast data
/// +--------------------------+
/// | subscriber_ack_array[32]  | Boolean array for subscriber acknowledgments
/// | subscriber_reg_array[32]  | Boolean array for subscriber registration status
/// +--------------------------+
/// | broadcast_data[35MB]     | Single broadcast slot accessible to all
/// +--------------------------+
/// ```
#[repr(C)]
pub struct SharedBroadcastBuffer {
    // Synchronous broadcast state
    subscriber_count: AtomicU32,        // Number of registered subscribers (0-32)
    broadcast_state: AtomicU32,         // Current broadcast state
    sequence_number: AtomicU64,         // Monotonically increasing sequence number
    broadcast_length: AtomicU32,        // Length of data in broadcast slot
    max_subscriber_id: AtomicU32,       // Highest subscriber ID for optimization

    // Dynamic subscriber tracking arrays
    subscriber_ack_array: [AtomicU32; MAX_DYNAMIC_SUBSCRIBERS],  // Acknowledgment status per subscriber
    subscriber_reg_array: [AtomicU32; MAX_DYNAMIC_SUBSCRIBERS],  // Registration status per subscriber

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
        ftruncate(&fd, total_size as i64)?;

        // Map the shared memory object into process address space
        let ptr = unsafe {
            mmap(
                ptr::null_mut(),
                total_size,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };

        if ptr == MAP_FAILED {
            shm_unlink(shm_name.as_str())?;
            return Err("Failed to mmap shared memory".into());
        }

        // Initialize the broadcast buffer
        let buffer = ptr as *mut SharedBroadcastBuffer;
        unsafe {
            ptr::write_volatile(&mut (*buffer).subscriber_count as *mut AtomicU32, AtomicU32::new(0));
            ptr::write_volatile(&mut (*buffer).broadcast_state as *mut AtomicU32, AtomicU32::new(STATE_COMPLETED));
            ptr::write_volatile(&mut (*buffer).sequence_number as *mut AtomicU64, AtomicU64::new(0));
            ptr::write_volatile(&mut (*buffer).broadcast_length as *mut AtomicU32, AtomicU32::new(0));
            ptr::write_volatile(&mut (*buffer).max_subscriber_id as *mut AtomicU32, AtomicU32::new(0));

            // Initialize subscriber arrays
            for i in 0..MAX_DYNAMIC_SUBSCRIBERS {
                ptr::write_volatile(&mut (*buffer).subscriber_ack_array[i] as *mut AtomicU32, AtomicU32::new(0));
                ptr::write_volatile(&mut (*buffer).subscriber_reg_array[i] as *mut AtomicU32, AtomicU32::new(0));
            }
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
                fd.as_raw_fd(),
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
        if subscriber_id >= MAX_DYNAMIC_SUBSCRIBERS {
            return Err(format!("Subscriber ID must be 0-{}", MAX_DYNAMIC_SUBSCRIBERS - 1).into());
        }

        // Check if already registered
        let current_reg = self.subscriber_reg_array[subscriber_id].load(Ordering::Acquire);
        if current_reg != 0 {
            return Ok(());
        }

        // Set registration status
        self.subscriber_reg_array[subscriber_id].store(1, Ordering::Release);

        // Increment subscriber count
        self.subscriber_count.fetch_add(1, Ordering::AcqRel);

        // Update maximum subscriber ID
        let current_max = self.max_subscriber_id.load(Ordering::Acquire);
        if subscriber_id as u32 > current_max {
            self.max_subscriber_id.store(subscriber_id as u32, Ordering::Release);
        }
        Ok(())
    }

    /// Deregister a subscriber from the broadcast buffer
    pub fn deregister_subscriber(&self, subscriber_id: usize) -> Result<(), Box<dyn std::error::Error>> {
        if subscriber_id >= MAX_DYNAMIC_SUBSCRIBERS {
            return Err(format!("Subscriber ID must be 0-{}", MAX_DYNAMIC_SUBSCRIBERS - 1).into());
        }

        // Check if already deregistered
        let current_reg = self.subscriber_reg_array[subscriber_id].load(Ordering::Acquire);
        if current_reg == 0 {
            return Ok(());
        }

        // Clear registration and acknowledgment status
        self.subscriber_reg_array[subscriber_id].store(0, Ordering::Release);
        self.subscriber_ack_array[subscriber_id].store(0, Ordering::Release);

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
        loop {
            let mut acknowledged_count = 0;
            let max_id = unsafe { (*self.buffer).max_subscriber_id.load(Ordering::Acquire) } as usize;
            let check_limit = if max_id == 0 { MAX_DYNAMIC_SUBSCRIBERS } else { max_id + 1 };

            for i in 0..check_limit {
                let is_registered = unsafe { (*self.buffer).subscriber_reg_array[i].load(Ordering::Acquire) };
                let is_acknowledged = unsafe { (*self.buffer).subscriber_ack_array[i].load(Ordering::Acquire) };

                if is_registered != 0 && is_acknowledged != 0 {
                    acknowledged_count += 1;
                }
            }

            if acknowledged_count == subscriber_count {
                break;
            }

            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        // Reset acknowledgment bits for next broadcast
        let max_id = unsafe { (*self.buffer).max_subscriber_id.load(Ordering::Acquire) } as usize;
        let check_limit = if max_id == 0 { MAX_DYNAMIC_SUBSCRIBERS } else { max_id + 1 };
        for i in 0..check_limit {
            let is_registered = unsafe { (*self.buffer).subscriber_reg_array[i].load(Ordering::Acquire) };
            if is_registered != 0 {
                unsafe { (*self.buffer).subscriber_ack_array[i].store(0, Ordering::Release); }
            }
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

    /// Get the current number of registered subscribers
    pub fn subscriber_count(&self) -> usize {
        let buffer = unsafe { &*self.buffer };
        buffer.get_subscriber_count() as usize
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
        unsafe {
            (*self.buffer).subscriber_ack_array[self.subscriber_id].store(1, Ordering::Release);
        }

        self.last_sequence = buffer.get_sequence_number();
        Ok(data)
    }

    /// Check if a new message is available
    pub fn has_new_message(&self) -> bool {
        let buffer = unsafe { &*self.buffer };
        let state = buffer.get_broadcast_state();
        let sequence = buffer.get_sequence_number();

        state == STATE_WAITING && sequence > self.last_sequence
    }

    /// Get the last processed message sequence number
    pub fn last_processed(&self) -> u64 {
        self.last_sequence
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

    shm_unlink(shm_name.as_str())?;
    Ok(())
}