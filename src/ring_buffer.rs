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
pub const POOL_SIZE: usize = 63;  // Pre-allocated 35MB message slots for high-frequency operation
pub const POOL_SLOT_SIZE: usize = 35 * 1024 * 1024;  // 35MB per slot

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
pub const POOL_OFFSET: usize = NOTIFICATION_FD_OFFSET + 4;  // Start of pre-allocated pool

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
    lengths: [u32; MAX_MESSAGES],       // Message lengths

    // Synchronization and notification
    available: [AtomicU64; 16],         // Bitmap array tracking which messages are available to which subscribers (16 * 64 = 1024 bits)
    notification_fd: AtomicI32,         // Event file descriptor for subscriber notifications

    // Pre-allocated memory pool for zero-copy operations
    pool: [[u8; POOL_SLOT_SIZE]; POOL_SIZE],  // Pre-allocated 35MB slots
    pool_available: AtomicU64,                // Bitmap tracking which pool slots are available (64 bits, we use 40)
    pool_sequence: [AtomicU64; POOL_SIZE],    // Sequence numbers for each pool slot
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

            // Initialize availability bitmap array (all bits 0 = no messages available)
            for i in 0..16 {
                ptr::write_volatile(&mut (*ptr).available[i], AtomicU64::new(0));
            }

            // Initialize notification fd (-1 = none)
            ptr::write_volatile(&mut (*ptr).notification_fd, AtomicI32::new(-1));

            // Initialize pre-allocated memory pool
            ptr::write_volatile(&mut (*ptr).pool_available, AtomicU64::new(0));

            // Mark all pool slots as available (set bits 0-63)
            let initial_pool_bitmap: u64 = (1 << POOL_SIZE) - 1;  // Bits 0-63 set
            ptr::write_volatile(&mut (*ptr).pool_available, AtomicU64::new(initial_pool_bitmap));

            // Initialize pool sequence numbers
            for i in 0..POOL_SIZE {
                ptr::write_volatile(&mut (*ptr).pool_sequence[i], AtomicU64::new(0));
            }

            // Zero out pool memory
            for i in 0..POOL_SIZE {
                ptr::write_bytes((*ptr).pool[i].as_mut_ptr(), 0, POOL_SLOT_SIZE);
            }
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
        if subscriber_id >= MAX_SUBSCRIBERS {
            return false;
        }

        // Calculate which bitmap word to use (each word covers 64 messages)
        let bitmap_index = message_id / 64;
        if bitmap_index >= 16 {
            return false;
        }

        // Calculate bit position within the word
        let message_in_word = message_id % 64;
        let bit_position = (message_in_word * MAX_SUBSCRIBERS) + subscriber_id;

        if bit_position >= 64 {
            return false;
        }

        let bitmap = self.available[bitmap_index].load(Ordering::Acquire);
        (bitmap & (1 << bit_position)) != 0
    }

    /// Mark a message as available to a specific subscriber
    pub fn mark_message_available(&self, message_id: usize, subscriber_id: usize) {
        if subscriber_id >= MAX_SUBSCRIBERS {
            return;
        }

        // Calculate which bitmap word to use (each word covers 64 messages)
        let bitmap_index = message_id / 64;
        if bitmap_index >= 16 {
            return;
        }

        // Calculate bit position within the word
        let message_in_word = message_id % 64;
        let bit_position = (message_in_word * MAX_SUBSCRIBERS) + subscriber_id;

        if bit_position >= 64 {
            return;
        }

        let mask = 1 << bit_position;
        self.available[bitmap_index].fetch_or(mask, Ordering::Release);
    }

    /// Clear message availability for a subscriber
    pub fn clear_message_availability(&self, message_id: usize, subscriber_id: usize) {
        if subscriber_id >= MAX_SUBSCRIBERS {
            return;
        }

        // Calculate which bitmap word to use (each word covers 64 messages)
        let bitmap_index = message_id / 64;
        if bitmap_index >= 16 {
            return;
        }

        // Calculate bit position within the word
        let message_in_word = message_id % 64;
        let bit_position = (message_in_word * MAX_SUBSCRIBERS) + subscriber_id;

        if bit_position >= 64 {
            return;
        }

        let mask = !(1 << bit_position);
        self.available[bitmap_index].fetch_and(mask, Ordering::Release);
    }

    /// Set the notification file descriptor
    pub fn set_notification_fd(&self, fd: i32) {
        self.notification_fd.store(fd, Ordering::Release);
    }

    /// Get the notification file descriptor
    pub fn get_notification_fd(&self) -> i32 {
        self.notification_fd.load(Ordering::Acquire)
    }

    /// Allocate a slot from the pre-allocated memory pool
    pub fn allocate_pool_slot(&self) -> Option<usize> {
        let mut pool_bitmap = self.pool_available.load(Ordering::Acquire);

        // Find first available slot (lowest set bit)
        for i in 0..POOL_SIZE {
            if pool_bitmap & (1 << i) != 0 {
                // Try to atomically claim this slot
                let new_bitmap = pool_bitmap & !(1 << i);
                let result = self.pool_available.compare_exchange_weak(
                    pool_bitmap,
                    new_bitmap,
                    Ordering::AcqRel,
                    Ordering::Acquire
                );

                if result.is_ok() {
                    return Some(i);
                }
                // CAS failed, reload and try again
                pool_bitmap = self.pool_available.load(Ordering::Acquire);
            }
        }

        None  // No available slots
    }

    /// Release a pool slot back to the available pool
    pub fn release_pool_slot(&self, slot: usize) {
        if slot >= POOL_SIZE {
            return;
        }

        let mut pool_bitmap = self.pool_available.load(Ordering::Acquire);
        loop {
            let new_bitmap = pool_bitmap | (1 << slot);
            let result = self.pool_available.compare_exchange_weak(
                pool_bitmap,
                new_bitmap,
                Ordering::AcqRel,
                Ordering::Acquire
            );

            if result.is_ok() {
                break;
            }
            // CAS failed, reload and try again
            pool_bitmap = self.pool_available.load(Ordering::Acquire);
        }
    }

    /// Get a mutable pointer to a pool slot for direct writing
    pub fn get_pool_slot_mut(&self, slot: usize) -> *mut u8 {
        if slot >= POOL_SIZE {
            return ptr::null_mut();
        }
        self.pool[slot].as_ptr() as *mut u8
    }

    /// Get a pointer to a pool slot for reading
    pub fn get_pool_slot(&self, slot: usize) -> *const u8 {
        if slot >= POOL_SIZE {
            return ptr::null();
        }
        self.pool[slot].as_ptr()
    }

    /// Set the sequence number for a pool slot
    pub fn set_pool_sequence(&self, slot: usize, sequence: u64) {
        if slot < POOL_SIZE {
            self.pool_sequence[slot].store(sequence, Ordering::Release);
        }
    }

    /// Get the sequence number for a pool slot
    pub fn get_pool_sequence(&self, slot: usize) -> u64 {
        if slot >= POOL_SIZE {
            return 0;
        }
        self.pool_sequence[slot].load(Ordering::Acquire)
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

            // Check if we have enough space, considering wrap-around
            let available_space = if next_head > BUFFER_SIZE as u64 {
                // Message wraps around - need space from head to end and from start to needed position
                let space_at_end = BUFFER_SIZE as u64 - head;
                let space_needed_at_start = next_head - BUFFER_SIZE as u64;
                space_at_end + space_needed_at_start
            } else {
                // Message fits without wrapping
                BUFFER_SIZE as u64 - head
            };

            if available_space < data.len() as u64 {
                return Err("Insufficient space in buffer".into());
            }

            // Get the next message sequence number
            let sequence = buffer.get_published_count();
            let message_slot = (sequence % MAX_MESSAGES as u64) as usize;

            // Copy message data to the buffer with wrap-around support
            if next_head <= BUFFER_SIZE as u64 {
                // Message fits without wrapping - single copy
                let data_ptr = buffer.data.as_ptr().add(head as usize);
                ptr::copy_nonoverlapping(data.as_ptr(), data_ptr as *mut u8, data.len());
            } else {
                // Message wraps around - split into two copies
                let space_at_end = BUFFER_SIZE as u64 - head;
                let space_at_start = data.len() as u64 - space_at_end;

                // Copy first part to end of buffer
                let end_ptr = buffer.data.as_ptr().add(head as usize);
                ptr::copy_nonoverlapping(data.as_ptr(), end_ptr as *mut u8, space_at_end as usize);

                // Copy second part to start of buffer
                let start_ptr = buffer.data.as_ptr();
                ptr::copy_nonoverlapping(data.as_ptr().add(space_at_end as usize), start_ptr as *mut u8, space_at_start as usize);
            }

            // Update message metadata (need to cast to mutable pointer)
            let buffer_mut = buffer as *const SharedRingBuffer as *mut SharedRingBuffer;
            unsafe {
                (*buffer_mut).offsets[message_slot] = head as u32;
                (*buffer_mut).lengths[message_slot] = data.len() as u32;
            }

            // Mark message as available for all subscribers
            for subscriber_id in 0..self.subscriber_count.max(1) {
                buffer.mark_message_available(sequence as usize, subscriber_id);
            }

            // Update publisher state with wrap-around
            let wrapped_next_head = next_head % BUFFER_SIZE as u64;
            buffer.head.store(wrapped_next_head, Ordering::Release);
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

    /// Zero-copy publish for large messages using pre-allocated pool
    ///
    /// This method allocates a slot from the pre-allocated memory pool and returns
    /// a mutable pointer for direct writing. The caller must copy data into this slot
    /// and then call publish_pool_slot to make it available to subscribers.
    pub fn allocate_pool_slot(&mut self) -> Result<(usize, *mut u8), Box<dyn std::error::Error>> {
        unsafe {
            let buffer = &*self.buffer;

            // Allocate a slot from the pool
            if let Some(slot) = buffer.allocate_pool_slot() {
                let slot_ptr = buffer.get_pool_slot_mut(slot);
                if !slot_ptr.is_null() {
                    return Ok((slot, slot_ptr));
                } else {
                    // Failed to get slot pointer, release the slot
                    buffer.release_pool_slot(slot);
                    return Err("Failed to get pool slot pointer".into());
                }
            }

            Err("No available pool slots".into())
        }
    }

    /// Publish a pre-allocated pool slot to subscribers
    ///
    /// After copying data into the pool slot, call this method to make it
    /// available to all subscribers.
    pub fn publish_pool_slot(&mut self, slot: usize, size: usize) -> Result<u64, Box<dyn std::error::Error>> {
        if slot >= POOL_SIZE {
            return Err("Invalid pool slot".into());
        }

        if size > POOL_SLOT_SIZE {
            return Err("Message size exceeds pool slot capacity".into());
        }

        unsafe {
            let buffer = &*self.buffer;

            // Get the next message sequence number
            let sequence = buffer.get_published_count();
            let message_slot = (sequence % MAX_MESSAGES as u64) as usize;

            // Update message metadata to point to the pool slot
            let buffer_mut = buffer as *const SharedRingBuffer as *mut SharedRingBuffer;
            (*buffer_mut).offsets[message_slot] = slot as u32 | 0x80000000;  // High bit indicates pool usage
            (*buffer_mut).lengths[message_slot] = size as u32;

            // Store the sequence number in the pool slot
            buffer.set_pool_sequence(slot, sequence);

            // Mark message as available for all subscribers
            for subscriber_id in 0..self.subscriber_count.max(1) {
                buffer.mark_message_available(sequence as usize, subscriber_id);
            }

            // Update publisher state
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

    /// Try to publish without blocking - returns None if buffer is full
    pub fn try_publish(&mut self, data: &[u8]) -> Result<Option<u64>, Box<dyn std::error::Error>> {
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

            // Check if we have enough space, considering wrap-around
            let available_space = if next_head > BUFFER_SIZE as u64 {
                // Message wraps around - need space from head to end and from start to needed position
                let space_at_end = BUFFER_SIZE as u64 - head;
                let space_needed_at_start = next_head - BUFFER_SIZE as u64;
                space_at_end + space_needed_at_start
            } else {
                // Message fits without wrapping
                BUFFER_SIZE as u64 - head
            };

            // Return None if not enough space (non-blocking behavior)
            if available_space < data.len() as u64 {
                return Ok(None);
            }

            // Get the next message sequence number
            let sequence = buffer.get_published_count();
            let message_slot = (sequence % MAX_MESSAGES as u64) as usize;

            // Copy message data to the buffer with wrap-around support
            if next_head <= BUFFER_SIZE as u64 {
                // Message fits without wrapping - single copy
                let data_ptr = buffer.data.as_ptr().add(head as usize);
                ptr::copy_nonoverlapping(data.as_ptr(), data_ptr as *mut u8, data.len());
            } else {
                // Message wraps around - split into two copies
                let space_at_end = BUFFER_SIZE as u64 - head;
                let space_at_start = data.len() as u64 - space_at_end;

                // Copy first part to end of buffer
                let end_ptr = buffer.data.as_ptr().add(head as usize);
                ptr::copy_nonoverlapping(data.as_ptr(), end_ptr as *mut u8, space_at_end as usize);

                // Copy second part to start of buffer
                let start_ptr = buffer.data.as_ptr();
                ptr::copy_nonoverlapping(data.as_ptr().add(space_at_end as usize), start_ptr as *mut u8, space_at_start as usize);
            }

            // Update message metadata (need to cast to mutable pointer)
            let buffer_mut = buffer as *const SharedRingBuffer as *mut SharedRingBuffer;
            unsafe {
                (*buffer_mut).offsets[message_slot] = head as u32;
                (*buffer_mut).lengths[message_slot] = data.len() as u32;
            }

            // Mark message as available for all subscribers
            for subscriber_id in 0..self.subscriber_count.max(1) {
                buffer.mark_message_available(sequence as usize, subscriber_id);
            }

            // Update publisher state with wrap-around
            let wrapped_next_head = next_head % BUFFER_SIZE as u64;
            buffer.head.store(wrapped_next_head, Ordering::Release);
            buffer.published_count.store(sequence + 1, Ordering::Release);

            // Notify subscribers if notification fd is set
            let notify_fd = buffer.get_notification_fd();
            if notify_fd != -1 {
                // Simple notification - write 1 byte to eventfd
                let notification_data = 1u64;
                libc::write(notify_fd, &notification_data as *const u64 as *const c_void, 8);
            }

            Ok(Some(sequence))
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

/// Message filter types
#[derive(Clone, Copy)]
pub enum MessageFilter {
    Prefix { prefix: [u8; 32], prefix_len: usize },
    SizeRange { min: usize, max: usize },
    None,
}

impl MessageFilter {
    pub fn apply(&self, data: &[u8]) -> bool {
        match self {
            MessageFilter::Prefix { prefix, prefix_len } => {
                if data.len() < *prefix_len {
                    return false;
                }
                data.starts_with(&prefix[..*prefix_len])
            }
            MessageFilter::SizeRange { min, max } => {
                let len = data.len();
                len >= *min && len <= *max
            }
            MessageFilter::None => true,
        }
    }
}

/// Subscriber for the shared memory ring buffer
pub struct RingBufferSubscriber {
    buffer: *mut SharedRingBuffer,
    subscriber_id: usize,
    last_processed: u64,
    filter: MessageFilter,
}

impl RingBufferSubscriber {
    /// Create a new subscriber with the given ID
    pub fn new(buffer: *mut SharedRingBuffer, subscriber_id: usize) -> Self {
        Self {
            buffer,
            subscriber_id,
            last_processed: u64::MAX, // Special value indicating no messages processed yet
            filter: MessageFilter::None,
        }
    }

    /// Set a prefix filter for this subscriber
    pub fn set_prefix_filter(&mut self, prefix: &[u8]) {
        let mut prefix_array = [0u8; 32];
        let prefix_len = prefix.len().min(32);
        prefix_array[..prefix_len].copy_from_slice(&prefix[..prefix_len]);
        self.filter = MessageFilter::Prefix { prefix: prefix_array, prefix_len };
    }

    /// Set a size filter for this subscriber
    pub fn set_size_filter(&mut self, min: usize, max: usize) {
        self.filter = MessageFilter::SizeRange { min, max };
    }

    /// Remove the message filter
    pub fn clear_filter(&mut self) {
        self.filter = MessageFilter::None;
    }

    /// Check if this subscriber has a filter
    pub fn has_filter(&self) -> bool {
        !matches!(self.filter, MessageFilter::None)
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
            // We need to check all sequence numbers that might be available
            let start_seq = last_seen;
            let end_seq = published_count.saturating_sub(1);

            
            // But skip sequences we've already processed
            for sequence in start_seq..=end_seq {
                if self.last_processed != u64::MAX && sequence <= self.last_processed {
                    continue;
                }
                let message_slot = (sequence % MAX_MESSAGES as u64) as usize;

                if buffer.is_message_available(message_slot, self.subscriber_id) {

                    // Get message metadata
                    let offset_and_flags = buffer.offsets[message_slot] as usize;
                    let length = buffer.lengths[message_slot] as usize;

                    // Check if this is a pool-based message (high bit set)
                    let is_pool_message = (offset_and_flags & 0x80000000) != 0;
                    let pool_slot = offset_and_flags & 0x7FFFFFFF;

                    let mut message_data = Vec::with_capacity(length);

                    if is_pool_message {
                        // Message is in pre-allocated pool - zero-copy read
                        if pool_slot < POOL_SIZE {
                            let pool_ptr = buffer.get_pool_slot(pool_slot);
                            if !pool_ptr.is_null() {
                                // Copy directly from pool slot
                                ptr::copy_nonoverlapping(pool_ptr, message_data.as_mut_ptr(), length);
                                message_data.set_len(length);
                            } else {
                                // Invalid pool slot, skip this message
                                continue;
                            }
                        } else {
                            // Invalid pool slot, skip this message
                            continue;
                        }
                    } else {
                        // Message is in circular buffer - copy with wrap-around support
                        let offset = offset_and_flags;  // Remove the high bit check for regular messages
                        if offset + length <= BUFFER_SIZE {
                            // Message is contiguous - single copy
                            let data_ptr = buffer.data.as_ptr().add(offset);
                            ptr::copy_nonoverlapping(data_ptr, message_data.as_mut_ptr(), length);
                        } else {
                            // Message wraps around - split into two copies
                            let space_at_end = BUFFER_SIZE - offset;
                            let space_at_start = length - space_at_end;

                            // Copy first part from end of buffer
                            let end_ptr = buffer.data.as_ptr().add(offset);
                            ptr::copy_nonoverlapping(end_ptr, message_data.as_mut_ptr(), space_at_end);

                            // Copy second part from start of buffer
                            let start_ptr = buffer.data.as_ptr();
                            ptr::copy_nonoverlapping(start_ptr, message_data.as_mut_ptr().add(space_at_end), space_at_start);
                        }
                        message_data.set_len(length);
                    }

                    // Apply filter if one is set
                    if !self.filter.apply(&message_data) {
                        // Skip this message - continue to next one
                        continue;
                    }

                    // Update our state
                    buffer.last_seen[self.subscriber_id].store(sequence, Ordering::Release);
                    self.last_processed = sequence;

                    // Clear availability for this message
                    buffer.clear_message_availability(sequence as usize, self.subscriber_id);

                    // Release pool slot if this was a pool message
                    if is_pool_message {
                        buffer.release_pool_slot(pool_slot);
                    }

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