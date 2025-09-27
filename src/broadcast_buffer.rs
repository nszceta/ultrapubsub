// Shared Memory Broadcast Buffer Implementation
// This module implements the synchronous broadcast architecture as specified
// in the OpenSpec change proposal for synchronous 1:N messaging.

use std::ptr;
use std::mem::MaybeUninit;
use std::os::fd::AsRawFd;
use nix::sys::mman::{shm_open, shm_unlink};
use nix::fcntl::OFlag;
use nix::sys::stat::Mode;
use nix::unistd::{ftruncate};
use libc::{mmap, MAP_FAILED, PROT_READ, PROT_WRITE, MAP_SHARED};
use std::io::{self, Write};

// Pthread types and constants for fast cross-process synchronization
use libc::{
    pthread_mutex_t, pthread_mutexattr_t, pthread_mutex_init, pthread_mutex_destroy,
    pthread_mutex_lock, pthread_mutex_unlock, pthread_mutexattr_init, pthread_mutexattr_destroy,
    pthread_mutexattr_setpshared, PTHREAD_PROCESS_SHARED
};

// Futex types and constants for ultra-fast cross-process synchronization
use libc::{
    c_int, c_void, syscall,
    SYS_futex, FUTEX_WAIT, FUTEX_WAKE, FUTEX_PRIVATE_FLAG
};

// Debug macro for stdout flushing (required for PyO3 debug output)
macro_rules! debug_print {
    ($($arg:tt)*) => {
        // Disabled for performance testing
    };
}

// Futex wrapper functions for maximum performance
#[inline(always)]
unsafe fn futex_wait(futex_addr: *mut u32, expected: u32, timeout_ms: u32) -> c_int {
    debug_print!("FUTEX_WAIT: addr={:?}, expected={}, timeout={}ms", futex_addr, expected, timeout_ms);

    let timespec = libc::timespec {
        tv_sec: (timeout_ms / 1000) as i64,
        tv_nsec: ((timeout_ms % 1000) * 1_000_000) as i64,
    };

    let result = syscall(SYS_futex,
                        futex_addr as *const c_void,
                        FUTEX_WAIT | FUTEX_PRIVATE_FLAG,
                        expected as c_int,
                        &timespec as *const libc::timespec) as c_int;

    debug_print!("FUTEX_WAIT result: {} (errno: {})", result, std::io::Error::last_os_error().raw_os_error().unwrap_or(0));
    result
}

#[inline(always)]
unsafe fn futex_wake(futex_addr: *mut u32, count: c_int) -> c_int {
    debug_print!("FUTEX_WAKE: addr={:?}, count={}", futex_addr, count);

    let result = syscall(SYS_futex,
                        futex_addr as *const c_void,
                        FUTEX_WAKE | FUTEX_PRIVATE_FLAG,
                        count) as c_int;

    debug_print!("FUTEX_WAKE result: {} (errno: {})", result, std::io::Error::last_os_error().raw_os_error().unwrap_or(0));
    result
}

// Debug macro for stdout flushing (required for PyO3 debug output)
macro_rules! debug_print {
    ($($arg:tt)*) => {
        {
            print!("[DEBUG] ");
            print!($($arg)*);
            print!("\n");
            io::stdout().flush().unwrap();
        }
    };
}

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
/// Thread-safe scoped access to shared data using RAII
///
/// This macro provides automatic lock management with proper cleanup.
/// Usage: with_lock!(buffer, { /* code that accesses shared data */ })
macro_rules! with_lock {
    ($buffer:expr, $block:block) => {
        {
            let buffer = $buffer as *const _ as *const SharedBroadcastBuffer;
            unsafe {
                (*buffer).mutex.lock();
            }
            let result = (|| $block)();
            unsafe {
                (*buffer).mutex.unlock();
            }
            result
        }
    };
}

/// Thread-safe scoped access to shared data with mutable reference
///
/// This macro is used for methods that need to modify shared data.
macro_rules! with_lock_mut {
    ($buffer:expr, $block:block) => {
        {
            let buffer = $buffer as *const _ as *mut SharedBroadcastBuffer;
            unsafe {
                (*buffer).mutex.lock();
            }
            let result = (|| $block)();
            unsafe {
                (*buffer).mutex.unlock();
            }
            result
        }
    };
}

/// Fast cross-process mutex using pthread with PTHREAD_PROCESS_SHARED
///
/// This provides:
/// - Zero system calls in uncontended case (fast path)
/// - ~10-30 nanoseconds per lock/unlock
/// - Proper cross-process synchronization
/// - Cache-line alignment to avoid false sharing
#[repr(C, align(64))]
pub struct ProcessSharedMutex {
    mutex: pthread_mutex_t,
}

impl ProcessSharedMutex {
    /// Initialize a new process-shared mutex
    ///
    /// This should only be called once by the process that creates the shared memory.
    pub fn init(&mut self) {
        unsafe {
            let mut attr = MaybeUninit::<pthread_mutexattr_t>::uninit();
            pthread_mutexattr_init(attr.as_mut_ptr());
            pthread_mutexattr_setpshared(attr.as_mut_ptr(), PTHREAD_PROCESS_SHARED);
            pthread_mutex_init(&mut self.mutex, attr.as_ptr());
            pthread_mutexattr_destroy(attr.as_mut_ptr());
        }
    }

    /// Lock the mutex - fast path uses only atomic operations
    #[inline]
    pub fn lock(&self) {
        unsafe {
            pthread_mutex_lock(&self.mutex as *const _ as *mut _);
        }
    }

    /// Unlock the mutex
    #[inline]
    pub fn unlock(&self) {
        unsafe {
            pthread_mutex_unlock(&self.mutex as *const _ as *mut _);
        }
    }

    /// Clean up the mutex
    pub fn destroy(&mut self) {
        unsafe {
            pthread_mutex_destroy(&mut self.mutex);
        }
    }
}

/// High-performance shared memory broadcast buffer for synchronous 1:N messaging
///
/// This implementation uses pthread mutex with PTHREAD_PROCESS_SHARED for maximum performance:
/// - Uncontended lock/unlock: ~10-30 nanoseconds (zero syscalls)
/// - Contended case: uses futex syscall (still very fast)
/// - Cache-line aligned to avoid false sharing
/// - Minimal memory overhead
///
/// Performance Characteristics:
/// - Registration: ~50ns (mutex lock + increment + unlock)
/// - Broadcast: ~100ns + time for acknowledgments
/// - Receive: ~30ns (mutex lock + read + unlock)
///
/// Memory Layout:
/// ```text
/// +--------------------------+
/// | mutex[64]                | Cache-aligned pthread mutex
/// | subscriber_count: u32    | Number of registered subscribers
/// | broadcast_state: u32     | Current broadcast state
/// | sequence_number: u64     | Monotonically increasing sequence
/// | broadcast_length: u32    | Length of current broadcast data
/// | max_subscriber_id: u32   | Highest subscriber ID for optimization
/// +--------------------------+
/// | subscriber_ack_array[32] | Acknowledgment status per subscriber
/// | subscriber_reg_array[32] | Registration status per subscriber
/// +--------------------------+
/// | broadcast_data[35MB]    | Single broadcast slot accessible to all
/// +--------------------------+
/// ```
#[repr(C, align(64))]
pub struct SharedBroadcastBuffer {
    // Process-shared synchronization (cache-aligned)
    mutex: ProcessSharedMutex,

    // Synchronous broadcast state (packed for cache efficiency)
    subscriber_count: u32,        // Number of registered subscribers (0-32)
    broadcast_state: u32,         // Current broadcast state
    sequence_number: u64,         // Monotonically increasing sequence number
    broadcast_length: u32,        // Length of data in broadcast slot
    max_subscriber_id: u32,       // Highest subscriber ID for optimization

    // Dynamic subscriber tracking arrays
    subscriber_ack_array: [u32; MAX_DYNAMIC_SUBSCRIBERS],  // Acknowledgment status per subscriber
    subscriber_reg_array: [u32; MAX_DYNAMIC_SUBSCRIBERS],  // Registration status per subscriber

    // Futex-based acknowledgment system
    ack_count: u32,                    // Number of acknowledgments received
    ack_futex: u32,                    // Futex for acknowledgment notifications

    // Single broadcast slot accessible to all subscribers
    pub broadcast_data: [u8; BROADCAST_SLOT_SIZE],  // 35MB single broadcast slot
}

impl SharedBroadcastBuffer {
    /// Create a new shared memory broadcast buffer
    ///
    /// This function allocates and initializes a new shared memory region
    /// containing the broadcast buffer structure. It should be called by the publisher process.
    pub fn create(name: &str) -> Result<*mut SharedBroadcastBuffer, Box<dyn std::error::Error>> {
        debug_print!("SharedBroadcastBuffer::create() called with name: {}", name);

        // Calculate total shared memory size
        let total_size = std::mem::size_of::<SharedBroadcastBuffer>();
        debug_print!("  Total shared memory size: {} bytes", total_size);

        // Create shared memory object
        let shm_name = if name.starts_with("/") {
            name.to_string()
        } else {
            format!("/{}", name)
        };
        debug_print!("  Shared memory name: {}", shm_name);

        let fd = shm_open(
            shm_name.as_bytes(),
            OFlag::O_CREAT | OFlag::O_RDWR | OFlag::O_EXCL,
            Mode::S_IRUSR | Mode::S_IWUSR,
        )?;
        debug_print!("  shm_open successful, fd: {}", fd.as_raw_fd());

        // Set the size of the shared memory object
        ftruncate(&fd, total_size as i64)?;
        debug_print!("  ftruncate successful");

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
        debug_print!("  mmap result: {:p}", ptr);

        if ptr == MAP_FAILED {
            debug_print!("  mmap failed!");
            shm_unlink(shm_name.as_str())?;
            return Err("Failed to mmap shared memory".into());
        }

        // Initialize the broadcast buffer
        let buffer = ptr as *mut SharedBroadcastBuffer;
        debug_print!("  Initializing broadcast buffer at {:p}", buffer);

        unsafe {
            // Initialize the process-shared mutex first
            (*buffer).mutex.init();

            // Initialize broadcast state with mutex protection
            (*buffer).subscriber_count = 0;
            (*buffer).broadcast_state = STATE_COMPLETED;
            (*buffer).sequence_number = 0;
            (*buffer).broadcast_length = 0;
            (*buffer).max_subscriber_id = 0;
            // Initialize futex-based acknowledgment system
            (*buffer).ack_count = 0;
            (*buffer).ack_futex = 0;

            // Initialize subscriber arrays
            for i in 0..MAX_DYNAMIC_SUBSCRIBERS {
                (*buffer).subscriber_ack_array[i] = 0;
                (*buffer).subscriber_reg_array[i] = 0;
            }
        }

        debug_print!("  SharedBroadcastBuffer::create() completed successfully");
        Ok(buffer)
    }

    /// Connect to an existing shared memory broadcast buffer
    ///
    /// This function attaches to an existing shared memory region created by the publisher.
    pub fn connect(name: &str) -> Result<*mut SharedBroadcastBuffer, Box<dyn std::error::Error>> {
        debug_print!("SharedBroadcastBuffer::connect() called with name: {}", name);

        let total_size = std::mem::size_of::<SharedBroadcastBuffer>();
        debug_print!("  Total shared memory size: {} bytes", total_size);

        let shm_name = if name.starts_with("/") {
            name.to_string()
        } else {
            format!("/{}", name)
        };
        debug_print!("  Shared memory name: {}", shm_name);

        let fd = shm_open(
            shm_name.as_bytes(),
            OFlag::O_RDWR,
            Mode::S_IRUSR | Mode::S_IWUSR,
        )?;
        debug_print!("  shm_open successful, fd: {}", fd.as_raw_fd());
        debug_print!("  File descriptor type: {}", if fd.as_raw_fd() == 4 { "PUBLISHER" } else { "SUBSCRIBER" });

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
        debug_print!("  mmap result: {:p}", ptr);

        if ptr == MAP_FAILED {
            debug_print!("  mmap failed!");
            return Err("Failed to mmap shared memory".into());
        }

        debug_print!("  SharedBroadcastBuffer::connect() completed successfully");
        Ok(ptr as *mut SharedBroadcastBuffer)
    }

    /// Register a new subscriber with the broadcast buffer
    pub fn register_subscriber(&mut self, subscriber_id: usize) -> Result<(), Box<dyn std::error::Error>> {
        debug_print!("SharedBroadcastBuffer::register_subscriber() called with id: {}", subscriber_id);

        if subscriber_id >= MAX_DYNAMIC_SUBSCRIBERS {
            debug_print!("  ERROR: Subscriber ID {} too large (max: {})", subscriber_id, MAX_DYNAMIC_SUBSCRIBERS - 1);
            return Err(format!("Subscriber ID must be 0-{}", MAX_DYNAMIC_SUBSCRIBERS - 1).into());
        }

        // Check if already registered and register using mutex
        let already_registered = with_lock!(self, {
            let current_reg = self.subscriber_reg_array[subscriber_id];
            debug_print!("  Current registration status for subscriber {}: {}", subscriber_id, current_reg);
            if current_reg != 0u32 {
                debug_print!("  Subscriber {} already registered", subscriber_id);
                return true;
            }

            // Set registration status
            debug_print!("  Setting registration status for subscriber {} to 1", subscriber_id);
            self.subscriber_reg_array[subscriber_id] = 1;

            // Increment subscriber count
            let old_count = self.subscriber_count;
            self.subscriber_count = old_count + 1;
            debug_print!("  Incremented subscriber count: {} -> {}", old_count, old_count + 1);

            // Update maximum subscriber ID
            let current_max = self.max_subscriber_id;
            if subscriber_id as u32 > current_max {
                debug_print!("  Updating max subscriber ID: {} -> {}", current_max, subscriber_id);
                self.max_subscriber_id = subscriber_id as u32;
            } else {
                debug_print!("  Max subscriber ID remains: {} (subscriber_id: {})", current_max, subscriber_id);
            }

            false
        });

        if already_registered {
            return Ok(());
        }

        debug_print!("  SharedBroadcastBuffer::register_subscriber() completed successfully for subscriber {}", subscriber_id);
        Ok(())
    }

    /// Deregister a subscriber from the broadcast buffer
    pub fn deregister_subscriber(&mut self, subscriber_id: usize) -> Result<(), Box<dyn std::error::Error>> {
        if subscriber_id >= MAX_DYNAMIC_SUBSCRIBERS {
            return Err(format!("Subscriber ID must be 0-{}", MAX_DYNAMIC_SUBSCRIBERS - 1).into());
        }

        // Check if already deregistered and deregister using mutex
        with_lock_mut!(self, {
            let current_reg = self.subscriber_reg_array[subscriber_id];
            if current_reg == 0u32 {
                return;
            }

            // Clear registration and acknowledgment status
            self.subscriber_reg_array[subscriber_id] = 0;
            self.subscriber_ack_array[subscriber_id] = 0;

            // Decrement subscriber count
            self.subscriber_count = self.subscriber_count - 1;
        });
        Ok(())
    }

    /// Get the current number of registered subscribers
    pub fn get_subscriber_count(&self) -> u32 {
        with_lock!(self, {
            self.subscriber_count
        })
    }

    /// Get the current broadcast state
    pub fn get_broadcast_state(&self) -> u32 {
        with_lock!(self, {
            self.broadcast_state
        })
    }

    /// Get the current sequence number
    pub fn get_sequence_number(&self) -> u64 {
        with_lock!(self, {
            self.sequence_number
        })
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

    /// Register a new subscriber with the broadcast buffer
    pub fn register_subscriber(&mut self) -> usize {
        let buffer = unsafe { &mut *self.buffer };

        // Find first available subscriber ID using mutex protection
        for i in 0..MAX_DYNAMIC_SUBSCRIBERS {
            let available = with_lock!(buffer, {
                let is_registered = buffer.subscriber_reg_array[i];
                if is_registered == 0u32 {
                    // Register this subscriber
                    buffer.subscriber_reg_array[i] = 1;
                    buffer.subscriber_count += 1;

                    // Update maximum subscriber ID
                    if i as u32 > buffer.max_subscriber_id {
                        buffer.max_subscriber_id = i as u32;
                    }
                    true
                } else {
                    false
                }
            });

            if available {
                return i;
            }
        }

        panic!("Maximum subscribers ({}) reached", MAX_DYNAMIC_SUBSCRIBERS);
    }

    /// Broadcast a message to all subscribers synchronously
    ///
    /// This method will wait until ALL subscribers have acknowledged receipt
    /// of the message before returning.
    pub fn broadcast(&mut self, data: &[u8]) -> Result<u64, Box<dyn std::error::Error>> {
        let broadcast_start_time = std::time::Instant::now();
        debug_print!("BroadcastPublisher::broadcast() called");
        debug_print!("  Data size: {} bytes", data.len());

        if data.len() > BROADCAST_SLOT_SIZE {
            debug_print!("  ERROR: Message too large for broadcast slot");
            return Err("Message too large for broadcast slot".into());
        }

        let buffer = unsafe { &mut *self.buffer };
        debug_print!("  Current state: {}, sequence: {}, subscribers: {}",
                     buffer.get_broadcast_state(),
                     buffer.get_sequence_number(),
                     buffer.get_subscriber_count());

        // Wait for any previous broadcast to complete using futex
        debug_print!("  Waiting for previous broadcast to complete...");
        let wait_start = std::time::Instant::now();
        let mut wait_count = 0;
        while buffer.get_broadcast_state() != STATE_COMPLETED {
            wait_count += 1;
            if wait_count % 1000 == 0 {
                debug_print!("    Still waiting... (wait_count: {})", wait_count);
            }

            // Use futex to wait efficiently for state changes
            unsafe {
                let current_state = buffer.broadcast_state;
                // Wait for state to change from current state, with 10ms timeout
                let result = futex_wait(&mut (*self.buffer).broadcast_state as *mut u32, current_state, 10);
                if result == -1 {
                    // Futex failed or timed out, continue loop
                    let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                    if errno != libc::ETIMEDOUT {
                        debug_print!("    Futex wait error: {}", errno);
                    }
                }
            }
        }
        let wait_duration = wait_start.elapsed();
        debug_print!("  Previous broadcast completed after {}.{:03}ms (wait_count: {})",
                     wait_duration.as_millis(),
                     wait_duration.as_micros() % 1000,
                     wait_count);

        let sequence = buffer.get_sequence_number() + 1;
        let subscriber_count = buffer.get_subscriber_count();
        debug_print!("  New sequence: {}, subscriber_count: {}", sequence, subscriber_count);

        // If no subscribers, just update sequence and return
        if subscriber_count == 0 {
            debug_print!("  No subscribers, updating sequence and returning");
            with_lock_mut!(unsafe { &mut *self.buffer }, {
                unsafe {
                    (*self.buffer).sequence_number = sequence;
                }
            });
            return Ok(sequence);
        }

        // Set state to PUBLISHING
        debug_print!("  Setting state to PUBLISHING");
        with_lock_mut!(unsafe { &mut *self.buffer }, {
            unsafe {
                (*self.buffer).broadcast_state = STATE_PUBLISHING;
                (*self.buffer).broadcast_length = data.len() as u32;
            }
        });

        // Copy data to broadcast slot
        debug_print!("  Copying data to broadcast slot");
        unsafe {
            ptr::copy_nonoverlapping(
                data.as_ptr(),
                (*self.buffer).broadcast_data.as_mut_ptr(),
                data.len()
            );
        }

        // Set state to WAITING and update sequence
        debug_print!("  Setting state to WAITING and updating sequence");
        with_lock_mut!(unsafe { &mut *self.buffer }, {
            unsafe {
                (*self.buffer).sequence_number = sequence;
                (*self.buffer).broadcast_state = STATE_WAITING;
            }
        });

        // Wake subscribers waiting for sequence number changes
        debug_print!("    📢 PUBLISHER attempting to wake subscribers...");
        unsafe {
            // Use only the lower 32 bits for futex operations
            let seq_futex = &mut (*self.buffer).broadcast_state; // Reuse state field as sequence futex
            debug_print!("    📍 PUBLISHER wake addr: {:?}, current state: {}", seq_futex as *mut u32, (*self.buffer).broadcast_state);
            let wake_result = futex_wake(seq_futex as *mut u32, 32); // Wake up to 32 subscribers
            debug_print!("    📞 PUBLISHER futex_wake result: {}", wake_result);
            if wake_result == -1 {
                debug_print!("    ❌ PUBLISHER sequence futex wake error: {}", std::io::Error::last_os_error().raw_os_error().unwrap_or(0));
            } else {
                debug_print!("    ✅ PUBLISHER woke {} subscribers waiting for sequence change", wake_result);
            }
        }

        // Use single unsafe block to reduce repeated dereferencing
        let buffer = unsafe { &mut *self.buffer };

        // Wait for all subscribers to acknowledge using futex-based acknowledgment counter
        debug_print!("  Waiting for all subscribers to acknowledge...");
        let ack_wait_start = std::time::Instant::now();
        let mut ack_wait_count = 0;

        // Reset acknowledgment counter before waiting
        with_lock_mut!(buffer, {
            buffer.ack_count = 0;
        });

        loop {
            let ack_check_start = std::time::Instant::now();
            let acknowledged_count = with_lock!(buffer, {
                let mut count = 0;
                let max_id = buffer.max_subscriber_id as usize;
                let check_limit = if max_id == 0 { MAX_DYNAMIC_SUBSCRIBERS } else { max_id + 1 };

                for i in 0..check_limit {
                    let is_registered = buffer.subscriber_reg_array[i];
                    let is_acknowledged = buffer.subscriber_ack_array[i];

                    if is_registered != 0u32 && is_acknowledged != 0u32 {
                        count += 1;
                    }
                    if ack_wait_count % 1000 == 0 && i < 5 {
                        debug_print!("    Subscriber {}: registered={}, ack={}", i, is_registered, is_acknowledged);
                    }
                }
                count
            });
            let ack_check_duration = ack_check_start.elapsed();

            if acknowledged_count == subscriber_count {
                debug_print!("  All {} subscribers acknowledged!", acknowledged_count);
                break;
            }

            ack_wait_count += 1;
            if ack_wait_count % 1000 == 0 {
                let ack_wait_duration = ack_wait_start.elapsed();
                debug_print!("    Waiting for acks... acknowledged: {}/{}, wait_count: {}, total_wait: {}.{:03}ms, last_check: {}.{:03}ms",
                             acknowledged_count, subscriber_count, ack_wait_count,
                             ack_wait_duration.as_millis(), ack_wait_duration.as_micros() % 1000,
                             ack_check_duration.as_micros() / 1000, ack_check_duration.as_micros() % 1000);
            }

            // Use futex to wait efficiently for acknowledgment counter updates
            debug_print!("    🔄 PUBLISHER calling futex_wait on ack_futex...");
            unsafe {
                let current_ack_count = buffer.ack_count;
                debug_print!("    📍 PUBLISHER current ack_count: {}, ack_futex: {}, addr: {:?}",
                             current_ack_count, buffer.ack_futex, &mut (*self.buffer).ack_futex as *mut u32);
                // Wait for ack_count to change from current value, with 5ms timeout
                let result = futex_wait(&mut (*self.buffer).ack_futex as *mut u32, current_ack_count, 5);
                debug_print!("    📞 PUBLISHER ack futex wait returned: {}", result);
                if result == -1 {
                    // Futex failed or timed out, continue loop
                    let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                    debug_print!("    ⚠️  PUBLISHER ack futex wait error: {} (ETIMEDOUT={})", errno, libc::ETIMEDOUT);
                    if errno != libc::ETIMEDOUT {
                        debug_print!("    ❌ PUBLISHER unexpected ack futex error: {}", errno);
                    }
                } else {
                    debug_print!("    ✅ PUBLISHER ack futex wait successful, checking acks again...");
                }
            }
            debug_print!("    🔍 PUBLISHER after ack futex wait: ack_count={}, ack_futex={}",
                         buffer.ack_count, buffer.ack_futex);
        }
        let total_ack_wait_duration = ack_wait_start.elapsed();
        debug_print!("  Total acknowledgment wait time: {}.{:03}ms ({} iterations)",
                     total_ack_wait_duration.as_millis(),
                     total_ack_wait_duration.as_micros() % 1000,
                     ack_wait_count);

        // Reset acknowledgment bits for next broadcast
        debug_print!("  Resetting acknowledgment bits");
        with_lock_mut!(buffer, {
            let max_id = buffer.max_subscriber_id as usize;
            let check_limit = if max_id == 0 { MAX_DYNAMIC_SUBSCRIBERS } else { max_id + 1 };
            for i in 0..check_limit {
                if buffer.subscriber_reg_array[i] != 0u32 {
                    buffer.subscriber_ack_array[i] = 0;
                }
            }
        });

        // Reset acknowledgment bits for next broadcast
        debug_print!("  Resetting acknowledgment bits");
        let reset_start = std::time::Instant::now();
        with_lock_mut!(buffer, {
            let max_id = buffer.max_subscriber_id as usize;
            let check_limit = if max_id == 0 { MAX_DYNAMIC_SUBSCRIBERS } else { max_id + 1 };
            for i in 0..check_limit {
                if buffer.subscriber_reg_array[i] != 0u32 {
                    buffer.subscriber_ack_array[i] = 0;
                }
            }
        });
        let reset_duration = reset_start.elapsed();
        debug_print!("  Acknowledgment reset took {}.{:03}ms",
                     reset_duration.as_millis(),
                     reset_duration.as_micros() % 1000);

        // Set state to COMPLETED
        debug_print!("  Setting state to COMPLETED");
        with_lock_mut!(buffer, {
            buffer.broadcast_state = STATE_COMPLETED;
        });

        // Wake any publishers waiting for state changes
        unsafe {
            let wake_result = futex_wake(&mut (*self.buffer).broadcast_state as *mut u32, 1);
            if wake_result == -1 {
                debug_print!("    State futex wake error: {}", std::io::Error::last_os_error().raw_os_error().unwrap_or(0));
            }
        }

        let total_broadcast_duration = broadcast_start_time.elapsed();
        debug_print!("  BroadcastPublisher::broadcast() completed successfully in {}.{:03}ms",
                     total_broadcast_duration.as_millis(),
                     total_broadcast_duration.as_micros() % 1000);
        Ok(sequence)
    }

    /// Wait for all subscribers to be ready
    pub fn wait_for_subscribers(&self, expected_count: u32) -> Result<(), Box<dyn std::error::Error>> {
        let buffer = unsafe { &mut *self.buffer };

        while buffer.get_subscriber_count() < expected_count {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        Ok(())
    }

    /// Get the current number of registered subscribers
    pub fn subscriber_count(&self) -> usize {
        let buffer = unsafe { &mut *self.buffer };
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
        let buffer = unsafe { &mut *self.buffer };
        buffer.register_subscriber(self.subscriber_id)
    }

    /// Deregister this subscriber from the broadcast system
    pub fn deregister(&self) -> Result<(), Box<dyn std::error::Error>> {
        let buffer = unsafe { &mut *self.buffer };
        buffer.deregister_subscriber(self.subscriber_id)
    }

    /// Wait for and receive the next broadcast message
    pub fn receive(&mut self) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        self.receive_with_timeout(None)
    }

    /// Wait for and receive the next broadcast message with optional timeout
    pub fn receive_with_timeout(&mut self, timeout_seconds: Option<f64>) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        debug_print!("BroadcastSubscriber::receive_with_timeout() called for subscriber {} with timeout: {:?}", self.subscriber_id, timeout_seconds);
        let buffer = unsafe { &mut *self.buffer };
        debug_print!("  Current state: {}, sequence: {}, last_sequence: {}",
                     buffer.get_broadcast_state(),
                     buffer.get_sequence_number(),
                     self.last_sequence);

        let start_time = std::time::Instant::now();
        let timeout_duration = timeout_seconds.map(|s| std::time::Duration::from_secs_f64(s));

        // Wait for next broadcast to start
        debug_print!("  Waiting for next broadcast to start...");
        let mut wait_count = 0;
        loop {
            let state = buffer.get_broadcast_state();
            let sequence = buffer.get_sequence_number();

            // Check if there's a new sequence, regardless of state
            // This handles the case where broadcast completes immediately (no subscribers)
            if sequence > self.last_sequence {
                debug_print!("  New broadcast available! state={}, sequence={}", state, sequence);
                break;
            }

            // Check timeout
            if let Some(timeout_dur) = timeout_duration {
                if start_time.elapsed() >= timeout_dur {
                    debug_print!("  Timeout waiting for broadcast");
                    return Err("Timeout waiting for broadcast".into());
                }
            }

            wait_count += 1;
            if wait_count % 100 == 0 {
                debug_print!("    ⏳ Still waiting for broadcast... state={}, sequence={}, wait_count={}",
                             state, sequence, wait_count);
            }

            // Use futex to wait efficiently for state changes (sequence changes trigger state changes)
            debug_print!("    🔄 SUBSCRIBER calling futex_wait on broadcast_state...");
            unsafe {
                let current_state = buffer.broadcast_state;
                debug_print!("    📍 SUBSCRIBER current state value: {}, addr: {:?}",
                             current_state, &mut (*self.buffer).broadcast_state as *mut u32);
                // Wait for state to change from current value, with 10ms timeout
                let result = futex_wait(&mut (*self.buffer).broadcast_state as *mut u32, current_state, 10);
                debug_print!("    📞 SUBSCRIBER futex wait returned: {}", result);
                if result == -1 {
                    // Futex failed or timed out, continue loop
                    let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                    debug_print!("    ⚠️  SUBSCRIBER futex wait error: {} (ETIMEDOUT={})", errno, libc::ETIMEDOUT);
                    if errno != libc::ETIMEDOUT {
                        debug_print!("    ❌ SUBSCRIBER unexpected futex error: {}", errno);
                    }
                } else {
                    debug_print!("    ✅ SUBSCRIBER futex wait successful, checking state again...");
                }
            }
            debug_print!("    🔍 SUBSCRIBER after futex wait: state={}, sequence={}",
                         buffer.get_broadcast_state(),
                         buffer.get_sequence_number());
        }

        // Read the broadcast data using the buffer reference we already have
        let length = with_lock!(buffer, { buffer.broadcast_length });
        debug_print!("  Reading broadcast data of length: {}", length);
        let mut data = vec![0u8; length as usize];

        // Copy data from broadcast slot
        unsafe {
            ptr::copy_nonoverlapping(
                buffer.broadcast_data.as_ptr(),
                data.as_mut_ptr(),
                length as usize
            );
        }
        debug_print!("  Data copied successfully");

        // Acknowledge receipt
        debug_print!("  Acknowledging receipt for subscriber {}", self.subscriber_id);
        debug_print!("    Before ack - subscriber {} registered: {}, ack: {}",
                     self.subscriber_id,
                     with_lock!(buffer, { buffer.subscriber_reg_array[self.subscriber_id] }),
                     with_lock!(buffer, { buffer.subscriber_ack_array[self.subscriber_id] }));

        debug_print!("    🔔 SUBSCRIBER acknowledging receipt...");
        with_lock_mut!(buffer, {
            buffer.subscriber_ack_array[self.subscriber_id] = 1;
            // Increment acknowledgment counter and wake futex
            buffer.ack_count += 1;
            let new_ack_count = buffer.ack_count;
            // Update futex value to wake waiting publisher
            buffer.ack_futex = new_ack_count;
            debug_print!("    📍 SUBSCRIBER set ack_count={}, ack_futex={}", new_ack_count, buffer.ack_futex);
        });

        // Wake the publisher waiting for acknowledgments
        debug_print!("    📢 SUBSCRIBER attempting to wake publisher...");
        unsafe {
            let wake_addr = &mut (*self.buffer).ack_futex as *mut u32;
            debug_print!("    📍 SUBSCRIBER wake addr: {:?}, current ack_futex: {}", wake_addr, (*self.buffer).ack_futex);
            let wake_result = futex_wake(wake_addr, 1);
            debug_print!("    📞 SUBSCRIBER futex_wake result: {}", wake_result);
            if wake_result == -1 {
                debug_print!("    ❌ SUBSCRIBER futex wake error: {}", std::io::Error::last_os_error().raw_os_error().unwrap_or(0));
            } else {
                debug_print!("    ✅ SUBSCRIBER woke {} publishers", wake_result);
            }
        }

        debug_print!("    After ack - subscriber {} registered: {}, ack: {}",
                     self.subscriber_id,
                     with_lock!(buffer, { buffer.subscriber_reg_array[self.subscriber_id] }),
                     with_lock!(buffer, { buffer.subscriber_ack_array[self.subscriber_id] }));

        self.last_sequence = buffer.get_sequence_number();
        debug_print!("  BroadcastSubscriber::receive_with_timeout() completed successfully, new last_sequence: {}", self.last_sequence);
        Ok(data)
    }

    /// Check if a new message is available
    pub fn has_new_message(&self) -> bool {
        let buffer = unsafe { &mut *self.buffer };
        let state = buffer.get_broadcast_state();
        let sequence = buffer.get_sequence_number();

        let has_new = state == STATE_WAITING && sequence > self.last_sequence;
        debug_print!("BroadcastSubscriber::has_new_message() for subscriber {}: state={}, sequence={}, last_sequence={}, has_new={}",
                     self.subscriber_id, state, sequence, self.last_sequence, has_new);
        has_new
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