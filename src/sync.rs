// Minimal Synchronization Module for UltraPubSub
// This module provides only the essential futex-based synchronization primitives
// for coordinating zero-copy numpy array broadcasting across processes.

use std::fs::{OpenOptions, File};
use std::io::{Read, Write, Seek};
use libc::{c_int, c_void, syscall, SYS_futex, FUTEX_WAIT, FUTEX_WAKE, FUTEX_PRIVATE_FLAG};

/// Simple state file for cross-process coordination
struct StateFile {
    file: File,
    path: String,
}

impl StateFile {
    fn create(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let path = format!("/tmp/ultrapubsub_{}.state", name);

        // Create and initialize state file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        file.set_len(16)?; // 4 words: state, seq, ack_count, subscriber_count

        // Initialize state
        let state = [0u32; 4]; // [state, sequence, ack_count, subscriber_count]
        let mut buffer = [0u8; 16];
        for i in 0..4 {
            buffer[i*4..(i+1)*4].copy_from_slice(&state[i].to_le_bytes());
        }
        file.write_all(&buffer)?;

        Ok(StateFile { file, path })
    }

    fn connect(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let path = format!("/tmp/ultrapubsub_{}.state", name);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;

        Ok(StateFile { file, path })
    }

    fn read_state(&mut self) -> Result<[u32; 4], Box<dyn std::error::Error>> {
        let mut buffer = [0u8; 16];
        self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.read_exact(&mut buffer)?;
        let mut state = [0u32; 4];
        for i in 0..4 {
            state[i] = u32::from_le_bytes(buffer[i*4..(i+1)*4].try_into().unwrap());
        }
        Ok(state)
    }

    fn write_state(&mut self, state: &[u32; 4]) -> Result<(), Box<dyn std::error::Error>> {
        let mut buffer = [0u8; 16];
        for i in 0..4 {
            buffer[i*4..(i+1)*4].copy_from_slice(&state[i].to_le_bytes());
        }
        self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.write_all(&buffer)?;
        self.file.sync_all()?;
        Ok(())
    }

    fn cleanup(&self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Futex wrapper for ultra-fast cross-process synchronization
#[inline(always)]
fn futex_wait(futex_addr: *mut u32, expected: u32, timeout_ms: u32) -> c_int {
    unsafe {
        let timespec = libc::timespec {
            tv_sec: (timeout_ms / 1000) as i64,
            tv_nsec: ((timeout_ms % 1000) * 1_000_000) as i64,
        };

        syscall(SYS_futex,
                 futex_addr as *const c_void,
                 FUTEX_WAIT | FUTEX_PRIVATE_FLAG,
                 expected as c_int,
                 &timespec as *const libc::timespec) as c_int
    }
}

/// Futex wake operation
#[inline(always)]
fn futex_wake(futex_addr: *mut u32, count: c_int) -> c_int {
    unsafe {
        syscall(SYS_futex,
                 futex_addr as *const c_void,
                 FUTEX_WAKE | FUTEX_PRIVATE_FLAG,
                 count) as c_int
    }
}

/// Minimal synchronization coordinator for zero-copy broadcasting
pub struct SyncCoordinator {
    state_file: StateFile,
    name: String,
}

impl SyncCoordinator {
    /// Create a new synchronization coordinator
    pub fn create(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let state_file = StateFile::create(name)?;
        Ok(SyncCoordinator {
            state_file,
            name: name.to_string(),
        })
    }

    /// Connect to existing synchronization coordinator
    pub fn connect(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let state_file = StateFile::connect(name)?;
        Ok(SyncCoordinator {
            state_file,
            name: name.to_string(),
        })
    }

    /// Register a new subscriber
    pub fn register_subscriber(&mut self) -> Result<u32, Box<dyn std::error::Error>> {
        let mut state = self.state_file.read_state()?;
        if state[3] >= 32 {
            return Err("Maximum subscribers reached".into());
        }
        state[3] += 1;
        self.state_file.write_state(&state)?;
        Ok(state[3] - 1)
    }

    /// Wait for all subscribers to be ready
    pub fn wait_for_subscribers(&mut self, timeout_ms: u32) -> bool {
        let start = std::time::Instant::now();

        loop {
            match self.state_file.read_state() {
                Ok(state) => {
                    if state[3] > 0 {
                        // Mark ready state
                        let mut new_state = state;
                        new_state[0] = 1; // ready state
                        let _ = self.state_file.write_state(&new_state);
                        return true;
                    }
                }
                Err(_) => return false,
            }

            if start.elapsed().as_millis() as u32 > timeout_ms {
                return false;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// Broadcast notification - notify subscribers new data is ready
    pub fn notify_broadcast(&mut self, sequence: u32) {
        let mut state = self.state_file.read_state().unwrap_or([0, 0, 0, 0]);
        state[0] = 2; // broadcasting state
        state[1] = sequence;
        state[2] = 0; // reset ack count

        let _ = self.state_file.write_state(&state);
    }

    /// Wait for broadcast notification (subscriber side)
    pub fn wait_for_broadcast(&mut self, timeout_ms: u32) -> Option<u32> {
        let start = std::time::Instant::now();

        loop {
            match self.state_file.read_state() {
                Ok(state) => {
                    if state[0] == 2 { // broadcasting state
                        return Some(state[1]); // return sequence
                    }
                }
                Err(_) => return None,
            }

            if start.elapsed().as_millis() as u32 > timeout_ms {
                return None;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// Acknowledge broadcast (subscriber side)
    pub fn acknowledge(&mut self, _subscriber_id: u32) {
        let mut state = self.state_file.read_state().unwrap_or([0, 0, 0, 0]);
        state[2] += 1; // increment ack count
        let _ = self.state_file.write_state(&state);
    }

    /// Wait for all acknowledgments (publisher side)
    pub fn wait_for_acknowledgments(&mut self, timeout_ms: u32) -> bool {
        let start = std::time::Instant::now();

        loop {
            match self.state_file.read_state() {
                Ok(state) => {
                    if state[2] >= state[3] && state[3] > 0 {
                        // Reset to idle state
                        let mut new_state = state;
                        new_state[0] = 0;
                        let _ = self.state_file.write_state(&new_state);
                        return true;
                    }
                }
                Err(_) => return false,
            }

            if start.elapsed().as_millis() as u32 > timeout_ms {
                return false;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    /// Get current subscriber count
    pub fn subscriber_count(&mut self) -> u32 {
        match self.state_file.read_state() {
            Ok(state) => state[3],
            Err(_) => 0,
        }
    }
}

impl Drop for SyncCoordinator {
    fn drop(&mut self) {
        self.state_file.cleanup();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_basic_sync() {
        let name = "test_basic_sync";

        // Create coordinator
        let mut coord = SyncCoordinator::create(name).unwrap();

        // Register a subscriber
        let sub_id = coord.register_subscriber().unwrap();
        assert_eq!(sub_id, 0);

        // Test wait for subscribers
        assert!(coord.wait_for_subscribers(1000));

        // Test broadcast notification
        coord.notify_broadcast(42);

        // Connect as subscriber
        let mut sub_coord = SyncCoordinator::connect(name).unwrap();

        // Test wait for broadcast
        let seq = sub_coord.wait_for_broadcast(1000).unwrap();
        assert_eq!(seq, 42);

        // Test acknowledgment
        sub_coord.acknowledge(sub_id);

        // Test wait for acknowledgments
        assert!(coord.wait_for_acknowledgments(1000));
    }
}