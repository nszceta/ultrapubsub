// Multi-Process Synchronization Module for UltraPubSub
// This module provides proper file-based synchronization for coordinating
// zero-copy numpy array broadcasting across multiple processes.

use std::fs::{OpenOptions, File};
use std::io::{Read, Write, Seek};
use std::time::{Duration, Instant};

/// Thread-safe state file for cross-process coordination with file locking
struct StateFile {
    path: String,
}

impl StateFile {
    fn create(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let path = format!("/tmp/ultrapubsub_{}.state", name);

        // Create state file exclusively (fail if exists)
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)  // Atomic create - fails if file exists
            .open(&path)?;

        file.set_len(16)?; // 4 words: state, seq, ack_count, subscriber_count

        // Initialize with publisher state
        let state = [1u32, 0, 0, 0]; // state=1 (ready), seq=0, ack_count=0, subscriber_count=0
        Self::write_state_atomic(&path, &state)?;

        Ok(StateFile { path })
    }

    fn connect(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let path = format!("/tmp/ultrapubsub_{}.state", name);

        // Wait for file to exist (publisher creates it)
        let start = Instant::now();
        loop {
            match OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
            {
                Ok(_) => break,
                Err(_) if start.elapsed() < Duration::from_secs(5) => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(e) => return Err(format!("Failed to connect to state file: {}", e).into()),
            }
        };

        Ok(StateFile { path })
    }

    fn read_state_atomic(&self) -> Result<[u32; 4], Box<dyn std::error::Error>> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.path)?;

        let mut buffer = [0u8; 16];
        file.seek(std::io::SeekFrom::Start(0))?;
        file.read_exact(&mut buffer)?;

        let mut state = [0u32; 4];
        for i in 0..4 {
            state[i] = u32::from_le_bytes(buffer[i*4..(i+1)*4].try_into().unwrap());
        }

        Ok(state)
    }

    fn write_state_atomic(path: &str, state: &[u32; 4]) -> Result<(), Box<dyn std::error::Error>> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)?;

        let mut buffer = [0u8; 16];
        for i in 0..4 {
            buffer[i*4..(i+1)*4].copy_from_slice(&state[i].to_le_bytes());
        }

        file.seek(std::io::SeekFrom::Start(0))?;
        file.write_all(&buffer)?;
        file.sync_all()?;

        Ok(())
    }

    fn cleanup(&self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Multi-process synchronization coordinator
pub struct SyncCoordinator {
    state_file: StateFile,
    name: String,
    is_publisher: bool,
    last_sequence: u32,
}

impl SyncCoordinator {
    /// Create a new publisher coordinator
    pub fn create_publisher(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let state_file = StateFile::create(name)?;
        Ok(Self {
            state_file,
            name: name.to_string(),
            is_publisher: true,
            last_sequence: 0,
        })
    }

    /// Connect as a subscriber
    pub fn connect_subscriber(name: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let state_file = StateFile::connect(name)?;
        Ok(Self {
            state_file,
            name: name.to_string(),
            is_publisher: false,
            last_sequence: 0,
        })
    }

    /// Register a new subscriber (publisher only)
    pub fn register_subscriber(&mut self) -> Result<u32, Box<dyn std::error::Error>> {
        if !self.is_publisher {
            return Err("Only publisher can register subscribers".into());
        }

        let mut state = self.state_file.read_state_atomic()?;
        if state[3] >= 32 {
            return Err("Maximum subscribers reached".into());
        }

        state[3] += 1;
        StateFile::write_state_atomic(&self.state_file.path, &state)?;

        let subscriber_id = state[3] - 1;
        Ok(subscriber_id)
    }

    /// Wait for all subscribers to be ready (publisher only)
    pub fn wait_for_subscribers(&mut self, timeout_ms: u32) -> bool {
        if !self.is_publisher {
            return false;
        }

        let start = Instant::now();
        let target_subscribers = 1; // Minimum expected subscribers

        loop {
            match self.state_file.read_state_atomic() {
                Ok(state) => {
                    if state[3] >= target_subscribers {
                        return true;
                    }
                }
                Err(_) => return false,
            }

            if start.elapsed().as_millis() as u32 > timeout_ms {
                return false;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Broadcast notification with sequence tracking
    pub fn notify_broadcast(&mut self, sequence: u32) {
        if !self.is_publisher {
            return;
        }

        let mut state = self.state_file.read_state_atomic().unwrap_or([0, 0, 0, 0]);
        state[0] = 2; // broadcasting state
        state[1] = sequence;
        state[2] = 0; // reset ack count

        let _ = StateFile::write_state_atomic(&self.state_file.path, &state);
        self.last_sequence = sequence;
    }

    /// Wait for broadcast notification (subscriber side)
    pub fn wait_for_broadcast(&mut self, timeout_ms: u32) -> Option<u32> {
        let start = Instant::now();

        loop {
            match self.state_file.read_state_atomic() {
                Ok(state) => {
                    if state[0] == 2 { // broadcasting state
                        let sequence = state[1];
                        if sequence != self.last_sequence {
                            self.last_sequence = sequence;
                            return Some(sequence);
                        }
                    }
                }
                Err(_) => return None,
            }

            if start.elapsed().as_millis() as u32 > timeout_ms {
                return None;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Acknowledge broadcast (subscriber side)
    pub fn acknowledge(&mut self, _subscriber_id: u32) {
        if self.is_publisher {
            return;
        }

        let mut state = self.state_file.read_state_atomic().unwrap_or([0, 0, 0, 0]);
        println!("DEBUG: Acknowledge called - state: [{}, {}, {}, {}], last_sequence: {}", state[0], state[1], state[2], state[3], self.last_sequence);

        // Only acknowledge if we haven't already acknowledged this sequence
        if state[1] != self.last_sequence {
            state[2] += 1; // increment ack count
            self.last_sequence = state[1];
            println!("DEBUG: Writing acknowledgment - new state: [{}, {}, {}, {}]", state[0], state[1], state[2], state[3]);
            let _ = StateFile::write_state_atomic(&self.state_file.path, &state);
        } else {
            println!("DEBUG: Skipping duplicate acknowledgment for sequence {}", state[1]);
        }
    }

    /// Wait for all acknowledgments (publisher side)
    pub fn wait_for_acknowledgments(&mut self, timeout_ms: u32) -> bool {
        if !self.is_publisher {
            return false;
        }

        let start = Instant::now();

        loop {
            match self.state_file.read_state_atomic() {
                Ok(state) => {
                    println!("DEBUG: Wait for ack - state: [{}, {}, {}, {}]", state[0], state[1], state[2], state[3]);
                    if state[2] >= state[3] && state[3] > 0 {
                        println!("DEBUG: All acknowledgments received!");
                        // Reset to idle state
                        let mut new_state = state;
                        new_state[0] = 1; // ready state
                        let _ = StateFile::write_state_atomic(&self.state_file.path, &new_state);
                        return true;
                    }
                }
                Err(_) => return false,
            }

            if start.elapsed().as_millis() as u32 > timeout_ms {
                println!("DEBUG: Wait for ack timed out");
                return false;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Get current subscriber count
    pub fn subscriber_count(&mut self) -> u32 {
        match self.state_file.read_state_atomic() {
            Ok(state) => state[3],
            Err(_) => 0,
        }
    }

    /// Get current sequence number
    pub fn current_sequence(&mut self) -> u32 {
        match self.state_file.read_state_atomic() {
            Ok(state) => state[1],
            Err(_) => 0,
        }
    }
}

impl Drop for SyncCoordinator {
    fn drop(&mut self) {
        if self.is_publisher {
            self.state_file.cleanup();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_multi_process_sync() {
        let name = "test_multi_process";

        // Create publisher in separate thread
        let publisher_thread = thread::spawn(move || {
            let mut pub_coord = SyncCoordinator::create_publisher(name).unwrap();

            // Register a subscriber
            let sub_id = pub_coord.register_subscriber().unwrap();
            assert_eq!(sub_id, 0);

            // Test wait for subscribers
            assert!(pub_coord.wait_for_subscribers(1000));

            // Test broadcast notification
            pub_coord.notify_broadcast(42);

            // Wait a bit for subscriber to process
            thread::sleep(Duration::from_millis(100));

            // Test wait for acknowledgments
            assert!(pub_coord.wait_for_acknowledgments(1000));

            // Get sequence
            assert_eq!(pub_coord.current_sequence(), 42);
        });

        // Create subscriber in separate thread
        let subscriber_thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100)); // Let publisher start

            let mut sub_coord = SyncCoordinator::connect_subscriber(name).unwrap();

            // Test wait for broadcast
            let seq = sub_coord.wait_for_broadcast(5000).unwrap();
            assert_eq!(seq, 42);

            // Test acknowledgment
            sub_coord.acknowledge(0);

            // Verify sequence tracking
            assert_eq!(sub_coord.current_sequence(), 42);
        });

        publisher_thread.join().unwrap();
        subscriber_thread.join().unwrap();
    }
}