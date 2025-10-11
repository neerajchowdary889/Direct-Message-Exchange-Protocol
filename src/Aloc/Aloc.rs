use std::path::PathBuf;
use std::collections::HashMap;
use shared_memory::{Shmem, ShmemConf, ShmemError};
use uuid::Uuid;
use chrono::Utc;
use thiserror::Error;
use serde::{Serialize, Deserialize};
use crate::Aloc::Channels::Structs::ChannelPartition;

use crate::ICP::structs::{SharedMemorySegment, DmepError, DmepResult};

/// Error types for memory allocation operations
#[derive(Error, Debug)]
pub enum AllocationError {
    #[error("Shared memory allocation failed: {0}")]
    SharedMemoryAllocation(String),
    #[error("Memory mapping failed: {0}")]
    MemoryMapping(String),
    #[error("File operation failed: {0}")]
    FileOperation(String),
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("Memory limit exceeded")]
    MemoryLimitExceeded,
    #[error("Segment not found: {0}")]
    SegmentNotFound(String),
    #[error("Channel already exists: {0}")]
    ChannelAlreadyExists(String),
    #[error("Channel not found: {0}")]
    ChannelNotFound(String),
}

impl From<ShmemError> for AllocationError {
    fn from(err: ShmemError) -> Self {
        AllocationError::SharedMemoryAllocation(err.to_string())
    }
}

/// Memory allocator for shared memory segments
pub struct Aloc {
    /// Define the channel partition allocation
    pub channel: HashMap<String, ChannelPartition>,
    /// Maximum total memory allocation size in bytes
    pub max_size: usize,
    /// Swap file path
    pub swap_file: PathBuf,
    /// Whether to use swap file
    pub use_swap: bool,
    /// Swap file size in bytes
    pub swap_size: usize,
    /// Memory usage threshold to start swapping (percentage 0-100)
    pub swapping_threshold: u8,
    /// Active shared memory segments
    pub segments: HashMap<String, SharedMemorySegment>,
    /// Shared memory objects
    pub shmem_objects: HashMap<String, Shmem>,
}

impl Aloc {
    /// Create a new memory allocator
    pub fn new(
        channel: HashMap<String, ChannelPartition>,
        max_size: usize,
        swap_file: PathBuf,
        use_swap: bool,
        swap_size: usize,
        swapping_threshold: u8,
        segments: HashMap<String, SharedMemorySegment>,
        shmem_objects: HashMap<String, Shmem>,
    ) -> Result<Self, AllocationError> {
        // Validate configuration
        if swapping_threshold > 100 {
            return Err(AllocationError::InvalidConfig(
                "Swapping threshold must be between 0 and 100".to_string(),
            ));
        }

        if use_swap && swap_size == 0 {
            return Err(AllocationError::InvalidConfig(
                "Swap size must be greater than 0 when swap is enabled".to_string(),
            ));
        }

        Ok(Self {
            channel: channel,
            max_size,
            swap_file,
            use_swap,
            swap_size,
            swapping_threshold,
            segments: segments,
            shmem_objects: shmem_objects,
        })
    }

    /// Add a channel to the channel vector
    pub fn add_channel(&mut self, channel_name: String, max_size: usize) -> Result<(), AllocationError> {
        if self.channel.contains_key(&channel_name) {
            return Err(AllocationError::ChannelAlreadyExists(
                format!("Channel '{}' already exists", channel_name)
            ));
        }

        // Check if adding this channel would exceed memory limit
        let current_channel_memory = self.get_total_channel_memory();
        if current_channel_memory + max_size > self.max_size {
            return Err(AllocationError::MemoryLimitExceeded);
        }

        let channel_name_clone = channel_name.clone();
        self.channel.insert(channel_name, SimpleChannelPartition {
            channel: channel_name_clone,
            max_size,
        });

        // Verify the memory limit after adding
        self.check_memory_limit()?;

        Ok(())
    }

    /// Remove a channel from the channel vector
    pub fn remove_channel(&mut self, channel_name: &str) -> Result<(), AllocationError> {
        if let Some(_channel_partition) = self.channel.remove(channel_name) {
            // Also remove any associated segments
            let segments_to_remove: Vec<String> = self.segments
                .iter()
                .filter(|(_, segment)| segment.channel == channel_name)
                .map(|(segment_id, _)| segment_id.clone())
                .collect();

            for segment_id in segments_to_remove {
                self.segments.remove(&segment_id);
                self.shmem_objects.remove(&segment_id);
            }

            Ok(())
        } else {
            Err(AllocationError::ChannelNotFound(
                format!("Channel '{}' not found", channel_name)
            ))
        }
    }

    /// Allocate a new shared memory segment to the Aloc:Channels in the self.channel vector
    pub fn allocate_shared_memory_segment(&mut self, channel: &str, max_size: usize) -> DmepResult<String> {
        // Check if we have enough total memory available
        let current_usage = self.get_total_memory_usage();
        if current_usage + max_size > self.max_size {
            return Err(DmepError::SharedMemory(
                "Insufficient memory: would exceed maximum allocation limit".to_string(),
            ));
        }

        // Check if the channel is already allocated
        if self.channel.contains_key(channel) {
            return Err(DmepError::SharedMemory(format!("Channel already allocated: {}", channel)));
        }

        // Generate unique segment ID
        let segment_id = format!("{}_segment_{}", channel, Uuid::new_v4());

        // Add the channel to the allocator
        self.channel.insert(channel.to_string(), SimpleChannelPartition {
            channel: channel.to_string(),
            max_size: max_size,
        });

        // Create shared memory segment
        let shmem = ShmemConf::new()
            .size(max_size)
            .create()
            .map_err(|e| DmepError::SharedMemory(format!("Failed to create shared memory: {}", e)))?;

        // Initialize the memory with zeros
        unsafe {
            std::ptr::write_bytes(shmem.as_ptr(), 0, max_size);
        }

        // Create segment metadata
        let segment = SharedMemorySegment::new(channel.to_string(), segment_id.clone(), max_size);

        // Store all components
        self.segments.insert(segment_id.clone(), segment);
        self.shmem_objects.insert(segment_id.clone(), shmem);

        Ok(segment_id)
    }

    /// Initialize all channels with shared memory segments
    pub fn initialize_all_channels(&mut self) -> DmepResult<Vec<String>> {
        let mut initialized_segments = Vec::new();
        let channel_names: Vec<String> = self.channel.keys().cloned().collect();

        for channel_name in channel_names {
            if let Some(channel_partition) = self.channel.get(&channel_name) {
                let max_size = channel_partition.max_size;
                
                // Check if segment already exists for this channel
                let segment_exists = self.segments.values().any(|segment| segment.channel == channel_name);
                
                if !segment_exists {
                    match self.allocate_shared_memory_segment(&channel_name, max_size) {
                        Ok(segment_id) => {
                            println!("Initialized channel '{}' with segment: {}", channel_name, segment_id);
                            initialized_segments.push(segment_id);
                        }
                        Err(e) => {
                            println!("Failed to initialize channel '{}': {}", channel_name, e);
                        }
                    }
                } else {
                    println!("Channel '{}' already has a memory segment", channel_name);
                }
            }
        }

        Ok(initialized_segments)
    }

    /// Initialize a specific channel with shared memory
    pub fn initialize_channel(&mut self, channel_name: &str) -> DmepResult<String> {
        if let Some(channel_partition) = self.channel.get(channel_name) {
            let max_size = channel_partition.max_size;
            
            // Check if segment already exists for this channel
            let segment_exists = self.segments.values().any(|segment| segment.channel == channel_name);
            
            if segment_exists {
                return Err(DmepError::SharedMemory(
                    format!("Channel '{}' already has a memory segment", channel_name)
                ));
            }

            self.allocate_shared_memory_segment(channel_name, max_size)
        } else {
            Err(DmepError::SharedMemory(
                format!("Channel '{}' not found", channel_name)
            ))
        }
    }

    /// Get all channel names
    pub fn get_channel_names(&self) -> Vec<String> {
        self.channel.keys().cloned().collect()
    }

    /// Get channel information
    pub fn get_channel_info(&self, channel_name: &str) -> Option<&SimpleChannelPartition> {
        self.channel.get(channel_name)
    }

    /// Get a mutable reference to a shared memory object
    pub fn get_shmem_mut(&mut self, segment_id: &str) -> DmepResult<&mut Shmem> {
        self.shmem_objects
            .get_mut(segment_id)
            .ok_or_else(|| DmepError::SharedMemory(format!("Segment not found: {}", segment_id)))
    }

    /// Get a reference to a shared memory object
    pub fn get_shmem(&self, segment_id: &str) -> DmepResult<&Shmem> {
        self.shmem_objects
            .get(segment_id)
            .ok_or_else(|| DmepError::SharedMemory(format!("Segment not found: {}", segment_id)))
    }

    /// Get segment metadata
    pub fn get_segment_info(&self, segment_id: &str) -> DmepResult<&SharedMemorySegment> {
        self.segments
            .get(segment_id)
            .ok_or_else(|| DmepError::SharedMemory(format!("Segment not found: {}", segment_id)))
    }

    /// Write data to a specific offset in a segment
    pub fn write_to_segment(
        &mut self,
        segment_id: &str,
        offset: usize,
        data: &[u8],
    ) -> DmepResult<()> {
        // Check bounds first
        let segment_size = self.get_segment_info(segment_id)?.size;
        if offset + data.len() > segment_size {
            return Err(DmepError::SharedMemory(
                "Write operation would exceed segment bounds".to_string(),
            ));
        }

        // Perform the write
        let shmem = self.get_shmem_mut(segment_id)?;
        unsafe {
            let ptr = shmem.as_ptr().add(offset);
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, data.len());
        }

        // Update segment metadata
        if let Some(segment) = self.segments.get_mut(segment_id) {
            segment.used = segment.used.max(offset + data.len());
            segment.message_count += 1;
            segment.touch();
        }

        Ok(())
    }

    /// Read data from a specific offset in a segment
    pub fn read_from_segment(
        &self,
        segment_id: &str,
        offset: usize,
        length: usize,
    ) -> DmepResult<Vec<u8>> {
        let segment = self.get_segment_info(segment_id)?;
        let shmem = self.get_shmem(segment_id)?;

        // Check bounds
        if offset + length > segment.size {
            return Err(DmepError::SharedMemory(
                "Read operation would exceed segment bounds".to_string(),
            ));
        }

        // Perform the read
        let mut data = vec![0u8; length];
        unsafe {
            let ptr = shmem.as_ptr().add(offset);
            std::ptr::copy_nonoverlapping(ptr, data.as_mut_ptr(), length);
        }

        Ok(data)
    }

    /// Deallocate a shared memory segment
    pub fn deallocate_segment(&mut self, segment_id: &str) -> DmepResult<()> {
        // Remove from all collections
        self.segments.remove(segment_id);
        self.shmem_objects.remove(segment_id);

        Ok(())
    }

    /// Get total memory usage across all segments
    pub fn get_total_memory_usage(&self) -> usize {
        self.segments.values().map(|s| s.used).sum()
    }

    /// Get total allocated memory for all channels
    pub fn get_total_channel_memory(&self) -> usize {
        self.channel.values().map(|c| c.max_size).sum()
    }

    /// Check if total channel memory exceeds the limit
    pub fn check_memory_limit(&self) -> Result<(), AllocationError> {
        let total_channel_memory = self.get_total_channel_memory();
        if total_channel_memory > self.max_size {
            return Err(AllocationError::InvalidConfig(format!(
                "Total memory usage of all channels combined ({}) exceeds max_size ({})", 
                total_channel_memory, 
                self.max_size
            )));
        }
        Ok(())
    }

    /// Get available memory
    pub fn get_available_memory(&self) -> usize {
        self.max_size - self.get_total_memory_usage()
    }

    /// Check if we should start swapping
    pub fn should_swap(&self) -> bool {
        if !self.use_swap {
            return false;
        }

        let usage_percentage = (self.get_total_memory_usage() * 100) / self.max_size;
        usage_percentage >= self.swapping_threshold as usize
    }

    /// Get all active segment IDs
    pub fn get_active_segments(&self) -> Vec<String> {
        self.segments.keys().cloned().collect()
    }

    /// Get memory statistics
    pub fn get_memory_stats(&self) -> (usize, usize, usize) {
        let total = self.max_size;
        let used = self.get_total_memory_usage();
        let available = total - used;
        (total, used, available)
    }

    /// Cleanup expired or unused segments
    pub fn cleanup_segments(&mut self) -> DmepResult<usize> {
        let mut cleaned = 0;
        let now = Utc::now();
        let cleanup_threshold = chrono::Duration::hours(1); // Clean segments older than 1 hour

        let expired_segments: Vec<String> = self
            .segments
            .iter()
            .filter(|(_, segment)| {
                now.signed_duration_since(segment.last_accessed) > cleanup_threshold
            })
            .map(|(id, _)| id.clone())
            .collect();

        for segment_id in expired_segments {
            self.deallocate_segment(&segment_id)?;
            cleaned += 1;
        }

        Ok(cleaned)
    }
}

impl Drop for Aloc {
    fn drop(&mut self) {
        // Cleanup all segments when the allocator is dropped
        let segment_ids: Vec<String> = self.segments.keys().cloned().collect();
        for segment_id in segment_ids {
            let _ = self.deallocate_segment(&segment_id);
        }
    }
}