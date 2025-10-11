use std::path::PathBuf;
use std::collections::HashMap;
use shared_memory::{Shmem, ShmemConf, ShmemError};
use uuid::Uuid;
use chrono::Utc;
use thiserror::Error;
use serde::{Serialize, Deserialize};
// Simple channel partition for basic memory allocation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimpleChannelPartition {
    pub channel: String,
    pub max_size: usize,
}
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
}

impl From<ShmemError> for AllocationError {
    fn from(err: ShmemError) -> Self {
        AllocationError::SharedMemoryAllocation(err.to_string())
    }
}

/// Memory allocator for shared memory segments
pub struct Aloc {
    /// Define the channel partition allocation
    channel: HashMap<String, SimpleChannelPartition>,
    /// Maximum total memory allocation size in bytes
    max_size: usize,
    /// Swap file path
    swap_file: PathBuf,
    /// Whether to use swap file
    use_swap: bool,
    /// Swap file size in bytes
    swap_size: usize,
    /// Memory usage threshold to start swapping (percentage 0-100)
    swapping_threshold: u8,
    /// Active shared memory segments
    segments: HashMap<String, SharedMemorySegment>,
    /// Shared memory objects
    shmem_objects: HashMap<String, Shmem>,
}

impl Aloc {
    /// Create a new memory allocator
    pub fn new(
        max_size: usize,
        swap_file: PathBuf,
        use_swap: bool,
        swap_size: usize,
        swapping_threshold: u8,
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
        
        // Check the memory allocation of all the channels combined is higher than the max_size then return an error
        let total_memory_usage = 0; // Will be calculated when channels are added
        if total_memory_usage > max_size {
            return Err(AllocationError::InvalidConfig(format!("Total memory usage of all channels combined is higher than the max_size: {}", total_memory_usage)));
        }

        Ok(Self {
            channel: HashMap::new(),
            max_size,
            swap_file,
            use_swap,
            swap_size,
            swapping_threshold,
            segments: HashMap::new(),
            shmem_objects: HashMap::new(),
        })
    }

    /// Allocate a new shared memory segment
    pub fn allocate_shared_memory_segment(&mut self, channel: &str, max_size: usize) -> DmepResult<String> {
        // Check if we have enough total memory available
        let current_usage = self.get_total_memory_usage();
        if current_usage + max_size > self.max_size {
            return Err(DmepError::SharedMemory(
                "Insufficient memory: would exceed maximum allocation limit".to_string(),
            ));
        }

        // Generate unique segment ID
        let segment_id = format!("dmep_segment_{}", Uuid::new_v4());

        // Check if the channel is already allocated
        if self.channel.contains_key(channel) {
            return Err(DmepError::SharedMemory(format!("Channel already allocated: {}", channel)));
        }

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