use crate::Aloc::Channels::Structs::ChannelPartition;
use shared_memory::Shmem;
use crate::ICP::Structs::{SharedMemorySegment, DmepError, DmepResult};
use crate::Aloc::Aloc::Aloc;
use uuid::Uuid;


pub fn INIT_SharedMemorySegment(channel: ChannelPartition) -> SharedMemorySegment {
    let segment_id = format!("{}_segment_{}", channel.channel, Uuid::new_v4());
    let segment = SharedMemorySegment::new(channel.channel, segment_id, channel.max_size);
    segment
}

/// Get all channel names
pub fn get_channel_names(partition: &Aloc) -> Vec<String> {
    partition.channel.keys().cloned().collect()
}

/// Get channel information
pub fn get_channel_info<'a>(partition: &'a Aloc, channel_name: &str) -> Option<&'a ChannelPartition> {
    partition.channel.get(channel_name)
}

/// Get a mutable reference to a shared memory object
pub fn get_shmem_mut<'a>(partition: &'a mut Aloc, segment_id: &str) -> DmepResult<&'a mut Shmem> {
    partition.shmem_objects
        .get_mut(segment_id)
        .ok_or_else(|| DmepError::SharedMemory(format!("Segment not found: {}", segment_id)))
}

/// Get a reference to a shared memory object
pub fn get_shmem<'a>(partition: &'a Aloc, segment_id: &str) -> DmepResult<&'a Shmem> {
    partition.shmem_objects
        .get(segment_id)
        .ok_or_else(|| DmepError::SharedMemory(format!("Segment not found: {}", segment_id)))
}

/// Get segment metadata
pub fn get_segment_info<'a>(partition: &'a Aloc, segment_id: &str) -> DmepResult<&'a SharedMemorySegment> {
    partition.segments
        .get(segment_id)
        .ok_or_else(|| DmepError::SharedMemory(format!("Segment not found: {}", segment_id)))
}

/// Get all active segment IDs
pub fn get_active_segments(partition: &Aloc) -> Vec<String> {
    partition.segments.keys().cloned().collect()
}

/// Get memory statistics
pub fn get_memory_stats(partition: &Aloc) -> (usize, usize, usize) {
    let total = partition.max_size;
    let used = get_total_memory_usage(partition);
    let available = total - used;
    (total, used, available)
}

/// Get total memory usage across all segments
pub fn get_total_memory_usage(partition: &Aloc) -> usize {
    partition.segments.values().map(|s| s.used).sum()
}

/// Get total allocated memory for all channels
pub fn get_total_channel_memory(partition: &Aloc) -> usize {
    partition.channel.values().map(|c| c.max_size).sum()
}

/// Get available memory
pub fn get_available_memory(partition: &Aloc) -> usize {
    partition.max_size - get_total_memory_usage(partition)
}
