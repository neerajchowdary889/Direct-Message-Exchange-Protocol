use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use chrono::Utc;


use crate::ICP::Structs::{Message, MessageType, DmepError, DmepResult, SharedMemorySegment};
use crate::Aloc::Channels::Structs::{ChannelConfig, ChannelState, ChannelPartitionError, SubscriberType, SubscriberInfo, ChannelStats, ChannelPartition, ChannelSummary, ChannelPartitionManager};


impl ChannelPartition {
    /// Create a new channel partition
    pub fn new(
        channel: String,
        max_size: usize,
        config: Option<ChannelConfig>,
    ) -> Result<Self, ChannelPartitionError> {
        if channel.is_empty() {
            return Err(ChannelPartitionError::InvalidConfiguration(
                "Channel name cannot be empty".to_string(),
            ));
        }

        if max_size == 0 {
            return Err(ChannelPartitionError::InvalidConfiguration(
                "Channel size must be greater than 0".to_string(),
            ));
        }

        Ok(Self {
            channel: channel,
            max_size:max_size,
            used_size: 0,
            state: ChannelState::Active,
            config: config.unwrap_or_default(),
            stats: ChannelStats::default(),
            created_at: Utc::now(),
            last_accessed: Utc::now(),
            segment_id: None,
            message_queue: Vec::new(),
            subscribers: HashMap::new(),
            metadata: HashMap::new(),
        })
    }

    /// Add a subscriber to this channel
    pub fn add_subscriber(
        &mut self,
        subscriber_id: String,
        subscriber_type: SubscriberType,
        language: String,
        target: String,
        timeout_ms: u64,
    ) -> Result<(), ChannelPartitionError> {
        if self.subscribers.contains_key(&subscriber_id) {
            return Err(ChannelPartitionError::ChannelAlreadyExists(
                format!("Subscriber {} already exists", subscriber_id),
            ));
        }

        let subscriber = SubscriberInfo {
            id: subscriber_id.clone(),
            subscriber_type,
            language,
            target,
            active: true,
            last_response_time: None,
            timeout_ms,
        };

        self.subscribers.insert(subscriber_id, subscriber);
        self.touch();
        Ok(())
    }

    /// Remove a subscriber from this channel
    pub fn remove_subscriber(&mut self, subscriber_id: &str) -> Result<(), ChannelPartitionError> {
        if !self.subscribers.contains_key(subscriber_id) {
            return Err(ChannelPartitionError::ChannelNotFound(
                format!("Subscriber {} not found", subscriber_id),
            ));
        }

        self.subscribers.remove(subscriber_id);
        self.touch();
        Ok(())
    }

    /// Publish a message to this channel
    pub fn publish_message(&mut self, message: Message) -> Result<(), ChannelPartitionError> {
        if self.state != ChannelState::Active {
            return Err(ChannelPartitionError::ChannelLocked(
                format!("Channel {} is not active", self.channel),
            ));
        }

        // Check message size
        let message_size = message.payload.len();
        if message_size > self.config.max_message_size {
            return Err(ChannelPartitionError::InsufficientMemory(
                format!("Message too large: {} bytes (max: {})", message_size, self.config.max_message_size),
            ));
        }

        // Check queue size
        if self.message_queue.len() >= self.config.max_queue_size {
            self.stats.messages_dropped += 1;
            return Err(ChannelPartitionError::InsufficientMemory(
                "Message queue is full".to_string(),
            ));
        }

        // Check memory usage
        if self.used_size + message_size > self.max_size {
            self.stats.messages_dropped += 1;
            return Err(ChannelPartitionError::InsufficientMemory(
                "Channel memory is full".to_string(),
            ));
        }

        // Add message to queue
        self.message_queue.push(message);
        self.used_size += message_size;
        self.stats.messages_sent += 1;
        self.stats.current_queue_size = self.message_queue.len();
        self.stats.last_message_time = Some(Utc::now());
        self.touch();

        Ok(())
    }

    /// Consume a message from this channel
    pub fn consume_message(&mut self) -> Result<Option<Message>, ChannelPartitionError> {
        if self.state != ChannelState::Active {
            return Err(ChannelPartitionError::ChannelLocked(
                format!("Channel {} is not active", self.channel),
            ));
        }

        if let Some(message) = self.message_queue.pop() {
            self.used_size -= message.payload.len();
            self.stats.messages_received += 1;
            self.stats.current_queue_size = self.message_queue.len();
            self.touch();
            Ok(Some(message))
        } else {
            Ok(None)
        }
    }

    /// Get messages for a specific subscriber
    pub fn get_messages_for_subscriber(
        &mut self,
        subscriber_id: &str,
        max_messages: usize,
    ) -> Result<Vec<Message>, ChannelPartitionError> {
        if !self.subscribers.contains_key(subscriber_id) {
            return Err(ChannelPartitionError::ChannelNotFound(
                format!("Subscriber {} not found", subscriber_id),
            ));
        }

        let mut messages = Vec::new();
        let mut consumed_count = 0;

        while consumed_count < max_messages {
            if let Some(message) = self.consume_message()? {
                messages.push(message);
                consumed_count += 1;
            } else {
                break;
            }
        }

        Ok(messages)
    }

    /// Set channel state
    pub fn set_state(&mut self, state: ChannelState) {
        self.state = state;
        self.touch();
    }

    /// Lock the channel (prevent new messages)
    pub fn lock(&mut self) {
        self.set_state(ChannelState::Locked);
    }

    /// Unlock the channel
    pub fn unlock(&mut self) {
        self.set_state(ChannelState::Active);
    }

    /// Check if channel is active
    pub fn is_active(&self) -> bool {
        self.state == ChannelState::Active
    }

    /// Get available memory
    pub fn get_available_memory(&self) -> usize {
        self.max_size - self.used_size
    }

    /// Get memory usage percentage
    pub fn get_memory_usage_percentage(&self) -> f64 {
        (self.used_size as f64 / self.max_size as f64) * 100.0
    }

    /// Check if channel needs swapping (memory usage > 80%)
    pub fn needs_swapping(&self) -> bool {
        self.get_memory_usage_percentage() > 80.0
    }

    /// Swap channel to disk (placeholder for future implementation)
    pub fn swap_to_disk(&mut self) -> Result<(), ChannelPartitionError> {
        if self.state == ChannelState::Swapping {
            return Err(ChannelPartitionError::ChannelLocked(
                "Channel is already swapping".to_string(),
            ));
        }

        self.set_state(ChannelState::Swapping);
        // TODO: Implement actual disk swapping logic
        self.set_state(ChannelState::Active);
        Ok(())
    }

    /// Restore channel from disk (placeholder for future implementation)
    pub fn restore_from_disk(&mut self) -> Result<(), ChannelPartitionError> {
        if self.state != ChannelState::Swapping {
            return Err(ChannelPartitionError::ChannelLocked(
                "Channel is not in swapping state".to_string(),
            ));
        }

        // TODO: Implement actual disk restoration logic
        self.set_state(ChannelState::Active);
        Ok(())
    }

    /// Serialize channel partition to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>, ChannelPartitionError> {
        bincode::serialize(self)
            .map_err(|e| ChannelPartitionError::SerializationError(e.to_string()))
    }

    /// Deserialize channel partition from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self, ChannelPartitionError> {
        bincode::deserialize(data)
            .map_err(|e| ChannelPartitionError::DeserializationError(e.to_string()))
    }

    /// Update last accessed time
    pub fn touch(&mut self) {
        self.last_accessed = Utc::now();
    }

    /// Get channel summary
    pub fn get_summary(&self) -> ChannelSummary {
        ChannelSummary {
            channel: self.channel.clone(),
            state: self.state.clone(),
            memory_usage_percentage: self.get_memory_usage_percentage(),
            queue_size: self.message_queue.len(),
            subscriber_count: self.subscribers.len(),
            active_subscribers: self.subscribers.values().filter(|s| s.active).count(),
            created_at: self.created_at,
            last_accessed: self.last_accessed,
        }
    }

    /// Cleanup expired messages
    pub fn cleanup_expired_messages(&mut self) -> usize {
        let now = Utc::now();
        let mut removed_count = 0;

        self.message_queue.retain(|message| {
            if message.ttl > 0 {
                let expiration = message.timestamp + chrono::Duration::seconds(message.ttl as i64);
                if now > expiration {
                    self.used_size -= message.payload.len();
                    removed_count += 1;
                    false
                } else {
                    true
                }
            } else {
                true
            }
        });

        self.stats.current_queue_size = self.message_queue.len();
        removed_count
    }
}


impl ChannelPartitionManager {
    /// Create a new channel partition manager
    pub fn new(total_memory_limit: usize) -> Self {
        Self {
            partitions: HashMap::new(),
            total_memory_limit,
            current_memory_usage: 0,
        }
    }

    /// Create a new channel partition
    pub fn create_channel(
        &mut self,
        channel: String,
        max_size: usize,
        config: Option<ChannelConfig>,
    ) -> Result<(), ChannelPartitionError> {
        if self.partitions.contains_key(&channel) {
            return Err(ChannelPartitionError::ChannelAlreadyExists(channel));
        }

        if self.current_memory_usage + max_size > self.total_memory_limit {
            return Err(ChannelPartitionError::InsufficientMemory(
                "Total memory limit would be exceeded".to_string(),
            ));
        }

        let partition = ChannelPartition::new(channel.clone(), max_size, config)?;
        self.partitions.insert(channel, Arc::new(Mutex::new(partition)));
        self.current_memory_usage += max_size;

        Ok(())
    }

    /// Delete a channel partition
    pub fn delete_channel(&mut self, channel: &str) -> Result<(), ChannelPartitionError> {
        if let Some(partition) = self.partitions.remove(channel) {
            let partition = partition.lock().unwrap();
            self.current_memory_usage -= partition.max_size;
            Ok(())
        } else {
            Err(ChannelPartitionError::ChannelNotFound(channel.to_string()))
        }
    }

    /// Get a channel partition
    pub fn get_channel(&self, channel: &str) -> Result<Arc<Mutex<ChannelPartition>>, ChannelPartitionError> {
        self.partitions
            .get(channel)
            .cloned()
            .ok_or_else(|| ChannelPartitionError::ChannelNotFound(channel.to_string()))
    }

    /// Get all channel summaries
    pub fn get_all_summaries(&self) -> Vec<ChannelSummary> {
        self.partitions
            .values()
            .map(|partition| {
                let partition = partition.lock().unwrap();
                partition.get_summary()
            })
            .collect()
    }

    /// Get total memory usage
    pub fn get_total_memory_usage(&self) -> usize {
        self.current_memory_usage
    }

    /// Get available memory
    pub fn get_available_memory(&self) -> usize {
        self.total_memory_limit - self.current_memory_usage
    }
}

#[cfg(test)]
mod tests {
    use crate::Aloc::Channels::Structs::{ChannelConfig, ChannelPartition};
#[test]
fn test_channel_config_default() {
    let config = ChannelConfig::default();
    println!("--> config: {:?}", config);
    assert_eq!(config.max_message_size, 1024 * 1024 * 10);
    assert_eq!(config.max_queue_size, 1000);
    assert_eq!(config.message_ttl, 3600);
    assert_eq!(config.persistent, false);
    assert_eq!(config.ordered, true);
    assert_eq!(config.deduplicate, false);
}

#[test]
fn test_channel_partition_new() {
    let config = ChannelConfig::default();
    let partition = ChannelPartition::new("test_channel".to_string(), 1024 * 1024 * 10, Some(config));
        println!("--> partition: {:?}", partition);
        let partition = partition.unwrap();
        assert_eq!(partition.channel, "test_channel");
        assert_eq!(partition.max_size, 1024 * 1024 * 10);
    }

}