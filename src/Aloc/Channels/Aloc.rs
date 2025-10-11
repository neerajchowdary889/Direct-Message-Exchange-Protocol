use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::ICP::structs::{Message, MessageType, DmepError, DmepResult, SharedMemorySegment};

/// Error types for channel partition operations
#[derive(Error, Debug)]
pub enum ChannelPartitionError {
    #[error("Channel partition not found: {0}")]
    ChannelNotFound(String),
    #[error("Channel partition already exists: {0}")]
    ChannelAlreadyExists(String),
    #[error("Insufficient memory for channel: {0}")]
    InsufficientMemory(String),
    #[error("Channel partition is locked: {0}")]
    ChannelLocked(String),
    #[error("Invalid channel configuration: {0}")]
    InvalidConfiguration(String),
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("Deserialization error: {0}")]
    DeserializationError(String),
}

/// Channel partition state
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ChannelState {
    Active,
    Inactive,
    Swapping,
    Locked,
    Error,
}

/// Channel partition configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelConfig {
    /// Maximum message size for this channel
    pub max_message_size: usize,
    /// Maximum number of messages in queue
    pub max_queue_size: usize,
    /// Message TTL in seconds
    pub message_ttl: u64,
    /// Whether to persist messages to disk
    pub persistent: bool,
    /// Whether to enable message ordering
    pub ordered: bool,
    /// Whether to enable message deduplication
    pub deduplicate: bool,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            max_message_size: 1024 * 1024, // 1MB
            max_queue_size: 1000,
            message_ttl: 3600, // 1 hour
            persistent: false,
            ordered: true,
            deduplicate: false,
        }
    }
}

/// Channel partition statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelStats {
    /// Total messages sent
    pub messages_sent: u64,
    /// Total messages received
    pub messages_received: u64,
    /// Total messages dropped
    pub messages_dropped: u64,
    /// Current queue size
    pub current_queue_size: usize,
    /// Memory usage in bytes
    pub memory_usage: usize,
    /// Last message timestamp
    pub last_message_time: Option<DateTime<Utc>>,
    /// Average message processing time in milliseconds
    pub avg_processing_time_ms: f64,
}

impl Default for ChannelStats {
    fn default() -> Self {
        Self {
            messages_sent: 0,
            messages_received: 0,
            messages_dropped: 0,
            current_queue_size: 0,
            memory_usage: 0,
            last_message_time: None,
            avg_processing_time_ms: 0.0,
        }
    }
}

/// Channel partition structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelPartition {
    /// Channel identifier
    pub channel: String,
    /// Maximum total memory allocation size in bytes
    pub max_size: usize,
    /// Current memory usage
    pub used_size: usize,
    /// Channel state
    pub state: ChannelState,
    /// Channel configuration
    pub config: ChannelConfig,
    /// Channel statistics
    pub stats: ChannelStats,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Last accessed timestamp
    pub last_accessed: DateTime<Utc>,
    /// Associated shared memory segment ID
    pub segment_id: Option<String>,
    /// Message queue for this channel
    pub message_queue: Vec<Message>,
    /// Subscribed modules/functions
    pub subscribers: HashMap<String, SubscriberInfo>,
    /// Channel metadata
    pub metadata: HashMap<String, String>,
}

/// Subscriber information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriberInfo {
    /// Subscriber ID
    pub id: String,
    /// Subscriber type (function, module, service)
    pub subscriber_type: SubscriberType,
    /// Language/runtime (rust, python, node, etc.)
    pub language: String,
    /// Function/module name to call
    pub target: String,
    /// Whether subscriber is active
    pub active: bool,
    /// Last response time
    pub last_response_time: Option<DateTime<Utc>>,
    /// Response timeout in milliseconds
    pub timeout_ms: u64,
}

/// Subscriber types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SubscriberType {
    Function,
    Module,
    Service,
    Webhook,
    Queue,
}

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
            channel: channel.clone(),
            max_size,
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

/// Channel summary for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelSummary {
    pub channel: String,
    pub state: ChannelState,
    pub memory_usage_percentage: f64,
    pub queue_size: usize,
    pub subscriber_count: usize,
    pub active_subscribers: usize,
    pub created_at: DateTime<Utc>,
    pub last_accessed: DateTime<Utc>,
}

/// Channel partition manager
pub struct ChannelPartitionManager {
    partitions: HashMap<String, Arc<Mutex<ChannelPartition>>>,
    total_memory_limit: usize,
    current_memory_usage: usize,
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
    use super::*;

    #[test]
    fn test_create_channel_partition() {
        let partition = ChannelPartition::new("test_channel".to_string(), 1024, None);
        assert!(partition.is_ok());
        
        let partition = partition.unwrap();
        assert_eq!(partition.channel, "test_channel");
        assert_eq!(partition.max_size, 1024);
        assert_eq!(partition.state, ChannelState::Active);
    }

    #[test]
    fn test_publish_consume_message() {
        let mut partition = ChannelPartition::new("test_channel".to_string(), 1024, None).unwrap();
        
        let message = Message::new(
            "test_channel".to_string(),
            "sender".to_string(),
            Some("recipient".to_string()),
            b"test message".to_vec(),
            MessageType::Direct,
        );

        assert!(partition.publish_message(message.clone()).is_ok());
        assert_eq!(partition.stats.messages_sent, 1);
        
        let consumed = partition.consume_message().unwrap();
        assert!(consumed.is_some());
        assert_eq!(partition.stats.messages_received, 1);
    }

    #[test]
    fn test_add_remove_subscriber() {
        let mut partition = ChannelPartition::new("test_channel".to_string(), 1024, None).unwrap();
        
        assert!(partition.add_subscriber(
            "sub1".to_string(),
            SubscriberType::Function,
            "rust".to_string(),
            "process_message".to_string(),
            5000,
        ).is_ok());
        
        assert!(partition.remove_subscriber("sub1").is_ok());
        assert!(partition.remove_subscriber("nonexistent").is_err());
    }
}