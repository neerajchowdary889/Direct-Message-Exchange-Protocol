use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use thiserror::Error;
use crate::ICP::structs::{Message};

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
    pub partitions: HashMap<String, Arc<Mutex<ChannelPartition>>>,
    pub total_memory_limit: usize,
    pub current_memory_usage: usize,
}
