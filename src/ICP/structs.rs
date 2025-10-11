use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use thiserror::Error;

/// Error types for the Direct Message Exchange Protocol
#[derive(Error, Debug, Clone, Serialize, Deserialize)]
pub enum DmepError {
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    #[error("Shared memory error: {0}")]
    SharedMemory(String),
    #[error("Subscription not found: {0}")]
    SubscriptionNotFound(String),
    #[error("Invalid message format: {0}")]
    InvalidMessage(String),
    #[error("Channel error: {0}")]
    ChannelError(String),
}

/// Result type for DMEP operations
pub type DmepResult<T> = Result<T, DmepError>;

/// Message priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessagePriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Message types for different communication patterns
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageType {
    /// Direct message to a specific recipient
    Direct,
    /// Broadcast message to all subscribers
    Broadcast,
    /// Request-response pattern
    Request,
    /// Response to a request
    Response,
    /// Heartbeat/keepalive message
    Heartbeat,
    /// System notification
    Notification,
}

/// Core message structure for the protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub channel: String,
    /// Unique message identifier
    pub id: Uuid,
    /// Message type
    pub message_type: MessageType,
    /// Sender identifier
    pub sender_id: String,
    /// Recipient identifier (empty for broadcast)
    pub recipient_id: Option<String>,
    /// Message priority
    pub priority: MessagePriority,
    /// Timestamp when message was created
    pub timestamp: DateTime<Utc>,
    /// Message payload (serialized data)
    pub payload: Vec<u8>,
    /// Optional correlation ID for request-response patterns
    pub correlation_id: Option<Uuid>,
    /// Message metadata
    pub metadata: HashMap<String, String>,
    /// Time-to-live in seconds (0 = no expiration)
    pub ttl: u64,
}

impl Message {
    /// Create a new message with default values
    pub fn new(
        channel: String,
        sender_id: String,
        recipient_id: Option<String>,
        payload: Vec<u8>,
        message_type: MessageType,
    ) -> Self {
        Self {
            channel,
            id: Uuid::new_v4(),
            message_type,
            sender_id,
            recipient_id,
            priority: MessagePriority::Normal,
            timestamp: Utc::now(),
            payload,
            correlation_id: None,
            metadata: HashMap::new(),
            ttl: 0,
        }
    }

    /// Create a request message
    pub fn new_request(channel: String, sender_id: String, recipient_id: String, payload: Vec<u8>) -> Self {
        let mut message = Self::new(channel, sender_id, Some(recipient_id), payload, MessageType::Request);
        message.correlation_id = Some(Uuid::new_v4());
        message
    }

    /// Create a response message
    pub fn new_response(
        channel: String,
        sender_id: String,
        recipient_id: String,
        payload: Vec<u8>,
        correlation_id: Uuid,
    ) -> Self {
        let mut message = Self::new(channel, sender_id, Some(recipient_id), payload, MessageType::Response);
        message.correlation_id = Some(correlation_id);
        message
    }

    /// Check if message has expired
    pub fn is_expired(&self) -> bool {
        if self.ttl == 0 {
            return false;
        }
        let now = Utc::now();
        let expiration = self.timestamp + chrono::Duration::seconds(self.ttl as i64);
        now > expiration
    }

    /// Serialize message to bytes for shared memory storage
    pub fn to_bytes(&self) -> DmepResult<Vec<u8>> {
        bincode::serialize(self).map_err(|e| DmepError::Serialization(e.to_string()))
    }

    /// Deserialize message from bytes
    pub fn from_bytes(data: &[u8]) -> DmepResult<Self> {
        bincode::deserialize(data).map_err(|e| DmepError::Deserialization(e.to_string()))
    }
}

/// Subscription information for message routing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    /// Unique subscription identifier
    pub id: Uuid,
    /// Subscriber identifier
    pub subscriber_id: String,
    /// Channel or topic being subscribed to
    pub channel: String,
    /// Subscription timestamp
    pub created_at: DateTime<Utc>,
    /// Optional filter criteria
    pub filters: HashMap<String, String>,
    /// Whether subscription is active
    pub is_active: bool,
}

impl Subscription {
    /// Create a new subscription
    pub fn new(subscriber_id: String, channel: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            subscriber_id,
            channel,
            created_at: Utc::now(),
            filters: HashMap::new(),
            is_active: true,
        }
    }

    /// Add a filter to the subscription
    pub fn add_filter(&mut self, key: String, value: String) {
        self.filters.insert(key, value);
    }
}

/// Shared memory segment information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedMemorySegment {
    /// look for the segment in a particular channel partition allocation
    pub channel: String,
    /// Segment identifier
    pub id: String,
    /// Segment size in bytes
    pub size: usize,
    /// Current usage in bytes
    pub used: usize,
    /// Timestamp when segment was created
    pub created_at: DateTime<Utc>,
    /// Last access timestamp
    pub last_accessed: DateTime<Utc>,
    /// Number of messages stored
    pub message_count: u32,
}

impl SharedMemorySegment {
    /// Create a new shared memory segment
    pub fn new(channel: String, id: String, size: usize) -> Self {
        let now = Utc::now();
        Self {
            channel,
            id,
            size,
            used: 0,
            created_at: now,
            last_accessed: now,
            message_count: 0,
        }
    }

    /// Check if segment has available space
    pub fn has_space(&self, required_bytes: usize) -> bool {
        self.used + required_bytes <= self.size
    }

    /// Get available space in bytes
    pub fn available_space(&self) -> usize {
        self.size.saturating_sub(self.used)
    }

    /// Update access timestamp
    pub fn touch(&mut self) {
        self.last_accessed = Utc::now();
    }
}

/// Message queue entry for shared memory storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageQueueEntry {
    /// Channel partition allocation
    pub channel: String,
    /// Message data
    pub message: Message,
    /// Offset in shared memory segment
    pub offset: usize,
    /// Size of serialized message
    pub size: usize,
    /// Timestamp when added to queue
    pub queued_at: DateTime<Utc>,
}

impl MessageQueueEntry {
    /// Create a new queue entry
    pub fn new(channel: String, message: Message, offset: usize, size: usize) -> Self {
        Self {
            channel,
            message,
            offset,
            size,
            queued_at: Utc::now(),
        }
    }
}

/// Protocol configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DmepConfig {
    /// Default message TTL in seconds
    pub default_ttl: u64,
    /// Maximum message size in bytes
    pub max_message_size: usize,
    /// Shared memory segment size in bytes
    pub segment_size: usize,
    /// Maximum number of segments
    pub max_segments: u32,
    /// Cleanup interval in seconds
    pub cleanup_interval: u64,
    /// Heartbeat interval in seconds
    pub heartbeat_interval: u64,
}

impl Default for DmepConfig {
    fn default() -> Self {
        Self {
            default_ttl: 3600, // 1 hour
            max_message_size: 1024 * 1024, // 1MB
            segment_size: 10 * 1024 * 1024, // 10MB
            max_segments: 10,
            cleanup_interval: 300, // 5 minutes
            heartbeat_interval: 30, // 30 seconds
        }
    }
}

/// Statistics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DmepStats {
    /// Total messages sent
    pub messages_sent: u64,
    /// Total messages received
    pub messages_received: u64,
    /// Total messages dropped
    pub messages_dropped: u64,
    /// Active subscriptions
    pub active_subscriptions: u32,
    /// Active shared memory segments
    pub active_segments: u32,
    /// Total memory usage in bytes
    pub memory_usage: usize,
    /// Last updated timestamp
    pub last_updated: DateTime<Utc>,
}

impl Default for DmepStats {
    fn default() -> Self {
        Self {
            messages_sent: 0,
            messages_received: 0,
            messages_dropped: 0,
            active_subscriptions: 0,
            active_segments: 0,
            memory_usage: 0,
            last_updated: Utc::now(),
        }
    }
}

impl DmepStats {
    /// Update the last updated timestamp
    pub fn touch(&mut self) {
        self.last_updated = Utc::now();
    }
}
