use crate::Aloc::Channels::Structs::{ChannelConfig, ChannelStats};


impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            max_message_size: 1024 * 1024 * 10, // 1MB
            max_queue_size: 1000,
            message_ttl: 3600, // 1 hour
            persistent: false,
            ordered: true,
            deduplicate: false,
        }
    }
}

impl ChannelConfig{
    pub fn new(
        max_message_size: usize,
        max_queue_size: usize,
        message_ttl: u64,
        persistent: bool,
        ordered: bool,
        deduplicate: bool,
    ) -> Self {
        Self {
            max_message_size,
            max_queue_size,
            message_ttl,
            persistent,
            ordered,
            deduplicate,
        }
    }
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