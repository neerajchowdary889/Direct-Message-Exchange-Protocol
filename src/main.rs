mod ICP;
mod Aloc;

use ICP::structs::*;
use Aloc::Aloc::Aloc as MemoryAllocator;
use Aloc::Channels::Aloc::{
    ChannelPartition, ChannelPartitionManager, ChannelConfig, ChannelState, 
    SubscriberType, ChannelPartitionError
};
use std::path::PathBuf;

fn main() {
    println!("Direct Message Exchange Protocol - Structs Demo");
    
    // Example: Create a message
    let message = Message::new(
        "channel_events".to_string(),
        "sender_123".to_string(),
        Some("recipient_456".to_string()),
        b"Hello, World!".to_vec(),
        MessageType::Direct,
    );
    
    println!("Created message: {:?}", message.id);
    
    // Example: Serialize message for shared memory
    match message.to_bytes() {
        Ok(bytes) => {
            println!("Message serialized to {} bytes", bytes.len());
            
            // Example: Deserialize message
            match Message::from_bytes(&bytes) {
                Ok(deserialized_message) => {
                    println!("Message deserialized successfully: {:?}", deserialized_message.id);
                }
                Err(e) => println!("Deserialization error: {}", e),
            }
        }
        Err(e) => println!("Serialization error: {}", e),
    }
    
    // Example: Create a subscription
    let subscription = Subscription::new("subscriber_789".to_string(), "channel_events".to_string());
    println!("Created subscription: {:?}", subscription.id);
    
    // Example: Create shared memory segment
    let segment = SharedMemorySegment::new("channel_events".to_string(), "segment_1".to_string(), 1024 * 1024);
    println!("Created shared memory segment: {} bytes", segment.size);
    
    // Example: Create configuration
    let config = DmepConfig::default();
    println!("Default configuration loaded");
    
    // Example: Create memory allocator
    let mut allocator = MemoryAllocator::new(
        10 * 1024 * 1024, // 10MB max size
        PathBuf::from("/tmp/dmep_swap"),
        false, // No swap for demo
        0,
        80, // Start swapping at 80% usage
    ).expect("Failed to create allocator");
    
    println!("Memory allocator created");
    
    // Define the channels
    let channels: Vec<&str> = vec!["channel_events", "channel_alerts", "channel_notifications", "channel_logs", "channel_metrics"];

    // Allocate shared memory segments for each channel
    for channel in channels {
        match allocator.allocate_shared_memory_segment(channel, 1024 * 1024) { // 1MB segment
            Ok(segment_id) => {
                println!("Allocated shared memory segment: {}", segment_id);
            }
            Err(e) => println!("Failed to allocate shared memory segment: {}", e),
        }
    }
    
    println!("All shared memory segments allocated");
    // Example: Allocate shared memory segment
    match allocator.allocate_shared_memory_segment("channel_events", 1024 * 1024) { // 1MB segment
        Ok(segment_id) => {
            println!("Allocated shared memory segment: {}", segment_id);
            
            // Example: Write data to segment
            let test_data = b"Hello from shared memory!";
            let data_len = test_data.len();
            match allocator.write_to_segment(&segment_id, 0, test_data) {
                Ok(_) => {
                    println!("Data written to segment");
                    
                    // Example: Read data back
                    match allocator.read_from_segment(&segment_id, 0, data_len) {
                        Ok(read_data) => {
                            println!("Data read from segment: {:?}", String::from_utf8_lossy(&read_data));
                        }
                        Err(e) => println!("Failed to read from segment: {}", e),
                    }
                }
                Err(e) => println!("Failed to write to segment: {}", e),
            }
            
            // Example: Get memory statistics
            let (total, used, available) = allocator.get_memory_stats();
            println!("Memory stats - Total: {} bytes, Used: {} bytes, Available: {} bytes", 
                     total, used, available);
        }
        Err(e) => println!("Failed to allocate segment: {}", e),
    }
    
    // ===== CHANNEL PARTITION SYSTEM DEMO =====
    println!("\n=== Channel Partition System Demo ===");
    
    // Create channel partition manager
    let mut channel_manager = ChannelPartitionManager::new(10 * 1024 * 1024); // 10MB total
    
    // Create different channels with different configurations
    let event_config = ChannelConfig {
        max_message_size: 1024,
        max_queue_size: 100,
        message_ttl: 1800, // 30 minutes
        persistent: false,
        ordered: true,
        deduplicate: true,
        ..Default::default()
    };
    
    let notification_config = ChannelConfig {
        max_message_size: 512,
        max_queue_size: 50,
        message_ttl: 3600, // 1 hour
        persistent: true,
        ordered: false,
        deduplicate: false,
        ..Default::default()
    };
    
    // Create channels
    match channel_manager.create_channel("events".to_string(), 2 * 1024 * 1024, Some(event_config)) {
        Ok(_) => println!("Created 'events' channel"),
        Err(e) => println!("Failed to create events channel: {}", e),
    }
    
    match channel_manager.create_channel("notifications".to_string(), 1 * 1024 * 1024, Some(notification_config)) {
        Ok(_) => println!("Created 'notifications' channel"),
        Err(e) => println!("Failed to create notifications channel: {}", e),
    }
    
    match channel_manager.create_channel("requests".to_string(), 3 * 1024 * 1024, None) {
        Ok(_) => println!("Created 'requests' channel"),
        Err(e) => println!("Failed to create requests channel: {}", e),
    }
    
    // Add subscribers to channels (simulating different language services)
    if let Ok(events_channel) = channel_manager.get_channel("events") {
        let mut channel = events_channel.lock().unwrap();
        
        // Add Rust function subscriber
        let _ = channel.add_subscriber(
            "rust_event_processor".to_string(),
            SubscriberType::Function,
            "rust".to_string(),
            "process_event".to_string(),
            5000,
        );
        
        // Add Python module subscriber
        let _ = channel.add_subscriber(
            "python_analytics".to_string(),
            SubscriberType::Module,
            "python".to_string(),
            "analytics.process_event".to_string(),
            10000,
        );
        
        // Add Node.js service subscriber
        let _ = channel.add_subscriber(
            "nodejs_logger".to_string(),
            SubscriberType::Service,
            "nodejs".to_string(),
            "logging-service".to_string(),
            3000,
        );
        
        println!("Added subscribers to events channel");
    }
    
    // Publish messages to different channels
    let event_message = Message::new(
        "events".to_string(),
        "user_service".to_string(),
        None,
        b"User logged in successfully".to_vec(),
        MessageType::Notification,
    );
    
    let notification_message = Message::new(
        "notifications".to_string(),
        "system".to_string(),
        Some("user_123".to_string()),
        b"System maintenance scheduled".to_vec(),
        MessageType::Notification,
    );
    
    let request_message = Message::new(
        "requests".to_string(),
        "client_app".to_string(),
        Some("api_service".to_string()),
        b"GET /api/users".to_vec(),
        MessageType::Request,
    );
    
    // Publish messages
    if let Ok(events_channel) = channel_manager.get_channel("events") {
        let mut channel = events_channel.lock().unwrap();
        match channel.publish_message(event_message) {
            Ok(_) => println!("Published event message"),
            Err(e) => println!("Failed to publish event: {}", e),
        }
    }
    
    if let Ok(notifications_channel) = channel_manager.get_channel("notifications") {
        let mut channel = notifications_channel.lock().unwrap();
        match channel.publish_message(notification_message) {
            Ok(_) => println!("Published notification message"),
            Err(e) => println!("Failed to publish notification: {}", e),
        }
    }
    
    if let Ok(requests_channel) = channel_manager.get_channel("requests") {
        let mut channel = requests_channel.lock().unwrap();
        match channel.publish_message(request_message) {
            Ok(_) => println!("Published request message"),
            Err(e) => println!("Failed to publish request: {}", e),
        }
    }
    
    // Simulate message consumption by subscribers
    if let Ok(events_channel) = channel_manager.get_channel("events") {
        let mut channel = events_channel.lock().unwrap();
        
        // Get messages for Rust subscriber
        match channel.get_messages_for_subscriber("rust_event_processor", 1) {
            Ok(messages) => {
                for message in messages {
                    println!("Rust processor received: {:?}", String::from_utf8_lossy(&message.payload));
                }
            }
            Err(e) => println!("Failed to get messages for Rust processor: {}", e),
        }
        
        // Get messages for Python subscriber
        match channel.get_messages_for_subscriber("python_analytics", 1) {
            Ok(messages) => {
                for message in messages {
                    println!("Python analytics received: {:?}", String::from_utf8_lossy(&message.payload));
                }
            }
            Err(e) => println!("Failed to get messages for Python analytics: {}", e),
        }
    }
    
    // Demonstrate channel state management
    if let Ok(events_channel) = channel_manager.get_channel("events") {
        let mut channel = events_channel.lock().unwrap();
        
        println!("Events channel state: {:?}", channel.state);
        channel.lock();
        println!("Events channel state after lock: {:?}", channel.state);
        channel.unlock();
        println!("Events channel state after unlock: {:?}", channel.state);
    }
    
    // Show channel statistics
    let summaries = channel_manager.get_all_summaries();
    println!("\n=== Channel Statistics ===");
    for summary in summaries {
        println!("Channel: {}", summary.channel);
        println!("  State: {:?}", summary.state);
        println!("  Memory Usage: {:.1}%", summary.memory_usage_percentage);
        println!("  Queue Size: {}", summary.queue_size);
        println!("  Subscribers: {} ({} active)", summary.subscriber_count, summary.active_subscribers);
        println!("  Created: {}", summary.created_at);
        println!("  Last Accessed: {}", summary.last_accessed);
        println!();
    }
    
    // Show overall memory usage
    println!("Total Memory Usage: {} bytes", channel_manager.get_total_memory_usage());
    println!("Available Memory: {} bytes", channel_manager.get_available_memory());
    
    // Demonstrate cross-language function call simulation
    println!("\n=== Cross-Language Function Call Simulation ===");
    
    // Simulate calling a Python function from Rust
    let python_call_message = Message::new(
        "requests".to_string(),
        "rust_service".to_string(),
        Some("python_ml_service".to_string()),
        b"{\"function\": \"predict\", \"data\": [1, 2, 3, 4, 5]}".to_vec(),
        MessageType::Request,
    );
    
    if let Ok(requests_channel) = channel_manager.get_channel("requests") {
        let mut channel = requests_channel.lock().unwrap();
        match channel.publish_message(python_call_message) {
            Ok(_) => println!("Sent function call to Python ML service"),
            Err(e) => println!("Failed to send function call: {}", e),
        }
        
        // Simulate Python service processing and responding
        match channel.consume_message() {
            Ok(Some(message)) => {
                println!("Python ML service received: {:?}", String::from_utf8_lossy(&message.payload));
                
                // Simulate Python response
                let python_response = Message::new_response(
                    "requests".to_string(),
                    "python_ml_service".to_string(),
                    "rust_service".to_string(),
                    b"{\"result\": 0.85, \"confidence\": 0.92}".to_vec(),
                    message.correlation_id.unwrap_or(message.id),
                );
                
                match channel.publish_message(python_response) {
                    Ok(_) => println!("Python ML service sent response back"),
                    Err(e) => println!("Failed to send response: {}", e),
                }
            }
            Ok(None) => println!("No messages in queue"),
            Err(e) => println!("Failed to consume message: {}", e),
        }
    }
    
    println!("\n=== Demo Complete ===");
}
