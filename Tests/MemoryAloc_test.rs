// write test case to test the ChannelConfig::default() and then ChannelPartition::new()
use crate::Aloc::Channels::Structs::{ChannelConfig, ChannelPartition};

#[cfg(test)]
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

#[cfg(test)]
fn test_channel_partition_new() {
    let config = ChannelConfig::default();
    let partition = ChannelPartition::new("test_channel", 1024 * 1024 * 10, Some(config));
    println!("--> partition: {:?}", partition);
    assert_eq!(partition.channel, "test_channel");
    assert_eq!(partition.max_size, 1024 * 1024 * 10);
    assert_eq!(partition.config, config);
}