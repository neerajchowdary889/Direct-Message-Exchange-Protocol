use crate::Aloc::Aloc::Aloc;

pub struct MemoryCRUD {
    pub aloc: Aloc,
}

// This is for the CRUD operations of data into channel's memory
impl MemoryCRUD{
    pub fn new(aloc: Aloc) -> Self {
        Self { aloc }
    }

    pub fn WriteToChannel(&mut self, channel: &str, data: &[u8]) -> DmepResult<()> {
        if data.len() <= 0 {
            return Err(DmepError::InvalidDataSize("Data size is 0 - nil data supplied".to_string()));
        }
        if data.len() > self.aloc.channel.get(channel).unwrap().config.max_message_size {
            return Err(DmepError::InvalidDataSize("Data size is greater than the channel's max message size".to_string()));
        }
        if self.aloc.channel.get(channel).unwrap().config.max_queue_size <= self.aloc.channel.get(channel).unwrap().message_queue.len() {
            return Err(DmepError::InvalidDataSize("Channel's message queue is full".to_string()));
        }
        // if channel is not there, return error
        if !self.aloc.channel.contains_key(channel) {
            return Err(DmepError::InvalidDataSize("Channel is not found".to_string()));
        }
        if !self.aloc.channel.get(channel).unwrap().is_active() {
            return Err(DmepError::InvalidDataSize("Channel is not active".to_string()));
        }
        self.aloc.write_to_channel(channel, data)
    }

    pub fn ConsumeFromChannel(&mut self, channel: &str, max_messages: usize) -> DmepResult<Vec<u8>> {
        if max_messages <= 0 {
            return Err(DmepError::InvalidDataSize("Max messages is 0 - nil data supplied".to_string()));
        }
        if channel.is_empty() {
            return Err(DmepError::InvalidDataSize("Channel is empty".to_string()));
        }
        // if channel is not there, return error
        if !self.aloc.channel.contains_key(channel) {
            return Err(DmepError::InvalidDataSize("Channel is not found".to_string()));
        }
        if !self.aloc.channel.get(channel).unwrap().is_active() {
            return Err(DmepError::InvalidDataSize("Channel is not active".to_string()));
        }
        self.aloc.consume_from_channel(channel, max_messages)
    }

}