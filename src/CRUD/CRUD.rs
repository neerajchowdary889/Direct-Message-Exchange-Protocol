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
        self.aloc.write_to_channel(channel, data)
    }

    pub fn ReadFromChannel(&mut self, channel: &str, offset: usize, length: usize) -> DmepResult<Vec<u8>> {
        if offset < 0 {
            return Err(DmepError::InvalidDataSize("Offset is less than 0".to_string()));
        }
        if length <= 0 {
            return Err(DmepError::InvalidDataSize("Length is 0 - nil data supplied".to_string()));
        }
        if offset + length > self.aloc.channel.get(channel).unwrap().config.max_message_size {
            return Err(DmepError::InvalidDataSize("Offset and length is greater than the channel's max message size".to_string()));
        }
        self.aloc.read_from_channel(channel, offset, length)
    }

}