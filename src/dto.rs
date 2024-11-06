pub mod record {
    include!(concat!(env!("OUT_DIR"), "/record.rs"));
}
use record::Record;

impl Record {

    pub fn new(offset: u64, data: Vec<u8>) -> Self {
        Record {
            offset,
            value: data
        }
    }

    pub fn from_data(data: Vec<u8>) -> Self {
        Record {
            offset: 0,
            value: data
        }
    }

    pub fn set_offset(&mut self, offset: u64) {
        self.offset = offset;
    }

}