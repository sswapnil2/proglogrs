mod store;
mod index;
mod segment;
mod config;
mod log;

use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};
use crate::dto::record::Record;

pub struct Log {
    records: Arc<RwLock<Vec<Record>>>
}

impl Log {

    pub fn new() -> Self {
        Log {
            records: Arc::new(RwLock::new(Vec::new()))
        }
    }

    pub fn append(&self, data: Vec<u8>) -> u64 {

        let mut records = self.records.write().unwrap();
        let offset = records.len() as u64;
        records.push(Record::new(offset, data));
        offset
    }

    pub fn read(&self, offset: u64) -> Option<Record> {

        if let Ok(records) = self.records.read() {
            return records.get(offset as usize).cloned();
        }
        None
    }


}


#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    pub fn test_log_single_thread() {
        let log = Log::new();
        let vec = String::from("SWapnil").as_bytes().to_vec();
        let offset = log.append(vec.clone());
        assert_eq!(0, offset);
        let out = log.read(offset);
        assert!(out.is_some());
        let value = out.unwrap();
        for i in 0..vec.len() {
            assert_eq!(vec.get(i).unwrap(), value.value.get(i).unwrap());
        }

    }

}