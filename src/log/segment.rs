


use std::io;
use std::path::Path;
use prost::Message;
use crate::dto::record::Record;
use crate::log::config::Config;
use crate::log::index::Index;
use crate::log::store::Store;


pub struct Segment<'a> {
    store: Store,
    index: Index,
    config: &'a Config,
    base_offset: usize,
    next_offset: usize
}

impl<'a> Segment<'a> {

    pub fn new(dir: &str, c: &'a Config, base_offset: usize) -> io::Result<Self> {

        let dir = Path::new(dir);

        let store_path = dir.join(format!("{}.store", base_offset));
        let index_path = dir.join(format!("{}.index", base_offset));

        let store = Store::new(&store_path)?;


        let index = Index::new(&index_path, c.segment_config.max_index_bytes)?;


        let next_offset = match index.read(-1) {
            Ok(val) => base_offset + val.0 as usize + 1,
            Err(_) => base_offset
        };

        Ok(Segment {
            store,
            index,
            base_offset,
            next_offset,
            config: c
        })
    }

    pub fn append(&mut self, mut record: Record) -> io::Result<u64> {

        let current_offset: u64 = self.next_offset as u64;
        record.set_offset(current_offset);

        let mut bytes: Vec<u8> = vec![];
        record.encode(&mut bytes)?;
        // save the record in store
        let position = self.store.append(bytes)?;

        // update the index file
        // relative offset
        self.index.write((self.next_offset - self.base_offset + 1) as u32, position)?;
        self.next_offset += 1;
        Ok(current_offset)
    }


    pub fn read(&self, offset: u64) -> io::Result<Record> {

        let index = (offset - self.base_offset as u64) as i32;

        let (_, position) = self.index.read(index)?;

        let value = self.store.read(position)?;

        let record: Record = Message::decode(&value[..])?;
        Ok(record)
    }

    pub fn close(&mut self) -> io::Result<()> {

        self.store.close()?;
        self.index.close()?;

        Ok(())
    }

}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use crate::log::config::SegmentConfig;
    use super::*;

    #[test]
    pub fn test_segment() -> io::Result<()> {

        let config = Config {
            segment_config: SegmentConfig {
                max_index_bytes: 120,
                max_store_bytes: 1000,
                initial_offset: 0,

            }
        };

        let dir = tempdir()?;
        let binding = dir.path().join("segments");

        if !binding.exists() {
            std::fs::create_dir(&binding)?;
        }

        let temp_dir = binding.to_str().unwrap();

        let mut segment= Segment::new(temp_dir, &config, 1000)?;

        let record = Record::from_data(String::from("abc").into_bytes());
        let offset = segment.append(record.clone()).unwrap();

        let record2 = segment.read(offset).unwrap();
        assert_eq!(record.value, record2.value);

        Ok(())
    }

}