use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Seek};
use std::path::Path;
use memmap2::{MmapMut, MmapOptions};

const OFFSET_WIDTH: usize = 4;
const POSITION_WIDTH: usize = 8;

const END_WIDTH: usize = POSITION_WIDTH + OFFSET_WIDTH;

pub struct Index {
    file: File,
    mem_map: MmapMut,
    size: usize
}



impl Index {

    pub(crate) fn new<P: AsRef<Path>>(path: P, file_size: usize) -> io::Result<Self> {

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(path)?;

        file.set_len(file_size as u64)?;
        let size = file.stream_position()? as usize;

        let mem_map = unsafe { MmapOptions::new().map_mut(&file)? };

        Ok(Index {
            file,
            mem_map,
            size
        })
    }


    pub fn close(&mut self) -> io::Result<()> {
        self.mem_map.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn write(&mut self, offset: u32, position: u64) -> io::Result<()> {

        println!("{}, {}", self.mem_map.len(), self.size + END_WIDTH);
        if self.mem_map.len() < self.size + END_WIDTH {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "End of map reached"));
        }
        self.mem_map[self.size..self.size + OFFSET_WIDTH].copy_from_slice(&offset.to_be_bytes());
        self.mem_map[self.size + OFFSET_WIDTH..self.size + END_WIDTH].copy_from_slice(&position.to_be_bytes());
        self.mem_map.flush()?;
        self.size += END_WIDTH;
        Ok(())
    }

    pub fn read(&self, index: i32) -> io::Result<(u32, u64)> {

        if self.size == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Empty index file"));
        }

        let position = {
            if index >= 0 {
                 index as usize * END_WIDTH
            } else {
                (self.size / END_WIDTH) - 1
            }
        };

        if self.size < position + END_WIDTH {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "End of map reached"));
        }

        let offset = u32::from_be_bytes(
            self.mem_map[position..position + OFFSET_WIDTH].try_into().map_err(|_| io::ErrorKind::InvalidData)?,
        );
        let position = u64::from_be_bytes(
          self.mem_map[position + OFFSET_WIDTH..position + END_WIDTH].try_into().map_err(|_| io::ErrorKind::InvalidData)?,
        );

        Ok((offset, position))
    }

}

impl Drop for Index {
    fn drop(&mut self) {
        if let Err(e) = self.mem_map.flush() {
            eprintln!("Error flushing mmap: {:?}", e);
        }

        // Sync the file to disk
        if let Err(e) = self.file.sync_all() {
            eprintln!("Error syncing file: {:?}", e);
        }
    }
}


#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use super::*;

    fn create_index(name: &str, size: usize) -> io::Result<Index> {
        let dir = tempdir()?;
        let path = dir.path().join(name);
        Index::new(&path, size)
    }

    #[test]
    pub fn test_index() -> io::Result<()>{
        let mut index = create_index("temp_index.temp", 120)?;

        for i in 0..10 {
            index.write(i, (i as u64 + 1) * 1000u64)?;
        }
        for i in 0..10 {
            assert_eq!( (i as u64 + 1) * 1000u64, index.read(i)?.1);
        }
        assert_eq!((9, 10 * 1000u64), index.read(9)?);

        drop(index);

        // load existing file
        let mut index = create_index("temp_index.temp", 108);

        Ok(())
    }

}