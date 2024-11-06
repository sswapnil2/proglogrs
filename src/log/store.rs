use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufWriter, Error, ErrorKind, Write};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::{Arc, Mutex};

// protocol
// start index of the data is the offset, and it is absolute in the file.
// first 8 bytes will be used to store length and the next bytes length

const DATA_LENGTH_BYTES: u64 = 8;

pub struct Store {
    file: File,
    writer: Arc<Mutex<BufWriter<File>>>,
    size: u64
}

impl Store {

    pub fn new(path: &Path) -> io::Result<Self> {


        let file = OpenOptions::new().read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(path)?;

        let size = file.metadata()?.len();

        Ok(Store {
            writer: Arc::new(Mutex::new(BufWriter::new(file.try_clone().unwrap()))),
            file,
            size
        })
    }

    pub fn append(&mut self, data: Vec<u8>) -> Result<u64, Error> {
        let mut buf_writer = self.writer.lock().unwrap();
        let len_bytes = (data.len() as u64).to_be_bytes();

        buf_writer.write(&len_bytes)?;
        buf_writer.write(&data)?;
        let offset = self.size;

        self.size = self.size + DATA_LENGTH_BYTES + data.len() as u64;

        Ok(offset)
    }

    pub fn read(& self, offset: u64) -> Result<Vec<u8>, Error> {
        let writer = self.writer.lock();
        writer.unwrap().flush()?;
        if offset > self.size {
            return Err(Error::from(ErrorKind::NotFound))
        }
        // read length of data
        let mut size_arr = [0u8; DATA_LENGTH_BYTES as usize];
        self.file.read_at(&mut size_arr, offset)?;

        // read actual data
        let size = u64::from_be_bytes(size_arr);
        let mut data = vec![0u8; size as usize];
        self.file.read_exact_at(&mut data, offset + DATA_LENGTH_BYTES)?;
        Ok(data)
    }

    pub fn close(&mut self) -> io::Result<()>{
        let mut writer = self.writer.lock().unwrap();
        writer.flush()?;
        Ok(())
    }


}

impl Drop for Store {
    fn drop(&mut self) {
        let mut writer = self.writer.lock().unwrap();
        writer.flush().expect("Error in flushing data to disk");
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use tempfile::tempdir;
    use crate::log::store::Store;

    #[test]
    pub fn test_store_first_time() -> io::Result<()>{

        let data = String::from("My name is Rahul").as_bytes().to_vec();
        let data_2 = String::from("My name is Amol").as_bytes().to_vec();


        let dir = tempdir()?;
        let path = dir.path().join("test_store_first_time.temp");
        let mut store = Store::new(path.as_path())?;

        let off_1 = add_value_to_store_and_verify(&mut store, data.clone())?;
        let off_2 = add_value_to_store_and_verify(&mut store, data_2.clone())?;

        drop(store);

        let mut store = Store::new(path.as_path())?;

        check_vecs(&data, &store.read(off_1)?);
        check_vecs(&data_2, &store.read(off_2)?);

        Ok(())
    }

    fn add_value_to_store_and_verify(store: &mut Store, data: Vec<u8>) -> io::Result<u64> {
        let offset = store.append(data.clone());
        assert!(offset.is_ok());
        let offset_val = offset?;
        let output = store.read(offset_val);
        assert!(output.is_ok());
        let o = output?;
        check_vecs(&data, &o);
        Ok(offset_val)
    }


    fn check_vecs(v1: &Vec<u8>, v2: &Vec<u8>) {
        for i in 0..v2.len() {
            assert_eq!(v1.get(i), v2.get(i));
        }
    }

}