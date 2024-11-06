use std::cell::RefCell;
use std::fs::read_dir;
use std::io;
use std::io::{Error, ErrorKind};
use std::path::Path;
use crate::log::config::Config;
use crate::log::segment::Segment;

struct Log<'a> {
    active_segment: RefCell<Option<Segment<'a>>>,
    segments: Vec<Segment<'a>>,
    dir: String,
    config: Config
}

impl Log {

    fn new(dir: String, mut c: Config) -> io::Result<Self> {

        if c.segment_config.max_store_bytes == 0 {
            c.segment_config.max_store_bytes = 1024;
        }

        if c.segment_config.max_index_bytes == 0 {
            c.segment_config.max_index_bytes = 1024;
        }

        let mut log = Log {
            dir,
            config: c,
            segments: vec![],
            active_segment: RefCell::new(None)
        };
        log.setup()?;
        Ok(log)
    }

    fn setup(&mut self) -> io::Result<()> {
        let path = Path::new(self.dir.as_str());
        if !path.exists() {
            return Err(Error::new(ErrorKind::NotFound, "Directory now found"));
        }

        if !path.is_dir() {
            return Err(Error::new(ErrorKind::NotFound, "Expected directory, but it was a file"));
        }

        let _ = read_dir(path)?
            .filter(|d| d.is_ok())
            .map(|d| d.unwrap())
            .map(|d| d.path())
            .filter(|d| {
                d.ends_with(".store")
            })
            .map(|f| {
                let arr: Vec<&str> = f.to_str().unwrap().split(".").collect();
                return arr.get(0);
            }

            )
            .map(|s| {});





        Ok(())
    }
}