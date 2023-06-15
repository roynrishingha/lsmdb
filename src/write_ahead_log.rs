use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

pub(crate) struct WriteAheadLog {
    file: File,
}

impl WriteAheadLog {
    /// Create a new Write Ahead Log in the given directory.
    pub(crate) fn new(log_file_path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(log_file_path)?;

        Ok(Self { file })
    }

    pub(crate) fn write(&mut self, data: &[u8]) -> io::Result<()> {
        self.file.write_all(data)?;
        self.file.flush()?;
        Ok(())
    }

    pub(crate) fn read(&mut self, position: u64, buffer: &mut [u8]) -> io::Result<usize> {
        self.file.seek(SeekFrom::Start(position))?;
        self.file.read(buffer)
    }

    pub(crate) fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()?;
        Ok(())
    }
}
