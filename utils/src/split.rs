use std::fs::File;
use std::io::{BufWriter, Write};
use zstd::stream::write::{AutoFinishEncoder, Encoder};

type WBufEncoder = BufWriter<AutoFinishEncoder<'static, File>>;

pub struct ZSplit {
    prefix: String,
    encoder: WBufEncoder,
    size_bytes: usize,
    bytes_written: usize,
    num_splits: usize,
    compression_level: i32,
    num_threads: u32,
    buffer_size: usize,
}

impl ZSplit {
    pub fn new(
        prefix: &str,
        size_bytes: usize,
        compression_level: i32,
        num_threads: u32,
        buffer_size: usize,
    ) -> std::io::Result<Self> {
        Ok(Self {
            prefix: String::from(prefix),
            encoder: Self::new_file(prefix, 1, compression_level, num_threads, buffer_size)?,
            size_bytes: size_bytes,
            bytes_written: 0,
            num_splits: 1,
            compression_level: compression_level,
            num_threads: num_threads,
            buffer_size: buffer_size,
        })
    }

    fn new_file(
        prefix: &str,
        idx: usize,
        compression_level: i32,
        num_threads: u32,
        buffer_size: usize,
    ) -> std::io::Result<WBufEncoder> {
        let file_name = format!("{}.{}.zst", prefix, idx);
        let file = File::create(file_name)?;
        let mut encoder = Encoder::new(file, compression_level)?;
        encoder.multithread(num_threads)?;
        Ok(BufWriter::with_capacity(buffer_size, encoder.auto_finish()))
    }

    fn rotate(&mut self) -> std::io::Result<()> {
        self.num_splits += 1;
        self.encoder = Self::new_file(
            &self.prefix,
            self.num_splits,
            self.compression_level,
            self.num_threads,
            self.buffer_size,
        )?;
        self.bytes_written = 0;
        Ok(())
    }

    pub fn write(&mut self, content: &[u8]) -> std::io::Result<()> {
        if self.bytes_written > self.size_bytes {
            self.rotate()?;
        }
        self.encoder.write_all(content)?;
        self.bytes_written += content.len();
        Ok(())
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.encoder.flush()
    }
}
