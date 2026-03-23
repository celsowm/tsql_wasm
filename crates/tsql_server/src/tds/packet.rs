use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const TDS7_LOGIN: u8 = 0x10;
pub const TDS7_PRELOGIN: u8 = 0x12;
pub const SQL_BATCH: u8 = 0x01;
pub const RPC: u8 = 0x03;
pub const TABULAR_RESULT: u8 = 0x04;
pub const ATTENTION: u8 = 0x06;

pub const STATUS_EOM: u8 = 0x01;
pub const STATUS_RESET: u8 = 0x08;

pub const HEADER_SIZE: usize = 8;

#[derive(Debug, Clone)]
pub struct PacketHeader {
    pub packet_type: u8,
    pub status: u8,
    pub length: u16,
    pub spid: u16,
    pub packet_id: u8,
    pub window: u8,
}

impl PacketHeader {
    pub fn new(packet_type: u8, length: u16) -> Self {
        Self {
            packet_type,
            status: STATUS_EOM,
            length,
            spid: 0,
            packet_id: 1,
            window: 0,
        }
    }

    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0] = self.packet_type;
        buf[1] = self.status;
        buf[2..4].copy_from_slice(&self.length.to_be_bytes());
        buf[4..6].copy_from_slice(&self.spid.to_be_bytes());
        buf[6] = self.packet_id;
        buf[7] = self.window;
        buf
    }

    pub fn from_bytes(buf: &[u8; HEADER_SIZE]) -> Self {
        Self {
            packet_type: buf[0],
            status: buf[1],
            length: u16::from_be_bytes([buf[2], buf[3]]),
            spid: u16::from_be_bytes([buf[4], buf[5]]),
            packet_id: buf[6],
            window: buf[7],
        }
    }
}

pub async fn read_packet<R: AsyncReadExt + Unpin>(
    reader: &mut R,
) -> io::Result<(PacketHeader, Vec<u8>)> {
    let mut header_buf = [0u8; HEADER_SIZE];
    reader.read_exact(&mut header_buf).await?;
    let header = PacketHeader::from_bytes(&header_buf);

    let data_len = header.length as usize - HEADER_SIZE;
    let mut data = vec![0u8; data_len];
    if data_len > 0 {
        reader.read_exact(&mut data).await?;
    }

    Ok((header, data))
}

pub async fn write_packet<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    packet_type: u8,
    data: &[u8],
) -> io::Result<()> {
    let length = (HEADER_SIZE + data.len()) as u16;
    let header = PacketHeader::new(packet_type, length);
    let header_bytes = header.to_bytes();

    writer.write_all(&header_bytes).await?;
    if !data.is_empty() {
        writer.write_all(data).await?;
    }
    writer.flush().await?;
    Ok(())
}

pub struct PacketBuilder {
    buf: Vec<u8>,
}

impl PacketBuilder {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
        }
    }

    pub fn put_u8(&mut self, v: u8) -> &mut Self {
        self.buf.push(v);
        self
    }

    pub fn put_u16_be(&mut self, v: u16) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn put_u16_le(&mut self, v: u16) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn put_u32_le(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn put_u32_be(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn put_i32_le(&mut self, v: i32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn put_i64_le(&mut self, v: i64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn put_u64_le(&mut self, v: u64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_le_bytes());
        self
    }

    pub fn put_bytes(&mut self, data: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(data);
        self
    }

    pub fn put_b_varchar(&mut self, s: &str) -> &mut Self {
        self.buf.push(s.len() as u8);
        self.buf.extend_from_slice(s.as_bytes());
        self
    }

    pub fn put_us_varchar(&mut self, s: &str) -> &mut Self {
        self.buf.extend_from_slice(&(s.len() as u16).to_be_bytes());
        self.buf.extend_from_slice(s.as_bytes());
        self
    }

    pub fn put_b_vchar_utf16(&mut self, s: &str) -> &mut Self {
        let utf16: Vec<u16> = s.encode_utf16().collect();
        self.buf.push(utf16.len() as u8);
        for c in &utf16 {
            self.buf.extend_from_slice(&c.to_le_bytes());
        }
        self
    }

    pub fn put_us_vchar_utf16(&mut self, s: &str) -> &mut Self {
        let utf16: Vec<u16> = s.encode_utf16().collect();
        self.buf
            .extend_from_slice(&(utf16.len() as u16).to_be_bytes());
        for c in &utf16 {
            self.buf.extend_from_slice(&c.to_le_bytes());
        }
        self
    }

    pub fn put_utf16le(&mut self, s: &str) -> &mut Self {
        for c in s.encode_utf16() {
            self.buf.extend_from_slice(&c.to_le_bytes());
        }
        self
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }
}

pub struct PacketReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> PacketReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn peek_u8(&self) -> Option<u8> {
        self.data.get(self.pos).copied()
    }

    pub fn read_u8(&mut self) -> io::Result<u8> {
        if self.pos >= self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF reading u8",
            ));
        }
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    pub fn read_u16_be(&mut self) -> io::Result<u16> {
        if self.pos + 2 > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF reading u16 BE",
            ));
        }
        let v = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    pub fn read_u16_le(&mut self) -> io::Result<u16> {
        if self.pos + 2 > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF reading u16 LE",
            ));
        }
        let v = u16::from_le_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    pub fn read_u32_le(&mut self) -> io::Result<u32> {
        if self.pos + 4 > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF reading u32 LE",
            ));
        }
        let v = u32::from_le_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    pub fn read_bytes(&mut self, n: usize) -> io::Result<&[u8]> {
        if self.pos + n > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF reading bytes",
            ));
        }
        let slice = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice)
    }

    pub fn skip(&mut self, n: usize) -> io::Result<()> {
        if self.pos + n > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF skipping",
            ));
        }
        self.pos += n;
        Ok(())
    }

    pub fn read_utf16le(&mut self, char_count: usize) -> io::Result<String> {
        let byte_count = char_count * 2;
        let bytes = self.read_bytes(byte_count)?;
        let mut u16s = Vec::with_capacity(char_count);
        for chunk in bytes.chunks_exact(2) {
            u16s.push(u16::from_le_bytes([chunk[0], chunk[1]]));
        }
        Ok(String::from_utf16_lossy(&u16s))
    }
}
