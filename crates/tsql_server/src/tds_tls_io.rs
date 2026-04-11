use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

const TDS_HEADER_SIZE: usize = 8;
const TDS_PRELOGIN: u8 = 0x12;
const TDS_STATUS_EOM: u8 = 0x01;

/// IO adapter that wraps/unwraps TDS framing around TLS handshake bytes.
///
/// During TLS handshake, TLS records are tunneled inside TDS PRELOGIN (0x12)
/// packets. After handshake completes, call `set_raw_mode()` to switch to
/// direct passthrough — post-handshake traffic is raw TLS over TCP.
pub struct TdsTlsIo {
    stream: TcpStream,
    raw_mode: Arc<AtomicBool>,
    // Read buffering
    read_buf: Vec<u8>,
    read_pos: usize,
    // Header reading state
    header_buf: [u8; TDS_HEADER_SIZE],
    header_read: usize,
    payload_remaining: usize,
    reading_header: bool,
    // Write buffering for partial writes
    write_buf: Vec<u8>,
    write_pos: usize,
}

impl TdsTlsIo {
    pub fn new(stream: TcpStream, raw_mode: Arc<AtomicBool>) -> Self {
        Self {
            stream,
            raw_mode,
            read_buf: Vec::with_capacity(4096),
            read_pos: 0,
            header_buf: [0u8; TDS_HEADER_SIZE],
            header_read: 0,
            payload_remaining: 0,
            reading_header: true,
            write_buf: Vec::new(),
            write_pos: 0,
        }
    }

    pub fn raw_mode_flag(&self) -> Arc<AtomicBool> {
        self.raw_mode.clone()
    }
}

impl AsyncRead for TdsTlsIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        if this.raw_mode.load(Ordering::Acquire) {
            return Pin::new(&mut this.stream).poll_read(cx, buf);
        }

        // Return any buffered payload bytes first
        if this.read_pos < this.read_buf.len() {
            let available = &this.read_buf[this.read_pos..];
            let to_copy = available.len().min(buf.remaining());
            buf.put_slice(&available[..to_copy]);
            this.read_pos += to_copy;
            if this.read_pos >= this.read_buf.len() {
                this.read_buf.clear();
                this.read_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        // Read TDS header
        if this.reading_header {
            while this.header_read < TDS_HEADER_SIZE {
                let mut tmp = ReadBuf::new(&mut this.header_buf[this.header_read..]);
                match Pin::new(&mut this.stream).poll_read(cx, &mut tmp) {
                    Poll::Ready(Ok(())) => {
                        let n = tmp.filled().len();
                        if n == 0 {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "TdsTlsIo: EOF reading TDS header",
                            )));
                        }
                        this.header_read += n;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }
            let pkt_type = this.header_buf[0];
            let total_len = u16::from_be_bytes([this.header_buf[2], this.header_buf[3]]) as usize;
            let payload_len = total_len.saturating_sub(TDS_HEADER_SIZE);
            log::debug!(
                "TdsTlsIo READ: TDS type=0x{:02X} total={} payload={}",
                pkt_type,
                total_len,
                payload_len
            );

            this.read_buf.clear();
            this.read_buf.resize(payload_len, 0);
            this.read_pos = 0;
            this.payload_remaining = payload_len;
            this.reading_header = false;
            this.header_read = 0;
        }

        // Read payload
        while this.payload_remaining > 0 {
            let offset = this.read_buf.len() - this.payload_remaining;
            let mut tmp = ReadBuf::new(&mut this.read_buf[offset..]);
            match Pin::new(&mut this.stream).poll_read(cx, &mut tmp) {
                Poll::Ready(Ok(())) => {
                    let n = tmp.filled().len();
                    if n == 0 {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "TdsTlsIo: EOF reading TDS payload",
                        )));
                    }
                    this.payload_remaining -= n;
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        this.reading_header = true;
        this.read_pos = 0;

        // Return payload data
        let to_copy = this.read_buf.len().min(buf.remaining());
        buf.put_slice(&this.read_buf[..to_copy]);
        this.read_pos = to_copy;
        if this.read_pos >= this.read_buf.len() {
            this.read_buf.clear();
            this.read_pos = 0;
        }

        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for TdsTlsIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        if this.raw_mode.load(Ordering::Acquire) {
            return Pin::new(&mut this.stream).poll_write(cx, buf);
        }

        // If we have a pending write_buf, flush it first
        if !this.write_buf.is_empty() {
            while this.write_pos < this.write_buf.len() {
                match Pin::new(&mut this.stream).poll_write(cx, &this.write_buf[this.write_pos..]) {
                    Poll::Ready(Ok(n)) => {
                        this.write_pos += n;
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Pending => return Poll::Pending,
                }
            }
            this.write_buf.clear();
            this.write_pos = 0;
        }

        // Wrap in TDS PRELOGIN packet
        log::debug!("TdsTlsIo WRITE: wrapping {} bytes in TDS 0x12", buf.len());
        let total_len = TDS_HEADER_SIZE + buf.len();
        this.write_buf = Vec::with_capacity(total_len);
        this.write_buf.push(TDS_PRELOGIN);
        this.write_buf.push(TDS_STATUS_EOM);
        this.write_buf
            .extend_from_slice(&(total_len as u16).to_be_bytes());
        this.write_buf.extend_from_slice(&[0u8; 2]); // spid
        this.write_buf.push(1); // packet_id
        this.write_buf.push(0); // window
        this.write_buf.extend_from_slice(buf);
        this.write_pos = 0;

        // Try to write as much as possible
        while this.write_pos < this.write_buf.len() {
            match Pin::new(&mut this.stream).poll_write(cx, &this.write_buf[this.write_pos..]) {
                Poll::Ready(Ok(n)) => {
                    this.write_pos += n;
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => {
                    if this.write_pos > 0 {
                        // Partial write — report all data consumed since we buffered it
                        return Poll::Ready(Ok(buf.len()));
                    }
                    return Poll::Pending;
                }
            }
        }

        this.write_buf.clear();
        this.write_pos = 0;
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}
