use super::packet::{PacketBuilder, PacketReader};
use std::io;

pub const PRELOGIN_VERSION: u8 = 0x00;
pub const PRELOGIN_ENCRYPTION: u8 = 0x01;
pub const PRELOGIN_INSTOPT: u8 = 0x02;
pub const PRELOGIN_THREADID: u8 = 0x03;
pub const PRELOGIN_MARS: u8 = 0x04;
pub const PRELOGIN_TRACEID: u8 = 0x05;
pub const PRELOGIN_TERMINATOR: u8 = 0xFF;

pub const ENCRYPT_NOT_SUP: u8 = 0x02;
pub const ENCRYPT_OFF: u8 = 0x00;
pub const ENCRYPT_ON: u8 = 0x01;
pub const ENCRYPT_REQUIRED: u8 = 0x03;

#[derive(Debug, Clone)]
pub struct PreloginData {
    pub version: Option<[u8; 6]>,
    pub encryption: u8,
    pub instance: Option<String>,
    pub thread_id: Option<u32>,
    pub mars: Option<u8>,
}

pub fn parse_prelogin(data: &[u8]) -> io::Result<PreloginData> {
    let mut reader = PacketReader::new(data);
    let mut version = None;
    let mut encryption = 0u8;
    let mut instance = None;
    let mut thread_id = None;
    let mut mars = None;

    loop {
        if reader.remaining() == 0 {
            break;
        }
        let token = reader.read_u8()?;
        if token == PRELOGIN_TERMINATOR {
            break;
        }
        let offset = reader.read_u16_be()?;
        let length = reader.read_u16_be()?;

        let saved_pos = reader.pos();

        let payload_offset = (offset as usize).saturating_sub(super::packet::HEADER_SIZE);
        if payload_offset + (length as usize) > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "prelogin token data out of bounds",
            ));
        }

        let token_data = &data[payload_offset..payload_offset + (length as usize)];

        match token {
            PRELOGIN_VERSION => {
                if token_data.len() >= 6 {
                    let mut v = [0u8; 6];
                    v.copy_from_slice(&token_data[..6]);
                    version = Some(v);
                }
            }
            PRELOGIN_ENCRYPTION => {
                if !token_data.is_empty() {
                    encryption = token_data[0];
                }
            }
            PRELOGIN_INSTOPT => {
                instance = Some(
                    String::from_utf8_lossy(token_data)
                        .trim_end_matches('\0')
                        .to_string(),
                );
            }
            PRELOGIN_THREADID => {
                if token_data.len() >= 4 {
                    thread_id = Some(u32::from_le_bytes([
                        token_data[0],
                        token_data[1],
                        token_data[2],
                        token_data[3],
                    ]));
                }
            }
            PRELOGIN_MARS => {
                if !token_data.is_empty() {
                    mars = Some(token_data[0]);
                }
            }
            _ => {
                // Unknown token, skip
            }
        }

        reader = PacketReader::new(data);
        reader.skip(saved_pos)?;
    }

    Ok(PreloginData {
        version,
        encryption,
        instance,
        thread_id,
        mars,
    })
}

pub fn build_prelogin_response(encryption: u8) -> Vec<u8> {
    // 5 tokens * 5 bytes + 1 terminator = 26
    let token_table_size: u16 = 26;

    // Data layout (offsets from start of data section):
    // VERSION:   0..6   (6 bytes)
    // ENCRYPTION: 6      (1 byte)
    // INSTOPT:   7       (1 byte)
    // THREADID:  8..12   (4 bytes)
    // MARS:      12      (1 byte)

    let version_offset = token_table_size + 0 + super::packet::HEADER_SIZE as u16;
    let enc_offset = token_table_size + 6 + super::packet::HEADER_SIZE as u16;
    let inst_offset = token_table_size + 7 + super::packet::HEADER_SIZE as u16;
    let thread_offset = token_table_size + 8 + super::packet::HEADER_SIZE as u16;
    let mars_offset = token_table_size + 12 + super::packet::HEADER_SIZE as u16;

    let mut b = PacketBuilder::with_capacity(26 + 13);

    // Token entries
    b.put_u8(PRELOGIN_VERSION);
    b.put_u16_be(version_offset);
    b.put_u16_be(6);

    b.put_u8(PRELOGIN_ENCRYPTION);
    b.put_u16_be(enc_offset);
    b.put_u16_be(1);

    b.put_u8(PRELOGIN_INSTOPT);
    b.put_u16_be(inst_offset);
    b.put_u16_be(1);

    b.put_u8(PRELOGIN_THREADID);
    b.put_u16_be(thread_offset);
    b.put_u16_be(4);

    b.put_u8(PRELOGIN_MARS);
    b.put_u16_be(mars_offset);
    b.put_u16_be(1);

    // Terminator
    b.put_u8(PRELOGIN_TERMINATOR);

    // Data section
    b.put_u8(16); // major
    b.put_u8(0);  // minor
    b.put_u16_be(0x1009); // build = 4105
    b.put_u16_be(1); // sub_build = 1
    b.put_u8(encryption); // encryption level
    b.put_u8(0x00); // instance (empty)
    b.put_u32_be(0); // thread id
    b.put_u8(0x00); // MARS off

    b.into_vec()
}
