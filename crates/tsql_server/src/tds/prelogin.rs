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

        if (offset as usize) + (length as usize) > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "prelogin token data out of bounds",
            ));
        }

        let token_data = &data[offset as usize..(offset as usize) + (length as usize)];

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
    // Build the data section first to compute offsets
    let mut data = Vec::with_capacity(32);

    // VERSION data: 6 bytes (major, minor, build LE, sub_build LE)
    let version_offset = 0u16;
    data.push(16); // major = 16
    data.push(0); // minor = 0
    data.extend_from_slice(&0x1009u16.to_be_bytes()); // build = 4105
    data.extend_from_slice(&1u16.to_be_bytes()); // sub_build = 1

    // ENCRYPTION data: 1 byte (match client's request, default to OFF)
    let enc_offset = version_offset + 6;
    data.push(encryption); // 0x00 = OFF (no TLS), 0x01 = ON

    // INSTOPT data: 1 byte (null-terminated empty instance)
    let inst_offset = enc_offset + 1;
    data.push(0x00);

    // THREADID data: 4 bytes (BE u32, per tiberius convention)
    let thread_offset = inst_offset + 1;
    data.extend_from_slice(&0u32.to_be_bytes());

    // MARS data: 1 byte
    let mars_offset = thread_offset + 4;
    data.push(0x00); // MARS off

    // Build the token table
    let token_table_size = 5 * 5 + 1; // 5 tokens * 5 bytes + 1 terminator = 26

    let mut b = PacketBuilder::with_capacity(token_table_size + data.len());

    // Token entries
    b.put_u8(PRELOGIN_VERSION);
    b.put_u16_be(token_table_size as u16 + version_offset);
    b.put_u16_be(6);

    b.put_u8(PRELOGIN_ENCRYPTION);
    b.put_u16_be(token_table_size as u16 + enc_offset);
    b.put_u16_be(1);

    b.put_u8(PRELOGIN_INSTOPT);
    b.put_u16_be(token_table_size as u16 + inst_offset);
    b.put_u16_be(1);

    b.put_u8(PRELOGIN_THREADID);
    b.put_u16_be(token_table_size as u16 + thread_offset);
    b.put_u16_be(4);

    b.put_u8(PRELOGIN_MARS);
    b.put_u16_be(token_table_size as u16 + mars_offset);
    b.put_u16_be(1);

    // Terminator
    b.put_u8(PRELOGIN_TERMINATOR);

    // Data section
    b.put_bytes(&data);

    b.into_vec()
}
