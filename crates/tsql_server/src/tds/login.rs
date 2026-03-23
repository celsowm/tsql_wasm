use super::packet::PacketReader;
use std::io;

pub const TDS_VERSION_74: u32 = 0x00000074;

#[derive(Debug, Clone)]
pub struct Login7Data {
    pub tds_version: u32,
    pub packet_size: u32,
    pub client_prog_ver: u32,
    pub client_pid: u32,
    pub connection_id: u32,
    pub option_flags1: u8,
    pub option_flags2: u8,
    pub type_flags: u8,
    pub option_flags3: u8,
    pub client_time_zone: i32,
    pub client_lcid: u32,
    pub hostname: String,
    pub username: String,
    pub password: String,
    pub app_name: String,
    pub server_name: String,
    pub client_interface_name: String,
    pub language: String,
    pub database: String,
    pub sspi: Vec<u8>,
    pub attach_db_file: String,
}

pub fn decode_password(encrypted: &[u8]) -> String {
    let mut bytes = Vec::with_capacity(encrypted.len());
    for &b in encrypted {
        let swapped = ((b & 0x0F) << 4) | ((b & 0xF0) >> 4);
        bytes.push(swapped ^ 0xA5);
    }
    // Decode as UTF-16LE
    let mut u16s = Vec::with_capacity(bytes.len() / 2);
    for chunk in bytes.chunks_exact(2) {
        u16s.push(u16::from_le_bytes([chunk[0], chunk[1]]));
    }
    String::from_utf16_lossy(&u16s)
}

pub fn parse_login7(data: &[u8]) -> io::Result<Login7Data> {
    let mut reader = PacketReader::new(data);

    // Fixed section: 94 bytes (pre-SSPI)
    if reader.remaining() < 94 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "login7 data too short",
        ));
    }

    let _length = reader.read_u32_le()?; // Total LOGIN7 length
    let tds_version = reader.read_u32_le()?;
    let packet_size = reader.read_u32_le()?;
    let client_prog_ver = reader.read_u32_le()?;
    let client_pid = reader.read_u32_le()?;
    let connection_id = reader.read_u32_le()?;
    let option_flags1 = reader.read_u8()?;
    let option_flags2 = reader.read_u8()?;
    let type_flags = reader.read_u8()?;
    let option_flags3 = reader.read_u8()?;
    let client_time_zone = reader.read_u32_le()? as i32;
    let client_lcid = reader.read_u32_le()?;

    // Offset/Length pairs (each 2+2 bytes)
    let ib_hostname = reader.read_u16_le()?;
    let cch_hostname = reader.read_u16_le()?;
    let ib_username = reader.read_u16_le()?;
    let cch_username = reader.read_u16_le()?;
    let ib_password = reader.read_u16_le()?;
    let cch_password = reader.read_u16_le()?;
    let ib_app_name = reader.read_u16_le()?;
    let cch_app_name = reader.read_u16_le()?;
    let ib_server_name = reader.read_u16_le()?;
    let cch_server_name = reader.read_u16_le()?;
    let _ib_unused = reader.read_u16_le()?;
    let _cb_unused = reader.read_u16_le()?;
    let ib_clt_int_name = reader.read_u16_le()?;
    let cch_clt_int_name = reader.read_u16_le()?;
    let ib_language = reader.read_u16_le()?;
    let cch_language = reader.read_u16_le()?;
    let ib_database = reader.read_u16_le()?;
    let cch_database = reader.read_u16_le()?;
    let _client_id = reader.read_bytes(6)?; // 6-byte MAC
    let ib_sspi = reader.read_u16_le()?;
    let cb_sspi = reader.read_u16_le()?;
    let ib_atch_db_file = reader.read_u16_le()?;
    let cch_atch_db_file = reader.read_u16_le()?;
    let _ib_change_password = reader.read_u16_le()?;
    let _cch_change_password = reader.read_u16_le()?;
    let _cb_sspi_long = reader.read_u32_le()?;

    // Now we're at offset 94 within the LOGIN7 structure
    // But data starts from the beginning of the LOGIN7 block
    // (the PacketReader was given the LOGIN7 data starting after the packet header)

    let extract_utf16 = |offset: u16, char_count: u16| -> String {
        let byte_offset = offset as usize;
        let byte_count = char_count as usize * 2;
        if byte_offset + byte_count > data.len() {
            return String::new();
        }
        let mut u16s = Vec::with_capacity(char_count as usize);
        for i in (0..byte_count).step_by(2) {
            u16s.push(u16::from_le_bytes([
                data[byte_offset + i],
                data[byte_offset + i + 1],
            ]));
        }
        String::from_utf16_lossy(&u16s)
    };

    let hostname = extract_utf16(ib_hostname, cch_hostname);
    let username = extract_utf16(ib_username, cch_username);
    let password = if cch_password > 0 && ib_password as usize + (cch_password as usize * 2) <= data.len() {
        let pw_data = &data[ib_password as usize..ib_password as usize + (cch_password as usize * 2)];
        decode_password(pw_data)
    } else {
        String::new()
    };
    let app_name = extract_utf16(ib_app_name, cch_app_name);
    let server_name = extract_utf16(ib_server_name, cch_server_name);
    let client_interface_name = extract_utf16(ib_clt_int_name, cch_clt_int_name);
    let language = extract_utf16(ib_language, cch_language);
    let database = extract_utf16(ib_database, cch_database);

    let sspi = if cb_sspi > 0 && ib_sspi as usize + cb_sspi as usize <= data.len() {
        data[ib_sspi as usize..ib_sspi as usize + cb_sspi as usize].to_vec()
    } else {
        Vec::new()
    };

    let attach_db_file = extract_utf16(ib_atch_db_file, cch_atch_db_file);

    Ok(Login7Data {
        tds_version,
        packet_size,
        client_prog_ver,
        client_pid,
        connection_id,
        option_flags1,
        option_flags2,
        type_flags,
        option_flags3,
        client_time_zone,
        client_lcid,
        hostname,
        username,
        password,
        app_name,
        server_name,
        client_interface_name,
        language,
        database,
        sspi,
        attach_db_file,
    })
}
