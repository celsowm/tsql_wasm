use super::packet::PacketReader;
use std::io;

/// A parsed RPC parameter with its name and a T-SQL literal value string.
#[derive(Debug, Clone)]
pub struct RpcParam {
    pub name: String,
    pub type_name: String,
    pub value_sql: String,
}

/// Result of parsing an RPC packet.
#[derive(Debug)]
pub struct RpcRequest {
    pub sql: String,
    pub params: Vec<RpcParam>,
}

/// Parse an RPC request packet (sp_executesql=10, sp_prepexec=13).
/// Returns None if not a supported RPC call.
pub fn parse_rpc(data: &[u8]) -> io::Result<Option<RpcRequest>> {
    let mut r = PacketReader::new(data);

    // Skip ALL_HEADERS
    if r.remaining() < 4 { return Ok(None); }
    let total_header_len = r.read_u32_le()? as usize;
    if total_header_len > 4 {
        let skip = total_header_len - 4;
        if r.remaining() < skip { return Ok(None); }
        r.skip(skip)?;
    }

    // ProcIDSwitch
    if r.remaining() < 2 { return Ok(None); }
    let name_len = r.read_u16_le()?;
    let proc_id: u16;
    if name_len == 0xFFFF {
        if r.remaining() < 2 { return Ok(None); }
        proc_id = r.read_u16_le()?;
    } else {
        let byte_len = name_len as usize * 2;
        if r.remaining() < byte_len { return Ok(None); }
        r.skip(byte_len)?;
        proc_id = 0;
    }

    // Option flags
    if r.remaining() < 2 { return Ok(None); }
    let _flags = r.read_u16_le()?;

    // Only handle sp_executesql=10 and sp_prepexec=13
    if proc_id != 10 && proc_id != 13 { return Ok(None); }

    // Param 1: SQL string
    let sql = match read_rpc_nvarchar_param(&mut r)? {
        Some(s) => s,
        None => return Ok(None),
    };

    // Param 2: parameter declaration string e.g. "@val INT,@name NVARCHAR(50)"
    let param_decl = read_rpc_nvarchar_param(&mut r)?.unwrap_or_default();
    let decl_parts = parse_param_decl(&param_decl);

    // Remaining params: actual values
    let mut params: Vec<RpcParam> = Vec::new();
    let mut idx = 0;
    while r.remaining() > 0 {
        if r.remaining() < 1 { break; }
        let name_len = r.read_u8()? as usize;
        let param_name = if name_len > 0 {
            if r.remaining() < name_len * 2 { break; }
            let bytes = r.read_bytes(name_len * 2)?;
            decode_utf16le(bytes)
        } else {
            decl_parts.get(idx).map(|(n, _)| n.clone()).unwrap_or_default()
        };

        if r.remaining() < 1 { break; }
        let _status = r.read_u8()?;
        if r.remaining() < 1 { break; }
        let type_id = r.read_u8()?;

        let (type_name, value_sql) = match read_typed_value(&mut r, type_id) {
            Ok(v) => v,
            Err(_) => break,
        };

        let final_type = if let Some((_, decl_type)) = decl_parts.get(idx) {
            decl_type.clone()
        } else {
            type_name
        };

        params.push(RpcParam { name: param_name, type_name: final_type, value_sql });
        idx += 1;
    }

    Ok(Some(RpcRequest { sql, params }))
}

/// Build DECLARE preamble for all RPC parameters.
pub fn build_param_preamble(params: &[RpcParam]) -> String {
    let mut out = String::new();
    for p in params {
        if p.name.is_empty() { continue; }
        out.push_str(&format!("DECLARE {} {} = {};\n", p.name, p.type_name, p.value_sql));
    }
    out
}

// ---------------------------------------------------------------------------

fn read_rpc_nvarchar_param(r: &mut PacketReader) -> io::Result<Option<String>> {
    if r.remaining() < 1 { return Ok(None); }
    let name_len = r.read_u8()? as usize;
    if name_len > 0 {
        if r.remaining() < name_len * 2 { return Ok(None); }
        r.skip(name_len * 2)?;
    }
    if r.remaining() < 1 { return Ok(None); }
    let _status = r.read_u8()?;
    if r.remaining() < 1 { return Ok(None); }
    let type_id = r.read_u8()?;

    match type_id {
        0xE7 => {
            if r.remaining() < 9 { return Ok(None); }
            let _max_len = r.read_u16_le()?;
            r.skip(5)?;
            let actual_len = r.read_u16_le()? as usize;
            if actual_len == 0xFFFF { return Ok(Some(String::new())); }
            if r.remaining() < actual_len { return Ok(None); }
            let bytes = r.read_bytes(actual_len)?;
            Ok(Some(decode_utf16le(bytes)))
        }
        0xF1 => {
            if r.remaining() < 8 { return Ok(None); }
            let _total = r.read_u64_le()?;
            let mut result = Vec::new();
            loop {
                if r.remaining() < 4 { break; }
                let chunk_len = r.read_u32_le()? as usize;
                if chunk_len == 0 { break; }
                if r.remaining() < chunk_len { break; }
                result.extend_from_slice(r.read_bytes(chunk_len)?);
            }
            Ok(Some(decode_utf16le(&result)))
        }
        _ => Ok(Some(String::new())),
    }
}

fn read_typed_value(r: &mut PacketReader, type_id: u8) -> io::Result<(String, String)> {
    match type_id {
        // INTNTYPE
        0x26 => {
            let max_len = r.read_u8()?;
            let actual_len = r.read_u8()?;
            if actual_len == 0 { return Ok((int_type_name(max_len), "NULL".into())); }
            if r.remaining() < actual_len as usize { return Ok((int_type_name(max_len), "NULL".into())); }
            let bytes = r.read_bytes(actual_len as usize)?;
            Ok((int_type_name(max_len), read_le_int(bytes).to_string()))
        }
        // BITNTYPE
        0x68 => {
            let _max = r.read_u8()?;
            let actual = r.read_u8()?;
            if actual == 0 { return Ok(("BIT".into(), "NULL".into())); }
            let b = r.read_u8()?;
            Ok(("BIT".into(), if b != 0 { "1".into() } else { "0".into() }))
        }
        // FLTNTYPE
        0x6D => {
            let _max_len = r.read_u8()?;
            let actual_len = r.read_u8()?;
            if actual_len == 0 { return Ok(("FLOAT".into(), "NULL".into())); }
            if r.remaining() < actual_len as usize { return Ok(("FLOAT".into(), "NULL".into())); }
            let bytes = r.read_bytes(actual_len as usize)?;
            let val = if actual_len == 4 {
                f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as f64
            } else {
                f64::from_le_bytes(bytes.try_into().unwrap_or([0u8; 8]))
            };
            Ok(("FLOAT".into(), format!("{}", val)))
        }
        // NVARCHARTYPE sized
        0xE7 => {
            if r.remaining() < 7 { return Ok(("NVARCHAR(MAX)".into(), "NULL".into())); }
            let max_len = r.read_u16_le()?;
            r.skip(5)?;
            let actual_len = r.read_u16_le()? as usize;
            if actual_len == 0xFFFF { return Ok(("NVARCHAR(MAX)".into(), "NULL".into())); }
            if r.remaining() < actual_len { return Ok(("NVARCHAR(MAX)".into(), "NULL".into())); }
            let bytes = r.read_bytes(actual_len)?;
            let s = decode_utf16le(bytes);
            let type_name = if max_len == 0xFFFF { "NVARCHAR(MAX)".into() } else { format!("NVARCHAR({})", max_len / 2) };
            Ok((type_name, format!("N'{}'", s.replace('\'', "''"))))
        }
        // BIGVARCHARTYPE
        0xA7 => {
            if r.remaining() < 7 { return Ok(("VARCHAR(MAX)".into(), "NULL".into())); }
            let max_len = r.read_u16_le()?;
            r.skip(5)?;
            let actual_len = r.read_u16_le()? as usize;
            if actual_len == 0xFFFF { return Ok(("VARCHAR(MAX)".into(), "NULL".into())); }
            if r.remaining() < actual_len { return Ok(("VARCHAR(MAX)".into(), "NULL".into())); }
            let bytes = r.read_bytes(actual_len)?;
            let s = String::from_utf8_lossy(bytes).to_string();
            let type_name = if max_len == 0xFFFF { "VARCHAR(MAX)".into() } else { format!("VARCHAR({})", max_len) };
            Ok((type_name, format!("'{}'", s.replace('\'', "''"))))
        }
        // DECIMALNTYPE / NUMERICNTYPE
        0x6A | 0x6C => {
            let _max_len = r.read_u8()?;
            let precision = r.read_u8()?;
            let scale = r.read_u8()?;
            let actual_len = r.read_u8()?;
            if actual_len == 0 { return Ok((format!("DECIMAL({},{})", precision, scale), "NULL".into())); }
            if r.remaining() < actual_len as usize { return Ok((format!("DECIMAL({},{})", precision, scale), "NULL".into())); }
            let bytes = r.read_bytes(actual_len as usize)?;
            let sign = bytes[0];
            let mut val: i128 = 0;
            for i in (1..bytes.len()).rev() { val = (val << 8) | bytes[i] as i128; }
            if sign == 0 { val = -val; }
            Ok((format!("DECIMAL({},{})", precision, scale), format_decimal(val, scale)))
        }
        // GUIDTYPE
        0x24 => {
            let actual_len = r.read_u8()?;
            if actual_len == 0 { return Ok(("UNIQUEIDENTIFIER".into(), "NULL".into())); }
            if r.remaining() < 16 { return Ok(("UNIQUEIDENTIFIER".into(), "NULL".into())); }
            let b = r.read_bytes(16)?;
            let guid = format!(
                "{:02X}{:02X}{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}-{:02X}{:02X}{:02X}{:02X}{:02X}{:02X}",
                b[3],b[2],b[1],b[0], b[5],b[4], b[7],b[6], b[8],b[9], b[10],b[11],b[12],b[13],b[14],b[15]
            );
            Ok(("UNIQUEIDENTIFIER".into(), format!("'{}'", guid)))
        }
        _ => Ok(("NVARCHAR(MAX)".into(), "NULL".into())),
    }
}

fn int_type_name(max_len: u8) -> String {
    match max_len { 1 => "TINYINT".into(), 2 => "SMALLINT".into(), 8 => "BIGINT".into(), _ => "INT".into() }
}

fn read_le_int(bytes: &[u8]) -> i64 {
    match bytes.len() {
        1 => bytes[0] as i64,
        2 => i16::from_le_bytes([bytes[0], bytes[1]]) as i64,
        4 => i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as i64,
        8 => i64::from_le_bytes(bytes.try_into().unwrap_or([0u8; 8])),
        _ => 0,
    }
}

fn decode_utf16le(bytes: &[u8]) -> String {
    let mut u16s = Vec::with_capacity(bytes.len() / 2);
    for chunk in bytes.chunks_exact(2) {
        u16s.push(u16::from_le_bytes([chunk[0], chunk[1]]));
    }
    String::from_utf16_lossy(&u16s).to_string()
}

fn format_decimal(val: i128, scale: u8) -> String {
    if scale == 0 { return val.to_string(); }
    let divisor = 10i128.pow(scale as u32);
    let int_part = val / divisor;
    let frac_part = (val % divisor).abs();
    format!("{}.{:0>width$}", int_part, frac_part, width = scale as usize)
}

fn parse_param_decl(decl: &str) -> Vec<(String, String)> {
    if decl.trim().is_empty() { return vec![]; }
    decl.split(',').filter_map(|part| {
        let part = part.trim();
        let mut iter = part.splitn(2, char::is_whitespace);
        let name = iter.next()?.trim().to_string();
        let type_name = iter.next()?.trim().to_string();
        if name.is_empty() || type_name.is_empty() { None } else { Some((name, type_name)) }
    }).collect()
}
