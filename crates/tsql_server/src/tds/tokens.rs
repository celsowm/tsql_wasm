use super::packet::PacketBuilder;
use super::type_mapping::{
    value_to_wire_bytes, TypeInfo, BIGBINARYTYPE, BIGCHARTYPE, BIGVARBINARYTYPE, BIGVARCHARTYPE,
    NCHARTYPE, NVARCHARTYPE,
};
use tsql_core::types::Value;

pub const COLMETADATA_TOKEN: u8 = 0x81;
pub const ROW_TOKEN: u8 = 0xD1;
pub const DONE_TOKEN: u8 = 0xFD;
pub const DONEINPROC_TOKEN: u8 = 0xFF;
pub const DONEPROC_TOKEN: u8 = 0xFE;
pub const ERROR_TOKEN: u8 = 0xAA;
pub const INFO_TOKEN: u8 = 0xAB;
pub const LOGINACK_TOKEN: u8 = 0xAD;
pub const ENVCHANGE_TOKEN: u8 = 0xE3;
pub const FEATUREEXTACK_TOKEN: u8 = 0xAE;
pub const RETURNSTATUS_TOKEN: u8 = 0x79;

pub const DONE_FINAL: u16 = 0x0000;
pub const DONE_MORE: u16 = 0x0001;
pub const DONE_ERROR: u16 = 0x0002;
pub const DONE_COUNT: u16 = 0x0010;
pub const DONE_ATTN: u16 = 0x0020;

pub const ENVCHANGE_PACKET_SIZE: u8 = 0x04;
pub const ENVCHANGE_DATABASE: u8 = 0x01;
pub const ENVCHANGE_LANGUAGE_TYPE: u8 = 0x02;
pub const ENVCHANGE_COLLATION: u8 = 0x07;

fn truncate_string_value(s: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        String::new()
    } else {
        s.chars().take(max_chars).collect()
    }
}

fn apply_textsize(value: &Value, ti: &TypeInfo, textsize: usize) -> Value {
    if value.is_null() {
        return match value {
            Value::Char(s) => Value::Char(s.clone()),
            Value::VarChar(s) => Value::VarChar(s.clone()),
            Value::NChar(s) => Value::NChar(s.clone()),
            Value::NVarChar(s) => Value::NVarChar(s.clone()),
            Value::Binary(v) => Value::Binary(v.clone()),
            Value::VarBinary(v) => Value::VarBinary(v.clone()),
            _ => value.clone(),
        };
    }

    match value {
        Value::Char(s) if ti.tds_type == BIGCHARTYPE => {
            Value::VarChar(truncate_string_value(s, textsize))
        }
        Value::VarChar(s) if ti.tds_type == BIGVARCHARTYPE => {
            Value::VarChar(truncate_string_value(s, textsize))
        }
        Value::NChar(s) if ti.tds_type == NCHARTYPE => {
            let max_chars = textsize / 2;
            Value::NVarChar(truncate_string_value(s, max_chars))
        }
        Value::NVarChar(s) if ti.tds_type == NVARCHARTYPE => {
            let max_chars = textsize / 2;
            Value::NVarChar(truncate_string_value(s, max_chars))
        }
        Value::Binary(v) if ti.tds_type == BIGBINARYTYPE => {
            let mut bytes = v.clone();
            if bytes.len() > textsize {
                bytes.truncate(textsize);
            }
            Value::VarBinary(bytes)
        }
        Value::VarBinary(v) if ti.tds_type == BIGVARBINARYTYPE => {
            let mut bytes = v.clone();
            if bytes.len() > textsize {
                bytes.truncate(textsize);
            }
            Value::VarBinary(bytes)
        }
        _ => value.clone(),
    }
}

pub fn write_colmetadata(b: &mut PacketBuilder, columns: &[String], types: &[TypeInfo]) {
    b.put_u8(COLMETADATA_TOKEN);
    b.put_u16_le(columns.len() as u16);

    for (i, col_name) in columns.iter().enumerate() {
        let ti = if i < types.len() {
            &types[i]
        } else {
            // Fallback
            &TypeInfo {
                tds_type: super::type_mapping::NVARCHARTYPE,
                length_prefix: vec![0x00, 0xFF],
                collation: Some(super::type_mapping::DEFAULT_COLLATION),
                scale: None,
                precision: None,
                flags: 0x0001,
            }
        };

        // UserType: 4 bytes (ULONG)
        b.put_u32_le(0);

        // Flags: 2 bytes (USHORT LE)
        b.put_u16_le(ti.flags);

        // TYPE_INFO
        b.put_u8(ti.tds_type);
        b.put_bytes(&ti.length_prefix);

        // Collation for char types
        if let Some(collation) = &ti.collation {
            b.put_bytes(collation);
        }

        // TYPE_INFO tail:
        // - DECIMAL/NUMERIC: precision + scale
        // - TIME/DATETIME2: scale
        if let Some(precision) = ti.precision {
            b.put_u8(precision);
            b.put_u8(ti.scale.unwrap_or(0));
        } else if let Some(scale) = ti.scale {
            b.put_u8(scale);
        }

        // Column name: B_VARCHAR (UTF-16LE)
        b.put_b_vchar_utf16(col_name);
    }
}

pub fn write_row(b: &mut PacketBuilder, row: &[Value], types: &[TypeInfo], textsize: usize) {
    b.put_u8(ROW_TOKEN);
    for (i, value) in row.iter().enumerate() {
        let ti = &types[i];
        let truncated = apply_textsize(value, ti, textsize);
        b.put_bytes(&value_to_wire_bytes(&truncated, ti));
    }
}

pub fn write_done(b: &mut PacketBuilder, status: u16, cur_cmd: u16, row_count: u64) {
    b.put_u8(DONE_TOKEN);
    b.put_u16_le(status);
    b.put_u16_le(cur_cmd);
    b.put_u64_le(row_count);
}

pub fn write_done_in_proc(b: &mut PacketBuilder, status: u16, cur_cmd: u16, row_count: u64) {
    b.put_u8(DONEINPROC_TOKEN);
    b.put_u16_le(status);
    b.put_u16_le(cur_cmd);
    b.put_u64_le(row_count);
}

pub fn write_doneproc(b: &mut PacketBuilder, status: u16, cur_cmd: u16, row_count: u64) {
    b.put_u8(DONEPROC_TOKEN);
    b.put_u16_le(status);
    b.put_u16_le(cur_cmd);
    b.put_u64_le(row_count);
}

pub fn write_returnstatus(b: &mut PacketBuilder, status: i32) {
    b.put_u8(RETURNSTATUS_TOKEN);
    b.put_i32_le(status);
}

#[allow(clippy::too_many_arguments)]
pub fn write_error(
    b: &mut PacketBuilder,
    number: i32,
    state: u8,
    class: u8,
    message: &str,
    server_name: &str,
    proc_name: &str,
    line_number: i32,
) {
    // First, build the data portion to know its length
    let mut data_b = PacketBuilder::new();
    data_b.put_i32_le(number);
    data_b.put_u8(state);
    data_b.put_u8(class);
    data_b.put_us_vchar_utf16(message);
    data_b.put_b_vchar_utf16(server_name);
    data_b.put_b_vchar_utf16(proc_name);
    data_b.put_i32_le(line_number);

    let data_bytes = data_b.as_bytes();

    b.put_u8(ERROR_TOKEN);
    b.put_u16_le(data_bytes.len() as u16);
    b.put_bytes(data_bytes);
}

pub fn write_envchange_packet_size(b: &mut PacketBuilder, new_size: u16, old_size: u16) {
    b.put_u8(ENVCHANGE_TOKEN);

    let new_val = format!("{}", new_size);
    let old_val = format!("{}", old_size);

    // Length = 1 (type) + 1 (new_len) + new_val.utf16_bytes + 1 (old_len) + old_val.utf16_bytes
    let new_utf16_bytes = new_val.len() * 2;
    let old_utf16_bytes = old_val.len() * 2;
    let total_len = 1 + 1 + new_utf16_bytes + 1 + old_utf16_bytes;
    b.put_u16_le(total_len as u16);

    b.put_u8(ENVCHANGE_PACKET_SIZE);

    // New value: B_VARCHAR (char count)
    b.put_u8(new_val.len() as u8);
    b.put_utf16le(&new_val);

    // Old value
    b.put_u8(old_val.len() as u8);
    b.put_utf16le(&old_val);
}

pub fn write_envchange_database(b: &mut PacketBuilder, new_db: &str, old_db: &str) {
    b.put_u8(ENVCHANGE_TOKEN);

    let new_utf16_bytes = new_db.len() * 2;
    let old_utf16_bytes = old_db.len() * 2;
    let total_len = 1 + 1 + new_utf16_bytes + 1 + old_utf16_bytes;
    b.put_u16_le(total_len as u16);

    b.put_u8(ENVCHANGE_DATABASE);

    b.put_u8(new_db.len() as u8);
    b.put_utf16le(new_db);

    b.put_u8(old_db.len() as u8);
    b.put_utf16le(old_db);
}

pub fn write_envchange_collation(b: &mut PacketBuilder) {
    b.put_u8(ENVCHANGE_TOKEN);

    // Latin1_General_CI_AS: LCID=0x0409, flags=0x0000, sortId=0x00
    let collation = [0x09, 0x04, 0x00, 0x00, 0x00];

    let total_len = 1 + 1 + 5 + 1 + 5;
    b.put_u16_le(total_len as u16);

    b.put_u8(ENVCHANGE_COLLATION);

    b.put_u8(5); // new value length
    b.put_bytes(&collation);

    b.put_u8(5); // old value length
    b.put_bytes(&collation);
}

pub fn write_envchange_language(b: &mut PacketBuilder, new_lang: &str, old_lang: &str) {
    b.put_u8(ENVCHANGE_TOKEN);

    let new_utf16_bytes = new_lang.len() * 2;
    let old_utf16_bytes = old_lang.len() * 2;
    let total_len = 1 + 1 + new_utf16_bytes + 1 + old_utf16_bytes;
    b.put_u16_le(total_len as u16);

    b.put_u8(ENVCHANGE_LANGUAGE_TYPE);

    b.put_u8(new_lang.len() as u8);
    b.put_utf16le(new_lang);

    b.put_u8(old_lang.len() as u8);
    b.put_utf16le(old_lang);
}

#[allow(clippy::too_many_arguments)]
pub fn write_info(
    b: &mut PacketBuilder,
    number: i32,
    state: u8,
    class: u8,
    message: &str,
    server_name: &str,
    proc_name: &str,
    line_number: i32,
) {
    let mut data_b = PacketBuilder::new();
    data_b.put_i32_le(number);
    data_b.put_u8(state);
    data_b.put_u8(class);
    data_b.put_us_vchar_utf16(message);
    data_b.put_b_vchar_utf16(server_name);
    data_b.put_b_vchar_utf16(proc_name);
    data_b.put_i32_le(line_number);

    let data_bytes = data_b.as_bytes();

    b.put_u8(INFO_TOKEN);
    b.put_u16_le(data_bytes.len() as u16);
    b.put_bytes(data_bytes);
}

pub fn write_loginack(b: &mut PacketBuilder, tds_version: u32) {
    // Build the data portion first
    let mut data_b = PacketBuilder::new();
    data_b.put_u8(0x01); // Interface: SQL_TSQL
    data_b.put_u32_be(tds_version); // TDS version (BE)

    let prog_name = "Microsoft SQL Server";
    data_b.put_u8(prog_name.encode_utf16().count() as u8); // ProgNameLen
    data_b.put_utf16le(prog_name); // ProgName

    // Version: 16.0.4105.1 (SQL Server 2022-ish)
    data_b.put_u8(16); // MajorVer
    data_b.put_u8(0); // MinorVer
    data_b.put_u16_le(0x1009); // BuildNum (4105)

    let data_bytes = data_b.as_bytes();

    b.put_u8(LOGINACK_TOKEN);
    b.put_u16_le(data_bytes.len() as u16);
    b.put_bytes(data_bytes);
}

pub fn write_result_set(
    b: &mut PacketBuilder,
    columns: &[String],
    types: &[TypeInfo],
    rows: &[Vec<Value>],
    cur_cmd: u16,
    textsize: usize,
) {
    write_colmetadata(b, columns, types);

    for row in rows {
        write_row(b, row, types, textsize);
    }

    write_done(b, DONE_COUNT, cur_cmd, rows.len() as u64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tds::type_mapping::value_to_type_info;

    #[test]
    fn write_row_truncates_varchar_payload() {
        let mut b = PacketBuilder::new();
        let row = vec![Value::VarChar("hello".to_string())];
        let types = vec![value_to_type_info(&row[0])];

        write_row(&mut b, &row, &types, 3);

        let bytes = b.as_bytes();
        assert_eq!(bytes[0], ROW_TOKEN);
        assert!(bytes
            .windows(5)
            .any(|w| w == [0x03, 0x00, b'h', b'e', b'l']));
    }

    #[test]
    fn write_row_truncates_varbinary_payload() {
        let mut b = PacketBuilder::new();
        let row = vec![Value::VarBinary(vec![0x01, 0x02, 0x03, 0x04])];
        let types = vec![value_to_type_info(&row[0])];

        write_row(&mut b, &row, &types, 2);

        let bytes = b.as_bytes();
        assert_eq!(bytes[0], ROW_TOKEN);
        assert!(bytes.windows(4).any(|w| w == [0x02, 0x00, 0x01, 0x02]));
    }
}
