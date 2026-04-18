use super::packet::{PacketBuilder, PacketReader};
use super::type_mapping::infer_column_types;
use iridium_core::error::DbError;
use std::io;

pub fn parse_sql_batch(data: &[u8]) -> io::Result<String> {
    let mut reader = PacketReader::new(data);

    // ALL_HEADERS: starts with TotalLength DWORD
    if reader.remaining() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SQL batch too short for ALL_HEADERS",
        ));
    }

    let total_length = reader.read_u32_le()? as usize;

    if total_length > 0 && total_length <= reader.remaining() + 4 {
        reader.skip(total_length - 4)?;
    } else if total_length > 4 {
        // Skip whatever header data remains
        let skip_len = (total_length - 4).min(reader.remaining());
        reader.skip(skip_len)?;
    }

    // Remaining data is UTF-16LE SQL text
    let remaining = reader.remaining();
    if remaining == 0 {
        return Ok(String::new());
    }

    let sql_bytes = reader.read_bytes(remaining)?;

    // Decode UTF-16LE
    let mut u16s = Vec::with_capacity(remaining / 2);
    for chunk in sql_bytes.chunks_exact(2) {
        u16s.push(u16::from_le_bytes([chunk[0], chunk[1]]));
    }
    // Handle odd byte at end (shouldn't happen, but be safe)
    if sql_bytes.len() % 2 != 0 {
        u16s.push(sql_bytes[sql_bytes.len() - 1] as u16);
    }

    Ok(String::from_utf16_lossy(&u16s))
}

pub struct BatchResult {
    pub data: Vec<u8>,
}

pub fn build_batch_response(
    columns: &[String],
    rows: &[Vec<iridium_core::types::Value>],
    row_count: u64,
    is_result_set: bool,
) -> BatchResult {
    let mut b = PacketBuilder::with_capacity(4096);

    if is_result_set && !columns.is_empty() {
        let types = infer_column_types(columns, rows);
        super::tokens::write_result_set(&mut b, columns, &types, rows, 1, 4096);
    } else {
        // DDL/DML with no result set
        super::tokens::write_done(&mut b, super::tokens::DONE_COUNT, 1, row_count);
    }

    BatchResult { data: b.into_vec() }
}

pub fn build_error_response(err: &DbError) -> BatchResult {
    let mut b = PacketBuilder::with_capacity(512);

    super::tokens::write_error(
        &mut b,
        err.number(),
        1,                    // state
        err.class_severity(), // class (severity)
        &err.to_string(),     // message
        "iridium_server",
        "",
        0,
    );

    super::tokens::write_done(
        &mut b,
        super::tokens::DONE_ERROR | super::tokens::DONE_COUNT,
        1,
        0,
    );

    BatchResult { data: b.into_vec() }
}
