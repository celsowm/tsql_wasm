use crate::tds::packet::PacketReader;
use crate::tds::type_mapping::{self, TypeInfo};
use iridium_core::types::Value;
use std::io;

pub struct BulkLoadData {
    pub columns: Vec<String>,
    pub column_types: Vec<TypeInfo>,
    pub rows: Vec<Vec<Value>>,
}

pub fn parse_bulk_load_data(
    data: &[u8],
    _expected_columns: &[iridium_core::ast::statements::ddl::ColumnSpec],
) -> io::Result<BulkLoadData> {
    let mut reader = PacketReader::new(data);
    let mut rows = Vec::new();

    // MS-TDS Bulk Load data stream consists of:
    // [COLMETADATA]
    // [ROW]*
    // [DONE]

    let token = reader.read_u8()?;
    if token != crate::tds::tokens::COLMETADATA_TOKEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Expected COLMETADATA (0x81), got 0x{:02X}", token),
        ));
    }

    let count = reader.read_u16_le()? as usize;
    let mut column_names = Vec::with_capacity(count);
    let mut column_types = Vec::with_capacity(count);

    for _ in 0..count {
        // UserType (4 bytes)
        reader.skip(4)?;
        // Flags (2 bytes)
        let _flags = reader.read_u16_le()?;
        // TYPE_INFO
        let ti = type_mapping::read_type_info(&mut reader)?;
        column_types.push(ti);
        // Column name (B_VARCHAR UTF-16LE)
        let name_len = reader.read_u8()? as usize;
        let name = reader.read_utf16le(name_len)?;
        column_names.push(name);
    }

    // Process rows until DONE (0xFD) or end of stream
    while reader.remaining() > 0 {
        let token = reader.read_u8()?;
        if token == crate::tds::tokens::DONE_TOKEN {
            break;
        }
        if token != crate::tds::tokens::ROW_TOKEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected ROW (0xD1), got 0x{:02X}", token),
            ));
        }

        let mut row = Vec::with_capacity(count);
        for ti in &column_types {
            let val = type_mapping::read_value(&mut reader, ti)?;
            row.push(val);
        }
        rows.push(row);
    }

    Ok(BulkLoadData {
        columns: column_names,
        column_types: column_types,
        rows,
    })
}
