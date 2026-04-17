use iridium_core::types::Value;
use std::io;
use super::packet::PacketReader;

pub const INTNTYPE: u8 = 0x26;
pub const GUIDTYPE: u8 = 0x24;
pub const BITNTYPE: u8 = 0x68;
pub const FLTNTYPE: u8 = 0x6D;
pub const MONEYNTYPE: u8 = 0x6E;
pub const DECIMALNTYPE: u8 = 0x6A;
pub const NUMERICNTYPE: u8 = 0x6C;
pub const BIGVARCHARTYPE: u8 = 0xA7;
pub const BIGCHARTYPE: u8 = 0xAF;
pub const NVARCHARTYPE: u8 = 0xE7;
pub const NCHARTYPE: u8 = 0xEF;
pub const BIGBINARYTYPE: u8 = 0xAD;
pub const BIGVARBINARYTYPE: u8 = 0xA5;
pub const DATENTYPE: u8 = 0x28;
pub const TIMENTYPE: u8 = 0x29;
pub const DATETIME2NTYPE: u8 = 0x2A;
pub const DATETIMNTYPE: u8 = 0x6F;

/// Default collation: Latin1_General_CI_AS (LCID=0x0409)
pub const DEFAULT_COLLATION: [u8; 5] = [0x09, 0x04, 0x00, 0x00, 0x00];

pub struct TypeInfo {
    pub tds_type: u8,
    pub length_prefix: Vec<u8>,
    pub collation: Option<[u8; 5]>,
    pub scale: Option<u8>,
    pub precision: Option<u8>,
    pub flags: u16,
}

pub fn value_to_type_info(value: &Value) -> TypeInfo {
    match value {
        Value::Bit(_) => TypeInfo {
            tds_type: BITNTYPE,
            length_prefix: vec![0x01],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        Value::TinyInt(_) => TypeInfo {
            tds_type: INTNTYPE,
            length_prefix: vec![0x01],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        Value::SmallInt(_) => TypeInfo {
            tds_type: INTNTYPE,
            length_prefix: vec![0x02],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        Value::Int(_) => TypeInfo {
            tds_type: INTNTYPE,
            length_prefix: vec![0x04],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        Value::BigInt(_) => TypeInfo {
            tds_type: INTNTYPE,
            length_prefix: vec![0x08],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        Value::Float(_) => TypeInfo {
            tds_type: FLTNTYPE,
            length_prefix: vec![0x08],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        Value::Decimal(_, scale) => {
            let precision = 38;
            let len = 17;
            TypeInfo {
                tds_type: NUMERICNTYPE,
                length_prefix: vec![len],
                collation: None,
                scale: Some(*scale),
                precision: Some(precision),
                flags: 0x0001,
            }
        }
        Value::Money(_) => TypeInfo {
            tds_type: MONEYNTYPE,
            length_prefix: vec![0x08],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        Value::SmallMoney(_) => TypeInfo {
            tds_type: MONEYNTYPE,
            length_prefix: vec![0x04],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        Value::Char(_) | Value::VarChar(_) => TypeInfo {
            tds_type: BIGVARCHARTYPE,
            length_prefix: 8000u16.to_le_bytes().to_vec(),
            collation: Some(DEFAULT_COLLATION),
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        Value::NChar(_) | Value::NVarChar(_) => TypeInfo {
            tds_type: NVARCHARTYPE,
            length_prefix: 8000u16.to_le_bytes().to_vec(),
            collation: Some(DEFAULT_COLLATION),
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        Value::Binary(v) => {
            let len = v.len().max(1) as u16;
            TypeInfo {
                tds_type: BIGBINARYTYPE,
                length_prefix: len.to_le_bytes().to_vec(),
                collation: None,
                scale: None,
                precision: None,
                flags: 0x0001,
            }
        }
        Value::VarBinary(v) => {
            let len = v.len().max(1) as u16;
            TypeInfo {
                tds_type: BIGVARBINARYTYPE,
                length_prefix: len.to_le_bytes().to_vec(),
                collation: None,
                scale: None,
                precision: None,
                flags: 0x0001,
            }
        }
        Value::Date(_) => TypeInfo {
            tds_type: DATENTYPE,
            length_prefix: vec![0x03],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        Value::Time(_) => TypeInfo {
            tds_type: TIMENTYPE,
            length_prefix: vec![0x05],
            collation: None,
            scale: Some(7),
            precision: None,
            flags: 0x0001,
        },
        Value::DateTime(_) | Value::SmallDateTime(_) => TypeInfo {
            tds_type: DATETIMNTYPE,
            length_prefix: vec![0x08],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        Value::DateTime2(_) => TypeInfo {
            tds_type: DATETIME2NTYPE,
            length_prefix: vec![0x08],
            collation: None,
            scale: Some(7),
            precision: None,
            flags: 0x0001,
        },
        Value::DateTimeOffset(_) => TypeInfo {
            tds_type: NVARCHARTYPE,
            length_prefix: 510u16.to_le_bytes().to_vec(),
            collation: Some(DEFAULT_COLLATION),
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        Value::UniqueIdentifier(_) => TypeInfo {
            tds_type: GUIDTYPE,
            length_prefix: vec![0x10],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        Value::Null | Value::SqlVariant(_) | Value::Vector(_) => TypeInfo {
            tds_type: NVARCHARTYPE,
            length_prefix: 510u16.to_le_bytes().to_vec(),
            collation: Some(DEFAULT_COLLATION),
            scale: None,
            precision: None,
            flags: 0x0001,
        },
    }
}

pub fn value_to_wire_bytes(value: &Value, ti: &TypeInfo) -> Vec<u8> {
    if value.is_null() {
        match ti.tds_type {
            BIGVARCHARTYPE | BIGCHARTYPE | NVARCHARTYPE | NCHARTYPE | BIGVARBINARYTYPE
            | BIGBINARYTYPE => {
                return vec![0xFF, 0xFF];
            }
            _ => return vec![0x00],
        }
    }

    match ti.tds_type {
        BITNTYPE => {
            let b = match value {
                Value::Bit(b) => *b,
                _ => value.to_integer_i64().unwrap_or(0) != 0,
            };
            vec![0x01, if b { 0x01 } else { 0x00 }]
        }
        INTNTYPE => {
            let len = ti.length_prefix[0];
            let mut buf = vec![len];
            let v = value.to_integer_i64().unwrap_or(0);
            match len {
                1 => buf.push(v as u8),
                2 => buf.extend_from_slice(&(v as i16).to_le_bytes()),
                4 => buf.extend_from_slice(&(v as i32).to_le_bytes()),
                8 => buf.extend_from_slice(&v.to_le_bytes()),
                _ => {}
            }
            buf
        }
        FLTNTYPE => {
            let len = ti.length_prefix[0];
            let mut buf = vec![len];
            let f = match value {
                Value::Float(bits) => f64::from_bits(*bits),
                _ => value.to_integer_i64().unwrap_or(0) as f64,
            };
            match len {
                4 => buf.extend_from_slice(&(f as f32).to_le_bytes()),
                8 => buf.extend_from_slice(&f.to_le_bytes()),
                _ => {}
            }
            buf
        }
        NUMERICNTYPE | DECIMALNTYPE => {
            let len = ti.length_prefix[0];
            let mut buf = vec![len];
            let raw = match value {
                Value::Decimal(r, _s) => *r,
                _ => {
                    value.to_integer_i64().unwrap_or(0) as i128
                        * 10i128.pow(ti.scale.unwrap_or(0) as u32)
                }
            };
            let negative = raw < 0;
            let abs_val = raw.unsigned_abs();
            buf.push(if negative { 0x00 } else { 0x01 });
            let limb_count = (len - 1) / 4;
            for i in 0..limb_count {
                let shift = (i as u32) * 32;
                let limb = ((abs_val >> shift) & 0xFFFFFFFF) as u32;
                buf.extend_from_slice(&limb.to_le_bytes());
            }
            log::info!("Decimal raw={}, bytes={:02X?}", raw, buf);
            buf
        }
        MONEYNTYPE => {
            let len = ti.length_prefix[0];
            let mut buf = vec![len];
            let m = match value {
                Value::Money(v) => *v,
                Value::SmallMoney(v) => *v as i128,
                _ => value.to_integer_i64().unwrap_or(0) as i128 * 10000,
            };
            match len {
                4 => buf.extend_from_slice(&(m as i32).to_le_bytes()),
                8 => buf.extend_from_slice(&(m as i64).to_le_bytes()),
                _ => {}
            }
            buf
        }
        BIGVARCHARTYPE | BIGCHARTYPE | NVARCHARTYPE | NCHARTYPE => {
            let s = value.to_string_value();
            let is_unicode = ti.tds_type == NVARCHARTYPE || ti.tds_type == NCHARTYPE;
            if is_unicode {
                let utf16: Vec<u16> = s.encode_utf16().collect();
                let mut buf = Vec::with_capacity(3 + utf16.len() * 2);
                let byte_len = (utf16.len() * 2) as u16;
                buf.extend_from_slice(&byte_len.to_le_bytes());
                for c in &utf16 {
                    buf.extend_from_slice(&c.to_le_bytes());
                }
                buf
            } else {
                let bytes = s.as_bytes();
                let mut buf = Vec::with_capacity(3 + bytes.len());
                buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(bytes);
                buf
            }
        }
        BIGVARBINARYTYPE | BIGBINARYTYPE => {
            let bytes = match value {
                Value::Binary(v) | Value::VarBinary(v) => v.clone(),
                _ => value.to_string_value().as_bytes().to_vec(),
            };
            let mut buf = Vec::with_capacity(3 + bytes.len());
            buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(&bytes);
            buf
        }
        DATENTYPE => {
            let s = value.to_string_value();
            let days = parse_date_to_days_since_2000(&s);
            let mut buf = vec![0x03];
            buf.extend_from_slice(&days.to_le_bytes()[..3]);
            buf
        }
        TIMENTYPE => {
            let s = value.to_string_value();
            let ticks = parse_time_to_ticks(&s);
            let mut buf = vec![0x05];
            buf.extend_from_slice(&ticks.to_le_bytes()[..5]);
            buf
        }
        DATETIMNTYPE => {
            let s = value.to_string_value();
            let (days, ticks) = parse_datetime(&s);
            let mut buf = vec![0x08];
            buf.extend_from_slice(&days.to_le_bytes());
            buf.extend_from_slice(&ticks.to_le_bytes());
            buf
        }
        DATETIME2NTYPE => {
            let s = value.to_string_value();
            let (days, ticks) = parse_datetime2(&s);
            let mut buf = vec![0x08];
            buf.extend_from_slice(&ticks.to_le_bytes()[..5]);
            buf.extend_from_slice(&days.to_le_bytes()[..3]);
            buf
        }
        GUIDTYPE => {
            let s = value.to_string_value();
            let bytes = parse_guid(&s);
            let mut buf = vec![0x10];
            buf.extend_from_slice(&bytes);
            buf
        }
        _ => vec![0x00],
    }
}

fn parse_date_to_days_since_2000(s: &str) -> i32 {
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() < 3 {
        return 0;
    }
    let y: i32 = parts[0].parse().unwrap_or(2000);
    let m: i32 = parts[1].parse().unwrap_or(1);
    let d: i32 = parts[2].parse().unwrap_or(1);

    let total_days = date_to_days(y, m, d);
    let base_days = date_to_days(2000, 1, 1);
    total_days - base_days
}

fn date_to_days(y: i32, m: i32, d: i32) -> i32 {
    let a = (14 - m) / 12;
    let yy = y + 4800 - a;
    let mm = m + 12 * a - 3;
    d + (153 * mm + 2) / 5 + 365 * yy + yy / 4 - yy / 100 + yy / 400 - 32045
}

fn parse_time_to_ticks(s: &str) -> u64 {
    let main_frac: Vec<&str> = s.split('.').collect();
    let parts: Vec<&str> = main_frac[0].split(':').collect();
    if parts.len() < 3 {
        return 0;
    }
    let h: u64 = parts[0].parse().unwrap_or(0);
    let mi: u64 = parts[1].parse().unwrap_or(0);
    let sec: u64 = parts[2].parse().unwrap_or(0);

    let mut ticks = h * 3600 + mi * 60 + sec;
    ticks *= 10_000_000;

    if main_frac.len() > 1 {
        let frac_str = main_frac[1];
        let frac_str = &frac_str[..frac_str.len().min(7)];
        let frac: u64 = frac_str.parse().unwrap_or(0);
        let scale_factor = 10u64.pow(7 - frac_str.len() as u32);
        ticks += frac * scale_factor;
    }

    ticks
}

fn parse_datetime(s: &str) -> (i32, u32) {
    let parts: Vec<&str> = s.split(' ').collect();
    if parts.is_empty() {
        return (0, 0);
    }
    let days = parse_date_to_days_since_2000(parts[0]);

    if parts.len() < 2 {
        return (days, 0);
    }

    let time_part = parts[1];
    let main_frac: Vec<&str> = time_part.split('.').collect();
    let hms: Vec<&str> = main_frac[0].split(':').collect();
    if hms.len() < 3 {
        return (days, 0);
    }
    let h: u32 = hms[0].parse().unwrap_or(0);
    let m: u32 = hms[1].parse().unwrap_or(0);
    let sec: u32 = hms[2].parse().unwrap_or(0);

    let mut ticks = h * 3600 + m * 60 + sec;
    ticks *= 300;

    if main_frac.len() > 1 {
        let ms_str = &main_frac[1][..main_frac[1].len().min(3)];
        let ms: u32 = ms_str.parse().unwrap_or(0);
        ticks += ms * 300 / 1000;
    }

    (days, ticks)
}

fn parse_datetime2(s: &str) -> (i32, u64) {
    let parts: Vec<&str> = s.split(' ').collect();
    if parts.is_empty() {
        return (0, 0);
    }
    let days = parse_date_to_days_since_2000(parts[0]);

    if parts.len() < 2 {
        return (days, 0);
    }

    let ticks = parse_time_to_ticks(parts[1]);
    (days, ticks)
}

fn parse_guid(s: &str) -> [u8; 16] {
    let hex: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if hex.len() < 32 {
        return [0u8; 16];
    }
    let mut bytes = [0u8; 16];
    for i in 0..16 {
        let byte_str = &hex[i * 2..i * 2 + 2];
        bytes[i] = u8::from_str_radix(byte_str, 16).unwrap_or(0);
    }

    let mut result = [0u8; 16];
    result[0..4].copy_from_slice(&bytes[0..4].iter().rev().cloned().collect::<Vec<_>>());
    result[4..6].copy_from_slice(&bytes[4..6].iter().rev().cloned().collect::<Vec<_>>());
    result[6..8].copy_from_slice(&bytes[6..8].iter().rev().cloned().collect::<Vec<_>>());
    result[8..16].copy_from_slice(&bytes[8..16]);
    result
}

pub fn runtime_type_to_tds(ty: &iridium_core::types::DataType) -> TypeInfo {
    match ty {
        iridium_core::types::DataType::Bit => TypeInfo {
            tds_type: BITNTYPE,
            length_prefix: vec![0x01],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::TinyInt => TypeInfo {
            tds_type: INTNTYPE,
            length_prefix: vec![0x01],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::SmallInt => TypeInfo {
            tds_type: INTNTYPE,
            length_prefix: vec![0x02],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::Int => TypeInfo {
            tds_type: INTNTYPE,
            length_prefix: vec![0x04],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::BigInt => TypeInfo {
            tds_type: INTNTYPE,
            length_prefix: vec![0x08],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::Float => TypeInfo {
            tds_type: FLTNTYPE,
            length_prefix: vec![0x08],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::Decimal { precision, scale } => {
            let len = if *precision <= 9 {
                5
            } else if *precision <= 19 {
                9
            } else if *precision <= 28 {
                13
            } else {
                17
            };
            TypeInfo {
                tds_type: NUMERICNTYPE,
                length_prefix: vec![len],
                collation: None,
                scale: Some(*scale),
                precision: Some(*precision),
                flags: 0x0001,
            }
        }
        iridium_core::types::DataType::Money => TypeInfo {
            tds_type: MONEYNTYPE,
            length_prefix: vec![0x08],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::SmallMoney => TypeInfo {
            tds_type: MONEYNTYPE,
            length_prefix: vec![0x04],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::Char { len } => TypeInfo {
            tds_type: BIGCHARTYPE,
            length_prefix: len.to_le_bytes().to_vec(),
            collation: Some(DEFAULT_COLLATION),
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::VarChar { max_len } => TypeInfo {
            tds_type: BIGVARCHARTYPE,
            length_prefix: max_len.to_le_bytes().to_vec(),
            collation: Some(DEFAULT_COLLATION),
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::NChar { len } => TypeInfo {
            tds_type: NCHARTYPE,
            length_prefix: (len * 2).to_le_bytes().to_vec(),
            collation: Some(DEFAULT_COLLATION),
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::NVarChar { max_len } => TypeInfo {
            tds_type: NVARCHARTYPE,
            length_prefix: (max_len * 2).to_le_bytes().to_vec(),
            collation: Some(DEFAULT_COLLATION),
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::Binary { len } => TypeInfo {
            tds_type: BIGBINARYTYPE,
            length_prefix: len.to_le_bytes().to_vec(),
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::VarBinary { max_len } => TypeInfo {
            tds_type: BIGVARBINARYTYPE,
            length_prefix: max_len.to_le_bytes().to_vec(),
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::Date => TypeInfo {
            tds_type: DATENTYPE,
            length_prefix: vec![0x03],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::Time => TypeInfo {
            tds_type: TIMENTYPE,
            length_prefix: vec![0x05],
            collation: None,
            scale: Some(7),
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::DateTime | iridium_core::types::DataType::SmallDateTime => TypeInfo {
            tds_type: DATETIMNTYPE,
            length_prefix: vec![0x08],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::DateTime2 => TypeInfo {
            tds_type: DATETIME2NTYPE,
            length_prefix: vec![0x08],
            collation: None,
            scale: Some(7),
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::DateTimeOffset => TypeInfo {
            tds_type: NVARCHARTYPE,
            length_prefix: 510u16.to_le_bytes().to_vec(),
            collation: Some(DEFAULT_COLLATION),
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::UniqueIdentifier => TypeInfo {
            tds_type: GUIDTYPE,
            length_prefix: vec![0x10],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::Vector { .. } => TypeInfo {
            tds_type: NVARCHARTYPE,
            length_prefix: 510u16.to_le_bytes().to_vec(),
            collation: Some(DEFAULT_COLLATION),
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::SqlVariant => TypeInfo {
            tds_type: NVARCHARTYPE,
            length_prefix: 510u16.to_le_bytes().to_vec(),
            collation: Some(DEFAULT_COLLATION),
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        iridium_core::types::DataType::Xml => TypeInfo {
            tds_type: BIGVARCHARTYPE,
            length_prefix: vec![0xFF, 0xFF],
            collation: Some(DEFAULT_COLLATION),
            scale: None,
            precision: None,
            flags: 0x0001,
        },
    }
}

pub fn infer_column_types(columns: &[String], rows: &[Vec<Value>]) -> Vec<TypeInfo> {
    columns
        .iter()
        .enumerate()
        .map(|(col_idx, _col_name)| {
            for row in rows {
                if col_idx < row.len() && !row[col_idx].is_null() {
                    return value_to_type_info(&row[col_idx]);
                }
            }
            TypeInfo {
                tds_type: NVARCHARTYPE,
                length_prefix: 510u16.to_le_bytes().to_vec(),
                collation: Some(DEFAULT_COLLATION),
                scale: None,
                precision: None,
                flags: 0x0001,
            }
        })
        .collect()
}

pub fn read_type_info(reader: &mut PacketReader) -> io::Result<TypeInfo> {
    let tds_type = reader.read_u8()?;
    let mut length_prefix = Vec::new();
    let mut scale = None;
    let mut precision = None;
    let mut collation = None;

    match tds_type {
        0x26 | 0x68 | 0x6D | 0x6E | 0x6F | 0x24 | 0x28 => {
            length_prefix.push(reader.read_u8()?);
        }
        0x29 | 0x2A | 0x2B => {
            let s = reader.read_u8()?;
            length_prefix.push(s);
            scale = Some(s);
        }
        0x6A | 0x6C => {
            length_prefix.push(reader.read_u8()?);
            precision = Some(reader.read_u8()?);
            scale = Some(reader.read_u8()?);
        }
        0xA7 | 0xAF | 0xE7 | 0xEF | 0xA5 | 0xAD => {
            length_prefix.extend_from_slice(reader.read_bytes(2)?);
            if matches!(tds_type, 0xA7 | 0xAF | 0xE7 | 0xEF) {
                let mut coll = [0u8; 5];
                coll.copy_from_slice(reader.read_bytes(5)?);
                collation = Some(coll);
            }
        }
        _ => {
            // Fix-length types
            match tds_type {
                0x30 | 0x32 | 0x34 | 0x38 | 0x3B | 0x3C | 0x3D | 0x7A | 0x7E => {}
                _ => return Err(io::Error::new(io::ErrorKind::InvalidData, format!("Unsupported TDS type: 0x{:02X}", tds_type))),
            }
        }
    }

    Ok(TypeInfo {
        tds_type,
        length_prefix,
        collation,
        scale,
        precision,
        flags: 0,
    })
}

pub fn read_value(reader: &mut PacketReader, ti: &TypeInfo) -> io::Result<Value> {
    match ti.tds_type {
        INTNTYPE => {
            let len = reader.read_u8()?;
            match len {
                0 => Ok(Value::Null),
                1 => Ok(Value::TinyInt(reader.read_u8()?)),
                2 => Ok(Value::SmallInt(reader.read_u16_le()? as i16)),
                4 => Ok(Value::Int(reader.read_u32_le()? as i32)),
                8 => Ok(Value::BigInt(reader.read_u64_le()? as i64)),
                _ => Err(io::Error::new(io::ErrorKind::InvalidData, format!("Invalid INTN length: {}", len))),
            }
        }
        BITNTYPE => {
            let len = reader.read_u8()?;
            match len {
                0 => Ok(Value::Null),
                1 => Ok(Value::Bit(reader.read_u8()? != 0)),
                _ => Err(io::Error::new(io::ErrorKind::InvalidData, format!("Invalid BITN length: {}", len))),
            }
        }
        NVARCHARTYPE | NCHARTYPE => {
            let len = reader.read_u16_le()?;
            if len == 0xFFFF {
                Ok(Value::Null)
            } else {
                Ok(Value::NVarChar(reader.read_utf16le(len as usize / 2)?))
            }
        }
        BIGVARCHARTYPE | BIGCHARTYPE => {
            let len = reader.read_u16_le()?;
            if len == 0xFFFF {
                Ok(Value::Null)
            } else {
                let bytes = reader.read_bytes(len as usize)?;
                Ok(Value::VarChar(String::from_utf8_lossy(bytes).into_owned()))
            }
        }
        0x30 => Ok(Value::TinyInt(reader.read_u8()?)),
        0x32 => Ok(Value::Bit(reader.read_u8()? != 0)),
        0x34 => Ok(Value::SmallInt(reader.read_u16_le()? as i16)),
        0x38 => Ok(Value::Int(reader.read_u32_le()? as i32)),
        0x7E => Ok(Value::BigInt(reader.read_u64_le()? as i64)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Unsupported TDS type for bulk read: 0x{:02X}", ti.tds_type),
        )),
    }
}
