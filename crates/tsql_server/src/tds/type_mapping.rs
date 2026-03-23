use tsql_core::types::Value;

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
            flags: 0x0001, // nullable
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
        Value::Decimal(_, scale) => TypeInfo {
            tds_type: DECIMALNTYPE,
            length_prefix: vec![0x0D], // 13 bytes for precision up to 38
            collation: None,
            scale: Some(*scale),
            precision: Some(38),
            flags: 0x0001,
        },
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
        Value::Char(s) => {
            let max_len = s.len().max(1) as u16;
            TypeInfo {
                tds_type: BIGCHARTYPE,
                length_prefix: {
                    let mut v = Vec::new();
                    v.extend_from_slice(&max_len.to_be_bytes());
                    v
                },
                collation: Some(DEFAULT_COLLATION),
                scale: None,
                precision: None,
                flags: 0x0001,
            }
        }
        Value::VarChar(s) => {
            let max_len = s.len().max(1) as u16;
            TypeInfo {
                tds_type: BIGVARCHARTYPE,
                length_prefix: {
                    let mut v = Vec::new();
                    v.extend_from_slice(&max_len.to_be_bytes());
                    v
                },
                collation: Some(DEFAULT_COLLATION),
                scale: None,
                precision: None,
                flags: 0x0001,
            }
        }
        Value::NChar(s) => {
            let max_len = s.len().max(1) as u16;
            TypeInfo {
                tds_type: NCHARTYPE,
                length_prefix: {
                    let mut v = Vec::new();
                    v.extend_from_slice(&max_len.to_be_bytes());
                    v
                },
                collation: Some(DEFAULT_COLLATION),
                scale: None,
                precision: None,
                flags: 0x0001,
            }
        }
        Value::NVarChar(s) => {
            let max_len = s.len().max(1) as u16;
            TypeInfo {
                tds_type: NVARCHARTYPE,
                length_prefix: {
                    let mut v = Vec::new();
                    v.extend_from_slice(&max_len.to_be_bytes());
                    v
                },
                collation: Some(DEFAULT_COLLATION),
                scale: None,
                precision: None,
                flags: 0x0001,
            }
        }
        Value::Binary(v) => {
            let len = v.len().max(1) as u16;
            TypeInfo {
                tds_type: BIGBINARYTYPE,
                length_prefix: {
                    let mut bv = Vec::new();
                    bv.extend_from_slice(&len.to_be_bytes());
                    bv
                },
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
                length_prefix: {
                    let mut bv = Vec::new();
                    bv.extend_from_slice(&len.to_be_bytes());
                    bv
                },
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
            length_prefix: vec![0x05], // scale 7 = 5 bytes
            collation: None,
            scale: Some(7),
            precision: None,
            flags: 0x0001,
        },
        Value::DateTime(_) => TypeInfo {
            tds_type: DATETIMNTYPE,
            length_prefix: vec![0x08],
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        Value::DateTime2(_) => TypeInfo {
            tds_type: DATETIME2NTYPE,
            length_prefix: vec![0x08], // scale 7 = 3 + 5 = 8 bytes
            collation: None,
            scale: Some(7),
            precision: None,
            flags: 0x0001,
        },
        Value::UniqueIdentifier(_) => TypeInfo {
            tds_type: GUIDTYPE,
            length_prefix: vec![0x10], // 16 bytes
            collation: None,
            scale: None,
            precision: None,
            flags: 0x0001,
        },
        Value::Null | Value::SqlVariant(_) => {
            // Default to NVARCHAR for unknown/null
            TypeInfo {
                tds_type: NVARCHARTYPE,
                length_prefix: {
                    let mut v = Vec::new();
                    v.extend_from_slice(&255u16.to_be_bytes());
                    v
                },
                collation: Some(DEFAULT_COLLATION),
                scale: None,
                precision: None,
                flags: 0x0001,
            }
        }
    }
}

pub fn value_to_wire_bytes(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => vec![0x00], // NULL indicator
        Value::Bit(b) => {
            if *b {
                vec![0x01, 0x01]
            } else {
                vec![0x01, 0x00]
            }
        }
        Value::TinyInt(v) => vec![0x01, *v],
        Value::SmallInt(v) => {
            let mut buf = vec![0x02];
            buf.extend_from_slice(&v.to_le_bytes());
            buf
        }
        Value::Int(v) => {
            let mut buf = vec![0x04];
            buf.extend_from_slice(&v.to_le_bytes());
            buf
        }
        Value::BigInt(v) => {
            let mut buf = vec![0x08];
            buf.extend_from_slice(&v.to_le_bytes());
            buf
        }
        Value::Float(v) => {
            let mut buf = vec![0x08];
            buf.extend_from_slice(&v.to_le_bytes());
            buf
        }
        Value::Decimal(raw, scale) => {
            // DECIMAL: sign byte (0=negative, 1=positive) + LE limbs
            // Using 13 bytes for precision 38
            let negative = *raw < 0;
            let abs_val = if negative {
                (-raw) as u128
            } else {
                (*raw) as u128
            };

            let mut buf = vec![0x0D]; // length = 13
            buf.push(if negative { 0x00 } else { 0x01 }); // sign
            buf.push(*scale);

            // 4 x 32-bit LE limbs
            let limbs = [
                (abs_val & 0xFFFFFFFF) as u32,
                ((abs_val >> 32) & 0xFFFFFFFF) as u32,
                ((abs_val >> 64) & 0xFFFFFFFF) as u32,
                ((abs_val >> 96) & 0xFFFFFFFF) as u32,
            ];
            for limb in &limbs {
                buf.extend_from_slice(&limb.to_le_bytes());
            }
            buf
        }
        Value::Money(v) => {
            // MONEY is stored as i64 (scaled by 10000)
            let mut buf = vec![0x08];
            buf.extend_from_slice(&v.to_le_bytes());
            buf
        }
        Value::SmallMoney(v) => {
            let mut buf = vec![0x04];
            buf.extend_from_slice(&(*v as i32).to_le_bytes());
            buf
        }
        Value::Char(s) | Value::VarChar(s) => {
            let bytes = s.as_bytes();
            let mut buf = Vec::with_capacity(2 + bytes.len());
            buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(bytes);
            buf
        }
        Value::NChar(s) | Value::NVarChar(s) => {
            let utf16: Vec<u16> = s.encode_utf16().collect();
            let mut buf = Vec::with_capacity(2 + utf16.len() * 2);
            buf.extend_from_slice(&(utf16.len() as u16).to_be_bytes());
            for c in &utf16 {
                buf.extend_from_slice(&c.to_le_bytes());
            }
            buf
        }
        Value::Binary(v) | Value::VarBinary(v) => {
            let mut buf = Vec::with_capacity(2 + v.len());
            buf.extend_from_slice(&(v.len() as u16).to_be_bytes());
            buf.extend_from_slice(v);
            buf
        }
        Value::Date(s) => {
            // Parse "YYYY-MM-DD" to days since 2000-01-01
            let days = parse_date_to_days_since_2000(s);
            let mut buf = vec![0x03];
            buf.extend_from_slice(&days.to_le_bytes()[..3]);
            buf
        }
        Value::Time(s) => {
            // Parse "HH:MM:SS.fffffff" to 100ns ticks (scale 7)
            let ticks = parse_time_to_ticks(s);
            // Time with scale 7: 5 bytes
            let mut buf = vec![0x05];
            buf.extend_from_slice(&ticks.to_le_bytes()[..5]);
            buf
        }
        Value::DateTime(s) => {
            // Parse "YYYY-MM-DD HH:MM:SS.fff" to days + 1/300 sec ticks
            let (days, ticks_300) = parse_datetime(s);
            let mut buf = vec![0x08];
            buf.extend_from_slice(&days.to_le_bytes());
            buf.extend_from_slice(&ticks_300.to_le_bytes());
            buf
        }
        Value::DateTime2(s) => {
            // Parse "YYYY-MM-DD HH:MM:SS.fffffff"
            let (days, ticks) = parse_datetime2(s);
            let mut buf = vec![0x08]; // length for scale 7
            buf.extend_from_slice(&ticks.to_le_bytes()[..5]); // 5 bytes time
            buf.extend_from_slice(&days.to_le_bytes()[..3]); // 3 bytes date
            buf
        }
        Value::UniqueIdentifier(s) => {
            // Parse "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" to 16 bytes
            let bytes = parse_guid(s);
            let mut buf = vec![0x10]; // length
            buf.extend_from_slice(&bytes);
            buf
        }
        Value::SqlVariant(v) => value_to_wire_bytes(v),
    }
}

fn parse_date_to_days_since_2000(s: &str) -> i32 {
    // Expected: "YYYY-MM-DD"
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() < 3 {
        return 0;
    }
    let y: i32 = parts[0].parse().unwrap_or(2000);
    let m: i32 = parts[1].parse().unwrap_or(1);
    let d: i32 = parts[2].parse().unwrap_or(1);

    // Days since 2000-01-01
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
    // Expected: "HH:MM:SS" or "HH:MM:SS.fffffff"
    let main_frac: Vec<&str> = s.split('.').collect();
    let parts: Vec<&str> = main_frac[0].split(':').collect();
    if parts.len() < 3 {
        return 0;
    }
    let h: u64 = parts[0].parse().unwrap_or(0);
    let mi: u64 = parts[1].parse().unwrap_or(0);
    let sec: u64 = parts[2].parse().unwrap_or(0);

    let mut ticks = h * 3600 + mi * 60 + sec;
    ticks *= 10_000_000; // Convert to 100ns ticks (scale 7)

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
    // Expected: "YYYY-MM-DD HH:MM:SS.fff"
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
    ticks *= 300; // Convert to 1/300 sec

    // Milliseconds
    if main_frac.len() > 1 {
        let ms_str = &main_frac[1][..main_frac[1].len().min(3)];
        let ms: u32 = ms_str.parse().unwrap_or(0);
        ticks += ms * 300 / 1000;
    }

    (days, ticks)
}

fn parse_datetime2(s: &str) -> (i32, u64) {
    // Expected: "YYYY-MM-DD HH:MM:SS.fffffff"
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

    // TDS GUID is stored with mixed-endian:
    // First 4 bytes reversed (LE), next 2 reversed (LE), next 2 reversed (LE), last 8 as-is (BE)
    // But the GUID string format is already in the "display" order
    // Data1: bytes[0..4] LE
    // Data2: bytes[4..6] LE
    // Data3: bytes[6..8] LE
    // Data4: bytes[8..16] BE
    let mut result = [0u8; 16];
    result[0..4].copy_from_slice(&bytes[0..4].iter().rev().cloned().collect::<Vec<_>>());
    result[4..6].copy_from_slice(&bytes[4..6].iter().rev().cloned().collect::<Vec<_>>());
    result[6..8].copy_from_slice(&bytes[6..8].iter().rev().cloned().collect::<Vec<_>>());
    result[8..16].copy_from_slice(&bytes[8..16]);
    result
}

pub fn infer_column_types(
    columns: &[String],
    rows: &[Vec<Value>],
) -> Vec<TypeInfo> {
    // For each column, look at the first non-null value to determine type
    columns
        .iter()
        .enumerate()
        .map(|(col_idx, _col_name)| {
            // Try to find first non-null value in this column
            for row in rows {
                if col_idx < row.len() && !row[col_idx].is_null() {
                    return value_to_type_info(&row[col_idx]);
                }
            }
            // Default to NVARCHAR(255) for all-null or empty columns
            TypeInfo {
                tds_type: NVARCHARTYPE,
                length_prefix: {
                    let mut v = Vec::new();
                    v.extend_from_slice(&255u16.to_be_bytes());
                    v
                },
                collation: Some(DEFAULT_COLLATION),
                scale: None,
                precision: None,
                flags: 0x0001,
            }
        })
        .collect()
}
