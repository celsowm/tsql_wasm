use crate::tds::packet::PacketReader;
use std::io;

use super::decode;
use super::types::{RpcParam, RpcRequest};

#[derive(Debug, Clone)]
enum RpcProcSelector {
    Id(u16),
    Name(String),
}

struct RpcFrameParser<'a> {
    reader: PacketReader<'a>,
}

/// Parse an RPC request packet (sp_executesql=10, sp_prepexec=13).
/// Returns None if not a supported RPC call.
pub fn parse_rpc(data: &[u8]) -> io::Result<Option<RpcRequest>> {
    RpcFrameParser::new(data).parse()
}

impl<'a> RpcFrameParser<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            reader: PacketReader::new(data),
        }
    }

    fn parse(mut self) -> io::Result<Option<RpcRequest>> {
        self.skip_all_headers()?;
        let proc_selector = self.read_proc_selector()?;
        self.skip_rpc_flags()?;

        // Only handle sp_executesql=10 and sp_prepexec=13
        if !is_supported_rpc_proc(&proc_selector) {
            log::warn!("Unsupported RPC procedure selector: {:?}", proc_selector);
            return Ok(None);
        }

        // Param 1: SQL string
        let sql = match self.read_rpc_nvarchar_param()? {
            Some(s) => s,
            None => return Ok(None),
        };

        // Param 2: parameter declaration string e.g. "@val INT,@name NVARCHAR(50)"
        let param_decl = self.read_rpc_nvarchar_param()?.unwrap_or_default();
        let decl_parts = parse_param_decl(&param_decl);

        let params = self.read_rpc_params(&decl_parts)?;
        Ok(Some(RpcRequest { sql, params }))
    }

    fn skip_all_headers(&mut self) -> io::Result<()> {
        if self.reader.remaining() < 4 {
            return Ok(());
        }
        let total_header_len = self.reader.read_u32_le()? as usize;
        if total_header_len > 4 {
            let skip = total_header_len - 4;
            if self.reader.remaining() < skip {
                return Ok(());
            }
            self.reader.skip(skip)?;
        }
        Ok(())
    }

    fn read_proc_selector(&mut self) -> io::Result<RpcProcSelector> {
        if self.reader.remaining() < 2 {
            return Ok(RpcProcSelector::Id(0));
        }
        let name_len = self.reader.read_u16_le()?;
        if name_len == 0xFFFF {
            if self.reader.remaining() < 2 {
                return Ok(RpcProcSelector::Id(0));
            }
            return Ok(RpcProcSelector::Id(self.reader.read_u16_le()?));
        }
        let byte_len = name_len as usize * 2;
        if self.reader.remaining() < byte_len {
            return Ok(RpcProcSelector::Id(0));
        }
        let bytes = self.reader.read_bytes(byte_len)?;
        Ok(RpcProcSelector::Name(decode::decode_utf16le(bytes)))
    }

    fn skip_rpc_flags(&mut self) -> io::Result<()> {
        if self.reader.remaining() < 2 {
            return Ok(());
        }
        let _flags = self.reader.read_u16_le()?;
        Ok(())
    }

    fn read_rpc_params(&mut self, decl_parts: &[(String, String)]) -> io::Result<Vec<RpcParam>> {
        let mut params: Vec<RpcParam> = Vec::new();
        let mut idx = 0usize;

        while self.reader.remaining() > 0 {
            let Some(param_name) = self.read_param_name(idx, decl_parts)? else {
                break;
            };
            if self.reader.remaining() < 2 {
                break;
            }
            let _status = self.reader.read_u8()?;
            let type_id = self.reader.read_u8()?;

            let decoded = decode::read_typed_value(&mut self.reader, type_id)?;
            let final_type = if let Some((_, decl_type)) = decl_parts.get(idx) {
                decl_type.clone()
            } else {
                decoded.type_name
            };

            params.push(RpcParam {
                name: param_name,
                type_name: final_type,
                value_sql: decoded.value_sql,
                tvp_rows: decoded.tvp_rows,
            });
            idx += 1;
        }

        Ok(params)
    }

    fn read_param_name(
        &mut self,
        idx: usize,
        decl_parts: &[(String, String)],
    ) -> io::Result<Option<String>> {
        if self.reader.remaining() < 1 {
            return Ok(None);
        }
        let name_len = self.reader.read_u8()? as usize;
        if name_len == 0 {
            return Ok(Some(
                decl_parts
                    .get(idx)
                    .map(|(n, _)| n.clone())
                    .unwrap_or_default(),
            ));
        }
        if self.reader.remaining() < name_len * 2 {
            return Ok(None);
        }
        let bytes = self.reader.read_bytes(name_len * 2)?;
        Ok(Some(decode::decode_utf16le(bytes)))
    }

    fn read_rpc_nvarchar_param(&mut self) -> io::Result<Option<String>> {
        if self.reader.remaining() < 1 {
            return Ok(None);
        }
        let name_len = self.reader.read_u8()? as usize;
        if name_len > 0 {
            if self.reader.remaining() < name_len * 2 {
                return Ok(None);
            }
            self.reader.skip(name_len * 2)?;
        }
        if self.reader.remaining() < 2 {
            return Ok(None);
        }
        let _status = self.reader.read_u8()?;
        let type_id = self.reader.read_u8()?;

        match type_id {
            0xE7 | 0xA7 => {
                if self.reader.remaining() < 9 {
                    return Ok(None);
                }
                let _max_len = self.reader.read_u16_le()?;
                self.reader.skip(5)?;
                let actual_len = self.reader.read_u16_le()? as usize;
                if actual_len == 0xFFFF {
                    return Ok(Some(String::new()));
                }
                if self.reader.remaining() < actual_len {
                    return Ok(None);
                }
                let bytes = self.reader.read_bytes(actual_len)?;
                if type_id == 0xE7 {
                    Ok(Some(decode::decode_utf16le(bytes)))
                } else {
                    Ok(Some(String::from_utf8_lossy(bytes).to_string()))
                }
            }
            0x63 | 0x23 => { // NTEXT or TEXT
                if self.reader.remaining() < 13 {
                    return Ok(None);
                }
                let _max_len = self.reader.read_u32_le()?;
                self.reader.skip(5)?;
                let actual_len = self.reader.read_u32_le()? as usize;
                if self.reader.remaining() < actual_len {
                    return Ok(None);
                }
                let bytes = self.reader.read_bytes(actual_len)?;
                if type_id == 0x63 {
                    Ok(Some(decode::decode_utf16le(bytes)))
                } else {
                    Ok(Some(String::from_utf8_lossy(bytes).to_string()))
                }
            }
            0xF1 => {
                if self.reader.remaining() < 8 {
                    return Ok(None);
                }
                let _total = self.reader.read_u64_le()?;
                let mut result = Vec::new();
                loop {
                    if self.reader.remaining() < 4 {
                        break;
                    }
                    let chunk_len = self.reader.read_u32_le()? as usize;
                    if chunk_len == 0 {
                        break;
                    }
                    if self.reader.remaining() < chunk_len {
                        break;
                    }
                    result.extend_from_slice(self.reader.read_bytes(chunk_len)?);
                }
                Ok(Some(decode::decode_utf16le(&result)))
            }
            _ => Ok(Some(String::new())),
        }
    }
}

fn is_supported_rpc_proc(proc: &RpcProcSelector) -> bool {
    match proc {
        RpcProcSelector::Id(id) => *id == 10 || *id == 13,
        RpcProcSelector::Name(name) => {
            let base = normalize_proc_name(name);
            base == "sp_executesql" || base == "sp_prepexec"
        }
    }
}

fn normalize_proc_name(name: &str) -> String {
    let mut part = name.trim();
    if let Some(last) = part.rsplit('.').next() {
        part = last;
    }
    part.trim_matches(|c| c == '[' || c == ']' || c == ' ')
        .to_ascii_lowercase()
}

fn parse_param_decl(decl: &str) -> Vec<(String, String)> {
    if decl.trim().is_empty() {
        return vec![];
    }
    decl.split(',')
        .filter_map(|part| {
            let part = part.trim();
            let mut iter = part.splitn(2, char::is_whitespace);
            let name = iter.next()?.trim().to_string();
            let type_name = iter.next()?.trim().to_string();
            if name.is_empty() || type_name.is_empty() {
                None
            } else {
                Some((name, type_name))
            }
        })
        .collect()
}
