use iridium_core::types::{DataType, Value};

use crate::tds::packet::PacketBuilder;
use crate::tds::tokens;

pub(crate) fn build_use_database_response(new_db: &str, old_db: &str) -> Vec<u8> {
    let mut b = PacketBuilder::new();
    tokens::write_envchange_database(&mut b, new_db, old_db);
    tokens::write_done(&mut b, tokens::DONE_FINAL, 1, 0);
    b.as_bytes().to_vec()
}

pub(crate) fn build_single_int_result(column_name: &str, value: i32) -> Vec<u8> {
    let mut b = PacketBuilder::new();
    let tds_types = vec![crate::tds::type_mapping::runtime_type_to_tds(
        &DataType::Int,
    )];
    tokens::write_colmetadata(&mut b, &[column_name.to_string()], &tds_types);
    tokens::write_row(&mut b, &[Value::Int(value)], &tds_types, 4096);
    tokens::write_done(&mut b, tokens::DONE_FINAL | tokens::DONE_COUNT, 1, 1);
    b.as_bytes().to_vec()
}
