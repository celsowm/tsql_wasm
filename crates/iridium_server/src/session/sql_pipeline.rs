use tokio::io::AsyncWriteExt;

use iridium_core::types::{DataType, Value};
use iridium_core::SessionId;

use crate::session::compat::{
    extract_leading_use_database, is_ssms_contained_auth_probe, is_sysdac_instances_probe,
    parse_simple_use_database,
};
use crate::session::response::{build_single_int_result, build_use_database_response};
use crate::tds::batch::{build_error_response, parse_sql_batch};
use crate::tds::packet::{self, PacketBuilder, TABULAR_RESULT};
use crate::tds::tokens;

use super::TdsSession;

pub(crate) async fn handle_sql_batch<W: AsyncWriteExt + Unpin>(
    session: &mut TdsSession,
    data: &[u8],
    writer: &mut W,
) -> Result<bool, iridium_core::error::DbError> {
    let sql = match parse_sql_batch(data) {
        Ok(s) => s,
        Err(e) => {
            let err = iridium_core::error::DbError::Parse(e.to_string());
            let err_resp = build_error_response(&err);
            let _ = packet::write_packet(writer, TABULAR_RESULT, &err_resp.data).await;
            return Ok(true);
        }
    };

    if !sql.trim().is_empty() {
        log::info!(
            "[conn={}] SQL batch received:\n{}",
            session.connection_id,
            crate::session::format_sql_for_log(sql.trim())
        );
    }

    execute_sql(session, sql.trim(), writer).await
}

pub(crate) async fn execute_sql<W: AsyncWriteExt + Unpin>(
    session: &mut TdsSession,
    sql: &str,
    writer: &mut W,
) -> Result<bool, iridium_core::error::DbError> {
    if sql.is_empty() {
        let mut b = PacketBuilder::new();
        tokens::write_done(&mut b, tokens::DONE_FINAL, 1, 0);
        let _ = packet::write_packet(writer, TABULAR_RESULT, b.as_bytes()).await;
        return Ok(true);
    }

    let session_id = session.session_id.ok_or_else(|| {
        iridium_core::error::DbError::Execution("session not initialized".to_string())
    })?;

    if is_ssms_contained_auth_probe(sql) {
        if let Some(db_name) = extract_leading_use_database(sql) {
            apply_use_database(session, session_id, &db_name, writer).await?;
        }
        let data = build_single_int_result("", 0);
        let _ = packet::write_packet(writer, TABULAR_RESULT, &data).await;
        return Ok(true);
    }

    if let Some(db_name) = parse_simple_use_database(sql) {
        apply_use_database(session, session_id, &db_name, writer).await?;
        return Ok(true);
    }

    crate::session::log_sql_execution(session.connection_id, sql);
    let force_sysdac_probe_int = is_sysdac_instances_probe(sql);
    match session.db.execute_session_batch_sql_multi(session_id, sql) {
        Ok(results) => {
            let count = results.len();
            let mut b = PacketBuilder::with_capacity(4096);
            let textsize = session
                .db
                .session_options(session_id)
                .map(|opts| opts.textsize.max(0) as usize)
                .unwrap_or(4096);

            for (i, result) in results.into_iter().enumerate() {
                let is_last = i == count - 1;
                write_result_set(
                    session,
                    &mut b,
                    result,
                    is_last,
                    force_sysdac_probe_int,
                    textsize,
                );
            }

            if count == 0 {
                tokens::write_done(&mut b, tokens::DONE_FINAL, 1, 0);
            }

            let _ = packet::write_packet(writer, TABULAR_RESULT, b.as_bytes()).await;
        }
        Err(e) => {
            log::warn!(
                "[conn={}] SQL execution failed for batch:\n{}\nerror: {}",
                session.connection_id,
                crate::session::format_sql_for_log(sql),
                e
            );
            let err_resp = build_error_response(&e);
            let _ = packet::write_packet(writer, TABULAR_RESULT, &err_resp.data).await;
        }
    }

    Ok(true)
}

pub(crate) async fn apply_use_database<W: AsyncWriteExt + Unpin>(
    session: &mut TdsSession,
    session_id: SessionId,
    db_name: &str,
    writer: &mut W,
) -> Result<(), iridium_core::error::DbError> {
    let old_db = session.database.clone();
    session.database = db_name.to_string();
    if let Err(e) = session
        .db
        .set_session_database(session_id, session.database.clone())
    {
        log::error!(
            "[conn={}] Failed to update session database context: {}",
            session.connection_id,
            e
        );
    }

    let data = build_use_database_response(&session.database, &old_db);
    let _ = packet::write_packet(writer, TABULAR_RESULT, &data).await;
    Ok(())
}

fn write_result_set(
    session: &TdsSession,
    packet: &mut PacketBuilder,
    result: Option<iridium_core::QueryResult>,
    is_last: bool,
    force_sysdac_probe_int: bool,
    textsize: usize,
) {
    match result {
        Some(mut query_result) => {
            let is_proc = query_result.is_procedure;
            let return_status = query_result.return_status;

            if !query_result.columns.is_empty() {
                if force_sysdac_probe_int
                    && query_result.columns.len() == 1
                    && query_result.rows.len() == 1
                {
                    query_result.column_types[0] = DataType::Int;
                    if let Some(row) = query_result.rows.get_mut(0) {
                        if let Some(value) = row.get_mut(0) {
                            let int_val = match &*value {
                                Value::Null => 0,
                                other => other.to_integer_i64().unwrap_or(0) as i32,
                            };
                            *value = Value::Int(int_val);
                        }
                    }
                }

                let mut types = Vec::new();
                log::debug!(
                    "[conn={}] Result set: columns={}, types={}",
                    session.connection_id,
                    query_result.columns.len(),
                    query_result.column_types.len()
                );
                for ct in &query_result.column_types {
                    types.push(crate::tds::type_mapping::runtime_type_to_tds(ct));
                }
                for (idx, col_name) in query_result.columns.iter().enumerate() {
                    if let (Some(runtime_ty), Some(tds_ty)) =
                        (query_result.column_types.get(idx), types.get(idx))
                    {
                        log::debug!(
                            "[conn={}] COLMETADATA[{}]: name='{}' runtime={:?} tds=0x{:02X} len={:02X?}",
                            session.connection_id,
                            idx,
                            col_name,
                            runtime_ty,
                            tds_ty.tds_type,
                            tds_ty.length_prefix
                        );
                    }
                }
                tokens::write_colmetadata(packet, &query_result.columns, &types);
                for row in &query_result.rows {
                    tokens::write_row(packet, row, &types, textsize);
                }

                if is_proc {
                    tokens::write_done_in_proc(
                        packet,
                        tokens::DONE_MORE | tokens::DONE_COUNT,
                        1,
                        query_result.rows.len() as u64,
                    );
                } else {
                    let done_status = if is_last && return_status.is_none() {
                        tokens::DONE_FINAL
                    } else {
                        tokens::DONE_MORE
                    };
                    tokens::write_done(
                        packet,
                        done_status | tokens::DONE_COUNT,
                        1,
                        query_result.rows.len() as u64,
                    );
                }
            } else if !is_proc {
                let done_status = if is_last && return_status.is_none() {
                    tokens::DONE_FINAL
                } else {
                    tokens::DONE_MORE
                };
                tokens::write_done(
                    packet,
                    done_status | tokens::DONE_COUNT,
                    1,
                    query_result.rows.len() as u64,
                );
            }

            if let Some(code) = return_status {
                tokens::write_returnstatus(packet, code);
                let done_status = if is_last {
                    tokens::DONE_FINAL
                } else {
                    tokens::DONE_MORE
                };
                tokens::write_doneproc(packet, done_status, 1, 0);
            }
        }
        None => {
            let done_status = if is_last {
                tokens::DONE_FINAL
            } else {
                tokens::DONE_MORE
            };
            tokens::write_done(packet, done_status, 1, 0);
        }
    }
}
