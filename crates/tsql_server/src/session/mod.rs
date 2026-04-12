use once_cell::sync::Lazy;
use regex::Regex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use tsql_core::types::{DataType, Value};
use tsql_core::{error::DbError, Database, SessionId, StatementExecutor};

use super::pool::{CheckoutError, SessionPool};
mod compat;
mod response;

use self::compat::{
    extract_leading_use_database, is_ssms_contained_auth_probe, parse_simple_use_database,
};
use self::response::{build_single_int_result, build_use_database_response};
use super::tds::batch::{build_error_response, parse_sql_batch};
use super::tds::login::parse_login7;
use super::tds::packet::{
    self, PacketBuilder, ATTENTION, RPC, SQL_BATCH, TABULAR_RESULT, TDS7_LOGIN, TDS7_PRELOGIN,
};
use super::tds::prelogin::{
    build_prelogin_response, parse_prelogin, ENCRYPT_NOT_SUP, ENCRYPT_OFF, ENCRYPT_ON,
    ENCRYPT_REQUIRED,
};
use super::tds::rpc::{build_param_preamble, parse_rpc};
use super::tds::tokens;
use super::tds_tls_io::TdsTlsIo;
use super::tls;
use super::ServerConfig;

trait AsyncReadWrite: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin> AsyncReadWrite for T {}

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

pub struct TdsSession {
    connection_id: u64,
    db: Database,
    config: Arc<ServerConfig>,
    session_pool: Arc<SessionPool>,
    session_id: Option<SessionId>,
    packet_size: u16,
    database: String,
}

impl TdsSession {
    pub fn new(db: Database, config: Arc<ServerConfig>, session_pool: Arc<SessionPool>) -> Self {
        let database = config.database.clone();
        Self {
            connection_id: NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed),
            db,
            config,
            session_pool,
            session_id: None,
            packet_size: 4096,
            database,
        }
    }

    pub async fn handle(self, mut stream: TcpStream) -> Result<(), String> {
        let mut needs_tls_upgrade = false;
        let mut login_packet = None;

        log::info!(
            "[conn={}] Starting handshake for incoming connection",
            self.connection_id
        );
        loop {
            let (header, data) = packet::read_message(&mut stream)
                .await
                .map_err(|e| format!("Handshake read error: {}", e))?;
            log_packet(self.connection_id, "handshake", &header, &data);

            if header.packet_type == TDS7_PRELOGIN {
                log::debug!(
                    "[conn={}] PRELOGIN data hex: {:02X?}",
                    self.connection_id,
                    data
                );
                let prelogin = parse_prelogin(&data).map_err(|e| e.to_string())?;
                log::debug!(
                    "[conn={}] PRELOGIN: version={:?}, encryption={}",
                    self.connection_id,
                    prelogin.version,
                    prelogin.encryption
                );

                // When TLS is disabled, respond based on client's request:
                // - If client requested ENCRYPT_NOT_SUP, echo ENCRYPT_NOT_SUP
                // - If client requested ENCRYPT_ON/REQUIRED, respond with ENCRYPT_NOT_SUP
                // - If client requested ENCRYPT_OFF, respond with ENCRYPT_OFF
                // This maintains compatibility with clients that use strict
                // prelogin encryption negotiation.
                let server_encrypt = if self.config.tls_enabled {
                    ENCRYPT_ON
                } else if prelogin.encryption == ENCRYPT_NOT_SUP
                    || prelogin.encryption == ENCRYPT_ON
                    || prelogin.encryption == ENCRYPT_REQUIRED
                {
                    ENCRYPT_NOT_SUP
                } else {
                    ENCRYPT_OFF
                };

                needs_tls_upgrade = self.config.tls_enabled
                    && (prelogin.encryption == ENCRYPT_ON
                        || prelogin.encryption == ENCRYPT_REQUIRED);

                let response = build_prelogin_response(server_encrypt);
                packet::write_packet(&mut stream, TDS7_PRELOGIN, &response)
                    .await
                    .map_err(|e| format!("Failed to write PRELOGIN response: {}", e))?;
                stream
                    .flush()
                    .await
                    .map_err(|e| format!("Failed to flush: {}", e))?;
                log::debug!(
                    "[conn={}] Sent PRELOGIN response (encryption={})",
                    self.connection_id,
                    server_encrypt
                );

                if needs_tls_upgrade {
                    break;
                }
            } else if header.packet_type == TDS7_LOGIN {
                log_packet(self.connection_id, "login", &header, &data);
                login_packet = Some((header, data));
                break;
            } else {
                return Err(format!(
                    "Expected PRELOGIN or LOGIN7, got 0x{:02X}",
                    header.packet_type
                ));
            }
        }

        self.handle_login(stream, login_packet, needs_tls_upgrade)
            .await
    }

    async fn handle_login(
        mut self,
        stream: TcpStream,
        login_packet: Option<(packet::PacketHeader, Vec<u8>)>,
        needs_tls_upgrade: bool,
    ) -> Result<(), String> {
        // Perform TLS upgrade if needed.
        // In TDS 7.4, the TLS handshake is tunneled inside TDS PRELOGIN (0x12)
        // packets. After handshake completion, traffic switches to raw TLS over TCP.
        let stream: Box<dyn AsyncReadWrite> = if needs_tls_upgrade {
            log::info!(
                "[conn={}] Client requested TLS, upgrading connection via TDS-wrapped handshake",
                self.connection_id
            );

            let tls_config = if let (Some(cert_path), Some(key_path)) =
                (&self.config.tls_cert_path, &self.config.tls_key_path)
            {
                tls::load_tls_config(cert_path, key_path)
                    .map_err(|e| format!("Failed to load TLS config: {}", e))?
            } else {
                return Err("TLS enabled but no certificate configured".to_string());
            };

            let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(tls_config));

            // Use TdsTlsIo to strip/wrap TDS framing during handshake
            let raw_mode = Arc::new(AtomicBool::new(false));
            let tds_io = TdsTlsIo::new(stream, raw_mode.clone());

            let tls_stream = acceptor
                .accept(tds_io)
                .await
                .map_err(|e| format!("TLS handshake failed: {}", e))?;

            // Switch to raw mode — post-handshake traffic is raw TLS over TCP
            raw_mode.store(true, Ordering::Release);

            log::info!(
                "[conn={}] TLS handshake completed, switched to raw mode",
                self.connection_id
            );
            Box::new(tls_stream)
        } else {
            Box::new(stream)
        };

        // Now use the stream for the rest
        let (mut reader, mut writer) = tokio::io::split(stream);

        let login_data = if let Some((_, data)) = login_packet {
            data
        } else {
            let (header, data) = packet::read_message(&mut reader)
                .await
                .map_err(|e| format!("Failed to read LOGIN7: {}", e))?;
            if header.packet_type != TDS7_LOGIN {
                return Err(format!(
                    "Expected LOGIN7 (0x10), got 0x{:02X}",
                    header.packet_type
                ));
            }
            data
        };

        let login = parse_login7(&login_data).map_err(|e| e.to_string())?;
        log::info!(
            "[conn={}] LOGIN7: user={}, database={}, app={}, packet_size={}",
            self.connection_id,
            login.username,
            login.database,
            login.app_name,
            login.packet_size
        );

        if let Some(ref creds) = self.config.auth {
            if login.username != creds.user || login.password != creds.password {
                log::warn!(
                    "[conn={}] Login rejected for user={} against configured SQL auth",
                    self.connection_id,
                    login.username
                );
                let err = DbError::Execution("Login failed for user.".to_string());
                let err_resp = build_error_response(&err);
                packet::write_packet(&mut writer, TABULAR_RESULT, &err_resp.data)
                    .await
                    .map_err(|e| e.to_string())?;
                return Ok(());
            }
            log::info!(
                "[conn={}] Login accepted for user={}",
                self.connection_id,
                login.username
            );
        } else {
            log::info!(
                "[conn={}] Login accepted with authentication disabled",
                self.connection_id
            );
        }

        if login.packet_size > 0 {
            self.packet_size = login.packet_size.min(32767) as u16;
        }
        if !login.database.is_empty() {
            self.database = login.database.clone();
        }

        let session_id = match self.session_pool.checkout(&self.db) {
            Ok(sid) => sid,
            Err(CheckoutError::Exhausted) => {
                let err = DbError::Execution("session pool exhausted".to_string());
                let err_resp = build_error_response(&err);
                packet::write_packet(&mut writer, TABULAR_RESULT, &err_resp.data)
                    .await
                    .map_err(|e| e.to_string())?;
                return Ok(());
            }
        };
        self.session_id = Some(session_id);

        // Set session metadata
        if let Err(e) = self.db.executor().set_session_metadata(
            session_id,
            Some(login.username.clone()),
            Some(login.app_name.clone()),
            Some(login.hostname.clone()),
            Some(self.database.clone()),
        ) {
            log::error!(
                "[conn={}] Failed to set session metadata: {}",
                self.connection_id,
                e
            );
        }

        // Build LOGINACK response
        // SSMS expects the full sequence: ENVCHANGE(PacketSize), ENVCHANGE(Database),
        // ENVCHANGE(Language), ENVCHANGE(Collation), LOGINACK, INFO, DONE_FINAL
        let mut b = PacketBuilder::with_capacity(512);
        tokens::write_envchange_packet_size(&mut b, self.packet_size, self.packet_size);
        tokens::write_envchange_database(&mut b, &self.database, "master");
        tokens::write_envchange_language(&mut b, "us_english", "");
        tokens::write_envchange_collation(&mut b);
        tokens::write_loginack(&mut b, 0x74000004);
        tokens::write_info(
            &mut b,
            5701,
            2,
            0,
            &format!("Changed database context to '{}'.", &self.database),
            "localhost",
            "",
            1,
        );
        tokens::write_info(
            &mut b,
            5703,
            1,
            0,
            "Changed language setting to us_english.",
            "localhost",
            "",
            1,
        );
        tokens::write_done(&mut b, tokens::DONE_FINAL, 0, 0);

        if let Err(e) = packet::write_packet(&mut writer, TABULAR_RESULT, b.as_bytes()).await {
            self.session_pool.checkin(&self.db, session_id);
            return Err(format!("Failed to write LOGINACK: {}", e));
        }
        log::debug!("[conn={}] Sent LOGINACK response", self.connection_id);

        // Main loop: handle SQL Batch and other packets
        loop {
            let result = packet::read_message(&mut reader).await;
            match result {
                Ok((header, data)) => {
                    log_packet(self.connection_id, "packet", &header, &data);
                    match header.packet_type {
                        SQL_BATCH => match self.handle_sql_batch(&data, &mut writer).await {
                            Ok(_) => {}
                            Err(e) => {
                                log::error!("[conn={}] SQL batch error: {}", self.connection_id, e);
                                let err_resp = build_error_response(&e);
                                let _ = packet::write_packet(
                                    &mut writer,
                                    TABULAR_RESULT,
                                    &err_resp.data,
                                )
                                .await;
                            }
                        },
                        RPC => match parse_rpc(&data) {
                            Ok(Some(rpc)) => {
                                let preamble = build_param_preamble(&rpc.params);
                                let full_sql = if preamble.is_empty() {
                                    rpc.sql
                                } else {
                                    format!("{}{}", preamble, rpc.sql)
                                };
                                match self.execute_sql(full_sql.trim(), &mut writer).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        log::error!("RPC SQL error: {}", e);
                                        let err_resp = build_error_response(&e);
                                        let _ = packet::write_packet(
                                            &mut writer,
                                            TABULAR_RESULT,
                                            &err_resp.data,
                                        )
                                        .await;
                                    }
                                }
                            }
                            Ok(None) => {
                                let preview_len = data.len().min(96);
                                log::warn!(
                                    "[conn={}] Unsupported RPC request ({} bytes), first {} bytes: {:02X?}",
                                    self.connection_id,
                                    data.len(),
                                    preview_len,
                                    &data[..preview_len]
                                );
                                let err = DbError::Parse(
                                    "unsupported RPC request; only sp_executesql and sp_prepexec are supported"
                                        .into(),
                                );
                                let err_resp = build_error_response(&err);
                                let _ = packet::write_packet(
                                    &mut writer,
                                    TABULAR_RESULT,
                                    &err_resp.data,
                                )
                                .await;
                            }
                            Err(e) => {
                                let err = tsql_core::error::DbError::Parse(e.to_string());
                                let err_resp = build_error_response(&err);
                                let _ = packet::write_packet(
                                    &mut writer,
                                    TABULAR_RESULT,
                                    &err_resp.data,
                                )
                                .await;
                            }
                        },
                        ATTENTION => {
                            log::debug!("[conn={}] ATTENTION received", self.connection_id);
                            let mut attn = PacketBuilder::new();
                            tokens::write_done(&mut attn, tokens::DONE_ATTN, 0, 0);
                            let _ =
                                packet::write_packet(&mut writer, TABULAR_RESULT, attn.as_bytes())
                                    .await;
                        }
                        _ => {
                            log::warn!(
                                "[conn={}] Unsupported packet type 0x{:02X}",
                                self.connection_id,
                                header.packet_type
                            );
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    log::debug!("[conn={}] Client disconnected", self.connection_id);
                    break;
                }
                Err(e) => {
                    log::error!("[conn={}] Read error: {}", self.connection_id, e);
                    break;
                }
            }
        }

        if let Some(sid) = self.session_id.take() {
            self.session_pool.checkin(&self.db, sid);
        }

        Ok(())
    }

    async fn handle_sql_batch<W: AsyncWriteExt + Unpin>(
        &mut self,
        data: &[u8],
        writer: &mut W,
    ) -> Result<bool, tsql_core::error::DbError> {
        let sql = match parse_sql_batch(data) {
            Ok(s) => s,
            Err(e) => {
                let err = tsql_core::error::DbError::Parse(e.to_string());
                let err_resp = build_error_response(&err);
                packet::write_packet(writer, TABULAR_RESULT, &err_resp.data)
                    .await
                    .map_err(|e| tsql_core::error::DbError::Execution(e.to_string()))?;
                return Ok(true);
            }
        };

        if !sql.trim().is_empty() {
            log::info!(
                "[conn={}] SQL batch received:\n{}",
                self.connection_id,
                format_sql_for_log(sql.trim())
            );
        }
        self.execute_sql(sql.trim(), writer).await
    }

    async fn execute_sql<W: AsyncWriteExt + Unpin>(
        &mut self,
        sql: &str,
        writer: &mut W,
    ) -> Result<bool, tsql_core::error::DbError> {
        if sql.is_empty() {
            let mut b = PacketBuilder::new();
            tokens::write_done(&mut b, tokens::DONE_FINAL, 1, 0);
            packet::write_packet(writer, TABULAR_RESULT, b.as_bytes())
                .await
                .map_err(|e| tsql_core::error::DbError::Execution(e.to_string()))?;
            return Ok(true);
        }

        let session_id = self.session_id.ok_or_else(|| {
            tsql_core::error::DbError::Execution("session not initialized".to_string())
        })?;

        if is_ssms_contained_auth_probe(sql) {
            if let Some(db_name) = extract_leading_use_database(sql) {
                self.apply_use_database(session_id, &db_name, writer)
                    .await?;
            }
            // SSMS expects a scalar response for this probe; returning 0 keeps the flow compatible.
            let data = build_single_int_result("", 0);
            packet::write_packet(writer, TABULAR_RESULT, &data)
                .await
                .map_err(|e| tsql_core::error::DbError::Execution(e.to_string()))?;
            return Ok(true);
        }

        if let Some(db_name) = parse_simple_use_database(sql) {
            self.apply_use_database(session_id, &db_name, writer)
                .await?;
            return Ok(true);
        }

        log_sql_execution(self.connection_id, sql);
        let force_sysdac_probe_int = self::compat::is_sysdac_instances_probe(sql);
        match self
            .db
            .executor()
            .execute_session_batch_sql_multi(session_id, sql)
        {
            Ok(results) => {
                let count = results.len();
                let mut b = PacketBuilder::with_capacity(4096);
                let textsize = self
                    .db
                    .session_options(session_id)
                    .map(|opts| opts.textsize.max(0) as usize)
                    .unwrap_or(4096);

                for (i, result) in results.into_iter().enumerate() {
                    let is_last = i == count - 1;

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
                                    self.connection_id,
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
                                            self.connection_id,
                                            idx,
                                            col_name,
                                            runtime_ty,
                                            tds_ty.tds_type,
                                            tds_ty.length_prefix
                                        );
                                    }
                                }
                                tokens::write_colmetadata(&mut b, &query_result.columns, &types);
                                for row in &query_result.rows {
                                    tokens::write_row(&mut b, row, &types, textsize);
                                }

                                if is_proc {
                                    tokens::write_done_in_proc(
                                        &mut b,
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
                                        &mut b,
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
                                    &mut b,
                                    done_status | tokens::DONE_COUNT,
                                    1,
                                    query_result.rows.len() as u64,
                                );
                            }

                            if let Some(code) = return_status {
                                tokens::write_returnstatus(&mut b, code);
                                let done_status = if is_last {
                                    tokens::DONE_FINAL
                                } else {
                                    tokens::DONE_MORE
                                };
                                tokens::write_doneproc(&mut b, done_status, 1, 0);
                            }
                        }
                        None => {
                            let done_status = if is_last {
                                tokens::DONE_FINAL
                            } else {
                                tokens::DONE_MORE
                            };
                            tokens::write_done(&mut b, done_status, 1, 0);
                        }
                    }
                }

                // If no statements at all, send a final DONE
                if count == 0 {
                    tokens::write_done(&mut b, tokens::DONE_FINAL, 1, 0);
                }

                packet::write_packet(writer, TABULAR_RESULT, b.as_bytes())
                    .await
                    .map_err(|e| tsql_core::error::DbError::Execution(e.to_string()))?;
            }
            Err(e) => {
                log::warn!(
                    "[conn={}] SQL execution failed for batch:\n{}\nerror: {}",
                    self.connection_id,
                    format_sql_for_log(sql),
                    e
                );
                let err_resp = build_error_response(&e);
                packet::write_packet(writer, TABULAR_RESULT, &err_resp.data)
                    .await
                    .map_err(|e| tsql_core::error::DbError::Execution(e.to_string()))?;
            }
        }

        Ok(true)
    }

    async fn apply_use_database<W: AsyncWriteExt + Unpin>(
        &mut self,
        session_id: SessionId,
        db_name: &str,
        writer: &mut W,
    ) -> Result<(), tsql_core::error::DbError> {
        let old_db = self.database.clone();
        self.database = db_name.to_string();
        if let Err(e) = self
            .db
            .executor()
            .set_session_database(session_id, self.database.clone())
        {
            log::error!(
                "[conn={}] Failed to update session database context: {}",
                self.connection_id,
                e
            );
        }

        let data = build_use_database_response(&self.database, &old_db);
        packet::write_packet(writer, TABULAR_RESULT, &data)
            .await
            .map_err(|e| tsql_core::error::DbError::Execution(e.to_string()))
    }
}

static SQL_KEYWORD_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)\b(SELECT|FROM|WHERE|GROUP BY|ORDER BY|HAVING|JOIN|INNER|LEFT|RIGHT|FULL|ON|AS|CAST|CONVERT|DECLARE|SET|IF|BEGIN|END|ELSE|EXEC|EXECUTE|INSERT|INTO|UPDATE|DELETE|CREATE|TABLE|VIEW|VALUES|TOP|DISTINCT|ISNULL|SERVERPROPERTY|OBJECT_ID|CROSS APPLY)\b",
    )
    .expect("valid SQL keyword regex")
});

fn format_sql_for_log(sql: &str) -> String {
    let text = if std::env::var("TSQL_LOG_FULL_SQL")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        sql.to_string()
    } else {
        truncate_for_log(sql, 4000)
    };

    if std::env::var("TSQL_LOG_SQL_HIGHLIGHT")
        .map(|v| v == "0" || v.eq_ignore_ascii_case("false"))
        .unwrap_or(false)
    {
        return text;
    }

    SQL_KEYWORD_RE
        .replace_all(&text, |caps: &regex::Captures| {
            format!("\x1b[1;36m{}\x1b[0m", &caps[0])
        })
        .to_string()
}

fn log_sql_execution(connection_id: u64, sql: &str) {
    if false {
        return;
    }

    log::info!(
        "[conn={}] Executing SQL:\n{}",
        connection_id,
        format_sql_for_log(sql)
    );
}

fn truncate_for_log(text: &str, max_chars: usize) -> String {
    let total_chars = text.chars().count();
    if total_chars <= max_chars {
        return text.to_string();
    }

    let cut_at = text
        .char_indices()
        .nth(max_chars)
        .map(|(idx, _)| idx)
        .unwrap_or(text.len());
    let mut preview = text[..cut_at].to_string();
    preview.push_str("... [truncated]");
    preview
}

fn log_packet(connection_id: u64, stage: &str, header: &packet::PacketHeader, data: &[u8]) {
    let preview_len = data.len().min(96);
    log::debug!(
        "[conn={}] {} packet type=0x{:02X} len={} preview={:02X?}",
        connection_id,
        stage,
        header.packet_type,
        header.length,
        &data[..preview_len]
    );
}
