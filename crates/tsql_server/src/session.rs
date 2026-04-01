use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use tsql_core::{Database, SessionId, StatementExecutor};

use super::pool::{CheckoutError, SessionPool};
use super::tds::batch::{build_error_response, parse_sql_batch};
use super::tds::login::parse_login7;
use super::tds::packet::{
    self, PacketBuilder, ATTENTION, RPC, SQL_BATCH, TABULAR_RESULT, TDS7_LOGIN,
    TDS7_PRELOGIN,
};
use super::tds::prelogin::{build_prelogin_response, parse_prelogin, ENCRYPT_NOT_SUP, ENCRYPT_OFF, ENCRYPT_ON, ENCRYPT_REQUIRED};
use super::tds::rpc::{build_param_preamble, parse_rpc};
use super::tds::tokens;
use super::tls;
use super::ServerConfig;

trait AsyncReadWrite: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin> AsyncReadWrite for T {}

pub struct TdsSession {
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

        loop {
            let (header, data) = packet::read_packet(&mut stream)
                .await
                .map_err(|e| format!("Handshake read error: {}", e))?;
            log::debug!("Received handshake packet type=0x{:02X} len={}", header.packet_type, header.length);

            if header.packet_type == TDS7_PRELOGIN {
                let prelogin = parse_prelogin(&data).map_err(|e| e.to_string())?;
                log::debug!("PRELOGIN: version={:?}, encryption={}", prelogin.version, prelogin.encryption);

                // When TLS is disabled, respond based on client's request:
                // - If client requested ENCRYPT_ON/REQUIRED, respond with ENCRYPT_NOT_SUP
                // - If client requested ENCRYPT_OFF, respond with ENCRYPT_OFF
                // This maintains compatibility with various TDS clients
                let server_encrypt = if self.config.tls_enabled {
                    ENCRYPT_ON
                } else if prelogin.encryption == ENCRYPT_ON || prelogin.encryption == ENCRYPT_REQUIRED {
                    ENCRYPT_NOT_SUP
                } else {
                    ENCRYPT_OFF
                };

                needs_tls_upgrade = self.config.tls_enabled
                    && (prelogin.encryption == ENCRYPT_ON || prelogin.encryption == ENCRYPT_REQUIRED);

                let response = build_prelogin_response(server_encrypt);
                packet::write_packet(&mut stream, TDS7_PRELOGIN, &response)
                    .await
                    .map_err(|e| format!("Failed to write PRELOGIN response: {}", e))?;
                stream.flush().await.map_err(|e| format!("Failed to flush: {}", e))?;
                log::debug!("Sent PRELOGIN response (encryption={})", server_encrypt);

                if needs_tls_upgrade {
                    break;
                }
            } else if header.packet_type == TDS7_LOGIN {
                login_packet = Some((header, data));
                break;
            } else {
                return Err(format!("Expected PRELOGIN or LOGIN7, got 0x{:02X}", header.packet_type));
            }
        }

        self.handle_login(stream, login_packet, needs_tls_upgrade).await
    }

    async fn handle_login(mut self, stream: TcpStream, login_packet: Option<(packet::PacketHeader, Vec<u8>)>, needs_tls_upgrade: bool) -> Result<(), String> {

        // Perform TLS upgrade if needed
        let stream: Box<dyn AsyncReadWrite> = if needs_tls_upgrade {
            log::info!("Client requested TLS, upgrading connection");
            
            let tls_config = if let (Some(cert_path), Some(key_path)) = 
                (&self.config.tls_cert_path, &self.config.tls_key_path) 
            {
                tls::load_tls_config(cert_path, key_path)
                    .map_err(|e| format!("Failed to load TLS config: {}", e))?
            } else {
                return Err("TLS enabled but no certificate configured".to_string());
            };

            let acceptor = tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(tls_config));
            
            let tls_stream = acceptor.accept(stream).await
                .map_err(|e| format!("TLS handshake failed: {}", e))?;
            
            log::info!("TLS handshake completed");
            Box::new(tls_stream)
        } else {
            Box::new(stream)
        };

        // Now use the stream for the rest
        let (mut reader, mut writer) = tokio::io::split(stream);

        let login_data = if let Some((_, data)) = login_packet {
            data
        } else {
            let (header, data) = packet::read_packet(&mut reader)
                .await
                .map_err(|e| format!("Failed to read LOGIN7: {}", e))?;
            if header.packet_type != TDS7_LOGIN {
                return Err(format!("Expected LOGIN7 (0x10), got 0x{:02X}", header.packet_type));
            }
            data
        };

        let login = parse_login7(&login_data).map_err(|e| e.to_string())?;
        log::info!(
            "LOGIN7: user={}, database={}, app={}, packet_size={}",
            login.username, login.database, login.app_name, login.packet_size
        );

        if let Some(ref creds) = self.config.auth {
            if login.username != creds.user || login.password != creds.password {
                let err_resp = build_error_response("Login failed for user.");
                packet::write_packet(&mut writer, TABULAR_RESULT, &err_resp.data)
                    .await
                    .map_err(|e| e.to_string())?;
                return Ok(());
            }
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
                let err_resp = build_error_response("session pool exhausted");
                packet::write_packet(&mut writer, TABULAR_RESULT, &err_resp.data)
                    .await
                    .map_err(|e| e.to_string())?;
                return Ok(());
            }
        };
        self.session_id = Some(session_id);

        // Build LOGINACK response
        let mut b = PacketBuilder::with_capacity(256);
        tokens::write_envchange_packet_size(&mut b, self.packet_size, self.packet_size);
        tokens::write_envchange_database(&mut b, &self.database, "master");
        tokens::write_envchange_collation(&mut b);
        tokens::write_loginack(&mut b, 0x74000004);
        tokens::write_done(&mut b, tokens::DONE_FINAL, 0, 0);

        if let Err(e) = packet::write_packet(&mut writer, TABULAR_RESULT, b.as_bytes()).await {
            self.session_pool.checkin(&self.db, session_id);
            return Err(format!("Failed to write LOGINACK: {}", e));
        }
        log::debug!("Sent LOGINACK response");

        // Main loop: handle SQL Batch and other packets
        loop {
            let result = packet::read_packet(&mut reader).await;
            match result {
                Ok((header, data)) => {
                    log::debug!("Received packet type=0x{:02X} len={}", header.packet_type, header.length);
                    match header.packet_type {
                        SQL_BATCH => {
                            if let Err(e) = self.handle_sql_batch(&data, &mut writer).await {
                                log::error!("SQL batch error: {}", e);
                                let err_resp = build_error_response(&e);
                                let _ = packet::write_packet(&mut writer, TABULAR_RESULT, &err_resp.data).await;
                                break;
                            }
                        }
                        RPC => {
                            match parse_rpc(&data) {
                                Ok(Some(rpc)) => {
                                    let preamble = build_param_preamble(&rpc.params);
                                    let full_sql = if preamble.is_empty() {
                                        rpc.sql
                                    } else {
                                        format!("{}{}", preamble, rpc.sql)
                                    };
                                    if let Err(e) = self.execute_sql(full_sql.trim(), &mut writer).await {
                                        log::error!("RPC SQL error: {}", e);
                                        let err_resp = build_error_response(&e);
                                        let _ = packet::write_packet(&mut writer, TABULAR_RESULT, &err_resp.data).await;
                                        break;
                                    }
                                }
                                Ok(None) => {
                                    let mut b = PacketBuilder::new();
                                    tokens::write_done(&mut b, tokens::DONE_FINAL, 1, 0);
                                    let _ = packet::write_packet(&mut writer, TABULAR_RESULT, b.as_bytes()).await;
                                }
                                Err(e) => {
                                    let err_resp = build_error_response(&format!("RPC parse error: {}", e));
                                    let _ = packet::write_packet(&mut writer, TABULAR_RESULT, &err_resp.data).await;
                                }
                            }
                        }
                        ATTENTION => {
                            log::debug!("ATTENTION received");
                            let mut attn = PacketBuilder::new();
                            tokens::write_done(&mut attn, tokens::DONE_ATTN, 0, 0);
                            let _ = packet::write_packet(&mut writer, TABULAR_RESULT, attn.as_bytes()).await;
                        }
                        _ => {
                            log::warn!("Unsupported packet type 0x{:02X}", header.packet_type);
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    log::debug!("Client disconnected");
                    break;
                }
                Err(e) => {
                    log::error!("Read error: {}", e);
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
    ) -> Result<bool, String> {
        let sql = match parse_sql_batch(data) {
            Ok(s) => s,
            Err(e) => {
                let err_resp = build_error_response(&format!("Failed to parse SQL batch: {}", e));
                packet::write_packet(writer, TABULAR_RESULT, &err_resp.data)
                    .await
                    .map_err(|e| e.to_string())?;
                return Ok(true);
            }
        };
        self.execute_sql(sql.trim(), writer).await
    }

    async fn execute_sql<W: AsyncWriteExt + Unpin>(
        &mut self,
        sql: &str,
        writer: &mut W,
    ) -> Result<bool, String> {

        if sql.is_empty() {
            let mut b = PacketBuilder::new();
            tokens::write_done(&mut b, tokens::DONE_FINAL, 1, 0);
            packet::write_packet(writer, TABULAR_RESULT, b.as_bytes())
                .await
                .map_err(|e| e.to_string())?;
            return Ok(true);
        }

        let session_id = self.session_id.ok_or_else(|| "session not initialized".to_string())?;

        let upper = sql.to_uppercase();
        if upper.starts_with("USE ") {
            let db_name = sql[4..].trim().trim_end_matches(';').trim().trim_matches('[').trim_matches(']');
            let old_db = self.database.clone();
            self.database = db_name.to_string();

            let mut b = PacketBuilder::new();
            tokens::write_envchange_database(&mut b, &self.database, &old_db);
            tokens::write_done(&mut b, tokens::DONE_FINAL, 1, 0);
            packet::write_packet(writer, TABULAR_RESULT, b.as_bytes())
                .await
                .map_err(|e| e.to_string())?;
            return Ok(true);
        }

        log::debug!("SQL: {}", sql);
        match StatementExecutor::execute_session_batch_sql_multi(&self.db, session_id, sql) {
            Ok(results) => {
                let count = results.len();
                let mut b = PacketBuilder::with_capacity(4096);

                for (i, result) in results.into_iter().enumerate() {
                    let is_last = i == count - 1;
                    let done_status = if is_last { tokens::DONE_FINAL } else { tokens::DONE_MORE };

                    match result {
                        Some(query_result) if !query_result.columns.is_empty() => {
                            let mut types = Vec::new();
                            log::debug!("Result set: columns={}, types={}", query_result.columns.len(), query_result.column_types.len());
                            for ct in &query_result.column_types {
                                types.push(crate::tds::type_mapping::runtime_type_to_tds(ct));
                            }
                            tokens::write_colmetadata(&mut b, &query_result.columns, &types);
                            for row in &query_result.rows {
                                tokens::write_row(&mut b, row, &types);
                            }
                            tokens::write_done(
                                &mut b,
                                done_status | tokens::DONE_COUNT,
                                1,
                                query_result.rows.len() as u64,
                            );
                        }
                        Some(query_result) => {
                            // DML/DDL with row count
                            tokens::write_done(
                                &mut b,
                                done_status | tokens::DONE_COUNT,
                                1,
                                query_result.rows.len() as u64,
                            );
                        }
                        None => {
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
                    .map_err(|e| e.to_string())?;
            }
            Err(e) => {
                let err_resp = build_error_response(&format!("{}", e));
                packet::write_packet(writer, TABULAR_RESULT, &err_resp.data)
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }

        Ok(true)
    }
}
