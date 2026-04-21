use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use iridium_core::error::DbError;
use super::{TdsSession, AsyncReadWrite};
use crate::tds::batch::build_error_response;
use crate::tds::bulk::parse_bulk_load_data;
use crate::tds::login::parse_login7;
use crate::tds::packet::{
    self, PacketBuilder, ATTENTION, BULK_LOAD, RPC, SQL_BATCH, TABULAR_RESULT, TDS7_LOGIN,
    TDS7_PRELOGIN,
};
use crate::tds::prelogin::{
    build_prelogin_response, parse_prelogin, ENCRYPT_NOT_SUP, ENCRYPT_OFF, ENCRYPT_ON,
    ENCRYPT_REQUIRED,
};
use crate::tds::rpc::{
    parse_param_decl, parse_rpc,
    CatalogProc, RpcRequest, CursorOp
};
use crate::tds::rpc::{
    build_param_preamble, build_param_preamble_with_decls
};
use crate::tds::tokens;
use crate::tds_tls_io::TdsTlsIo;
use crate::tls;
use crate::pool::CheckoutError;
use super::PreparedStatement;

impl TdsSession {
    pub async fn handle(self, mut stream: TcpStream) -> Result<(), String> {
        let mut needs_tls_upgrade = false;
        let mut login_packet = None;

        log::info!(
            "[conn={}] Starting handshake for incoming connection",
            self.connection_id
        );
        loop {
            let result: Result<_, std::io::Error> = packet::read_message(&mut stream).await;
            let (header, data) = result.map_err(|e| format!("Handshake read error: {}", e))?;
            crate::session::log_packet(self.connection_id, "handshake", &header, &data);

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
                crate::session::log_packet(self.connection_id, "login", &header, &data);
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

    pub(crate) async fn handle_login(
        mut self,
        stream: TcpStream,
        login_packet: Option<(packet::PacketHeader, Vec<u8>)>,
        needs_tls_upgrade: bool,
    ) -> Result<(), String> {
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

            let raw_mode = Arc::new(AtomicBool::new(false));
            let tds_io = TdsTlsIo::new(stream, raw_mode.clone());

            let tls_stream = acceptor
                .accept(tds_io)
                .await
                .map_err(|e| format!("TLS handshake failed: {}", e))?;

            raw_mode.store(true, Ordering::Release);

            log::info!(
                "[conn={}] TLS handshake completed, switched to raw mode",
                self.connection_id
            );
            Box::new(tls_stream)
        } else {
            Box::new(stream)
        };

        let (mut reader, mut writer) = tokio::io::split(stream);

        let login_data = if let Some((_, data)) = login_packet {
            data
        } else {
            let result: Result<_, std::io::Error> = packet::read_message(&mut reader).await;
            let (header, data) = result.map_err(|e| format!("Failed to read LOGIN7: {}", e))?;
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
                let _ = packet::write_packet(&mut writer, TABULAR_RESULT, &err_resp.data).await;
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

        let session_id = match self.session_pool.checkout(self.db.as_ref()) {
            Ok(sid) => sid,
            Err(CheckoutError::Exhausted) => {
                let err = DbError::Execution("session pool exhausted".to_string());
                let err_resp = build_error_response(&err);
                let _ = packet::write_packet(&mut writer, TABULAR_RESULT, &err_resp.data).await;
                return Ok(());
            }
        };
        self.session_id = Some(session_id);

        if let Err(e) = self.db.set_session_metadata(
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
            self.session_pool.checkin(self.db.as_ref(), session_id);
            return Err(format!("Failed to write LOGINACK: {}", e));
        }
        log::debug!("[conn={}] Sent LOGINACK response", self.connection_id);

        loop {
            let result: Result<_, std::io::Error> = packet::read_message(&mut reader).await;
            match result {
                Ok((header, data)) => {
                    crate::session::log_packet(self.connection_id, "packet", &header, &data);

                    if header.status & packet::STATUS_RESET != 0 {
                        log::debug!("[conn={}] STATUS_RESET connection", self.connection_id);
                        self.prepared_stmts.clear();
                    }

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
                                match rpc {
                                    RpcRequest::Sql(sql_req) => {
                                        let preamble = build_param_preamble(&sql_req.params);
                                        let full_sql = if preamble.is_empty() {
                                            sql_req.sql
                                        } else {
                                            format!("{}{}", preamble, sql_req.sql)
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
                                    RpcRequest::Cursor(cursor_req) => {
                                        log::debug!(
                                            "[conn={}] Cursor RPC: {:?}",
                                            self.connection_id,
                                            cursor_req.cursor_op
                                        );
                                        if let Some(session_id) = self.session_id {
                                            match cursor_req.cursor_op {
                                                CursorOp::Open => {
                                                    let sql = cursor_req.sql.unwrap_or_default();
                                                    let scroll_opt =
                                                        cursor_req.scroll_opt.unwrap_or(0);
                                                    match self.db.cursor_rpc_open(
                                                        session_id, &sql, scroll_opt,
                                                    ) {
                                                        Ok((handle, _result)) => {
                                                            let mut buf = PacketBuilder::new();
                                                            tokens::write_output_int(
                                                                &mut buf, "@cursor", handle,
                                                            );
                                                            tokens::write_done(
                                                                &mut buf,
                                                                tokens::DONE_FINAL,
                                                                0,
                                                                0,
                                                            );
                                                            let _ = packet::write_packet(
                                                                &mut writer,
                                                                TABULAR_RESULT,
                                                                buf.as_bytes(),
                                                            )
                                                            .await;
                                                        }
                                                        Err(e) => {
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
                                                CursorOp::Fetch => {
                                                    let handle =
                                                        cursor_req.cursor_handle.unwrap_or(0);
                                                    let fetch_type =
                                                        cursor_req.fetch_type.unwrap_or(2);
                                                    let row_num = cursor_req.row_num.unwrap_or(0);
                                                    let n_rows = cursor_req.n_rows.unwrap_or(1);
                                                    match self.db.cursor_rpc_fetch(
                                                        session_id, handle, fetch_type, row_num,
                                                        n_rows,
                                                    ) {
                                                        Ok(fetch_result) => {
                                                            if fetch_result.rows.is_empty() {
                                                                let mut buf = PacketBuilder::new();
                                                                tokens::write_done(
                                                                    &mut buf,
                                                                    tokens::DONE_FINAL,
                                                                    0,
                                                                    0,
                                                                );
                                                                let _ = packet::write_packet(
                                                                    &mut writer,
                                                                    TABULAR_RESULT,
                                                                    buf.as_bytes(),
                                                                )
                                                                .await;
                                                            } else {
                                                                let mut buf = PacketBuilder::new();
                                                                let col_types: Vec<_> = fetch_result.column_types.iter()
                                                                    .map(crate::tds::type_mapping::runtime_type_to_tds)
                                                                    .collect();
                                                                tokens::write_colmetadata(
                                                                    &mut buf,
                                                                    &fetch_result.columns,
                                                                    &col_types,
                                                                );
                                                                for row in &fetch_result.rows {
                                                                    tokens::write_row(
                                                                        &mut buf, row, &col_types,
                                                                        0,
                                                                    );
                                                                }
                                                                tokens::write_done(
                                                                    &mut buf,
                                                                    tokens::DONE_FINAL
                                                                        | tokens::DONE_COUNT,
                                                                    0,
                                                                    fetch_result.rows.len() as u64,
                                                                );
                                                                let _ = packet::write_packet(
                                                                    &mut writer,
                                                                    TABULAR_RESULT,
                                                                    buf.as_bytes(),
                                                                )
                                                                .await;
                                                            }
                                                        }
                                                        Err(e) => {
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
                                                CursorOp::Close => {
                                                    let handle =
                                                        cursor_req.cursor_handle.unwrap_or(0);
                                                    match self
                                                        .db
                                                        .cursor_rpc_close(session_id, handle)
                                                    {
                                                        Ok(()) => {
                                                            let mut buf = PacketBuilder::new();
                                                            tokens::write_done(
                                                                &mut buf,
                                                                tokens::DONE_FINAL,
                                                                0,
                                                                0,
                                                            );
                                                            let _ = packet::write_packet(
                                                                &mut writer,
                                                                TABULAR_RESULT,
                                                                buf.as_bytes(),
                                                            )
                                                            .await;
                                                        }
                                                        Err(e) => {
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
                                                CursorOp::Unprepare => {
                                                    let handle =
                                                        cursor_req.cursor_handle.unwrap_or(0);
                                                    match self
                                                        .db
                                                        .cursor_rpc_deallocate(session_id, handle)
                                                    {
                                                        Ok(()) => {
                                                            let mut buf = PacketBuilder::new();
                                                            tokens::write_done(
                                                                &mut buf,
                                                                tokens::DONE_FINAL,
                                                                0,
                                                                0,
                                                            );
                                                            let _ = packet::write_packet(
                                                                &mut writer,
                                                                TABULAR_RESULT,
                                                                buf.as_bytes(),
                                                            )
                                                            .await;
                                                        }
                                                        Err(e) => {
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
                                                CursorOp::Prepare
                                                | CursorOp::PrepExec => {
                                                    let sql = match cursor_req.sql {
                                                        Some(ref s) if !s.is_empty() => s.clone(),
                                                        _ => {
                                                            let err = DbError::Execution("cursor prepare requires SQL statement".into());
                                                            let err_resp =
                                                                build_error_response(&err);
                                                            let _ = packet::write_packet(
                                                                &mut writer,
                                                                TABULAR_RESULT,
                                                                &err_resp.data,
                                                            )
                                                            .await;
                                                            continue;
                                                        }
                                                    };
                                                    let scroll_opt =
                                                        cursor_req.scroll_opt.unwrap_or(0);
                                                    match self.db.cursor_rpc_open(
                                                        session_id, &sql, scroll_opt,
                                                    ) {
                                                        Ok((handle, _result)) => {
                                                            let mut buf = PacketBuilder::new();
                                                            tokens::write_output_int(
                                                                &mut buf, "@cursor", handle,
                                                            );
                                                            tokens::write_done(
                                                                &mut buf,
                                                                tokens::DONE_FINAL,
                                                                0,
                                                                0,
                                                            );
                                                            let _ = packet::write_packet(
                                                                &mut writer,
                                                                TABULAR_RESULT,
                                                                buf.as_bytes(),
                                                            )
                                                            .await;
                                                        }
                                                        Err(e) => {
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
                                                CursorOp::Execute => {
                                                    let handle =
                                                        cursor_req.cursor_handle.unwrap_or(0);
                                                    let scroll_opt =
                                                        cursor_req.scroll_opt.unwrap_or(0);
                                                    let n_rows = cursor_req.n_rows.unwrap_or(1);
                                                    match self.db.cursor_rpc_fetch(
                                                        session_id, handle, 2, 0, n_rows,
                                                    ) {
                                                        Ok(fetch_result) => {
                                                            let _ = scroll_opt;
                                                            if fetch_result.rows.is_empty() {
                                                                let mut buf = PacketBuilder::new();
                                                                tokens::write_output_int(
                                                                    &mut buf, "@cursor", handle,
                                                                );
                                                                tokens::write_done(
                                                                    &mut buf,
                                                                    tokens::DONE_FINAL,
                                                                    0,
                                                                    0,
                                                                );
                                                                let _ = packet::write_packet(
                                                                    &mut writer,
                                                                    TABULAR_RESULT,
                                                                    buf.as_bytes(),
                                                                )
                                                                .await;
                                                            } else {
                                                                let mut buf = PacketBuilder::new();
                                                                let col_types: Vec<_> = fetch_result.column_types.iter()
                                                                    .map(crate::tds::type_mapping::runtime_type_to_tds)
                                                                    .collect();
                                                                tokens::write_output_int(
                                                                    &mut buf, "@cursor", handle,
                                                                );
                                                                tokens::write_colmetadata(
                                                                    &mut buf,
                                                                    &fetch_result.columns,
                                                                    &col_types,
                                                                );
                                                                for row in &fetch_result.rows {
                                                                    tokens::write_row(
                                                                        &mut buf, row, &col_types,
                                                                        0,
                                                                    );
                                                                }
                                                                tokens::write_done(
                                                                    &mut buf,
                                                                    tokens::DONE_FINAL
                                                                        | tokens::DONE_COUNT,
                                                                    0,
                                                                    fetch_result.rows.len() as u64,
                                                                );
                                                                let _ = packet::write_packet(
                                                                    &mut writer,
                                                                    TABULAR_RESULT,
                                                                    buf.as_bytes(),
                                                                )
                                                                .await;
                                                            }
                                                        }
                                                        Err(e) => {
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
                                                CursorOp::Option => {
                                                    let _ = cursor_req;
                                                    let mut buf = PacketBuilder::new();
                                                    tokens::write_done(
                                                        &mut buf,
                                                        tokens::DONE_FINAL,
                                                        0,
                                                        0,
                                                    );
                                                    let _ = packet::write_packet(
                                                        &mut writer,
                                                        TABULAR_RESULT,
                                                        buf.as_bytes(),
                                                    )
                                                    .await;
                                                }
                                            }
                                        } else {
                                            let err = DbError::Execution(
                                                "no session for cursor operation".into(),
                                            );
                                            let err_resp = build_error_response(&err);
                                            let _ = packet::write_packet(
                                                &mut writer,
                                                TABULAR_RESULT,
                                                &err_resp.data,
                                            )
                                            .await;
                                        }
                                    }
                                    RpcRequest::Prepare(req) => {
                                        let handle = self.prepared_stmts.len() as u32 + 1;
                                        let param_decls = parse_param_decl(&req.param_decl);
                                        let stmt = PreparedStatement {
                                            sql: req.sql.clone(),
                                            param_decls: param_decls.clone(),
                                        };
                                        self.prepared_stmts.insert(handle, stmt);
                                        let mut buf = PacketBuilder::new();
                                        tokens::write_output_int(
                                            &mut buf,
                                            "@handle",
                                            handle as i32,
                                        );
                                        tokens::write_done(&mut buf, tokens::DONE_FINAL, 0, 0);
                                        let _ = packet::write_packet(
                                            &mut writer,
                                            TABULAR_RESULT,
                                            buf.as_bytes(),
                                        )
                                        .await;
                                    }
                                    RpcRequest::Execute(req) => {
                                        let handle = req.stmt_handle as u32;
                                        if let Some(stmt) = self.prepared_stmts.get(&handle) {
                                            let param_decls = &stmt.param_decls;
                                            let preamble = build_param_preamble_with_decls(
                                                &req.params,
                                                param_decls,
                                            );
                                            let full_sql = if preamble.is_empty() {
                                                stmt.sql.clone()
                                            } else {
                                                format!("{}{}", preamble, stmt.sql)
                                            };
                                            match self
                                                .execute_sql(full_sql.trim(), &mut writer)
                                                .await
                                            {
                                                Ok(_) => {}
                                                Err(e) => {
                                                    log::error!("sp_execute error: {}", e);
                                                    let err_resp = build_error_response(&e);
                                                    let _ = packet::write_packet(
                                                        &mut writer,
                                                        TABULAR_RESULT,
                                                        &err_resp.data,
                                                    )
                                                    .await;
                                                }
                                            }
                                        } else {
                                            let err = DbError::Execution(format!(
                                                "Invalid statement handle: {}",
                                                req.stmt_handle
                                            ));
                                            let err_resp = build_error_response(&err);
                                            let _ = packet::write_packet(
                                                &mut writer,
                                                TABULAR_RESULT,
                                                &err_resp.data,
                                            )
                                            .await;
                                        }
                                    }
                                    RpcRequest::Unprepare(req) => {
                                        let handle = req.stmt_handle as u32;
                                        self.prepared_stmts.remove(&handle);
                                        let mut buf = PacketBuilder::new();
                                        tokens::write_done(&mut buf, tokens::DONE_FINAL, 0, 0);
                                        let _ = packet::write_packet(
                                            &mut writer,
                                            TABULAR_RESULT,
                                            buf.as_bytes(),
                                        )
                                        .await;
                                    }
                                    RpcRequest::PrepExec(req) => {
                                        let handle = self.prepared_stmts.len() as u32 + 1;
                                        let param_decls = parse_param_decl(&req.param_decl);
                                        let stmt = PreparedStatement {
                                            sql: req.sql.clone(),
                                            param_decls: param_decls.clone(),
                                        };
                                        self.prepared_stmts.insert(handle, stmt);
                                        let preamble = build_param_preamble_with_decls(
                                            &req.params,
                                            &param_decls,
                                        );
                                        let full_sql = if preamble.is_empty() {
                                            req.sql.clone()
                                        } else {
                                            format!("{}{}", preamble, req.sql)
                                        };
                                        match self.execute_sql(full_sql.trim(), &mut writer).await {
                                            Ok(_) => {}
                                            Err(e) => {
                                                log::error!("sp_prepexec error: {}", e);
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
                                    RpcRequest::ResetConnection => {
                                        self.prepared_stmts.clear();
                                        let mut buf = PacketBuilder::new();
                                        tokens::write_done(&mut buf, tokens::DONE_FINAL, 0, 0);
                                        let _ = packet::write_packet(
                                            &mut writer,
                                            TABULAR_RESULT,
                                            buf.as_bytes(),
                                        )
                                        .await;
                                    }
                                    RpcRequest::Catalog(cat_req) => {
                                        let sql = match cat_req.proc {
                                            CatalogProc::Tables => {
                                                let table_name = cat_req
                                                    .params.first()
                                                    .map(|p| p.value_sql.trim_matches('\''))
                                                    .unwrap_or("%");
                                                let table_owner = cat_req
                                                    .params
                                                    .get(1)
                                                    .map(|p| p.value_sql.trim_matches('\''))
                                                    .unwrap_or("%");
                                                format!(
                                                    "SELECT DB_NAME() AS TABLE_QUALIFIER, s.name AS TABLE_OWNER, t.name AS TABLE_NAME, 'TABLE' AS TABLE_TYPE, NULL AS REMARKS FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.id WHERE t.name LIKE '{}' AND s.name LIKE '{}'",
                                                    table_name, table_owner
                                                )
                                            }
                                            CatalogProc::Columns => {
                                                let table_name = cat_req
                                                    .params.first()
                                                    .map(|p| p.value_sql.trim_matches('\''))
                                                    .unwrap_or("%");
                                                format!(
                                                    "SELECT s.name AS TABLE_OWNER, t.name AS TABLE_NAME, c.name AS COLUMN_NAME, c.column_id AS ORDINAL_POSITION, ty.name AS TYPE_NAME FROM sys.columns c JOIN sys.tables t ON c.object_id = t.object_id JOIN sys.schemas s ON t.schema_id = s.id JOIN sys.types ty ON c.user_type_id = ty.user_type_id WHERE t.name LIKE '{}' ORDER BY t.name, c.column_id",
                                                    table_name
                                                )
                                            }
                                            CatalogProc::SprocColumns => {
                                                let proc_name = cat_req
                                                    .params.first()
                                                    .map(|p| p.value_sql.trim_matches('\''))
                                                    .unwrap_or("%");
                                                format!(
                                                    "SELECT s.name AS PROCEDURE_OWNER, r.name AS PROCEDURE_NAME, p.name AS COLUMN_NAME, p.parameter_id AS ORDINAL_POSITION, ty.name AS TYPE_NAME FROM sys.parameters p JOIN sys.routines r ON p.object_id = r.object_id JOIN sys.schemas s ON r.schema_id = s.id JOIN sys.types ty ON p.user_type_id = ty.user_type_id WHERE r.name LIKE '{}' ORDER BY r.name, p.parameter_id",
                                                    proc_name
                                                )
                                            }
                                            CatalogProc::PrimaryKeys => {
                                                let table_name = cat_req
                                                    .params.first()
                                                    .map(|p| p.value_sql.trim_matches('\''))
                                                    .unwrap_or("%");
                                                format!(
                                                    "SELECT s.name AS TABLE_OWNER, t.name AS TABLE_NAME, c.name AS COLUMN_NAME, c.column_id AS KEY_SEQ, pk.name AS PK_NAME FROM sys.columns c JOIN sys.tables t ON c.object_id = t.object_id JOIN sys.schemas s ON t.schema_id = s.id JOIN (SELECT ic.object_id, ic.column_id, i.name FROM sys.index_columns ic JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id WHERE i.is_primary_key = 1) pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id WHERE t.name LIKE '{}' ORDER BY t.name, c.column_id",
                                                    table_name
                                                )
                                            }
                                            CatalogProc::DescribeCursor => {
                                                let cursor_handle = cat_req
                                                    .params
                                                    .get(2)
                                                    .and_then(|p| p.value_sql.parse::<i32>().ok())
                                                    .unwrap_or(0);
                                                if let Some(session_id) = self.session_id {
                                                    match self.db.cursor_rpc_fetch(
                                                        session_id,
                                                        cursor_handle,
                                                        2,
                                                        0,
                                                        1,
                                                    ) {
                                                        Ok(fetch_result) => {
                                                            let mut buf = PacketBuilder::new();
                                                            let col_names = vec![
                                                                "reference_name".to_string(),
                                                                "cursor_name".to_string(),
                                                                "cursor_scope".to_string(),
                                                                "status".to_string(),
                                                                "model".to_string(),
                                                                "concurrency".to_string(),
                                                                "scrollable".to_string(),
                                                                "open_status".to_string(),
                                                                "cursor_rows".to_string(),
                                                                "fetch_status".to_string(),
                                                                "column_count".to_string(),
                                                                "row_count".to_string(),
                                                                "last_operation".to_string(),
                                                                "cursor_handle".to_string(),
                                                            ];
                                                            let col_types: Vec<_> = col_names.iter()
                                                                .map(|_| crate::tds::type_mapping::runtime_type_to_tds(&iridium_core::types::DataType::Int))
                                                                .collect();
                                                            let cursor_name = format!(
                                                                "#rpc_cursor_{}",
                                                                cursor_handle
                                                            );
                                                            let row = vec![
                                                                iridium_core::types::Value::NVarChar(cursor_name.clone()),
                                                                iridium_core::types::Value::NVarChar(cursor_name),
                                                                iridium_core::types::Value::Int(1),
                                                                iridium_core::types::Value::Int(0),
                                                                iridium_core::types::Value::Int(0),
                                                                iridium_core::types::Value::Int(1),
                                                                iridium_core::types::Value::Int(1),
                                                                iridium_core::types::Value::Int(if fetch_result.rows.is_empty() { 0 } else { 1 }),
                                                                iridium_core::types::Value::Int(fetch_result.rows.len() as i32),
                                                                iridium_core::types::Value::Int(fetch_result.fetch_status),
                                                                iridium_core::types::Value::Int(fetch_result.columns.len() as i32),
                                                                iridium_core::types::Value::Int(fetch_result.rows.len() as i32),
                                                                iridium_core::types::Value::Int(0),
                                                                iridium_core::types::Value::Int(cursor_handle),
                                                            ];
                                                            tokens::write_colmetadata(
                                                                &mut buf, &col_names, &col_types,
                                                            );
                                                            tokens::write_row(
                                                                &mut buf, &row, &col_types, 0,
                                                            );
                                                            tokens::write_done(
                                                                &mut buf,
                                                                tokens::DONE_FINAL
                                                                    | tokens::DONE_COUNT,
                                                                0,
                                                                1,
                                                            );
                                                            let _ = packet::write_packet(
                                                                &mut writer,
                                                                TABULAR_RESULT,
                                                                buf.as_bytes(),
                                                            )
                                                            .await;
                                                        }
                                                        Err(e) => {
                                                            let err_resp = build_error_response(&e);
                                                            let _ = packet::write_packet(
                                                                &mut writer,
                                                                TABULAR_RESULT,
                                                                &err_resp.data,
                                                            )
                                                            .await;
                                                        }
                                                    }
                                                } else {
                                                    let err = DbError::Execution(
                                                        "no session for cursor operation".into(),
                                                    );
                                                    let err_resp = build_error_response(&err);
                                                    let _ = packet::write_packet(
                                                        &mut writer,
                                                        TABULAR_RESULT,
                                                        &err_resp.data,
                                                    )
                                                    .await;
                                                }
                                                String::new()
                                            }
                                        };
                                        if !sql.is_empty() {
                                            match self.execute_sql(&sql, &mut writer).await {
                                                Ok(_) => {}
                                                Err(e) => {
                                                    log::error!("Catalog RPC error: {}", e);
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
                                let err = DbError::Parse("unsupported RPC request".into());
                                let err_resp = build_error_response(&err);
                                let _ = packet::write_packet(
                                    &mut writer,
                                    TABULAR_RESULT,
                                    &err_resp.data,
                                )
                                .await;
                            }
                            Err(e) => {
                                let err = iridium_core::error::DbError::Parse(e.to_string());
                                let err_resp = build_error_response(&err);
                                let _ = packet::write_packet(
                                    &mut writer,
                                    TABULAR_RESULT,
                                    &err_resp.data,
                                )
                                .await;
                            }
                        },
                        BULK_LOAD => {
                            log::info!("[conn={}] BULK_LOAD received", self.connection_id);
                            if let Some(session_id) = self.session_id {
                                let (active, target, columns, received_metadata) = self
                                    .db
                                    .session_options(session_id)
                                    .map(|_opts| self.db.get_bulk_load_state(session_id))
                                    .unwrap_or((false, None, None, false));

                                if active && target.is_some() && columns.is_some() {
                                    let target = target.unwrap();
                                    let columns = columns.unwrap();

                                    let mut reader = packet::PacketReader::new(&data);
                                    let mut column_types = Vec::new();

                                    if !received_metadata {
                                        let token = reader.read_u8().map_err(|e| e.to_string())?;
                                        if token != tokens::COLMETADATA_TOKEN {
                                            return Err(format!(
                                                "Expected COLMETADATA (0x81), got 0x{:02X}",
                                                token
                                            ));
                                        }
                                        let count =
                                            reader.read_u16_le().map_err(|e| e.to_string())?
                                                as usize;
                                        for _ in 0..count {
                                            reader.skip(4).map_err(|e| e.to_string())?;
                                            let _flags =
                                                reader.read_u16_le().map_err(|e| e.to_string())?;
                                            let ti = crate::tds::type_mapping::read_type_info(
                                                &mut reader,
                                            )
                                            .map_err(|e| e.to_string())?;
                                            column_types.push(ti);
                                            let name_len =
                                                reader.read_u8().map_err(|e| e.to_string())?
                                                    as usize;
                                            let _name = reader
                                                .read_utf16le(name_len)
                                                .map_err(|e| e.to_string())?;
                                        }
                                        self.db
                                            .set_bulk_load_active(
                                                session_id,
                                                true,
                                                target.clone(),
                                                columns.clone(),
                                                true,
                                            )
                                            .map_err(|e| e.to_string())?;
                                    }

                                    match parse_bulk_load_data(&data, &columns) {
                                        Ok(bulk_data) => {
                                            log::info!(
                                                "[conn={}] Parsed {} bulk rows for table {}",
                                                self.connection_id,
                                                bulk_data.rows.len(),
                                                target.name
                                            );

                                            let mut sql = String::new();
                                            let col_names: Vec<String> =
                                                columns.iter().map(|c| c.name.clone()).collect();
                                            let col_list = col_names.join(", ");

                                            for row in bulk_data.rows {
                                                let vals: Vec<String> = row
                                                    .iter()
                                                    .map(|v| v.to_sql_literal())
                                                    .collect();
                                                sql.push_str(&format!(
                                                    "INSERT INTO {}.{} ({}) VALUES ({});\n",
                                                    target.schema_or_dbo(),
                                                    target.name,
                                                    col_list,
                                                    vals.join(", ")
                                                ));
                                            }

                                            self.db
                                                .set_bulk_load_active(
                                                    session_id,
                                                    false,
                                                    target.clone(),
                                                    columns.clone(),
                                                    false,
                                                )
                                                .map_err(|e| e.to_string())?;

                                            match self.execute_sql(&sql, &mut writer).await {
                                                Ok(_) => {}
                                                Err(e) => {
                                                    log::error!(
                                                        "Bulk insert execution error: {}",
                                                        e
                                                    );
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
                                        Err(e) => {
                                            log::error!("Bulk data parse error: {}", e);
                                            let err = DbError::Parse(e.to_string());
                                            let err_resp = build_error_response(&err);
                                            let _ = packet::write_packet(
                                                &mut writer,
                                                TABULAR_RESULT,
                                                &err_resp.data,
                                            )
                                            .await;
                                        }
                                    }
                                } else {
                                    log::warn!("[conn={}] Received BULK_LOAD but bulk load was not expected", self.connection_id);
                                }
                            }
                        }
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
            self.session_pool.checkin(self.db.as_ref(), sid);
        }

        Ok(())
    }
}
