use once_cell::sync::Lazy;
use regex::Regex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use iridium_core::SessionId;

use super::pool::SessionPool;
use crate::ServerDatabase;
use super::ServerConfig;

pub mod compat;
pub mod response;
pub mod execution;
pub mod handshake;

use crate::tds::packet;

pub(crate) trait AsyncReadWrite: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin> AsyncReadWrite for T {}

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

pub struct TdsSession {
    connection_id: u64,
    db: Arc<dyn ServerDatabase>,
    config: Arc<ServerConfig>,
    session_pool: Arc<SessionPool>,
    session_id: Option<SessionId>,
    packet_size: u16,
    database: String,
    prepared_stmts: HashMap<u32, PreparedStatement>,
}

#[derive(Debug, Clone)]
pub struct PreparedStatement {
    pub sql: String,
    pub param_decls: Vec<(String, String)>,
}

impl TdsSession {
    pub fn new(
        db: Arc<dyn ServerDatabase>,
        config: Arc<ServerConfig>,
        session_pool: Arc<SessionPool>,
    ) -> Self {
        let database = config.database.clone();
        Self {
            connection_id: NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed),
            db,
            config,
            session_pool,
            session_id: None,
            packet_size: 4096,
            database,
            prepared_stmts: HashMap::new(),
        }
    }
}

static SQL_KEYWORD_RE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"(?i)\b(SELECT|FROM|WHERE|GROUP BY|ORDER BY|HAVING|JOIN|INNER|LEFT|RIGHT|FULL|ON|AS|CAST|CONVERT|DECLARE|SET|IF|BEGIN|END|ELSE|EXEC|EXECUTE|INSERT|INTO|UPDATE|DELETE|CREATE|TABLE|VIEW|VALUES|TOP|DISTINCT|ISNULL|SERVERPROPERTY|OBJECT_ID|CROSS APPLY)\b",
    )
    .expect("valid SQL keyword regex")
});

pub(crate) fn format_sql_for_log(sql: &str) -> String {
    let text = if std::env::var("IRIDIUM_LOG_FULL_SQL")
        .or_else(|_| std::env::var("TSQL_LOG_FULL_SQL"))
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        sql.to_string()
    } else {
        truncate_for_log(sql, 4000)
    };

    if std::env::var("IRIDIUM_LOG_SQL_HIGHLIGHT")
        .or_else(|_| std::env::var("TSQL_LOG_SQL_HIGHLIGHT"))
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

pub(crate) fn log_sql_execution(connection_id: u64, sql: &str) {
    if false {
        return;
    }

    log::info!(
        "[conn={}] Executing SQL:\n{}",
        connection_id,
        format_sql_for_log(sql)
    );
}

pub(crate) fn truncate_for_log(text: &str, max_chars: usize) -> String {
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

pub(crate) fn log_packet(connection_id: u64, stage: &str, header: &packet::PacketHeader, data: &[u8]) {
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
