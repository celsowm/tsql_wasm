use once_cell::sync::Lazy;
use regex::Regex;

pub(crate) fn parse_simple_use_database(sql: &str) -> Option<String> {
    static SIMPLE_USE_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?is)^\s*use\s+(\[[^\]]+\]|[A-Za-z0-9_]+)\s*;?\s*$")
            .expect("valid simple USE regex")
    });
    let caps = SIMPLE_USE_RE.captures(sql.trim())?;
    let raw = caps.get(1)?.as_str().trim();
    Some(raw.trim_matches('[').trim_matches(']').to_string())
}

pub(crate) fn extract_leading_use_database(sql: &str) -> Option<String> {
    static LEADING_USE_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?is)^\s*use\s+(\[[^\]]+\]|[A-Za-z0-9_]+)\s*;")
            .expect("valid leading USE regex")
    });
    let caps = LEADING_USE_RE.captures(sql.trim())?;
    let raw = caps.get(1)?.as_str().trim();
    Some(raw.trim_matches('[').trim_matches(']').to_string())
}

pub(crate) fn is_ssms_contained_auth_probe(sql: &str) -> bool {
    let normalized = normalize_whitespace_lower(sql);
    normalized.contains("if (db_id() = 1)")
        && normalized.contains("authenticating_database_id")
        && normalized.contains("sys.dm_exec_sessions")
        && normalized.contains("@@spid")
}

pub(crate) fn is_sysdac_instances_probe(sql: &str) -> bool {
    let normalized = normalize_whitespace_lower(sql);
    normalized == "select case when object_id('dbo.sysdac_instances') is not null then 1 else 0 end"
}

fn normalize_whitespace_lower(sql: &str) -> String {
    sql.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}
