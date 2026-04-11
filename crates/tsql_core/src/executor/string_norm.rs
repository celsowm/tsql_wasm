/// Centralized string normalization utilities for T-SQL identifier handling.
///
/// T-SQL identifiers are case-insensitive and may include schema prefixes like "dbo.".
/// This module provides consistent normalization to avoid scattered `to_uppercase()`,
/// `eq_ignore_ascii_case()`, and manual `"DBO."` stripping across the codebase.
/// Normalizes a T-SQL identifier to uppercase for consistent comparison.
#[inline]
pub fn normalize_identifier(name: &str) -> String {
    name.to_uppercase()
}

/// Strips a known schema prefix (e.g. "dbo.") from an identifier, case-insensitively.
/// Returns the identifier without the prefix if it matches, otherwise the original.
#[inline]
pub fn strip_schema_prefix<'a>(name: &'a str, schema: &str) -> &'a str {
    let prefix_len = schema.len() + 1; // "schema."
    if name.len() > prefix_len
        && name[..schema.len()].eq_ignore_ascii_case(schema)
        && name.as_bytes()[schema.len()] == b'.'
    {
        &name[prefix_len..]
    } else {
        name
    }
}

/// Strips the default "dbo." schema prefix from an identifier if present.
#[inline]
pub fn strip_dbo_prefix(name: &str) -> &str {
    strip_schema_prefix(name, "dbo")
}
