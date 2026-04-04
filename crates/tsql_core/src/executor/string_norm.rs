/// Centralized string normalization utilities for T-SQL identifier handling.
///
/// T-SQL identifiers are case-insensitive and may include schema prefixes like "dbo.".
/// This module provides consistent normalization to avoid scattered `to_uppercase()`,
/// `eq_ignore_ascii_case()`, and manual `"DBO."` stripping across the codebase.

/// Normalizes a T-SQL identifier to uppercase for consistent comparison.
#[inline]
#[allow(dead_code)]
pub fn normalize_identifier(name: &str) -> String {
    name.to_uppercase()
}

/// Returns true if two identifiers are equal, ignoring ASCII case.
#[inline]
#[allow(dead_code)]
pub fn eq_ignoring_case(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

/// Strips a known schema prefix (e.g. "dbo.") from an identifier, case-insensitively.
/// Returns the identifier without the prefix if it matches, otherwise the original.
#[inline]
pub fn strip_schema_prefix<'a>(name: &'a str, schema: &str) -> &'a str {
    let prefix_len = schema.len() + 1; // "schema."
    if name.len() > prefix_len && name[..prefix_len].eq_ignore_ascii_case(schema) && name.as_bytes()[schema.len()] == b'.' {
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

/// Normalizes an identifier: strips the dbo prefix and uppercases the result.
#[inline]
#[allow(dead_code)]
pub fn normalize_name(name: &str) -> String {
    strip_dbo_prefix(name).to_uppercase()
}
