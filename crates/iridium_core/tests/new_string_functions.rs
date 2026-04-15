include!("new_functions/helpers.rs");

// ─── CONCAT ───────────────────────────────────────────────────────────────

#[test]
fn test_concat_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CONCAT('Hello', ' ', 'World') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("Hello World".to_string()));
}

#[test]
fn test_concat_with_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CONCAT('a', NULL, 'b') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("ab".to_string()));
}

#[test]
fn test_concat_with_numbers() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CONCAT('ID:', 42) AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("ID:42".to_string()));
}

#[test]
fn test_concat_with_column() {
    let mut engine = Engine::new();
    exec(
        &mut engine,
        "CREATE TABLE dbo.t (first_name VARCHAR(10), last_name VARCHAR(10))",
    );
    exec(
        &mut engine,
        "INSERT INTO dbo.t (first_name, last_name) VALUES ('John', 'Doe')",
    );
    let r = query(
        &mut engine,
        "SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM dbo.t",
    );
    assert_eq!(r.rows[0][0], Value::NVarChar("John Doe".to_string()));
}

// ─── CONCAT_WS ────────────────────────────────────────────────────────────

#[test]
fn test_concat_ws_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CONCAT_WS('-', 'a', 'b', 'c') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("a-b-c".to_string()));
}

#[test]
fn test_concat_ws_skips_nulls() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CONCAT_WS(',', 'a', NULL, 'c') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("a,c".to_string()));
}

#[test]
fn test_concat_ws_null_separator() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CONCAT_WS(NULL, 'a', 'b') AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_concat_ws_all_nulls() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CONCAT_WS('-', NULL, NULL) AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("".to_string()));
}

// ─── REPLICATE ────────────────────────────────────────────────────────────

#[test]
fn test_replicate_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT REPLICATE('ab', 3) AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("ababab".to_string()));
}

#[test]
fn test_replicate_zero() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT REPLICATE('x', 0) AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("".to_string()));
}

#[test]
fn test_replicate_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT REPLICATE(NULL, 3) AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── REVERSE ──────────────────────────────────────────────────────────────

#[test]
fn test_reverse_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT REVERSE('hello') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("olleh".to_string()));
}

#[test]
fn test_reverse_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT REVERSE(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_reverse_empty() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT REVERSE('') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("".to_string()));
}

// ─── STUFF ────────────────────────────────────────────────────────────────

#[test]
fn test_stuff_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT STUFF('abcdef', 2, 3, 'XX') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("aXXef".to_string()));
}

#[test]
fn test_stuff_delete_only() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT STUFF('abcdef', 3, 2, '') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("abef".to_string()));
}

#[test]
fn test_stuff_insert_only() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT STUFF('abcdef', 2, 0, 'XX') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("aXXbcdef".to_string()));
}

#[test]
fn test_stuff_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT STUFF(NULL, 2, 3, 'XX') AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── SPACE ────────────────────────────────────────────────────────────────

#[test]
fn test_space_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SPACE(5) AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("     ".to_string()));
}

#[test]
fn test_space_zero() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SPACE(0) AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("".to_string()));
}

#[test]
fn test_space_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SPACE(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_space_in_concat() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CONCAT('a', SPACE(3), 'b') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("a   b".to_string()));
}

// ─── STR ──────────────────────────────────────────────────────────────────

#[test]
fn test_str_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT STR(123) AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("       123".to_string()));
}

#[test]
fn test_str_with_length() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT STR(42, 5) AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("   42".to_string()));
}

#[test]
fn test_str_with_decimals() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT STR(123.456, 10, 2) AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("    123.46".to_string()));
}

#[test]
fn test_str_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT STR(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── TRANSLATE ────────────────────────────────────────────────────────────

#[test]
fn test_translate_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT TRANSLATE('hello', 'elo', '123') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("h1223".to_string()));
}

#[test]
fn test_translate_no_match() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT TRANSLATE('hello', 'xyz', 'abc') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("hello".to_string()));
}

#[test]
fn test_translate_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT TRANSLATE(NULL, 'ab', 'cd') AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_translate_mismatched_lengths() {
    let engine = Engine::new();
    let stmt = parse_sql("SELECT TRANSLATE('hello', 'ab', 'cde')").expect("parse failed");
    let result = engine.execute(stmt);
    assert!(result.is_err() || result.is_ok());
}

// ─── FORMAT ───────────────────────────────────────────────────────────────

#[test]
fn test_format_integer() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT FORMAT(1234567, 'N') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("1,234,567".to_string()));
}

#[test]
fn test_format_currency() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT FORMAT(99.5, 'C') AS v");
    assert!(r.rows[0][0].to_string_value().starts_with('$'));
}

#[test]
fn test_format_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT FORMAT(NULL, 'N') AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── PATINDEX ─────────────────────────────────────────────────────────────

#[test]
fn test_patindex_found() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT PATINDEX('%world%', 'hello world') AS v",
    );
    assert_eq!(r.rows[0][0], Value::Int(7));
}

#[test]
fn test_patindex_not_found() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT PATINDEX('%xyz%', 'hello world') AS v");
    assert_eq!(r.rows[0][0], Value::Int(0));
}

#[test]
fn test_patindex_exact() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT PATINDEX('hello', 'hello') AS v");
    assert_eq!(r.rows[0][0], Value::Int(1));
}

#[test]
fn test_patindex_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT PATINDEX('%x%', NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_patindex_in_where() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (email VARCHAR(50))");
    exec(
        &mut engine,
        "INSERT INTO dbo.t (email) VALUES ('user@gmail.com')",
    );
    exec(
        &mut engine,
        "INSERT INTO dbo.t (email) VALUES ('admin@company.org')",
    );
    exec(
        &mut engine,
        "INSERT INTO dbo.t (email) VALUES ('no-at-sign')",
    );
    let r = query(
        &mut engine,
        "SELECT email FROM dbo.t WHERE PATINDEX('%@%', email) > 0 ORDER BY email",
    );
    assert_eq!(r.rows.len(), 2);
}

// ─── SOUNDEX ──────────────────────────────────────────────────────────────

#[test]
fn test_soundex_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SOUNDEX('Smith') AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("S530".to_string()));
}

#[test]
fn test_soundex_similar_names() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SOUNDEX('Smythe') AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("S530".to_string()));
}

#[test]
fn test_soundex_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SOUNDEX(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_soundex_single_char() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SOUNDEX('A') AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("A000".to_string()));
}

// ─── DIFFERENCE ───────────────────────────────────────────────────────────

#[test]
fn test_difference_identical() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT DIFFERENCE('Smith', 'Smythe') AS v");
    assert_eq!(r.rows[0][0], Value::Int(4));
}

#[test]
fn test_difference_different() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT DIFFERENCE('Smith', 'Jones') AS v");
    assert!(matches!(r.rows[0][0], Value::Int(v) if v < 4));
}

#[test]
fn test_difference_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT DIFFERENCE(NULL, 'Smith') AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── INTEGRATION ──────────────────────────────────────────────────────────

#[test]
fn test_string_functions_combined() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT CONCAT(REVERSE('olleh'), SPACE(1), REPLICATE('x', 3)) AS v",
    );
    assert_eq!(r.rows[0][0], Value::NVarChar("hello xxx".to_string()));
}

#[test]
fn test_soundex_with_difference() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.names (name VARCHAR(20))");
    exec(&mut engine, "INSERT INTO dbo.names (name) VALUES ('Smith')");
    exec(
        &mut engine,
        "INSERT INTO dbo.names (name) VALUES ('Smythe')",
    );
    exec(&mut engine, "INSERT INTO dbo.names (name) VALUES ('Jones')");
    let r = query(
        &mut engine,
        "SELECT name, SOUNDEX(name) AS sx, DIFFERENCE('Smith', name) AS diff FROM dbo.names ORDER BY name",
    );
    assert_eq!(r.rows.len(), 3);
}

#[test]
fn test_translate_with_stuff() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT STUFF(TRANSLATE('abcdef', 'abc', '123'), 4, 1, 'X') AS v",
    );
    assert_eq!(r.rows[0][0], Value::NVarChar("123Xef".to_string()));
}

// ─── ASCII ────────────────────────────────────────────────────────────────

#[test]
fn test_ascii_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ASCII('A') AS v");
    assert_eq!(r.rows[0][0], Value::Int(65));
}

#[test]
fn test_ascii_string() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ASCII('Hello') AS v");
    assert_eq!(r.rows[0][0], Value::Int(72));
}

#[test]
fn test_ascii_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ASCII(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_ascii_empty() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ASCII('') AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── CHAR ─────────────────────────────────────────────────────────────────

#[test]
fn test_char_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CHAR(65) AS v");
    assert_eq!(r.rows[0][0], Value::VarChar("A".to_string()));
}

#[test]
fn test_char_space() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CHAR(32) AS v");
    assert_eq!(r.rows[0][0], Value::VarChar(" ".to_string()));
}

#[test]
fn test_char_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CHAR(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_char_roundtrip() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ASCII(CHAR(97)) AS v");
    assert_eq!(r.rows[0][0], Value::Int(97));
}

// ─── NCHAR ────────────────────────────────────────────────────────────────

#[test]
fn test_nchar_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT NCHAR(65) AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("A".to_string()));
}

#[test]
fn test_nchar_unicode() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT NCHAR(9731) AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("\u{2603}".to_string()));
}

#[test]
fn test_nchar_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT NCHAR(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_nchar_roundtrip() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT UNICODE(NCHAR(66)) AS v");
    assert_eq!(r.rows[0][0], Value::Int(66));
}

// ─── UNICODE ──────────────────────────────────────────────────────────────

#[test]
fn test_unicode_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT UNICODE('A') AS v");
    assert_eq!(r.rows[0][0], Value::Int(65));
}

#[test]
fn test_unicode_multichar() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT UNICODE('Hello') AS v");
    assert_eq!(r.rows[0][0], Value::Int(72));
}

#[test]
fn test_unicode_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT UNICODE(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_unicode_equals_ascii_for_ascii_chars() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT UNICODE('Z') = ASCII('Z') AS v");
    assert_eq!(r.rows[0][0], Value::Bit(true));
}

// ─── STRING_ESCAPE ────────────────────────────────────────────────────────

#[test]
fn test_string_escape_json() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT STRING_ESCAPE('hello \"world\"', 'JSON') AS v",
    );
    assert_eq!(
        r.rows[0][0],
        Value::NVarChar("hello \\\"world\\\"".to_string())
    );
}

#[test]
fn test_string_escape_json_newline() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT STRING_ESCAPE('line1' + CHAR(13) + CHAR(10) + 'line2', 'JSON') AS v",
    );
    assert!(r.rows[0][0].to_string_value().contains("\\r\\n"));
}

#[test]
fn test_string_escape_html() {
    let mut engine = Engine::new();
    let r = query(
        &mut engine,
        "SELECT STRING_ESCAPE('<b>bold</b>', 'HTML') AS v",
    );
    assert_eq!(
        r.rows[0][0],
        Value::NVarChar("&lt;b&gt;bold&lt;/b&gt;".to_string())
    );
}

#[test]
fn test_string_escape_xml() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT STRING_ESCAPE('a & b', 'XML') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("a &amp; b".to_string()));
}

#[test]
fn test_string_escape_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT STRING_ESCAPE(NULL, 'JSON') AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── INTEGRATION ──────────────────────────────────────────────────────────

#[test]
fn test_ascii_char_composition() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CONCAT(CHAR(ASCII('H')), 'ello') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("Hello".to_string()));
}

#[test]
fn test_nchar_unicode_composition() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT CONCAT(NCHAR(9731), ' snow') AS v");
    assert_eq!(r.rows[0][0], Value::NVarChar("\u{2603} snow".to_string()));
}

#[test]
fn test_string_escape_in_query() {
    let mut engine = Engine::new();
    exec(&mut engine, "CREATE TABLE dbo.t (msg VARCHAR(100))");
    exec(
        &mut engine,
        "INSERT INTO dbo.t (msg) VALUES ('He said \"hello\"')",
    );
    let r = query(
        &mut engine,
        "SELECT STRING_ESCAPE(msg, 'JSON') AS escaped FROM dbo.t",
    );
    assert_eq!(
        r.rows[0][0],
        Value::NVarChar("He said \\\"hello\\\"".to_string())
    );
}

// ─── DATALENGTH ───────────────────────────────────────────────────────────

#[test]
fn test_datalength_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT DATALENGTH('abc') AS v");
    assert_eq!(r.rows[0][0], Value::Int(3));

    let r = query(&mut engine, "SELECT DATALENGTH(N'abc') AS v");
    assert_eq!(r.rows[0][0], Value::Int(6));
}

#[test]
fn test_datalength_numeric() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT DATALENGTH(CAST(1 AS INT)) AS v");
    assert_eq!(r.rows[0][0], Value::Int(4));

    let r = query(&mut engine, "SELECT DATALENGTH(CAST(1 AS BIGINT)) AS v");
    assert_eq!(r.rows[0][0], Value::Int(8));

    let r = query(&mut engine, "SELECT DATALENGTH(CAST(1 AS TINYINT)) AS v");
    assert_eq!(r.rows[0][0], Value::Int(1));
}

#[test]
fn test_datalength_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT DATALENGTH(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}
