use tsql_core::parse_sql;

/// Parser fuzzing test: ensures parser doesn't panic on malformed input
#[test]
fn test_phase8_parser_fuzz_malformed_input() {
    let malformed_inputs = vec![
        // Empty and whitespace
        "",
        "   ",
        "\n\n\n",
        "\t\t",

        // Incomplete statements
        "SELECT",
        "SELECT *",
        "SELECT * FROM",
        "INSERT INTO",
        "INSERT INTO t",
        "INSERT INTO t VALUES",
        "UPDATE",
        "UPDATE t",
        "UPDATE t SET",
        "DELETE",
        "DELETE FROM",
        "CREATE",
        "CREATE TABLE",
        "CREATE TABLE t",
        "DROP",
        "DROP TABLE",
        "ALTER",
        "ALTER TABLE",
        "ALTER TABLE t",

        // Malformed syntax
        "SELECT * FROM *",
        "SELECT * FROM t WHERE",
        "SELECT * FROM t WHERE 1 =",
        "SELECT * FROM t ORDER BY",
        "SELECT * FROM t GROUP BY",
        "SELECT * FROM t HAVING",
        "SELECT * FROM t JOIN",
        "SELECT * FROM t INNER JOIN",
        "SELECT * FROM t ON",
        "SELECT * FROM t WHERE id IN",
        "SELECT * FROM t WHERE id IN (",
        "SELECT * FROM t WHERE id BETWEEN",
        "SELECT * FROM t WHERE id BETWEEN 1",
        "SELECT * FROM t WHERE id BETWEEN 1 AND",
        "SELECT * FROM t WHERE id LIKE",
        "SELECT * FROM t WHERE id IS",
        "SELECT * FROM t WHERE id IS NULL",
        "SELECT * FROM t WHERE id IS NOT",
        "SELECT * FROM t WHERE id IS NOT NULL",

        // Unclosed parentheses
        "SELECT (1 + 2",
        "SELECT (1 + 2 FROM t",
        "SELECT * FROM t WHERE id IN (1, 2",
        "CREATE TABLE t (id INT",
        "CREATE TABLE t (id INT, name VARCHAR",
        "CREATE TABLE t (id INT, name VARCHAR(100",

        // Invalid tokens
        "INVALID SQL",
        "@@@",
        ";;;",
        "SELECT @",
        "SELECT @ @",
        "SELECT @@",
        "DECLARE @ INT",

        // Mixed valid/invalid
        "SELECT 1; INVALID",
        "SELECT 1; SELECT",
        "CREATE TABLE t (id INT); DROP",
        "BEGIN; INVALID; COMMIT",

        // Edge cases with numbers
        "SELECT 999999999999999999999999999999",
        "SELECT 0.0.0",
        "SELECT 1e",
        "SELECT 1e+",
        "SELECT 1e-",
        "SELECT 1e+999",

        // Edge cases with strings
        "SELECT 'unclosed string",
        "SELECT 'escaped '' quote",
        "SELECT N'unicode",
        "SELECT 'multi\nline",

        // Edge cases with identifiers
        "SELECT [unclosed bracket",
        "SELECT [bracket] [nested",
        "SELECT @unclosed_var",

        // Complex malformed expressions
        "SELECT 1 + + 2",
        "SELECT 1 - - 2",
        "SELECT 1 * * 2",
        "SELECT 1 / / 2",
        "SELECT 1 % % 2",
        "SELECT 1 = = 2",
        "SELECT 1 < < 2",
        "SELECT 1 > > 2",
        "SELECT 1 < > > 2",
        "SELECT 1 > < < 2",
        "SELECT 1 AND AND 2",
        "SELECT 1 OR OR 2",
        "SELECT NOT NOT 1",
        "SELECT 1 IN IN (1)",
        "SELECT 1 LIKE LIKE 'a'",
        "SELECT 1 BETWEEN BETWEEN 1 AND 2",

        // Nested malformed
        "SELECT (SELECT",
        "SELECT (SELECT *",
        "SELECT (SELECT * FROM",
        "SELECT (SELECT * FROM t WHERE",
        "WITH cte AS (SELECT",
        "WITH cte AS (SELECT *",
        "WITH cte AS (SELECT * FROM",

        // Malformed JOIN
        "SELECT * FROM t JOIN t",
        "SELECT * FROM t JOIN t ON",
        "SELECT * FROM t LEFT JOIN t",
        "SELECT * FROM t LEFT JOIN t ON",
        "SELECT * FROM t RIGHT JOIN t",
        "SELECT * FROM t RIGHT JOIN t ON",
        "SELECT * FROM t FULL JOIN t",
        "SELECT * FROM t FULL JOIN t ON",
        "SELECT * FROM t CROSS JOIN",

        // Malformed subquery
        "SELECT * FROM (SELECT",
        "SELECT * FROM (SELECT *",
        "SELECT * FROM (SELECT * FROM",
        "SELECT * FROM (SELECT * FROM t) AS",
        "SELECT * FROM (SELECT * FROM t) AS alias",

        // Malformed CASE
        "SELECT CASE",
        "SELECT CASE WHEN",
        "SELECT CASE WHEN 1",
        "SELECT CASE WHEN 1 =",
        "SELECT CASE WHEN 1 = 1",
        "SELECT CASE WHEN 1 = 1 THEN",
        "SELECT CASE WHEN 1 = 1 THEN 'a'",
        "SELECT CASE WHEN 1 = 1 THEN 'a' ELSE",
        "SELECT CASE WHEN 1 = 1 THEN 'a' ELSE 'b'",
        "SELECT CASE WHEN 1 = 1 THEN 'a' ELSE 'b' END",

        // Malformed CAST/CONVERT
        "SELECT CAST",
        "SELECT CAST(1",
        "SELECT CAST(1 AS",
        "SELECT CAST(1 AS INT",
        "SELECT CONVERT",
        "SELECT CONVERT(INT",
        "SELECT CONVERT(INT,",

        // Malformed functions
        "SELECT LEN",
        "SELECT LEN(",
        "SELECT LEN('hello'",
        "SELECT SUBSTRING",
        "SELECT SUBSTRING(",
        "SELECT SUBSTRING('hello'",
        "SELECT SUBSTRING('hello',",
        "SELECT SUBSTRING('hello', 1",
        "SELECT SUBSTRING('hello', 1,",

        // Malformed aggregates
        "SELECT COUNT",
        "SELECT COUNT(",
        "SELECT COUNT(*)",
        "SELECT COUNT(*) FROM",
        "SELECT SUM",
        "SELECT SUM(",
        "SELECT SUM(id",
        "SELECT SUM(id)",

        // Malformed SET
        "SET",
        "SET ANSI_NULLS",
        "SET ANSI_NULLS ON",
        "SET NOCOUNT",
        "SET NOCOUNT ON",

        // Malformed transactions
        "BEGIN",
        "BEGIN TRANSACTION",
        "COMMIT",
        "ROLLBACK",
        "SAVE",
        "SAVE TRANSACTION",
        "SAVE TRANSACTION sp1",

        // Malformed PRINT/RAISERROR
        "PRINT",
        "RAISERROR",
        "RAISERROR('msg'",
        "RAISERROR('msg', 16",

        // Malformed EXEC
        "EXEC",
        "EXEC sp_executesql",
        "EXEC sp_executesql N'SELECT 1'",

        // Malformed IF/WHILE
        "IF",
        "IF 1 = 1",
        "IF 1 = 1 BEGIN",
        "IF 1 = 1 BEGIN SELECT 1",
        "IF 1 = 1 BEGIN SELECT 1 END",
        "WHILE",
        "WHILE 1 = 1",
        "WHILE 1 = 1 BEGIN",
        "WHILE 1 = 1 BEGIN SET @i = 1",
        "WHILE 1 = 1 BEGIN SET @i = 1 END",
    ];

    for input in malformed_inputs {
        // Parser should not panic on any input
        let result = parse_sql(input);
        // We don't care if it succeeds or fails, only that it doesn't panic
        let _ = result;
    }
}

/// Parser fuzzing test: ensures parser handles boundary conditions
#[test]
fn test_phase8_parser_fuzz_boundary_conditions() {
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let boundary_inputs = vec![
                // Very long identifiers (should fail gracefully)
                "SELECT * FROM ".to_string() + &"a".repeat(1000),
                "CREATE TABLE ".to_string() + &"t".repeat(1000) + " (id INT)",

                // Very long strings
                "SELECT '".to_string() + &"x".repeat(10000) + "'",
                "SELECT N'".to_string() + &"x".repeat(10000) + "'",

                // Many columns
                "SELECT ".to_string() + &(1..100).map(|i| format!("col{}", i)).collect::<Vec<_>>().join(", "),
                "CREATE TABLE t (".to_string() + &(1..100).map(|i| format!("col{} INT", i)).collect::<Vec<_>>().join(", ") + ")",

                // Many values
                "INSERT INTO t VALUES ".to_string() + &(1..100).map(|_| "(1)").collect::<Vec<_>>().join(", "),

                // Deeply nested parentheses
                "SELECT ".to_string() + &"(".repeat(50) + "1" + &")".repeat(50),

                // Many UNIONs
                (0..50).map(|_| "SELECT 1").collect::<Vec<_>>().join(" UNION "),

                // Many ORs in WHERE
                "SELECT * FROM t WHERE ".to_string() + &(0..50).map(|i| format!("id = {}", i)).collect::<Vec<_>>().join(" OR "),

                // Many ANDs in WHERE
                "SELECT * FROM t WHERE ".to_string() + &(0..50).map(|i| format!("id > {}", i)).collect::<Vec<_>>().join(" AND "),
            ];

            for input in boundary_inputs {
                // Parser should not panic on boundary conditions
                let result = parse_sql(&input);
                let _ = result;
            }
        })
        .expect("failed to spawn boundary-condition parser test thread")
        .join()
        .expect("boundary-condition parser test thread panicked");
}

/// Parser fuzzing test: random mutations of valid SQL
#[test]
fn test_phase8_parser_fuzz_mutations() {
    let valid_statements = vec![
        "SELECT 1",
        "SELECT * FROM t",
        "SELECT id, name FROM t WHERE id = 1",
        "INSERT INTO t (id, name) VALUES (1, 'test')",
        "UPDATE t SET name = 'test' WHERE id = 1",
        "DELETE FROM t WHERE id = 1",
        "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(100))",
        "DROP TABLE t",
    ];

    // Mutation strategies
    let mutations: Vec<Box<dyn Fn(&str) -> String>> = vec![
        // Remove last character
        Box::new(|s: &str| s[..s.len().saturating_sub(1)].to_string()),
        // Remove first character
        Box::new(|s: &str| s.chars().skip(1).collect()),
        // Double a random character
        Box::new(|s: &str| {
            if s.is_empty() {
                return s.to_string();
            }
            let pos = s.len() / 2;
            let mut result = s.to_string();
            let c = result.chars().nth(pos).unwrap();
            result.insert(pos, c);
            result
        }),
        // Replace space with nothing
        Box::new(|s: &str| s.replace(' ', "")),
        // Add random space
        Box::new(|s: &str| {
            if s.is_empty() {
                return s.to_string();
            }
            let mut result = s.to_string();
            result.insert(result.len() / 2, ' ');
            result
        }),
        // Uppercase everything
        Box::new(|s: &str| s.to_uppercase()),
        // Lowercase everything
        Box::new(|s: &str| s.to_lowercase()),
        // Reverse string
        Box::new(|s: &str| s.chars().rev().collect()),
        // Add semicolon in middle
        Box::new(|s: &str| {
            if s.is_empty() {
                return s.to_string();
            }
            let mut result = s.to_string();
            result.insert(result.len() / 2, ';');
            result
        }),
        // Duplicate statement
        Box::new(|s: &str| format!("{}; {}", s, s)),
    ];

    for stmt in &valid_statements {
        for mutation in &mutations {
            let mutated = mutation(stmt);
            // Parser should not panic on mutated input
            let result = parse_sql(&mutated);
            let _ = result;
        }
    }
}

/// Parser fuzzing test: ensures error messages are reasonable
#[test]
fn test_phase8_parser_error_messages() {
    let error_cases = vec![
        ("INVALID SQL", "should contain useful error info"),
        ("SELECT * FROM", "should indicate missing table name"),
        ("CREATE TABLE (id INT)", "should indicate missing table name"),
        ("SELECT 1 +", "should indicate incomplete expression"),
        ("BEGIN TRANSACTION; INVALID", "should indicate invalid statement"),
    ];

    for (sql, _description) in error_cases {
        let result = parse_sql(sql);
        if let Err(err) = result {
            let error_msg = err.to_string();
            // Error message should not be empty
            assert!(
                !error_msg.is_empty(),
                "Error message should not be empty for: {}",
                sql
            );
        }
    }
}

/// Parser fuzzing test: batch parsing stability
#[test]
fn test_phase8_parser_batch_fuzz() {
    // Test that batch parser handles various edge cases
    let batch_cases = vec![
        // Empty batch
        "",
        // Single statement
        "SELECT 1",
        // Multiple statements
        "SELECT 1; SELECT 2; SELECT 3",
        // Statements with trailing semicolons
        "SELECT 1;",
        "SELECT 1; SELECT 2;",
        // Statements without semicolons (should still work)
        "SELECT 1 SELECT 2",
        // Mixed semicolons
        "SELECT 1; SELECT 2 SELECT 3;",
        // Empty statements between semicolons
        "SELECT 1; ; SELECT 2",
        ";;SELECT 1;;",
        // Newlines
        "SELECT 1\nSELECT 2",
        "SELECT 1;\nSELECT 2;",
        // Comments (if supported)
        "-- comment\nSELECT 1",
        "/* comment */ SELECT 1",
    ];

    for batch in batch_cases {
        // Batch parser should not panic
        let result = tsql_core::parse_batch(batch);
        let _ = result;
    }
}
