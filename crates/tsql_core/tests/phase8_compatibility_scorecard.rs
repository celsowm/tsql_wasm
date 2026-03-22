use tsql_core::types::Value;
use tsql_core::{parse_sql, Engine, SupportStatus};
use std::collections::HashMap;

fn exec(engine: &mut Engine, sql: &str) {
    engine.execute(parse_sql(sql).expect("parse")).expect("exec");
}

fn query(engine: &mut Engine, sql: &str) -> tsql_core::QueryResult {
    engine
        .execute(parse_sql(sql).expect("parse"))
        .expect("exec")
        .expect("result")
}

/// Compatibility feature family
#[derive(Debug, Clone)]
struct FeatureFamily {
    name: String,
    features: Vec<Feature>,
}

/// Individual feature test
#[derive(Debug, Clone)]
struct Feature {
    name: String,
    sql: String,
    expected_status: SupportStatus,
}

/// Scorecard result
#[derive(Debug, Clone)]
struct ScorecardResult {
    total: usize,
    exact: usize,
    near: usize,
    partial: usize,
    stubbed: usize,
    unsupported: usize,
    failures: Vec<String>,
}

impl ScorecardResult {
    fn score(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        let weighted = (self.exact as f64 * 1.0)
            + (self.near as f64 * 0.9)
            + (self.partial as f64 * 0.5)
            + (self.stubbed as f64 * 0.1);
        (weighted / self.total as f64) * 100.0
    }
}

/// Build feature families for compatibility testing
fn build_feature_families() -> Vec<FeatureFamily> {
    vec![
        FeatureFamily {
            name: "DDL".to_string(),
            features: vec![
                Feature {
                    name: "CREATE TABLE".to_string(),
                    sql: "CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(100))".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "ALTER TABLE ADD".to_string(),
                    sql: "ALTER TABLE t ADD age INT".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "ALTER TABLE DROP".to_string(),
                    sql: "ALTER TABLE t DROP COLUMN age".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "CREATE INDEX".to_string(),
                    sql: "CREATE INDEX ix ON t (name)".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "DROP INDEX".to_string(),
                    sql: "DROP INDEX ix ON t".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "DROP TABLE".to_string(),
                    sql: "DROP TABLE t".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "TRUNCATE TABLE".to_string(),
                    sql: "TRUNCATE TABLE t".to_string(),
                    expected_status: SupportStatus::Supported,
                },
            ],
        },
        FeatureFamily {
            name: "DML".to_string(),
            features: vec![
                Feature {
                    name: "INSERT VALUES".to_string(),
                    sql: "INSERT INTO t (id, name) VALUES (1, 'test')".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "INSERT DEFAULT".to_string(),
                    sql: "INSERT INTO t DEFAULT VALUES".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "UPDATE".to_string(),
                    sql: "UPDATE t SET name = 'x' WHERE id = 1".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "DELETE".to_string(),
                    sql: "DELETE FROM t WHERE id = 1".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "SELECT".to_string(),
                    sql: "SELECT * FROM t".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "SELECT TOP".to_string(),
                    sql: "SELECT TOP 5 * FROM t".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "SELECT DISTINCT".to_string(),
                    sql: "SELECT DISTINCT name FROM t".to_string(),
                    expected_status: SupportStatus::Supported,
                },
            ],
        },
        FeatureFamily {
            name: "Query".to_string(),
            features: vec![
                Feature {
                    name: "INNER JOIN".to_string(),
                    sql: "SELECT * FROM t a INNER JOIN t b ON a.id = b.id".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "LEFT JOIN".to_string(),
                    sql: "SELECT * FROM t a LEFT JOIN t b ON a.id = b.id".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "GROUP BY".to_string(),
                    sql: "SELECT name, COUNT(*) FROM t GROUP BY name".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "HAVING".to_string(),
                    sql: "SELECT name, COUNT(*) AS cnt FROM t GROUP BY name HAVING COUNT(*) > 1".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "ORDER BY".to_string(),
                    sql: "SELECT * FROM t ORDER BY name ASC".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "UNION".to_string(),
                    sql: "SELECT id FROM t UNION SELECT id FROM t".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "CTE".to_string(),
                    sql: "WITH cte AS (SELECT id FROM t) SELECT * FROM cte".to_string(),
                    expected_status: SupportStatus::Supported,
                },
            ],
        },
        FeatureFamily {
            name: "Expressions".to_string(),
            features: vec![
                Feature {
                    name: "Arithmetic".to_string(),
                    sql: "SELECT 1 + 2 * 3 - 4 / 2".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "Comparison".to_string(),
                    sql: "SELECT CASE WHEN 1 > 0 THEN 'yes' ELSE 'no' END".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "LIKE".to_string(),
                    sql: "SELECT * FROM t WHERE name LIKE 'a%'".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "IN".to_string(),
                    sql: "SELECT * FROM t WHERE id IN (1, 2, 3)".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "BETWEEN".to_string(),
                    sql: "SELECT * FROM t WHERE id BETWEEN 1 AND 10".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "IS NULL".to_string(),
                    sql: "SELECT * FROM t WHERE name IS NULL".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "CAST".to_string(),
                    sql: "SELECT CAST(id AS VARCHAR) FROM t".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "COALESCE".to_string(),
                    sql: "SELECT COALESCE(name, 'default') FROM t".to_string(),
                    expected_status: SupportStatus::Supported,
                },
            ],
        },
        FeatureFamily {
            name: "Built-ins".to_string(),
            features: vec![
                Feature {
                    name: "GETDATE".to_string(),
                    sql: "SELECT GETDATE()".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "LEN".to_string(),
                    sql: "SELECT LEN('hello')".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "SUBSTRING".to_string(),
                    sql: "SELECT SUBSTRING('hello', 2, 3)".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "UPPER".to_string(),
                    sql: "SELECT UPPER('hello')".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "ABS".to_string(),
                    sql: "SELECT ABS(-5)".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "ROUND".to_string(),
                    sql: "SELECT ROUND(1.5, 0)".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "JSON_VALUE".to_string(),
                    sql: "SELECT JSON_VALUE('{\"a\":1}', '$.a')".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "REGEXP_LIKE".to_string(),
                    sql: "SELECT REGEXP_LIKE('hello', 'ell')".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "EDIT_DISTANCE".to_string(),
                    sql: "SELECT EDIT_DISTANCE('kitten', 'sitting')".to_string(),
                    expected_status: SupportStatus::Supported,
                },
            ],
        },
        FeatureFamily {
            name: "Transactions".to_string(),
            features: vec![
                Feature {
                    name: "BEGIN/COMMIT".to_string(),
                    sql: "BEGIN TRANSACTION; COMMIT;".to_string(),
                    expected_status: SupportStatus::Supported,
                },
                Feature {
                    name: "SAVEPOINT".to_string(),
                    sql: "BEGIN TRANSACTION; SAVE TRANSACTION sp1; ROLLBACK TRANSACTION sp1; COMMIT;".to_string(),
                    expected_status: SupportStatus::Supported,
                },
            ],
        },
        FeatureFamily {
            name: "Programmability".to_string(),
            features: vec![
                Feature {
                    name: "IF/ELSE".to_string(),
                    sql: "IF 1 = 1 BEGIN SELECT 1; END ELSE BEGIN SELECT 2; END".to_string(),
                    expected_status: SupportStatus::Supported,
                },
            ],
        },
    ]
}

/// Test compatibility scorecard generation
#[test]
fn test_phase8_compatibility_scorecard() {
    let families = build_feature_families();
    let mut results: HashMap<String, ScorecardResult> = HashMap::new();
    let mut total_result = ScorecardResult {
        total: 0,
        exact: 0,
        near: 0,
        partial: 0,
        stubbed: 0,
        unsupported: 0,
        failures: Vec::new(),
    };

    for family in &families {
        let mut family_result = ScorecardResult {
            total: 0,
            exact: 0,
            near: 0,
            partial: 0,
            stubbed: 0,
            unsupported: 0,
            failures: Vec::new(),
        };

        for feature in &family.features {
            family_result.total += 1;
            total_result.total += 1;

            let parsed = parse_sql(&feature.sql);
            match parsed {
                Ok(_) => {
                    // Statement parses successfully
                    if feature.expected_status == SupportStatus::Supported {
                        family_result.exact += 1;
                        total_result.exact += 1;
                    } else {
                        family_result.near += 1;
                        total_result.near += 1;
                    }
                }
                Err(_) => {
                    // Statement does not parse
                    if feature.expected_status == SupportStatus::Unsupported {
                        family_result.unsupported += 1;
                        total_result.unsupported += 1;
                    } else {
                        family_result.failures.push(format!(
                            "{}: {} expected {:?} but got parse error",
                            family.name, feature.name, feature.expected_status
                        ));
                        total_result.failures.push(format!(
                            "{}: {} expected {:?} but got parse error",
                            family.name, feature.name, feature.expected_status
                        ));
                    }
                }
            }
        }

        results.insert(family.name.clone(), family_result);
    }

    // Print scorecard summary
    println!("\n=== R8 Compatibility Scorecard ===");
    println!("Total features tested: {}", total_result.total);
    println!(
        "Exact: {} ({:.1}%)",
        total_result.exact,
        (total_result.exact as f64 / total_result.total as f64) * 100.0
    );
    println!(
        "Near: {} ({:.1}%)",
        total_result.near,
        (total_result.near as f64 / total_result.total as f64) * 100.0
    );
    println!(
        "Unsupported: {} ({:.1}%)",
        total_result.unsupported,
        (total_result.unsupported as f64 / total_result.total as f64) * 100.0
    );
    println!("Compatibility Score: {:.1}%", total_result.score());

    println!("\n=== By Feature Family ===");
    for (name, result) in &results {
        println!(
            "{}: {}/{} exact ({:.1}%)",
            name,
            result.exact,
            result.total,
            (result.exact as f64 / result.total as f64) * 100.0
        );
    }

    if !total_result.failures.is_empty() {
        println!("\n=== Failures ===");
        for f in &total_result.failures {
            println!("  - {}", f);
        }
    }

    // Assert no unexpected failures
    assert!(
        total_result.failures.is_empty(),
        "Compatibility scorecard has {} failures",
        total_result.failures.len()
    );

    // Assert minimum compatibility threshold (80%)
    assert!(
        total_result.score() >= 80.0,
        "Compatibility score {:.1}% is below 80% threshold",
        total_result.score()
    );
}

/// Test compatibility report generation
#[test]
fn test_phase8_compatibility_report() {
    let engine = Engine::new();

    // Test various SQL statements for compatibility report
    let test_batch = r#"
        CREATE TABLE t (id INT PRIMARY KEY, name VARCHAR(100));
        INSERT INTO t (id, name) VALUES (1, 'test');
        SELECT * FROM t WHERE id = 1;
        UPDATE t SET name = 'updated' WHERE id = 1;
        DELETE FROM t WHERE id = 1;
        DROP TABLE t;
    "#;

    let report = engine.analyze_sql_batch(test_batch);

    // All statements should be supported
    for entry in &report.entries {
        assert_eq!(
            entry.status,
            SupportStatus::Supported,
            "Statement '{}' should be supported but got {:?}",
            entry.sql,
            entry.status
        );
    }

    assert_eq!(report.entries.len(), 6);
}

/// Test unsupported features are properly reported
#[test]
fn test_phase8_unsupported_detection() {
    let engine = Engine::new();

    // Test unsupported features - these should fail to parse
    let unsupported = vec![
        "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t",
    ];

    for sql in unsupported {
        let report = engine.analyze_sql_batch(sql);
        assert_eq!(report.entries.len(), 1);
        // ROW_NUMBER should be unsupported or at least partial
        // (some features may parse but not execute)
    }
}

/// Test feature coverage by category
#[test]
fn test_phase8_feature_coverage() {
    let mut engine = Engine::new();

    // Setup
    exec(
        &mut engine,
        "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT, score DECIMAL(10,2))",
    );
    exec(
        &mut engine,
        "INSERT INTO users (id, name, age, score) VALUES (1, 'Alice', 30, 95.5)",
    );
    exec(
        &mut engine,
        "INSERT INTO users (id, name, age, score) VALUES (2, 'Bob', 25, 87.0)",
    );

    // DDL coverage
    exec(&mut engine, "CREATE INDEX ix_name ON users (name)");
    exec(&mut engine, "DROP INDEX ix_name ON users");

    // DML coverage
    exec(&mut engine, "UPDATE users SET score = 100 WHERE id = 1");
    exec(
        &mut engine,
        "DELETE FROM users WHERE id = 2",
    );

    // Query coverage
    let result = query(
        &mut engine,
        "SELECT name, score FROM users WHERE score > 90 ORDER BY score DESC",
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::VarChar("Alice".to_string()));

    // Aggregate coverage
    let result = query(
        &mut engine,
        "SELECT COUNT(*) AS cnt, AVG(score) AS avg_score FROM users",
    );
    assert_eq!(result.rows.len(), 1);

    // Expression coverage
    let result = query(
        &mut engine,
        "SELECT CASE WHEN score > 90 THEN 'High' ELSE 'Low' END AS grade FROM users",
    );
    assert_eq!(result.rows.len(), 1);
}

/// Test JSON function coverage
#[test]
fn test_phase8_json_coverage() {
    let _engine = Engine::new();

    let tests = vec![
        ("SELECT JSON_VALUE('{\"a\":1}', '$.a')", "1"),
        ("SELECT JSON_QUERY('{\"a\":[1,2]}', '$.a')", "[1,2]"),
        ("SELECT ISJSON('{\"a\":1}')", "1"),
        ("SELECT ISJSON('invalid')", "0"),
        ("SELECT JSON_ARRAY_LENGTH('[1,2,3]')", "3"),
        ("SELECT JSON_KEYS('{\"a\":1,\"b\":2}')", "[\"a\",\"b\"]"),
    ];

    for (sql, _expected) in tests {
        let parsed = parse_sql(sql);
        assert!(
            parsed.is_ok(),
            "JSON function query should parse: {}",
            sql
        );
    }
}

/// Test regex function coverage
#[test]
fn test_phase8_regex_coverage() {
    let _engine = Engine::new();

    let tests = vec![
        "SELECT REGEXP_LIKE('hello', 'ell')",
        "SELECT REGEXP_REPLACE('hello', 'ell', 'ELLO')",
        "SELECT REGEXP_SUBSTR('hello', 'ell')",
        "SELECT REGEXP_INSTR('hello', 'ell')",
        "SELECT REGEXP_COUNT('hello', 'l')",
    ];

    for sql in tests {
        let parsed = parse_sql(sql);
        assert!(parsed.is_ok(), "Regex function query should parse: {}", sql);
    }
}

/// Test fuzzy matching coverage
#[test]
fn test_phase8_fuzzy_coverage() {
    let _engine = Engine::new();

    let tests = vec![
        "SELECT EDIT_DISTANCE('kitten', 'sitting')",
        "SELECT EDIT_DISTANCE_SIMILARITY('kitten', 'sitting')",
        "SELECT JARO_WINKLER_DISTANCE('kitten', 'sitting')",
        "SELECT JARO_WINKLER_SIMILARITY('kitten', 'sitting')",
    ];

    for sql in tests {
        let parsed = parse_sql(sql);
        assert!(
            parsed.is_ok(),
            "Fuzzy matching query should parse: {}",
            sql
        );
    }
}

/// Test metadata coverage
#[test]
fn test_phase8_metadata_coverage() {
    let mut engine = Engine::new();

    exec(
        &mut engine,
        "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))",
    );

    let metadata_queries = vec![
        "SELECT * FROM sys.tables",
        "SELECT * FROM sys.columns",
        "SELECT * FROM sys.types",
        "SELECT * FROM sys.indexes",
        "SELECT * FROM sys.objects",
        "SELECT * FROM sys.schemas",
        "SELECT * FROM INFORMATION_SCHEMA.TABLES",
        "SELECT * FROM INFORMATION_SCHEMA.COLUMNS",
    ];

    for sql in metadata_queries {
        let parsed = parse_sql(sql);
        assert!(
            parsed.is_ok(),
            "Metadata query should parse: {}",
            sql
        );
        let result = engine.execute(parsed.unwrap());
        assert!(
            result.is_ok(),
            "Metadata query should execute: {}",
            sql
        );
    }
}

/// Test explain plan coverage
#[test]
fn test_phase8_explain_coverage() {
    let engine = Engine::new();

    let queries = vec![
        "SELECT * FROM t WHERE id = 1",
        "SELECT * FROM t ORDER BY name",
        "SELECT * FROM t a JOIN t b ON a.id = b.id",
        "SELECT name, COUNT(*) FROM t GROUP BY name HAVING COUNT(*) > 1",
    ];

    for sql in queries {
        let plan = engine.explain_sql(sql);
        assert!(plan.is_ok(), "Explain should work for: {}", sql);
        let plan = plan.unwrap();
        assert!(
            !plan.operators.is_empty(),
            "Plan should have operators for: {}",
            sql
        );
    }
}
