use std::collections::HashSet;
use std::path::PathBuf;

use serde::Deserialize;

use tsql_core::types::Value;
use tsql_core::{Database, StatementExecutor};
use tsql_server::playground;

#[derive(Debug, Deserialize)]
struct ContractSuite {
    cases: Vec<ContractCase>,
}

#[derive(Debug, Deserialize)]
struct ContractCase {
    name: String,
    scope: String,
    sql: String,
    must_succeed: bool,
    result_set_count: usize,
    result_sets: Vec<ResultSetContract>,
}

#[derive(Debug, Deserialize)]
struct ResultSetContract {
    required_columns: Vec<String>,
    min_rows: Option<usize>,
}

fn load_contract_suite() -> ContractSuite {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("ssms_object_explorer_cases.json");
    let contents = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read fixture at {}: {}", path.display(), e));
    serde_json::from_str(&contents)
        .unwrap_or_else(|e| panic!("failed to parse fixture at {}: {}", path.display(), e))
}

fn assert_case_contract(case: &ContractCase, result_sets: &[tsql_core::QueryResult]) {
    assert_eq!(
        result_sets.len(),
        case.result_set_count,
        "case '{}' returned {} result sets, expected {}",
        case.name,
        result_sets.len(),
        case.result_set_count
    );

    for (set_idx, expected_set) in case.result_sets.iter().enumerate() {
        let result = result_sets
            .get(set_idx)
            .unwrap_or_else(|| panic!("case '{}' missing result set {}", case.name, set_idx));

        if let Some(min_rows) = expected_set.min_rows {
            assert!(
                result.rows.len() >= min_rows,
                "case '{}' result set {} expected at least {} rows, got {}",
                case.name,
                set_idx,
                min_rows,
                result.rows.len()
            );
        }

        let actual_columns: HashSet<String> = result
            .columns
            .iter()
            .map(|c| c.to_ascii_lowercase())
            .collect();
        for required in &expected_set.required_columns {
            assert!(
                actual_columns.contains(&required.to_ascii_lowercase()),
                "case '{}' result set {} missing column '{}' (actual: {:?})",
                case.name,
                set_idx,
                required,
                actual_columns
            );
        }
    }
}

fn value_to_lower_text(value: &Value) -> Option<String> {
    match value {
        Value::VarChar(v) | Value::NVarChar(v) | Value::Char(v) => Some(v.to_ascii_lowercase()),
        _ => None,
    }
}

fn assert_seeded_table_presence(case: &ContractCase, result_sets: &[tsql_core::QueryResult]) {
    if case.name != "tables_list_from_sys_tables" {
        return;
    }

    let result = result_sets
        .first()
        .unwrap_or_else(|| panic!("case '{}' returned no result sets", case.name));
    let table_name_idx = result
        .columns
        .iter()
        .position(|c| c.eq_ignore_ascii_case("table_name") || c.eq_ignore_ascii_case("name"))
        .unwrap_or_else(|| panic!("case '{}' missing table name column", case.name));

    let table_names: HashSet<String> = result
        .rows
        .iter()
        .filter_map(|row| row.get(table_name_idx))
        .filter_map(value_to_lower_text)
        .collect();

    let expected = [
        "customers",
        "products",
        "orders",
        "orderitems",
        "employees",
        "categories",
    ];
    for table in expected {
        assert!(
            table_names.contains(table),
            "case '{}' did not return seeded table '{}'",
            case.name,
            table
        );
    }
}

fn run_scope(scope: &str) {
    let suite = load_contract_suite();
    let cases: Vec<&ContractCase> = suite.cases.iter().filter(|c| c.scope == scope).collect();
    assert!(
        !cases.is_empty(),
        "no fixture cases found for scope '{}'",
        scope
    );

    let db = Database::new();
    playground::seed_playground(&db).expect("failed to seed playground");
    let session_id = db.create_session();

    for case in cases {
        let execution = db
            .executor()
            .execute_session_batch_sql_multi(session_id, case.sql.as_str());

        if case.must_succeed {
            assert!(
                execution.is_ok(),
                "case '{}' failed to execute: {:?}",
                case.name,
                execution.err()
            );
        } else {
            continue;
        }

        let result_sets: Vec<tsql_core::QueryResult> = execution
            .expect("execution result unexpectedly missing")
            .into_iter()
            .flatten()
            .filter(|result| !result.columns.is_empty())
            .collect();

        assert_case_contract(case, &result_sets);
        assert_seeded_table_presence(case, &result_sets);
    }

    db.close_session(session_id)
        .expect("failed to close session after replay");
}

#[test]
fn ssms_object_explorer_bootstrap_contract() {
    run_scope("bootstrap");
}

#[test]
fn ssms_object_explorer_tables_contract() {
    run_scope("tables");
}

#[test]
fn ssms_object_explorer_routines_contract() {
    run_scope("routines");
}

#[test]
fn ssms_object_explorer_database_properties_contract() {
    run_scope("database_properties");
}

#[test]
fn ssms_object_explorer_server_properties_contract() {
    run_scope("server_properties");
}

#[test]
fn ssms_object_explorer_view_column_usage_contract() {
    run_scope("view_column_usage");
}

#[test]
fn ssms_object_explorer_indexes_contract() {
    run_scope("indexes");
}

#[test]
fn ssms_object_explorer_foreign_keys_contract() {
    run_scope("foreign_keys");
}

#[test]
fn ssms_object_explorer_constraints_contract() {
    run_scope("constraints");
}

#[test]
fn ssms_object_explorer_triggers_contract() {
    run_scope("triggers");
}

#[test]
fn ssms_object_explorer_schemas_contract() {
    run_scope("schemas");
}

#[test]
fn ssms_object_explorer_views_contract() {
    run_scope("views");
}

#[test]
fn ssms_object_explorer_routine_parameters_contract() {
    run_scope("routine_parameters");
}

#[test]
fn ssms_object_explorer_routine_definition_contract() {
    run_scope("routine_definition");
}

#[test]
fn ssms_object_explorer_partitions_contract() {
    run_scope("partitions");
}

#[test]
fn ssms_object_explorer_stats_contract() {
    run_scope("stats");
}

#[test]
fn ssms_object_explorer_extended_properties_contract() {
    run_scope("extended_properties");
}

#[test]
fn ssms_object_explorer_database_principals_contract() {
    run_scope("database_principals");
}

#[test]
fn ssms_object_explorer_database_permissions_contract() {
    run_scope("database_permissions");
}

#[test]
fn ssms_object_explorer_database_role_members_contract() {
    run_scope("database_role_members");
}

#[test]
fn ssms_object_explorer_table_types_contract() {
    run_scope("table_types");
}

#[test]
fn ssms_object_explorer_identity_columns_contract() {
    run_scope("identity_columns");
}

#[test]
fn ssms_object_explorer_computed_columns_contract() {
    run_scope("computed_columns");
}

#[test]
fn ssms_object_explorer_all_objects_contract() {
    run_scope("all_objects");
}

#[test]
fn ssms_object_explorer_metadata_dependencies_contract() {
    run_scope("metadata_dependencies");
}
