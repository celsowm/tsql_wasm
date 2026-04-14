use crate::error::DbError;
use crate::types::Value;

pub fn edit_distance(s1: &str, s2: &str) -> i32 {
    let len1 = s1.chars().count();
    let len2 = s2.chars().count();

    if len1 == 0 {
        return len2 as i32;
    }
    if len2 == 0 {
        return len1 as i32;
    }

    let chars1: Vec<char> = s1.chars().collect();
    let chars2: Vec<char> = s2.chars().collect();

    let mut prev_row: Vec<usize> = (0..=len2).collect();
    let mut curr_row: Vec<usize> = vec![0; len2 + 1];

    for i in 1..=len1 {
        curr_row[0] = i;
        for j in 1..=len2 {
            let cost = if chars1[i - 1] == chars2[j - 1] { 0 } else { 1 };
            curr_row[j] = (prev_row[j] + 1)
                .min(curr_row[j - 1] + 1)
                .min(prev_row[j - 1] + cost);
        }
        std::mem::swap(&mut prev_row, &mut curr_row);
    }

    prev_row[len2] as i32
}

pub fn eval_edit_distance(
    args: &[crate::ast::Expr],
    row: &[crate::executor::model::ContextTable],
    ctx: &mut crate::executor::context::ExecutionContext,
    catalog: &dyn crate::catalog::Catalog,
    storage: &dyn crate::storage::Storage,
    clock: &dyn crate::executor::clock::Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution(
            "EDIT_DISTANCE expects 2 arguments".into(),
        ));
    }
    let s1 = crate::executor::evaluator::eval_expr(&args[0], row, ctx, catalog, storage, clock)?
        .to_string_value();
    let s2 = crate::executor::evaluator::eval_expr(&args[1], row, ctx, catalog, storage, clock)?
        .to_string_value();
    Ok(Value::Int(edit_distance(&s1, &s2)))
}

pub fn eval_edit_distance_similarity(
    args: &[crate::ast::Expr],
    row: &[crate::executor::model::ContextTable],
    ctx: &mut crate::executor::context::ExecutionContext,
    catalog: &dyn crate::catalog::Catalog,
    storage: &dyn crate::storage::Storage,
    clock: &dyn crate::executor::clock::Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution(
            "EDIT_DISTANCE_SIMILARITY expects 2 arguments".into(),
        ));
    }
    let s1 = crate::executor::evaluator::eval_expr(&args[0], row, ctx, catalog, storage, clock)?
        .to_string_value();
    let s2 = crate::executor::evaluator::eval_expr(&args[1], row, ctx, catalog, storage, clock)?
        .to_string_value();
    Ok(decimal_from_unit_interval(edit_distance_similarity(
        &s1, &s2,
    )))
}

pub fn edit_distance_similarity(s1: &str, s2: &str) -> f64 {
    let len1 = s1.chars().count();
    let len2 = s2.chars().count();

    if len1 == 0 && len2 == 0 {
        return 1.0;
    }

    let max_len = len1.max(len2);
    let dist = edit_distance(s1, s2) as usize;

    1.0 - (dist as f64 / max_len as f64)
}

pub fn jaro_winkler_distance(s1: &str, s2: &str) -> f64 {
    if s1 == s2 {
        return 0.0;
    }

    let jaro = jaro_similarity(s1, s2);
    let prefix_len = common_prefix_len(s1, s2).min(4);
    let winkler = jaro + (prefix_len as f64 * 0.1 * (1.0 - jaro));

    1.0 - winkler.min(1.0)
}

pub fn eval_jaro_winkler_distance(
    args: &[crate::ast::Expr],
    row: &[crate::executor::model::ContextTable],
    ctx: &mut crate::executor::context::ExecutionContext,
    catalog: &dyn crate::catalog::Catalog,
    storage: &dyn crate::storage::Storage,
    clock: &dyn crate::executor::clock::Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution(
            "JARO_WINKLER_DISTANCE expects 2 arguments".into(),
        ));
    }
    let s1 = crate::executor::evaluator::eval_expr(&args[0], row, ctx, catalog, storage, clock)?
        .to_string_value();
    let s2 = crate::executor::evaluator::eval_expr(&args[1], row, ctx, catalog, storage, clock)?
        .to_string_value();
    Ok(decimal_from_unit_interval(jaro_winkler_distance(&s1, &s2)))
}

pub fn eval_jaro_winkler_similarity(
    args: &[crate::ast::Expr],
    row: &[crate::executor::model::ContextTable],
    ctx: &mut crate::executor::context::ExecutionContext,
    catalog: &dyn crate::catalog::Catalog,
    storage: &dyn crate::storage::Storage,
    clock: &dyn crate::executor::clock::Clock,
) -> Result<Value, DbError> {
    if args.len() != 2 {
        return Err(DbError::Execution(
            "JARO_WINKLER_SIMILARITY expects 2 arguments".into(),
        ));
    }
    let s1 = crate::executor::evaluator::eval_expr(&args[0], row, ctx, catalog, storage, clock)?
        .to_string_value();
    let s2 = crate::executor::evaluator::eval_expr(&args[1], row, ctx, catalog, storage, clock)?
        .to_string_value();
    Ok(decimal_from_unit_interval(jaro_winkler_similarity(
        &s1, &s2,
    )))
}

fn decimal_from_unit_interval(value: f64) -> Value {
    Value::Decimal((value * 1_000_000_000.0).round() as i128, 9)
}

pub fn jaro_winkler_similarity(s1: &str, s2: &str) -> f64 {
    if s1 == s2 {
        return 1.0;
    }

    let jaro = jaro_similarity(s1, s2);
    let prefix_len = common_prefix_len(s1, s2).min(4);
    let winkler = jaro + (prefix_len as f64 * 0.1 * (1.0 - jaro));

    winkler.min(1.0)
}

fn jaro_similarity(s1: &str, s2: &str) -> f64 {
    let len1 = s1.chars().count();
    let len2 = s2.chars().count();

    if len1 == 0 && len2 == 0 {
        return 1.0;
    }
    if len1 == 0 || len2 == 0 {
        return 0.0;
    }

    let match_distance = (len1.max(len2) / 2).saturating_sub(1);
    let chars1: Vec<char> = s1.chars().collect();
    let chars2: Vec<char> = s2.chars().collect();

    let mut matches1 = vec![false; len1];
    let mut matches2 = vec![false; len2];
    let mut matches = 0;
    let mut transpositions = 0;

    for i in 0..len1 {
        let start = i.saturating_sub(match_distance);
        let end = (i + match_distance + 1).min(len2);

        for j in start..end {
            if matches2[j] || chars1[i] != chars2[j] {
                continue;
            }
            matches1[i] = true;
            matches2[j] = true;
            matches += 1;
            break;
        }
    }

    if matches == 0 {
        return 0.0;
    }

    let mut k = 0;
    for i in 0..len1 {
        if !matches1[i] {
            continue;
        }
        while !matches2[k] {
            k += 1;
        }
        if chars1[i] != chars2[k] {
            transpositions += 1;
        }
        k += 1;
    }

    let m = matches as f64;
    ((m / len1 as f64) + (m / len2 as f64) + ((m - transpositions as f64 / 2.0) / m)) / 3.0
}

fn common_prefix_len(s1: &str, s2: &str) -> usize {
    s1.chars()
        .zip(s2.chars())
        .take_while(|(a, b)| a == b)
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edit_distance() {
        assert_eq!(edit_distance("", ""), 0);
        assert_eq!(edit_distance("abc", "abc"), 0);
        assert_eq!(edit_distance("abc", ""), 3);
        assert_eq!(edit_distance("", "abc"), 3);
        assert_eq!(edit_distance("kitten", "sitting"), 3);
    }

    #[test]
    fn test_edit_distance_similarity() {
        assert!((edit_distance_similarity("abc", "abc") - 1.0).abs() < 0.001);
        assert!(edit_distance_similarity("abc", "xyz") < 0.5);
    }

    #[test]
    fn test_jaro_winkler_similarity() {
        assert!((jaro_winkler_similarity("abc", "abc") - 1.0).abs() < 0.001);
        assert!(jaro_winkler_similarity("MARTHA", "MARHTA") > 0.9);
    }

    #[test]
    fn test_jaro_winkler_distance() {
        assert!((jaro_winkler_distance("abc", "abc") - 0.0).abs() < 0.001);
        assert!(jaro_winkler_distance("MARTHA", "MARHTA") < 0.1);
    }
}
