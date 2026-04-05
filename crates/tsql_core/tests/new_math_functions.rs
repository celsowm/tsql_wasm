include!("new_functions/helpers.rs");

use std::f64::consts::PI;

fn to_f64(val: &Value) -> f64 {
    match val {
        Value::Float(bits) => f64::from_bits(*bits),
        Value::Decimal(raw, scale) => {
            let divisor = 10f64.powi(*scale as i32);
            *raw as f64 / divisor
        }
        Value::Int(v) => *v as f64,
        Value::BigInt(v) => *v as f64,
        _ => panic!("unexpected type: {:?}", val),
    }
}

// ─── ACOS ─────────────────────────────────────────────────────────────────

#[test]
fn test_acos_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ACOS(1) AS v");
    assert!((to_f64(&r.rows[0][0]) - 0.0).abs() < 1e-10);
}

#[test]
fn test_acos_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ACOS(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_acos_half() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ACOS(0.5) AS v");
    assert!((to_f64(&r.rows[0][0]) - (PI / 3.0)).abs() < 1e-10);
}

// ─── ASIN ─────────────────────────────────────────────────────────────────

#[test]
fn test_asin_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ASIN(0) AS v");
    assert!((to_f64(&r.rows[0][0]) - 0.0).abs() < 1e-10);
}

#[test]
fn test_asin_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ASIN(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

#[test]
fn test_asin_half() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ASIN(0.5) AS v");
    assert!((to_f64(&r.rows[0][0]) - (PI / 6.0)).abs() < 1e-10);
}

// ─── ATAN ─────────────────────────────────────────────────────────────────

#[test]
fn test_atan_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ATAN(0) AS v");
    assert!((to_f64(&r.rows[0][0]) - 0.0).abs() < 1e-10);
}

#[test]
fn test_atan_one() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ATAN(1) AS v");
    assert!((to_f64(&r.rows[0][0]) - (PI / 4.0)).abs() < 1e-5);
}

#[test]
fn test_atan_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ATAN(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── ATN2 ─────────────────────────────────────────────────────────────────

#[test]
fn test_atn2_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ATN2(1, 1) AS v");
    assert!((to_f64(&r.rows[0][0]) - (PI / 4.0)).abs() < 1e-10);
}

#[test]
fn test_atn2_zero_x() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ATN2(1, 0) AS v");
    assert!((to_f64(&r.rows[0][0]) - (PI / 2.0)).abs() < 1e-10);
}

#[test]
fn test_atn2_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ATN2(NULL, 1) AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── COS ──────────────────────────────────────────────────────────────────

#[test]
fn test_cos_zero() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT COS(0) AS v");
    assert!((to_f64(&r.rows[0][0]) - 1.0).abs() < 1e-10);
}

#[test]
fn test_cos_pi() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT COS(PI()) AS v");
    assert!((to_f64(&r.rows[0][0]) - (-1.0)).abs() < 1e-10);
}

#[test]
fn test_cos_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT COS(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── COT ──────────────────────────────────────────────────────────────────

#[test]
fn test_cot_pi_over_4() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT COT(PI() / 4) AS v");
    assert!((to_f64(&r.rows[0][0]) - 1.0).abs() < 1e-10);
}

#[test]
fn test_cot_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT COT(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── DEGREES ──────────────────────────────────────────────────────────────

#[test]
fn test_degrees_pi() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT DEGREES(PI()) AS v");
    assert!((to_f64(&r.rows[0][0]) - 180.0).abs() < 1e-8);
}

#[test]
fn test_degrees_zero() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT DEGREES(0) AS v");
    assert!((to_f64(&r.rows[0][0]) - 0.0).abs() < 1e-10);
}

#[test]
fn test_degrees_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT DEGREES(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── EXP ──────────────────────────────────────────────────────────────────

#[test]
fn test_exp_zero() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT EXP(0) AS v");
    assert!((to_f64(&r.rows[0][0]) - 1.0).abs() < 1e-10);
}

#[test]
fn test_exp_one() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT EXP(1) AS v");
    assert!((to_f64(&r.rows[0][0]) - std::f64::consts::E).abs() < 1e-5);
}

#[test]
fn test_exp_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT EXP(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── LOG ──────────────────────────────────────────────────────────────────

#[test]
fn test_log_e() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT LOG(EXP(1)) AS v");
    assert!((to_f64(&r.rows[0][0]) - 1.0).abs() < 1e-6);
}

#[test]
fn test_log_with_base() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT LOG(8, 2) AS v");
    assert!((to_f64(&r.rows[0][0]) - 3.0).abs() < 1e-10);
}

#[test]
fn test_log_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT LOG(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── LOG10 ────────────────────────────────────────────────────────────────

#[test]
fn test_log10_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT LOG10(100.0) AS v");
    assert!((to_f64(&r.rows[0][0]) - 2.0).abs() < 1e-10);
}

#[test]
fn test_log10_thousand() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT LOG10(1000.0) AS v");
    assert!((to_f64(&r.rows[0][0]) - 3.0).abs() < 1e-10);
}

#[test]
fn test_log10_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT LOG10(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── PI ───────────────────────────────────────────────────────────────────

#[test]
fn test_pi_value() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT PI() AS v");
    assert!((to_f64(&r.rows[0][0]) - PI).abs() < 1e-10);
}

// ─── RADIANS ──────────────────────────────────────────────────────────────

#[test]
fn test_radians_180() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT RADIANS(180.0) AS v");
    assert!((to_f64(&r.rows[0][0]) - PI).abs() < 1e-8);
}

#[test]
fn test_radians_90() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT RADIANS(90.0) AS v");
    assert!((to_f64(&r.rows[0][0]) - (PI / 2.0)).abs() < 1e-8);
}

#[test]
fn test_radians_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT RADIANS(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── SIN ──────────────────────────────────────────────────────────────────

#[test]
fn test_sin_zero() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SIN(0) AS v");
    assert!((to_f64(&r.rows[0][0]) - 0.0).abs() < 1e-10);
}

#[test]
fn test_sin_pi_over_2() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SIN(PI() / 2) AS v");
    assert!((to_f64(&r.rows[0][0]) - 1.0).abs() < 1e-10);
}

#[test]
fn test_sin_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SIN(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── SQUARE ───────────────────────────────────────────────────────────────

#[test]
fn test_square_basic() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SQUARE(5) AS v");
    assert!((to_f64(&r.rows[0][0]) - 25.0).abs() < 1e-10);
}

#[test]
fn test_square_negative() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SQUARE(-3) AS v");
    assert!((to_f64(&r.rows[0][0]) - 9.0).abs() < 1e-10);
}

#[test]
fn test_square_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SQUARE(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── TAN ──────────────────────────────────────────────────────────────────

#[test]
fn test_tan_zero() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT TAN(0) AS v");
    assert!((to_f64(&r.rows[0][0]) - 0.0).abs() < 1e-10);
}

#[test]
fn test_tan_pi_over_4() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT TAN(PI() / 4) AS v");
    assert!((to_f64(&r.rows[0][0]) - 1.0).abs() < 1e-10);
}

#[test]
fn test_tan_null() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT TAN(NULL) AS v");
    assert!(r.rows[0][0].is_null());
}

// ─── INTEGRATION ──────────────────────────────────────────────────────────

#[test]
fn test_trig_identity_sin_cos() {
    let mut engine = Engine::new();
    // sin²(x) + cos²(x) = 1
    let r = query(
        &mut engine,
        "SELECT SQUARE(SIN(PI() / 6)) + SQUARE(COS(PI() / 6)) AS v",
    );
    assert!((to_f64(&r.rows[0][0]) - 1.0).abs() < 1e-10);
}

#[test]
fn test_degrees_radians_roundtrip() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT DEGREES(RADIANS(45.0)) AS v");
    assert!((to_f64(&r.rows[0][0]) - 45.0).abs() < 1e-8);
}

#[test]
fn test_log_exp_inverse() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT LOG(EXP(5)) AS v");
    assert!((to_f64(&r.rows[0][0]) - 5.0).abs() < 1e-6);
}

#[test]
fn test_sqrt_square_inverse() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT SQRT(SQUARE(7)) AS v");
    assert!((to_f64(&r.rows[0][0]) - 7.0).abs() < 1e-8);
}

#[test]
fn test_atn2_atan_relationship() {
    let mut engine = Engine::new();
    let r = query(&mut engine, "SELECT ABS(ATN2(1, 1) - ATAN(1)) AS v");
    assert!(to_f64(&r.rows[0][0]) < 1e-5);
}

#[test]
fn test_cot_tan_relationship() {
    let mut engine = Engine::new();
    // COT(x) * TAN(x) = 1
    let r = query(&mut engine, "SELECT COT(PI() / 6) * TAN(PI() / 6) AS v");
    assert!((to_f64(&r.rows[0][0]) - 1.0).abs() < 1e-8);
}
