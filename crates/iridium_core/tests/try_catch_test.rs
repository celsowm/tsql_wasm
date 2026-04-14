use iridium_core::Engine;

#[test]
fn test_try_catch_basic() {
    let engine = Engine::new();

    let batch = "
        BEGIN TRY
            -- Cause an error (divide by zero or explicit raiserror)
            RAISERROR('failed here', 16, 1);
        END TRY
        BEGIN CATCH
            PRINT 'Caught: ' + ERROR_MESSAGE();
        END CATCH
    ";
    for stmt in iridium_core::parser::parse_batch(batch).unwrap() {
        engine.execute(stmt).unwrap();
    }
    let output = engine.print_output();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0], "Caught: execution error: failed here");
}

#[test]
fn test_try_catch_no_error() {
    let engine = Engine::new();

    let batch = "
        BEGIN TRY
            PRINT 'Success';
        END TRY
        BEGIN CATCH
            PRINT 'Should not see this';
        END CATCH
    ";
    for stmt in iridium_core::parser::parse_batch(batch).unwrap() {
        engine.execute(stmt).unwrap();
    }
    let output = engine.print_output();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0], "Success");
}

#[test]
fn test_try_catch_nested() {
    let engine = Engine::new();

    let batch = "
        BEGIN TRY
            BEGIN TRY
                RAISERROR('Inner error', 16, 1);
            END TRY
            BEGIN CATCH
                RAISERROR('Rethrown: ' + ERROR_MESSAGE(), 16, 1);
            END CATCH
        END TRY
        BEGIN CATCH
            PRINT 'Outer caught: ' + ERROR_MESSAGE();
        END CATCH
    ";
    for stmt in iridium_core::parser::parse_batch(batch).unwrap() {
        engine.execute(stmt).unwrap();
    }
    let output = engine.print_output();
    assert_eq!(output.len(), 1);
    assert_eq!(
        output[0],
        "Outer caught: execution error: Rethrown: execution error: Inner error"
    );
}

#[test]
fn test_error_functions_null_outside_catch() {
    let engine = Engine::new();
    let res = engine.query("SELECT ERROR_MESSAGE() AS Msg").unwrap();
    assert!(res.rows[0][0].is_null());
}

