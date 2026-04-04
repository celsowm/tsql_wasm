use crate::error::DbError;
use crate::types::Value;
use crate::executor::context::ExecutionContext;

type SystemVarHandler = fn(&ExecutionContext) -> Value;

struct SystemVariable {
    name: &'static str,
    handler: SystemVarHandler,
}

static SYSTEM_VARIABLES: &[SystemVariable] = &[
    SystemVariable { name: "@@FETCH_STATUS", handler: |ctx| Value::Int(*ctx.session.fetch_status) },
    SystemVariable { name: "@@ERROR", handler: |ctx| {
        let code = ctx.frame.last_error.as_ref().map(|e| e.number()).unwrap_or(0);
        Value::Int(code)
    }},
    SystemVariable { name: "@@LANGUAGE", handler: |_| Value::NVarChar("us_english".into()) },
    SystemVariable { name: "@@TEXTSIZE", handler: |_| Value::Int(2147483647) },
    SystemVariable { name: "@@MAX_PRECISION", handler: |_| Value::TinyInt(38) },
    SystemVariable { name: "@@DATEFIRST", handler: |ctx| Value::TinyInt(ctx.metadata.datefirst as u8) },
    SystemVariable { name: "@@TRANCOUNT", handler: |ctx| Value::Int(ctx.frame.trancount as i32) },
    SystemVariable { name: "@@IDENTITY", handler: |ctx| match ctx.current_scope_identity().or(*ctx.session.last_identity) {
        Some(id) => Value::BigInt(id),
        None => Value::Null,
    } },
    SystemVariable { name: "@@PROCID", handler: |ctx| match ctx.current_procid() {
        Some(id) => Value::Int(id),
        None => Value::Null,
    } },
    SystemVariable { name: "@@SERVERNAME", handler: |_| Value::NVarChar("localhost".into()) },
    SystemVariable { name: "@@SERVICENAME", handler: |_| Value::NVarChar("MSSQLSERVER".into()) },
    SystemVariable { name: "@@SPID", handler: |ctx| Value::SmallInt(ctx.session_id() as i16) },
    SystemVariable { name: "@@VERSION", handler: |_| Value::NVarChar("Microsoft SQL Server 2022 (RTM) - 16.0.1000.6 (tsql_wasm emulator)".into()) },
    SystemVariable { name: "@@MICROSOFTVERSION", handler: |_| Value::Int(0x0c000000) },
];

pub(crate) fn resolve_system_variable(
    name: &str,
    ctx: &ExecutionContext,
) -> Result<Option<Value>, DbError> {
    for var in SYSTEM_VARIABLES {
        if var.name.eq_ignore_ascii_case(name) {
            return Ok(Some((var.handler)(ctx)));
        }
    }
    Ok(None)
}
