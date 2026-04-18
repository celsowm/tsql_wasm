use crate::executor::context::ExecutionContext;

#[derive(Debug, Clone, Copy)]
pub(crate) struct DatabaseDef {
    pub id: i32,
    pub name: &'static str,
    pub compatibility_level: u8,
    pub recovery_model: &'static str,
}

const BUILTIN_DATABASES: &[DatabaseDef] = &[
    DatabaseDef {
        id: 1,
        name: "master",
        compatibility_level: 160,
        recovery_model: "FULL",
    },
    DatabaseDef {
        id: 2,
        name: "tempdb",
        compatibility_level: 160,
        recovery_model: "SIMPLE",
    },
    DatabaseDef {
        id: 3,
        name: "model",
        compatibility_level: 160,
        recovery_model: "FULL",
    },
    DatabaseDef {
        id: 4,
        name: "msdb",
        compatibility_level: 160,
        recovery_model: "FULL",
    },
    DatabaseDef {
        id: 5,
        name: "iridium_sql",
        compatibility_level: 160,
        recovery_model: "FULL",
    },
];

pub(crate) fn builtin_databases() -> impl Iterator<Item = &'static DatabaseDef> {
    BUILTIN_DATABASES.iter()
}

pub(crate) fn database_id_for_name(name: &str) -> Option<i32> {
    builtin_databases()
        .find(|db| db.name.eq_ignore_ascii_case(name))
        .map(|db| db.id)
}

pub(crate) fn database_name_for_id(id: i32) -> Option<&'static str> {
    builtin_databases().find(|db| db.id == id).map(|db| db.name)
}

pub(crate) fn recovery_model_for_name(name: &str) -> Option<&'static str> {
    builtin_databases()
        .find(|db| db.name.eq_ignore_ascii_case(name))
        .map(|db| db.recovery_model)
}

pub(crate) fn current_database_name<'a>(ctx: &'a ExecutionContext<'a>) -> &'a str {
    ctx.metadata
        .database
        .as_deref()
        .unwrap_or(&ctx.metadata.original_database)
}

pub(crate) fn current_database_id(ctx: &ExecutionContext<'_>) -> i32 {
    database_id_for_name(current_database_name(ctx)).unwrap_or(0)
}

pub(crate) fn authenticating_database_id(ctx: &ExecutionContext<'_>) -> i32 {
    database_id_for_name(&ctx.metadata.original_database).unwrap_or(0)
}
