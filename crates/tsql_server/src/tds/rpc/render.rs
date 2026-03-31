use super::types::RpcParam;

/// Build DECLARE preamble for all RPC parameters.
pub fn build_param_preamble(params: &[RpcParam]) -> String {
    let mut out = String::new();
    for p in params {
        if p.name.is_empty() {
            continue;
        }
        if let Some(rows) = &p.tvp_rows {
            let decl_type = normalize_decl_type(&p.type_name);
            out.push_str(&format!("DECLARE {} {};\n", p.name, decl_type));
            for row in rows {
                out.push_str(&format!(
                    "INSERT INTO {} VALUES ({});\n",
                    p.name,
                    row.join(", ")
                ));
            }
        } else {
            out.push_str(&format!(
                "DECLARE {} {} = {};\n",
                p.name, p.type_name, p.value_sql
            ));
        }
    }
    out
}

pub(super) fn normalize_decl_type(type_name: &str) -> String {
    let t = type_name.trim();
    if let Some(idx) = t.to_uppercase().find(" READONLY") {
        t[..idx].trim().to_string()
    } else {
        t.to_string()
    }
}
