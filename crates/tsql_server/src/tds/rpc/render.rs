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

pub fn build_param_preamble_with_decls(
    params: &[super::types::RpcParam],
    decls: &[(String, String)],
) -> String {
    let mut out = String::new();
    for (i, p) in params.iter().enumerate() {
        if p.name.is_empty() {
            continue;
        }
        let type_name = if i < decls.len() {
            normalize_decl_type(&decls[i].1)
        } else {
            normalize_decl_type(&p.type_name)
        };
        if let Some(rows) = &p.tvp_rows {
            out.push_str(&format!("DECLARE {} {};\n", p.name, type_name));
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
                p.name, type_name, p.value_sql
            ));
        }
    }
    out
}
