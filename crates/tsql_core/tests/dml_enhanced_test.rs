#[cfg(test)]
mod dml_enhanced_tests {
    use tsql_core::ast::*;
    use tsql_core::parser::parse_sql;

    #[test]
    fn test_update_top() {
        let sql = "UPDATE TOP (10) t SET a = 1";
        let stmt = parse_sql(sql).unwrap();
        if let Statement::Dml(DmlStatement::Update(u)) = stmt {
            assert!(u.top.is_some());
            if let Expr::Integer(n) = u.top.unwrap().value {
                assert_eq!(n, 10);
            } else {
                panic!("Expected integer top value");
            }
        } else {
            panic!("Expected Update statement");
        }
    }

    #[test]
    fn test_delete_top() {
        let sql = "DELETE TOP (5) FROM t WHERE a = 1";
        let stmt = parse_sql(sql).unwrap();
        if let Statement::Dml(DmlStatement::Delete(d)) = stmt {
            assert!(d.top.is_some());
            if let Expr::Integer(n) = d.top.unwrap().value {
                assert_eq!(n, 5);
            } else {
                panic!("Expected integer top value");
            }
        } else {
            panic!("Expected Delete statement");
        }
    }

    #[test]
    fn test_insert_exec() {
        let sql = "INSERT INTO t (col1) EXEC my_proc @param1 = 1";
        let stmt = parse_sql(sql).unwrap();
        if let Statement::Dml(DmlStatement::Insert(i)) = stmt {
            assert_eq!(i.table.name, "t");
            assert_eq!(i.columns, Some(vec!["col1".to_string()]));
            match i.source {
                InsertSource::Exec(s) => {
                    if let Statement::Procedural(ProceduralStatement::ExecProcedure(p)) = *s {
                        assert_eq!(p.name.name, "my_proc");
                    } else {
                        panic!("Expected ExecProcedure in InsertSource::Exec");
                    }
                }
                _ => panic!("Expected InsertSource::Exec"),
            }
        } else {
            panic!("Expected Insert statement");
        }
    }

    #[test]
    fn test_table_hints() {
        let sql = "SELECT * FROM t WITH (NOLOCK, TABLOCK)";
        let stmt = parse_sql(sql).unwrap();
        if let Statement::Dml(DmlStatement::Select(s)) = stmt {
            match s.from_clause.unwrap() {
                FromNode::Table(tref) => {
                    assert_eq!(tref.hints.len(), 2);
                    assert_eq!(tref.hints[0], "NOLOCK");
                    assert_eq!(tref.hints[1], "TABLOCK");
                }
                _ => panic!("Expected table FROM node"),
            }
        } else {
            panic!("Expected Select statement");
        }
    }
}
