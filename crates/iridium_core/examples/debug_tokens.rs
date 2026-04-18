use iridium_core::parse_sql;

fn main() {
    let sql = "SELECT * FROM INFORMATION_SCHEMA.TABLES";
    println!("Testing: {}", sql);

    match parse_sql(sql) {
        Ok(_) => println!("OK"),
        Err(e) => println!("ERROR: {}", e),
    }

    match parse_sql("SELECT * FROM [INFORMATION_SCHEMA].[TABLES]") {
        Ok(_) => println!("Quoted works"),
        Err(e) => println!("Quoted ERROR: {}", e),
    }

    match parse_sql("SELECT * FROM INFORMATION_SCHEMA") {
        Ok(_) => println!("Schema works"),
        Err(e) => println!("Schema ERROR: {}", e),
    }
}
