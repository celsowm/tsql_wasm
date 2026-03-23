use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔌 Conectando ao playground em localhost:14330...");

    let mut config = Config::new();
    config.host("localhost");
    config.port(14330);
    config.authentication(tiberius::AuthMethod::sql_server("sa", "123456"));
    config.trust_cert();
    config.encrypt(tiberius::EncryptionLevel::Off);

    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut client = Client::connect(config, tcp.compat_write()).await?;
    println!("✅ Conectado!\n");

    // Testar tabelas
    println!("📋 Testando tabelas do playground...\n");

    // vCustomerOrders
    println!("--- vCustomerOrders ---");
    let stream = client.query("SELECT TOP 3 * FROM dbo.vCustomerOrders ORDER BY CustomerId", &[]).await?;
    let rows: Vec<tiberius::Row> = stream.into_first_result().await?;
    for row in rows {
        println!("  Customer: {} {} | Orders: {} | Total: ${}", 
            row.get::<&str, _>(1).unwrap_or("NULL"),
            row.get::<&str, _>(2).unwrap_or("NULL"),
            row.get::<i32, _>(4).unwrap_or(0),
            row.get::<f64, _>(5).unwrap_or(0.0)
        );
    }
    println!();

    // Products
    println!("--- Products (TOP 5) ---");
    let stream = client.query("SELECT TOP 5 Name, Price, Stock FROM dbo.Products ORDER BY ProductId", &[]).await?;
    let rows: Vec<tiberius::Row> = stream.into_first_result().await?;
    for row in rows {
        println!("  {} | ${} | Stock: {}", 
            row.get::<&str, _>(0).unwrap_or("NULL"),
            row.get::<f64, _>(1).unwrap_or(0.0),
            row.get::<i32, _>(2).unwrap_or(0)
        );
    }
    println!();

    // vProductSales
    println!("--- vProductSales (TOP 3) ---");
    let stream = client.query("SELECT TOP 3 ProductName, TotalSold, TotalRevenue FROM dbo.vProductSales ORDER BY TotalSold DESC", &[]).await?;
    let rows: Vec<tiberius::Row> = stream.into_first_result().await?;
    for row in rows {
        println!("  {} | Vendidos: {} | Receita: ${}", 
            row.get::<&str, _>(0).unwrap_or("NULL"),
            row.get::<i32, _>(1).unwrap_or(0),
            row.get::<f64, _>(2).unwrap_or(0.0)
        );
    }
    println!();

    // Employees
    println!("--- vEmployeeHierarchy ---");
    let stream = client.query("SELECT TOP 5 FirstName, LastName, Department, ManagerName FROM dbo.vEmployeeHierarchy ORDER BY EmployeeId", &[]).await?;
    let rows: Vec<tiberius::Row> = stream.into_first_result().await?;
    for row in rows {
        let manager = row.get::<&str, _>(4).unwrap_or("N/A");
        println!("  {} {} | {} | Manager: {}", 
            row.get::<&str, _>(0).unwrap_or("NULL"),
            row.get::<&str, _>(1).unwrap_or("NULL"),
            row.get::<&str, _>(2).unwrap_or("NULL"),
            if manager.is_empty() { "CEO" } else { manager }
        );
    }

    println!("\n✅ Playground funcionando corretamente!");
    Ok(())
}
