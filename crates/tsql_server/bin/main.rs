use tsql_server::{playground, Credentials, ServerConfig, TdsServer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_logger();

    let mut args: Vec<String> = std::env::args().collect();
    args.remove(0); // remove program name

    let mut config = ServerConfig::default();
    let mut playground_mode = false;
    let mut database_arg_provided = false;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--host" | "-h" => {
                i += 1;
                config.host = args.get(i).cloned().unwrap_or_default();
            }
            "--port" | "-p" => {
                i += 1;
                config.port = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(1433);
            }
            "--user" | "-u" => {
                i += 1;
                let user = args.get(i).cloned().unwrap_or_default();
                if config.auth.is_none() {
                    config.auth = Some(Credentials {
                        user: user.clone(),
                        password: String::new(),
                    });
                } else if let Some(ref mut creds) = config.auth {
                    creds.user = user;
                }
            }
            "--password" | "-P" => {
                i += 1;
                let pass = args.get(i).cloned().unwrap_or_default();
                if config.auth.is_none() {
                    config.auth = Some(Credentials {
                        user: String::new(),
                        password: pass,
                    });
                } else if let Some(ref mut creds) = config.auth {
                    creds.password = pass;
                }
            }
            "--database" | "-d" => {
                i += 1;
                config.database = args.get(i).cloned().unwrap_or_default();
                database_arg_provided = true;
            }
            "--pool-min" => {
                i += 1;
                config.pool_min_size = args
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(config.pool_min_size);
            }
            "--pool-max" => {
                i += 1;
                config.pool_max_size = args
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(config.pool_max_size);
            }
            "--pool-idle-timeout" => {
                i += 1;
                config.pool_idle_timeout_secs = args
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(config.pool_idle_timeout_secs);
            }
            "--tls" | "-t" => {
                config.tls_enabled = true;
            }
            "--no-tls" => {
                config.tls_enabled = false;
            }
            "--tls-cert" => {
                i += 1;
                config.tls_cert_path = args.get(i).cloned();
            }
            "--tls-key" => {
                i += 1;
                config.tls_key_path = args.get(i).cloned();
            }
            "--tls-gen" => {
                let cert_path = "tls_cert.pem";
                let key_path = "tls_key.pem";
                println!("Generating self-signed TLS certificate...");
                tsql_server::tls::generate_self_signed_cert(cert_path, key_path)?;
                println!("Generated {} and {}", cert_path, key_path);
                config.tls_cert_path = Some(cert_path.to_string());
                config.tls_key_path = Some(key_path.to_string());
                config.tls_enabled = true;
            }
            "--playground" => {
                playground_mode = true;
            }
            "--help" => {
                print_help();
                return Ok(());
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                print_help();
                std::process::exit(1);
            }
        }
        i += 1;
    }

    if let Some(ref creds) = config.auth {
        if creds.user.is_empty() && creds.password.is_empty() {
            config.auth = None;
        }
    }

    if config.tls_enabled && config.tls_cert_path.is_none() {
        eprintln!("Error: TLS is enabled but no certificate specified.");
        eprintln!("Use --tls-gen to generate a self-signed certificate, or --tls-cert and --tls-key to specify existing files.");
        std::process::exit(1);
    }

    // In playground mode, default to a user database so SSMS Object Explorer
    // can enumerate it as a regular database node. Respect explicit --database.
    if playground_mode && !database_arg_provided {
        config.database = "tsql_wasm".to_string();
    }

    log::info!("tsql-server v{}", env!("CARGO_PKG_VERSION"));
    log::info!("Host: {}", config.host);
    log::info!("Port: {}", config.port);
    log::info!(
        "Auth: {}",
        if config.auth.is_some() {
            "enabled"
        } else {
            "disabled (accept any login)"
        }
    );
    log::info!(
        "TLS: {}",
        if config.tls_enabled {
            format!("enabled ({})", config.tls_cert_path.as_ref().unwrap())
        } else {
            "disabled".to_string()
        }
    );
    log::info!("Database: {}", config.database);
    log::info!(
        "Session pool: min={}, max={}, idle_timeout={}s",
        config.pool_min_size, config.pool_max_size, config.pool_idle_timeout_secs
    );

    // Create database and seed playground if enabled
    let db = tsql_core::Database::new();
    if playground_mode {
        log::info!("Starting in PLAYGROUND mode...");
        log::info!("Seeding database with sample tables, views, and data...");
        if let Err(e) = playground::seed_playground(&db) {
            log::warn!("Some playground seed operations failed: {}", e);
        }
        log::info!("Playground database ready");
        log::info!("Sample tables: dbo.Customers, dbo.Products, dbo.Orders, dbo.OrderItems, dbo.Employees, dbo.Categories");
        log::info!("Sample views: dbo.vCustomerOrders, dbo.vOrderDetails, dbo.vProductSales, dbo.vEmployeeHierarchy, dbo.vMonthlySales");
    }

    let server = TdsServer::new_with_database(db, config);
    server.run().await?;

    Ok(())
}

fn init_logger() {
    use std::io::Write;
    use std::sync::mpsc;
    use std::thread;

    struct AsyncPipeWriter {
        sender: mpsc::Sender<Vec<u8>>,
    }

    impl Write for AsyncPipeWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.sender
                .send(buf.to_vec())
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::BrokenPipe, "logger worker stopped"))?;
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    thread::spawn(move || {
        let stderr = std::io::stderr();
        let mut handle = stderr.lock();
        while let Ok(buf) = rx.recv() {
            if handle.write_all(&buf).is_err() {
                break;
            }
            if handle.flush().is_err() {
                break;
            }
        }
    });

    let mut builder =
        env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"));
    builder.target(env_logger::Target::Pipe(Box::new(AsyncPipeWriter { sender: tx })));
    builder.format(|buf, record| {
        let ts = buf.timestamp_millis();
        writeln!(
            buf,
            "[{} {:<5} {}] {}",
            ts,
            record.level(),
            record.target(),
            record.args()
        )?;
        writeln!(buf)
    });
    builder.init();
}

fn print_help() {
    println!("tsql-server - TDS 7.4 SQL Server emulator");
    println!();
    println!("USAGE:");
    println!("  tsql-server [OPTIONS]");
    println!();
    println!("OPTIONS:");
    println!("  --host, -h <HOST>       Hostname to bind to (default: 127.0.0.1)");
    println!("  --port, -p <PORT>       Port to listen on (default: 1433)");
    println!("  --user, -u <USER>       Username for authentication (optional)");
    println!("  --password, -P <PASS>   Password for authentication (optional)");
    println!("  --database, -d <DB>     Default database name (default: master)");
    println!("  --pool-min <N>          Minimum pooled sessions to keep ready (default: 1)");
    println!("  --pool-max <N>          Maximum pooled sessions allowed (default: 50)");
    println!("  --pool-idle-timeout <S> Idle timeout in seconds for extra sessions (default: 300)");
    println!("  --tls, -t               Enable TLS (default: enabled)");
    println!("  --no-tls                Disable TLS");
    println!("  --tls-cert <PATH>       TLS certificate file (required if TLS enabled)");
    println!("  --tls-key <PATH>        TLS private key file (required if TLS enabled)");
    println!("  --tls-gen               Generate self-signed TLS certificate");
    println!("  --playground            Start with sample tables, views, and data");
    println!("  --help                  Show this help message");
    println!();
    println!("EXAMPLES:");
    println!("  tsql-server                         # TLS enabled, no auth");
    println!("  tsql-server --tls-gen               # Generate cert and start server");
    println!("  tsql-server --port 14330            # No auth, port 14330");
    println!("  tsql-server --pool-min 2 --pool-max 100 --pool-idle-timeout 60");
    println!("  tsql-server -u sa -P Test@12345     # With auth");
    println!("  tsql-server --playground            # Playground mode with sample data");
    println!("  tsql-server --playground --no-tls   # Playground without TLS");
    println!();
    println!("PLAYGROUND MODE:");
    println!("  When --playground is enabled, the server starts with pre-loaded");
    println!("  sample tables (Customers, Products, Orders, etc.) and views for");
    println!("  testing SQL Server clients without manual setup.");
    println!();
    println!("NOTES:");
    println!("  - TLS is enabled by default");
    println!("  - Use --tls-gen to generate a self-signed certificate for testing");
}
