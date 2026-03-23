use std::sync::Arc;
use tokio::net::TcpListener;

use tsql_core::Database;

use super::session::TdsSession;
use super::ServerConfig;

pub struct TdsServer {
    db: Database,
    config: Arc<ServerConfig>,
    listener: Option<TcpListener>,
}

impl TdsServer {
    pub fn new(config: ServerConfig) -> Self {
        Self {
            db: Database::new(),
            config: Arc::new(config),
            listener: None,
        }
    }

    pub fn new_with_database(db: Database, config: ServerConfig) -> Self {
        Self {
            db,
            config: Arc::new(config),
            listener: None,
        }
    }

    pub async fn bind(&mut self) -> Result<std::net::SocketAddr, Box<dyn std::error::Error>> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = TcpListener::bind(&addr).await?;
        let local_addr = listener.local_addr()?;
        log::info!("TDS Server listening on {}", local_addr);
        self.listener = Some(listener);
        Ok(local_addr)
    }

    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.listener.as_ref().and_then(|l| l.local_addr().ok())
    }

    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = if let Some(l) = self.listener.take() {
            l
        } else {
            TcpListener::bind(&addr).await?
        };

        let local_addr = listener.local_addr()?;
        log::info!("TDS Server listening on {}", local_addr);

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    log::info!("Accepted connection from {}", peer_addr);
                    stream.set_nodelay(true).ok();

                    let db = self.db.clone();
                    let config = self.config.clone();

                    tokio::spawn(async move {
                        let session = TdsSession::new(db, config);
                        if let Err(e) = session.handle(stream).await {
                            log::error!("Session error for {}: {}", peer_addr, e);
                        }
                        log::info!("Connection from {} closed", peer_addr);
                    });
                }
                Err(e) => {
                    log::error!("Accept error: {}", e);
                }
            }
        }
    }
}
