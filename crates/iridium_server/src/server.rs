use std::sync::Arc;
use tokio::net::TcpListener;

use iridium_core::PersistentDatabase;

use super::pool::SessionPool;
use super::session::TdsSession;
use super::ServerConfig;
use crate::ServerDatabase;

pub struct TdsServer {
    db: Arc<dyn ServerDatabase>,
    config: Arc<ServerConfig>,
    session_pool: Arc<SessionPool>,
    listener: Option<TcpListener>,
}

impl TdsServer {
    pub fn new(config: ServerConfig) -> Self {
        let session_pool = Arc::new(SessionPool::from_config(&config));
        let data_dir = config.resolved_data_dir();
        Self {
            db: Arc::new(
                PersistentDatabase::new_persistent(&data_dir)
                    .expect("failed to initialize persistent database"),
            ),
            config: Arc::new(config),
            session_pool,
            listener: None,
        }
    }

    pub fn new_with_database<D>(db: D, config: ServerConfig) -> Self
    where
        D: ServerDatabase + 'static,
    {
        let session_pool = Arc::new(SessionPool::from_config(&config));
        Self {
            db: Arc::new(db),
            config: Arc::new(config),
            session_pool,
            listener: None,
        }
    }

    pub async fn bind(&mut self) -> Result<std::net::SocketAddr, Box<dyn std::error::Error>> {
        Self::validate_config(&self.config)?;
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
        Self::validate_config(&self.config)?;
        self.session_pool.ensure_min_sessions(self.db.as_ref());

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
                    let session_pool = self.session_pool.clone();

                    tokio::spawn(async move {
                        let session = TdsSession::new(db, config, session_pool);
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

    fn validate_config(config: &ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
        if config.pool_max_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid pool config: pool_max_size must be > 0",
            )
            .into());
        }
        if config.pool_min_size > config.pool_max_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "invalid pool config: pool_min_size must be <= pool_max_size",
            )
            .into());
        }
        Ok(())
    }
}
