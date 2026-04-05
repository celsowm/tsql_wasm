pub mod playground;
pub mod pool;
pub mod server;
pub mod session;
pub mod tds;
pub mod tls;

pub use server::TdsServer;

#[derive(Debug, Clone)]
pub struct Credentials {
    pub user: String,
    pub password: String,
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub auth: Option<Credentials>,
    pub database: String,
    pub packet_size: u16,
    pub tls_enabled: bool,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
    pub pool_min_size: usize,
    pub pool_max_size: usize,
    pub pool_idle_timeout_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 1433,
            auth: None,
            database: "master".to_string(),
            packet_size: 4096,
            tls_enabled: true,
            tls_cert_path: None,
            tls_key_path: None,
            pool_min_size: 1,
            pool_max_size: 50,
            pool_idle_timeout_secs: 300,
        }
    }
}

impl ServerConfig {
    pub fn no_auth() -> Self {
        Self::default()
    }

    pub fn with_auth(user: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            auth: Some(Credentials {
                user: user.into(),
                password: password.into(),
            }),
            ..Self::default()
        }
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    pub fn database(mut self, db: impl Into<String>) -> Self {
        self.database = db.into();
        self
    }

    pub fn with_tls_cert(
        mut self,
        cert_path: impl Into<String>,
        key_path: impl Into<String>,
    ) -> Self {
        self.tls_enabled = true;
        self.tls_cert_path = Some(cert_path.into());
        self.tls_key_path = Some(key_path.into());
        self
    }

    pub fn disable_tls(mut self) -> Self {
        self.tls_enabled = false;
        self
    }

    pub fn pool_min_size(mut self, size: usize) -> Self {
        self.pool_min_size = size;
        self
    }

    pub fn pool_max_size(mut self, size: usize) -> Self {
        self.pool_max_size = size;
        self
    }

    pub fn pool_idle_timeout_secs(mut self, secs: u64) -> Self {
        self.pool_idle_timeout_secs = secs;
        self
    }
}
