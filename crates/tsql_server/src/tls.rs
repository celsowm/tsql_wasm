use rcgen::generate_simple_self_signed;
use rustls::ServerConfig;
use std::io;

pub fn generate_self_signed_cert(cert_path: &str, key_path: &str) -> io::Result<()> {
    let certified_key = generate_simple_self_signed(vec!["localhost".to_string()])
        .map_err(io::Error::other)?;

    let pem_cert = certified_key.cert.pem();
    let pem_key = certified_key.key_pair.serialize_pem();

    std::fs::write(cert_path, pem_cert)?;
    std::fs::write(key_path, pem_key)?;

    Ok(())
}

pub fn load_tls_config(cert_path: &str, key_path: &str) -> io::Result<ServerConfig> {
    let cert_data = std::fs::read(cert_path)?;
    let key_data = std::fs::read(key_path)?;

    let mut cert_cursor = std::io::Cursor::new(cert_data);
    let cert = rustls_pemfile::read_one(&mut cert_cursor)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
        .and_then(|item| match item {
            rustls_pemfile::Item::X509Certificate(cert) => Some(cert),
            _ => None,
        })
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "No certificate found"))?;

    let mut key_cursor = std::io::Cursor::new(key_data);
    let key = rustls_pemfile::read_one(&mut key_cursor)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    let private_key = match key {
        Some(rustls_pemfile::Item::Pkcs8Key(key)) => rustls::pki_types::PrivateKeyDer::Pkcs8(key),
        Some(rustls_pemfile::Item::Pkcs1Key(key)) => rustls::pki_types::PrivateKeyDer::Pkcs1(key),
        Some(rustls_pemfile::Item::Sec1Key(key)) => rustls::pki_types::PrivateKeyDer::Sec1(key),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "No private key found",
            ))
        }
    };

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], private_key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    Ok(config)
}
