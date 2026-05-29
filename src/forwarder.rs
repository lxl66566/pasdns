use std::{
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
    time::Duration,
};

use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use tokio_rustls::TlsConnector;

use crate::config::Protocol;

const FORWARD_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_DNS_MESSAGE_SIZE: usize = 4096;
const CACHE_TEMP_FILE: &str = "pasdns.cache.tmp";

static TLS_CONNECTOR: LazyLock<TlsConnector> = LazyLock::new(|| {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
});

#[derive(Debug, thiserror::Error)]
pub enum ForwardError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("timeout")]
    Timeout,
    #[error("unsupported protocol")]
    UnsupportedProtocol,
}

pub type Result<T> = std::result::Result<T, ForwardError>;

async fn connect_via_socks5(proxy: SocketAddr, target: SocketAddr) -> Result<TcpStream> {
    let mut stream = timeout(FORWARD_TIMEOUT, TcpStream::connect(proxy))
        .await
        .map_err(|_| ForwardError::Timeout)??;

    stream
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .map_err(|_| ForwardError::Io(std::io::Error::other("failed to send SOCKS5 greeting")))?;

    let mut greeting = [0u8; 2];
    timeout(FORWARD_TIMEOUT, stream.read_exact(&mut greeting))
        .await
        .map_err(|_| ForwardError::Timeout)??;

    if greeting[0] != 0x05 {
        return Err(ForwardError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "not a SOCKS5 proxy",
        )));
    }
    if greeting[1] != 0x00 {
        return Err(ForwardError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "SOCKS5 proxy requires authentication (not supported)",
        )));
    }

    let mut req = vec![0x05, 0x01, 0x00];
    match target.ip() {
        std::net::IpAddr::V4(v4) => {
            req.push(0x01);
            req.extend_from_slice(&v4.octets());
        }
        std::net::IpAddr::V6(v6) => {
            req.push(0x04);
            req.extend_from_slice(&v6.octets());
        }
    }
    req.extend_from_slice(&target.port().to_be_bytes());
    stream.write_all(&req).await?;

    let mut resp = [0u8; 4];
    timeout(FORWARD_TIMEOUT, stream.read_exact(&mut resp))
        .await
        .map_err(|_| ForwardError::Timeout)??;

    if resp[1] != 0x00 {
        return Err(ForwardError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            format!("SOCKS5 connect failed with code {}", resp[1]),
        )));
    }

    match resp[3] {
        0x01 => {
            let mut skip = [0u8; 4];
            timeout(FORWARD_TIMEOUT, stream.read_exact(&mut skip))
                .await
                .map_err(|_| ForwardError::Timeout)??;
        }
        0x04 => {
            let mut skip = [0u8; 16];
            timeout(FORWARD_TIMEOUT, stream.read_exact(&mut skip))
                .await
                .map_err(|_| ForwardError::Timeout)??;
        }
        0x03 => {
            let mut len_buf = [0u8; 1];
            timeout(FORWARD_TIMEOUT, stream.read_exact(&mut len_buf))
                .await
                .map_err(|_| ForwardError::Timeout)??;
            let mut skip = vec![0u8; len_buf[0] as usize];
            timeout(FORWARD_TIMEOUT, stream.read_exact(&mut skip))
                .await
                .map_err(|_| ForwardError::Timeout)??;
        }
        _ => {}
    }
    let mut port = [0u8; 2];
    timeout(FORWARD_TIMEOUT, stream.read_exact(&mut port))
        .await
        .map_err(|_| ForwardError::Timeout)??;

    Ok(stream)
}

pub async fn forward_udp(raw_query: &[u8], upstream: SocketAddr) -> Result<Vec<u8>> {
    let socket = tokio::net::UdpSocket::bind("0.0.0.0:0").await?;
    timeout(FORWARD_TIMEOUT, socket.send_to(raw_query, upstream))
        .await
        .map_err(|_| ForwardError::Timeout)??;
    let mut buf = vec![0u8; MAX_DNS_MESSAGE_SIZE];
    let len = timeout(FORWARD_TIMEOUT, socket.recv(&mut buf))
        .await
        .map_err(|_| ForwardError::Timeout)??;
    buf.truncate(len);
    Ok(buf)
}

pub async fn forward_tcp(
    raw_query: &[u8],
    upstream: SocketAddr,
    proxy_addr: Option<SocketAddr>,
) -> Result<Vec<u8>> {
    let mut stream = if let Some(proxy) = proxy_addr {
        connect_via_socks5(proxy, upstream).await?
    } else {
        timeout(FORWARD_TIMEOUT, TcpStream::connect(upstream))
            .await
            .map_err(|_| ForwardError::Timeout)??
    };
    let len = u16::try_from(raw_query.len()).map_err(|_| {
        ForwardError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "query too large",
        ))
    })?;
    timeout(FORWARD_TIMEOUT, async {
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(raw_query).await
    })
    .await
    .map_err(|_| ForwardError::Timeout)??;

    let mut len_buf = [0u8; 2];
    timeout(FORWARD_TIMEOUT, stream.read_exact(&mut len_buf))
        .await
        .map_err(|_| ForwardError::Timeout)??;
    let response_len = u16::from_be_bytes(len_buf) as usize;
    let mut response = vec![0u8; response_len];
    timeout(FORWARD_TIMEOUT, stream.read_exact(&mut response))
        .await
        .map_err(|_| ForwardError::Timeout)??;
    Ok(response)
}

pub async fn forward_dot(raw_query: &[u8], upstream: SocketAddr) -> Result<Vec<u8>> {
    let tcp_stream = timeout(FORWARD_TIMEOUT, TcpStream::connect(upstream))
        .await
        .map_err(|_| ForwardError::Timeout)??;

    let server_name =
        rustls::pki_types::ServerName::try_from(upstream.ip().to_string()).map_err(|e| {
            ForwardError::Io(std::io::Error::other(format!("invalid server name: {e}")))
        })?;
    let tls_stream = TLS_CONNECTOR
        .connect(server_name, tcp_stream)
        .await
        .map_err(|e| ForwardError::Io(std::io::Error::other(format!("tls handshake: {e}"))))?;

    let mut stream = tls_stream;
    let len = u16::try_from(raw_query.len()).map_err(|_| {
        ForwardError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "query too large",
        ))
    })?;
    timeout(FORWARD_TIMEOUT, async {
        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(raw_query).await
    })
    .await
    .map_err(|_| ForwardError::Timeout)??;

    let mut len_buf = [0u8; 2];
    timeout(FORWARD_TIMEOUT, stream.read_exact(&mut len_buf))
        .await
        .map_err(|_| ForwardError::Timeout)??;
    let response_len = u16::from_be_bytes(len_buf) as usize;
    let mut response = vec![0u8; response_len];
    timeout(FORWARD_TIMEOUT, stream.read_exact(&mut response))
        .await
        .map_err(|_| ForwardError::Timeout)??;
    Ok(response)
}

pub async fn forward_doh(raw_query: &[u8], url: &str, server_addr: SocketAddr) -> Result<Vec<u8>> {
    let tcp_stream = timeout(FORWARD_TIMEOUT, TcpStream::connect(server_addr))
        .await
        .map_err(|_| ForwardError::Timeout)??;

    let parsed = http::Uri::try_from(url)
        .map_err(|e| ForwardError::Io(std::io::Error::other(format!("invalid doh url: {e}"))))?;
    let host = parsed.host().unwrap_or("");
    let server_name = rustls::pki_types::ServerName::try_from(host.to_string()).map_err(|e| {
        ForwardError::Io(std::io::Error::other(format!("invalid server name: {e}")))
    })?;
    let tls_stream = TLS_CONNECTOR
        .connect(server_name, tcp_stream)
        .await
        .map_err(|e| ForwardError::Io(std::io::Error::other(format!("tls handshake: {e}"))))?;

    let (mut client, connection) = h2::client::handshake(tls_stream)
        .await
        .map_err(|e| ForwardError::Io(std::io::Error::other(format!("h2 handshake: {e}"))))?;

    tokio::spawn(async move {
        let _ = connection.await;
    });

    let path = parsed
        .path_and_query()
        .map_or("/dns-query", |pq| pq.as_str());
    let request = http::Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/dns-message")
        .header("accept", "application/dns-message")
        .header("host", host)
        .body(())
        .map_err(|e| ForwardError::Io(std::io::Error::other(format!("h2 request: {e}"))))?;

    let (response, mut send_stream) = client
        .send_request(request, false)
        .map_err(|e| ForwardError::Io(std::io::Error::other(format!("h2 send_request: {e}"))))?;

    send_stream
        .send_data(Bytes::from(raw_query.to_vec()), true)
        .map_err(|e| ForwardError::Io(std::io::Error::other(format!("h2 send_data: {e}"))))?;

    let response = timeout(FORWARD_TIMEOUT, response)
        .await
        .map_err(|_| ForwardError::Timeout)?
        .map_err(|e| ForwardError::Io(std::io::Error::other(format!("h2 response: {e}"))))?;

    let mut body = response.into_body();
    let mut data = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk =
            chunk.map_err(|e| ForwardError::Io(std::io::Error::other(format!("h2 body: {e}"))))?;
        data.extend_from_slice(&chunk);
    }
    Ok(data)
}

pub async fn forward(
    raw_query: &[u8],
    upstream: SocketAddr,
    protocol: Protocol,
    proxy_addr: Option<SocketAddr>,
    doh_url: Option<&str>,
) -> Result<Vec<u8>> {
    match protocol {
        Protocol::Udp => forward_udp(raw_query, upstream).await,
        Protocol::Tcp => forward_tcp(raw_query, upstream, proxy_addr).await,
        Protocol::Dot => forward_dot(raw_query, upstream).await,
        Protocol::Doh => {
            let url = doh_url.ok_or(ForwardError::UnsupportedProtocol)?;
            forward_doh(raw_query, url, upstream).await
        }
    }
}

pub fn atomic_write(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    let tmp_path = parent.join(CACHE_TEMP_FILE);

    let _ = std::fs::remove_file(&tmp_path);

    let mut file = std::fs::File::create(&tmp_path)?;
    file.write_all(data)?;
    file.sync_all()?;
    drop(file);

    rename_or_replace(&tmp_path, path)?;
    Ok(())
}

#[cfg(windows)]
fn rename_or_replace(from: &Path, to: &Path) -> std::io::Result<()> {
    if to.exists() {
        std::fs::remove_file(to)?;
    }
    std::fs::rename(from, to)
}

#[cfg(not(windows))]
fn rename_or_replace(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::rename(from, to)
}

pub fn temp_path_for(path: &Path) -> PathBuf {
    path.parent()
        .unwrap_or(Path::new("."))
        .join(CACHE_TEMP_FILE)
}
