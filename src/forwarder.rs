use std::{net::SocketAddr, time::Duration};

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
    time::timeout,
};

use crate::config::Protocol;

const FORWARD_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_DNS_MESSAGE_SIZE: usize = 4096;

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

pub async fn forward_udp(raw_query: &[u8], upstream: SocketAddr) -> Result<Vec<u8>> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
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

pub async fn forward_tcp(raw_query: &[u8], upstream: SocketAddr) -> Result<Vec<u8>> {
    let mut stream = timeout(FORWARD_TIMEOUT, TcpStream::connect(upstream))
        .await
        .map_err(|_| ForwardError::Timeout)??;
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

pub async fn forward(
    raw_query: &[u8],
    upstream: SocketAddr,
    protocol: Protocol,
) -> Result<Vec<u8>> {
    match protocol {
        Protocol::Udp => forward_udp(raw_query, upstream).await,
        Protocol::Tcp => forward_tcp(raw_query, upstream).await,
        _ => Err(ForwardError::UnsupportedProtocol),
    }
}
