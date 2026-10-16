//! Bounded WebSocket I/O and active liveness checks, independent of user traffic.
use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::time::{Interval, interval, sleep_until, timeout};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};

const PING_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);
const PONG_TIMEOUT: Duration = Duration::from_secs(15);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Default)]
pub struct SocketHealth(Arc<Mutex<Option<Instant>>>);

#[derive(Debug, Serialize)]
pub struct HealthSnapshot {
    pub healthy: bool,
    pub last_pong_age_seconds: Option<u64>,
}

impl SocketHealth {
    pub fn snapshot(&self) -> HealthSnapshot {
        let age = self
            .0
            .lock()
            .expect("socket health lock poisoned")
            .map(|t| t.elapsed());
        HealthSnapshot {
            healthy: age.is_some_and(|age| age < PING_INTERVAL + PONG_TIMEOUT + WRITE_TIMEOUT),
            last_pong_age_seconds: age.map(|age| age.as_secs()),
        }
    }
}

pub struct MonitoredSocket<S> {
    stream: WebSocketStream<S>,
    health: SocketHealth,
    ticks: Interval,
    pending: Option<(Vec<u8>, tokio::time::Instant)>,
    sequence: u64,
    pong_timeout: Duration,
}

impl<S: AsyncRead + AsyncWrite + Unpin> MonitoredSocket<S> {
    pub fn new(stream: WebSocketStream<S>, health: SocketHealth) -> Self {
        Self {
            stream,
            health,
            ticks: interval(PING_INTERVAL),
            pending: None,
            sequence: 0,
            pong_timeout: PONG_TIMEOUT,
        }
    }

    pub async fn send(&mut self, message: Message) -> Result<()> {
        timeout(WRITE_TIMEOUT, self.stream.send(message))
            .await
            .context("Slack socket write timed out")?
            .context("Slack socket write failed")
    }

    pub async fn next(&mut self) -> Result<Option<Message>> {
        loop {
            let deadline = self
                .pending
                .as_ref()
                .map(|(_, deadline)| *deadline)
                .unwrap_or_else(|| tokio::time::Instant::now() + PING_INTERVAL);
            tokio::select! {
                biased;
                _ = sleep_until(deadline), if self.pending.is_some() => {
                    bail!("Slack socket heartbeat timed out waiting for pong");
                }
                _ = self.ticks.tick(), if self.pending.is_none() => {
                    self.sequence += 1;
                    let nonce = self.sequence.to_be_bytes().to_vec();
                    self.send(Message::Ping(nonce.clone())).await?;
                    self.pending = Some((nonce, tokio::time::Instant::now() + self.pong_timeout));
                }
                message = self.stream.next() => {
                    match message.transpose().context("Slack socket receive error")? {
                        Some(Message::Pong(payload)) => {
                            if self.pending.as_ref().is_some_and(|(nonce, _)| nonce.as_slice() == payload.as_slice()) {
                                *self.health.0.lock().expect("socket health lock poisoned") = Some(Instant::now());
                                self.pending = None;
                            }
                        }
                        Some(Message::Ping(payload)) => self.send(Message::Pong(payload)).await?,
                        message => return Ok(message),
                    }
                }
            }
        }
    }

    pub async fn close(&mut self) {
        let _ = timeout(WRITE_TIMEOUT, self.stream.close(None)).await;
    }
}

impl<S> Drop for MonitoredSocket<S> {
    fn drop(&mut self) {
        *self.health.0.lock().expect("socket health lock poisoned") = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_tungstenite::tungstenite::protocol::Role;

    async fn pair() -> (
        MonitoredSocket<tokio::io::DuplexStream>,
        WebSocketStream<tokio::io::DuplexStream>,
        SocketHealth,
    ) {
        let (a, b) = tokio::io::duplex(4096);
        let client = WebSocketStream::from_raw_socket(a, Role::Client, None).await;
        let server = WebSocketStream::from_raw_socket(b, Role::Server, None).await;
        let health = SocketHealth::default();
        let mut client = MonitoredSocket::new(client, health.clone());
        client.pong_timeout = Duration::from_millis(100);
        (client, server, health)
    }

    #[tokio::test]
    async fn silent_connection_times_out_even_when_tcp_stays_open() {
        let (mut client, _server, health) = pair().await;
        let error = timeout(Duration::from_secs(2), client.next())
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("heartbeat timed out"));
        assert!(!health.snapshot().healthy);
    }

    #[tokio::test]
    async fn matching_pong_updates_health_and_drop_clears_it() {
        let (mut client, mut server, health) = pair().await;
        let peer = tokio::spawn(async move {
            assert!(matches!(
                server.next().await.unwrap().unwrap(),
                Message::Ping(_)
            ));
            // Reading a ping queues tungstenite's automatic pong; flush it.
            server.flush().await.unwrap();
            server.send(Message::Text("event".into())).await.unwrap();
        });
        assert!(matches!(
            timeout(Duration::from_secs(2), client.next())
                .await
                .unwrap()
                .unwrap(),
            Some(Message::Text(_))
        ));
        assert!(health.snapshot().healthy);
        drop(client);
        assert!(!health.snapshot().healthy);
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn unrelated_pong_and_user_traffic_do_not_hide_dead_heartbeat() {
        let (mut client, mut server, health) = pair().await;
        // Send unsolicited data without reading the ping (which would queue an automatic pong).
        server.send(Message::Pong(vec![99])).await.unwrap();
        server.send(Message::Text("event".into())).await.unwrap();
        assert!(matches!(
            client.next().await.unwrap(),
            Some(Message::Text(_))
        ));
        assert!(!health.snapshot().healthy);
        let error = timeout(Duration::from_secs(2), client.next())
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("heartbeat timed out"));
    }
}
