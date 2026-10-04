use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::AbortHandle;
use tokio::time::timeout;

use crate::engine::BoxedStream;
use crate::peer::handshake::{self, Handshake, HANDSHAKE_LENGTH};
use crate::peer_id;

pub const DEFAULT_LISTEN_PORT: u16 = 6881;
const DEFAULT_GLOBAL_PENDING: usize = 64;
const DEFAULT_PER_IP_PENDING: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ts_rs::TS)]
#[ts(export)]
pub struct ListenerStatus {
    pub active: bool,
    pub port: u16,
    pub error: Option<String>,
}

pub struct Incoming {
    pub addr: SocketAddr,
    pub remote: Handshake,
    pub stream: BoxedStream,
}

impl std::fmt::Debug for Incoming {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Incoming")
            .field("addr", &self.addr)
            .field("remote", &self.remote)
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
pub struct Registry {
    inner: Mutex<HashMap<[u8; 20], mpsc::Sender<Incoming>>>,
}

impl Registry {
    pub fn register(&self, info_hash: [u8; 20], sender: mpsc::Sender<Incoming>) {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(info_hash, sender);
    }

    pub fn unregister(&self, info_hash: &[u8; 20]) {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(info_hash);
    }

    fn route(&self, info_hash: &[u8; 20]) -> Option<mpsc::Sender<Incoming>> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(info_hash)
            .cloned()
    }
}

#[derive(Clone)]
pub struct ListenerOptions {
    pub port: u16,
    pub handshake_timeout: Duration,
    pub global_pending: usize,
    pub per_ip_pending: usize,
    pub our_peer_id: [u8; 20],
    pub dht_active: Arc<AtomicBool>,
}

impl Default for ListenerOptions {
    fn default() -> Self {
        ListenerOptions {
            port: DEFAULT_LISTEN_PORT,
            handshake_timeout: Duration::from_secs(10),
            global_pending: DEFAULT_GLOBAL_PENDING,
            per_ip_pending: DEFAULT_PER_IP_PENDING,
            our_peer_id: *peer_id::session(),
            dht_active: Arc::new(AtomicBool::new(false)),
        }
    }
}

pub struct Listener {
    status: watch::Receiver<ListenerStatus>,
    abort: AbortHandle,
}

impl Listener {
    pub fn status(&self) -> watch::Receiver<ListenerStatus> {
        self.status.clone()
    }

    pub fn shutdown(&self) {
        self.abort.abort();
    }
}

pub fn spawn(options: ListenerOptions, registry: Arc<Registry>) -> Listener {
    let (status_tx, status_rx) = watch::channel(ListenerStatus {
        active: false,
        port: options.port,
        error: None,
    });
    let task = tokio::spawn(run(options, registry, status_tx));
    Listener {
        status: status_rx,
        abort: task.abort_handle(),
    }
}

pub async fn bind(options: ListenerOptions, registry: Arc<Registry>) -> Result<Listener, String> {
    let bound = TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, options.port))
        .await
        .map_err(|err| format!("cannot bind listen port {}: {err}", options.port))?;
    let (status_tx, status_rx) = watch::channel(ListenerStatus {
        active: false,
        port: options.port,
        error: None,
    });
    let task = tokio::spawn(run_bound(bound, options, registry, status_tx));
    Ok(Listener {
        status: status_rx,
        abort: task.abort_handle(),
    })
}

#[derive(Default)]
struct PendingState {
    total: usize,
    per_ip: HashMap<IpAddr, usize>,
}

async fn run(
    options: ListenerOptions,
    registry: Arc<Registry>,
    status: watch::Sender<ListenerStatus>,
) {
    let bound = TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, options.port)).await;
    match bound {
        Ok(listener) => {
            let _ = run_bound(listener, options, registry, status).await;
        }
        Err(err) => {
            let _ = status.send(ListenerStatus {
                active: false,
                port: options.port,
                error: Some(format!("cannot bind listen port {}: {err}", options.port)),
            });
        }
    }
}

async fn run_bound(
    listener: TcpListener,
    options: ListenerOptions,
    registry: Arc<Registry>,
    status: watch::Sender<ListenerStatus>,
) {
    let port = listener
        .local_addr()
        .map(|addr| addr.port())
        .unwrap_or(options.port);
    let _ = status.send(ListenerStatus {
        active: true,
        port,
        error: None,
    });
    let pending = Arc::new(Mutex::new(PendingState::default()));
    loop {
        tokio::select! {
            _ = status.closed() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, addr)) => {
                    if !reserve_pending(&pending, addr.ip(), &options) {
                        continue;
                    }
                    let state = PendingConnection {
                        registry: registry.clone(),
                        pending: pending.clone(),
                        ip: addr.ip(),
                        options: options.clone(),
                    };
                    tokio::spawn(handle_connection(stream, addr, state));
                }
                Err(_) => continue,
            },
        }
    }
}

struct PendingConnection {
    registry: Arc<Registry>,
    pending: Arc<Mutex<PendingState>>,
    ip: IpAddr,
    options: ListenerOptions,
}

impl PendingConnection {
    fn release(self) {
        let mut state = self
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.total = state.total.saturating_sub(1);
        if let Some(count) = state.per_ip.get_mut(&self.ip) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.per_ip.remove(&self.ip);
            }
        }
    }
}

fn reserve_pending(state: &Mutex<PendingState>, ip: IpAddr, options: &ListenerOptions) -> bool {
    let mut guard = state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if guard.total >= options.global_pending {
        return false;
    }
    let per_ip = guard.per_ip.get(&ip).copied().unwrap_or(0);
    if per_ip >= options.per_ip_pending {
        return false;
    }
    guard.total += 1;
    guard.per_ip.insert(ip, per_ip + 1);
    true
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    addr: SocketAddr,
    state: PendingConnection,
) {
    let _ = negotiate(stream, addr, &state).await;
    state.release();
}

enum NegotiateError {
    Timeout,
    #[allow(dead_code)]
    Io(std::io::Error),
    Protocol,
    SelfConnection,
    UnknownInfoHash,
    RouteUnavailable,
}

async fn negotiate(
    stream: tokio::net::TcpStream,
    addr: SocketAddr,
    state: &PendingConnection,
) -> Result<(), NegotiateError> {
    let mut stream = stream;
    let mut buffer = [0u8; HANDSHAKE_LENGTH];
    match timeout(
        state.options.handshake_timeout,
        stream.read_exact(&mut buffer),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(err)) => return Err(NegotiateError::Io(err)),
        Err(_) => return Err(NegotiateError::Timeout),
    }
    let remote = handshake::decode(&buffer).map_err(|_| NegotiateError::Protocol)?;
    if remote.peer_id == state.options.our_peer_id {
        return Err(NegotiateError::SelfConnection);
    }
    let Some(sender) = state.registry.route(&remote.info_hash) else {
        return Err(NegotiateError::UnknownInfoHash);
    };
    let reserved = if state.options.dht_active.load(Ordering::Relaxed) {
        crate::extensions::with_dht_bit([0; 8])
    } else {
        [0; 8]
    };
    let reply = handshake::encode(&Handshake {
        info_hash: remote.info_hash,
        reserved,
        peer_id: state.options.our_peer_id,
    });
    stream.write_all(&reply).await.map_err(NegotiateError::Io)?;
    let incoming = Incoming {
        addr,
        remote,
        stream: Box::new(stream),
    };
    sender
        .try_send(incoming)
        .map_err(|_| NegotiateError::RouteUnavailable)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer_id;
    use std::time::Instant;

    fn options(
        port: u16,
        handshake_timeout: Duration,
        global: usize,
        per_ip: usize,
    ) -> ListenerOptions {
        ListenerOptions {
            port,
            handshake_timeout,
            global_pending: global,
            per_ip_pending: per_ip,
            ..ListenerOptions::default()
        }
    }

    fn handshake_bytes(info_hash: [u8; 20], peer_id: [u8; 20]) -> [u8; 68] {
        handshake::encode(&Handshake {
            info_hash,
            reserved: [0; 8],
            peer_id,
        })
    }

    async fn connect_and_send(addr: SocketAddr, bytes: &[u8; 68]) -> tokio::net::TcpStream {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.write_all(bytes).await.unwrap();
        stream
    }

    #[tokio::test]
    async fn accepts_and_routes_known_info_hash() {
        let registry = Arc::new(Registry::default());
        let (tx, mut rx) = mpsc::channel(4);
        registry.register([7u8; 20], tx);
        let listener = spawn(options(0, Duration::from_secs(5), 16, 8), registry);
        let status = listener.status();
        let mut status = status;
        while !status.borrow().active {
            if status.changed().await.is_err() {
                panic!("listener never became active");
            }
        }
        let port = status.borrow().port;
        let mut peer = connect_and_send(
            SocketAddr::from(([127, 0, 0, 1], port)),
            &handshake_bytes([7u8; 20], [9u8; 20]),
        )
        .await;
        let mut reply = [0u8; 68];
        peer.read_exact(&mut reply).await.unwrap();
        let echoed = handshake::decode(&reply).unwrap();
        assert_eq!(echoed.info_hash, [7u8; 20]);
        assert_eq!(echoed.peer_id, *peer_id::session());
        let incoming = rx.recv().await.unwrap();
        assert_eq!(incoming.remote.peer_id, [9u8; 20]);
        listener.shutdown();
    }

    #[tokio::test]
    async fn closes_unknown_info_hash_without_reply() {
        let registry = Arc::new(Registry::default());
        let listener = spawn(options(0, Duration::from_secs(5), 16, 8), registry);
        let mut status = listener.status();
        while !status.borrow().active {
            if status.changed().await.is_err() {
                panic!("listener never became active");
            }
        }
        let port = status.borrow().port;
        let mut peer = connect_and_send(
            SocketAddr::from(([127, 0, 0, 1], port)),
            &handshake_bytes([1u8; 20], [2u8; 20]),
        )
        .await;
        let mut reply = [0u8; 68];
        let result = timeout(Duration::from_secs(2), peer.read_exact(&mut reply)).await;
        assert!(matches!(result, Ok(Err(_)) | Err(_)));
        listener.shutdown();
    }

    #[tokio::test]
    async fn drops_connections_that_never_handshake() {
        let registry = Arc::new(Registry::default());
        let listener = spawn(options(0, Duration::from_millis(150), 16, 8), registry);
        let mut status = listener.status();
        while !status.borrow().active {
            if status.changed().await.is_err() {
                panic!("listener never became active");
            }
        }
        let port = status.borrow().port;
        let mut peer = tokio::net::TcpStream::connect(SocketAddr::from(([127, 0, 0, 1], port)))
            .await
            .unwrap();
        let mut byte = [0u8; 1];
        let start = Instant::now();
        let result = timeout(Duration::from_secs(2), peer.read_exact(&mut byte)).await;
        assert!(matches!(result, Ok(Err(_)) | Err(_)));
        assert!(start.elapsed() < Duration::from_secs(2));
        listener.shutdown();
    }

    #[tokio::test]
    async fn enforces_per_ip_pending_cap() {
        let registry = Arc::new(Registry::default());
        let (tx, _rx) = mpsc::channel(4);
        registry.register([7u8; 20], tx);
        let listener = spawn(options(0, Duration::from_secs(30), 16, 1), registry);
        let mut status = listener.status();
        while !status.borrow().active {
            if status.changed().await.is_err() {
                panic!("listener never became active");
            }
        }
        let port = status.borrow().port;
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let holder = tokio::net::TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut second = connect_and_send(addr, &handshake_bytes([7u8; 20], [3u8; 20])).await;
        let mut reply = [0u8; 68];
        let result = timeout(Duration::from_millis(500), second.read_exact(&mut reply)).await;
        assert!(matches!(result, Ok(Err(_)) | Err(_)));
        drop(holder);
        listener.shutdown();
    }

    #[tokio::test]
    async fn enforces_global_pending_cap() {
        let registry = Arc::new(Registry::default());
        let (tx, _rx) = mpsc::channel(4);
        registry.register([7u8; 20], tx);
        let listener = spawn(options(0, Duration::from_secs(30), 1, 16), registry);
        let mut status = listener.status();
        while !status.borrow().active {
            if status.changed().await.is_err() {
                panic!("listener never became active");
            }
        }
        let port = status.borrow().port;
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let holder = tokio::net::TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut second = connect_and_send(addr, &handshake_bytes([7u8; 20], [3u8; 20])).await;
        let mut reply = [0u8; 68];
        let result = timeout(Duration::from_millis(500), second.read_exact(&mut reply)).await;
        assert!(matches!(result, Ok(Err(_)) | Err(_)));
        drop(holder);
        listener.shutdown();
    }

    #[tokio::test]
    async fn reports_bind_failure_gracefully() {
        let blocker = std::net::TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, 0)).unwrap();
        let port = blocker.local_addr().unwrap().port();
        let listener = spawn(
            options(port, Duration::from_secs(5), 16, 8),
            Arc::new(Registry::default()),
        );
        let mut status = listener.status();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let snapshot = status.borrow().clone();
            if snapshot.error.is_some() {
                assert!(!snapshot.active);
                assert_eq!(snapshot.port, port);
                assert!(snapshot.error.clone().unwrap().contains(&port.to_string()));
                break;
            }
            assert!(Instant::now() < deadline, "bind failure was never reported");
            if timeout(Duration::from_millis(500), status.changed())
                .await
                .is_err()
            {
                continue;
            }
        }
        listener.shutdown();
    }
}
