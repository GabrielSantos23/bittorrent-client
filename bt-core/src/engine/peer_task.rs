use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;
use tokio::time::{interval_at, sleep_until, timeout, Instant as TokioInstant, MissedTickBehavior};

use crate::error::PeerError;
use crate::peer::handshake::Handshake;
use crate::peer::{Bitfield, Message, PeerConfig, PeerConnection};
use crate::ratelimit::UploadBucket;

use super::storage::Storage;

pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send + ?Sized> Stream for T {}

pub type BoxedStream = Box<dyn Stream>;

pub type HaveMap = Arc<RwLock<Bitfield>>;

#[derive(Clone, Copy)]
pub struct DhtHandshake {
    pub reserved: [u8; 8],
    pub our_port: Option<u16>,
}

impl Default for DhtHandshake {
    fn default() -> Self {
        DhtHandshake {
            reserved: crate::extensions::reserved_with_extensions(),
            our_port: None,
        }
    }
}

const UNPRODUCTIVE_TIMEOUT: Duration = Duration::from_secs(120);
pub const MAX_REQUEST_LENGTH: usize = 16 * 1024;
pub const MAX_TOLERATED_LENGTH: usize = 32 * 1024;
const SERVE_QUEUE_CAPACITY: usize = 64;
const WRITE_QUEUE_CAPACITY: usize = 64;

pub trait Dial: Send + Sync + 'static {
    fn dial(
        &self,
        addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<BoxedStream>> + Send>>;
}

pub struct TcpDial {
    connect_timeout: Duration,
}

impl TcpDial {
    pub fn new(connect_timeout: Duration) -> TcpDial {
        TcpDial { connect_timeout }
    }
}

impl Dial for TcpDial {
    fn dial(
        &self,
        addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<BoxedStream>> + Send>> {
        let connect_timeout = self.connect_timeout;
        Box::pin(async move {
            match timeout(connect_timeout, TcpStream::connect(addr)).await {
                Ok(Ok(stream)) => Ok(Box::new(stream) as BoxedStream),
                Ok(Err(err)) => Err(err),
                Err(_) => Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "connect timed out",
                )),
            }
        })
    }
}

#[derive(Debug)]
pub enum PeerCommand {
    Request { index: u32, begin: u32, length: u32 },
    Cancel { index: u32, begin: u32, length: u32 },
    Have(u32),
    Choke,
    Unchoke,
    Extended { extension_id: u8, payload: Vec<u8> },
    Stop,
}

#[derive(Debug)]
pub enum PeerEvent {
    Handshaken {
        addr: SocketAddr,
        peer_id: [u8; 20],
    },
    Bitfield {
        addr: SocketAddr,
        bitfield: Bitfield,
    },
    Have {
        addr: SocketAddr,
        index: u32,
    },
    Choke {
        addr: SocketAddr,
    },
    Unchoke {
        addr: SocketAddr,
    },
    Interested {
        addr: SocketAddr,
    },
    NotInterested {
        addr: SocketAddr,
    },
    Block {
        addr: SocketAddr,
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Uploaded {
        addr: SocketAddr,
        bytes: u64,
    },
    Extended {
        addr: SocketAddr,
        extension_id: u8,
        payload: Vec<u8>,
    },
    Port {
        addr: SocketAddr,
        port: u16,
    },
    Disconnected {
        addr: SocketAddr,
        reason: Option<String>,
    },
}

pub(crate) struct PeerTask {
    pub addr: SocketAddr,
    pub info_hash: [u8; 20],
    pub our_peer_id: [u8; 20],
    pub piece_count: Option<usize>,
    pub config: PeerConfig,
    pub dial: Arc<dyn Dial>,
    pub extension_handshake: Option<Vec<u8>>,
    pub have: HaveMap,
    pub storage: Option<Arc<Storage>>,
    pub uploads: Arc<UploadBucket>,
    pub dht: DhtHandshake,
}

pub(crate) struct IncomingPeer {
    pub addr: SocketAddr,
    pub piece_count: Option<usize>,
    pub config: PeerConfig,
    pub remote: Handshake,
    pub stream: BoxedStream,
    pub extension_handshake: Option<Vec<u8>>,
    pub have: HaveMap,
    pub storage: Option<Arc<Storage>>,
    pub uploads: Arc<UploadBucket>,
    pub dht: DhtHandshake,
}

pub(crate) async fn run_peer_task(
    task: PeerTask,
    mut commands: mpsc::Receiver<PeerCommand>,
    events: mpsc::Sender<PeerEvent>,
) {
    let reason = match connect_outgoing(&task).await {
        Ok((connection, remote)) => {
            let _ = events
                .send(PeerEvent::Handshaken {
                    addr: task.addr,
                    peer_id: remote.peer_id,
                })
                .await;
            let served = serve_established(
                task.addr,
                connection,
                task.piece_count,
                task.extension_handshake,
                remote.reserved,
                task.dht,
                task.have,
                task.storage,
                task.uploads,
                &mut commands,
                &events,
                task.config.keep_alive_interval,
            )
            .await;
            close_label(served.as_ref().err())
        }
        Err(err) => close_label(Some(&err)),
    };
    let _ = events
        .send(PeerEvent::Disconnected {
            addr: task.addr,
            reason: Some(reason),
        })
        .await;
}

pub(crate) async fn run_incoming_peer_task(
    task: IncomingPeer,
    mut commands: mpsc::Receiver<PeerCommand>,
    events: mpsc::Sender<PeerEvent>,
) {
    let connection = PeerConnection::new(task.stream, task.remote, task.piece_count, task.config);
    let remote_reserved = task.remote.reserved;
    let _ = events
        .send(PeerEvent::Handshaken {
            addr: task.addr,
            peer_id: task.remote.peer_id,
        })
        .await;
    let served = serve_established(
        task.addr,
        connection,
        task.piece_count,
        task.extension_handshake,
        remote_reserved,
        task.dht,
        task.have,
        task.storage,
        task.uploads,
        &mut commands,
        &events,
        task.config.keep_alive_interval,
    )
    .await;
    let _ = events
        .send(PeerEvent::Disconnected {
            addr: task.addr,
            reason: Some(close_label(served.as_ref().err())),
        })
        .await;
}

fn close_label(err: Option<&PeerError>) -> String {
    match err {
        None => "clean".to_string(),
        Some(PeerError::Io(err)) => format!("io-{:?}", err.kind()),
        Some(PeerError::Timeout) => "timeout".to_string(),
        Some(PeerError::ConnectionClosed) => "connection-closed".to_string(),
        Some(PeerError::OversizedMessage(_)) => "oversized-message".to_string(),
        Some(PeerError::Handshake(_)) => "handshake".to_string(),
        Some(PeerError::Message(_)) => "message".to_string(),
    }
}

async fn connect_outgoing(
    task: &PeerTask,
) -> Result<(PeerConnection<BoxedStream>, Handshake), PeerError> {
    let stream = task.dial.dial(task.addr).await?;
    let connection = PeerConnection::connect_stream(
        stream,
        task.info_hash,
        task.our_peer_id,
        task.piece_count,
        task.config,
        task.dht.reserved,
    )
    .await?;
    let remote = connection.remote();
    Ok((connection, remote))
}

#[allow(clippy::too_many_arguments)]
async fn serve_established<S>(
    addr: SocketAddr,
    connection: PeerConnection<S>,
    piece_count: Option<usize>,
    extension_handshake: Option<Vec<u8>>,
    remote_reserved: [u8; 8],
    dht: DhtHandshake,
    have: HaveMap,
    storage: Option<Arc<Storage>>,
    uploads: Arc<UploadBucket>,
    commands: &mut mpsc::Receiver<PeerCommand>,
    events: &mpsc::Sender<PeerEvent>,
    keep_alive_interval: Duration,
) -> Result<(), PeerError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let remote_extensions = crate::extensions::supports_extensions(&remote_reserved);
    let serving = storage
        .as_ref()
        .map(|storage| (storage.piece_length(), storage.total_length()));
    let (mut reader, writer) = connection.into_halves();
    let (write_tx, write_rx) = mpsc::channel::<Message>(WRITE_QUEUE_CAPACITY);
    let writer_task = tokio::spawn(writer_loop(writer, write_rx));
    let (serve_tx, serve_rx) = mpsc::channel::<ServeRequest>(SERVE_QUEUE_CAPACITY);
    let cancelled: Arc<Mutex<HashSet<(u32, u32, u32)>>> = Arc::new(Mutex::new(HashSet::new()));
    let serve_task = tokio::spawn(serve_loop(
        serve_rx,
        ServeContext {
            addr,
            storage: storage.clone(),
            uploads,
            write_tx: write_tx.clone(),
            cancelled: cancelled.clone(),
            events: events.clone(),
        },
    ));

    if crate::extensions::supports_dht(&remote_reserved) {
        if let Some(port) = dht.our_port {
            let _ = write_tx.send(Message::Port(port)).await;
        }
    }
    let snapshot = have
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    if snapshot.count() > 0 {
        let _ = write_tx.send(Message::Bitfield(snapshot.clone())).await;
    }
    let initial_interest = match piece_count {
        Some(count) if snapshot.count() >= count => Message::NotInterested,
        _ => Message::Interested,
    };
    let _ = write_tx.send(initial_interest).await;
    if let Some(payload) = extension_handshake.filter(|_| remote_extensions) {
        let _ = write_tx
            .send(Message::Extended {
                extension_id: crate::extensions::EXTENSION_HANDSHAKE_ID,
                payload,
            })
            .await;
    }

    let mut last_useful = TokioInstant::now();
    let mut keep_alive = interval_at(
        TokioInstant::now() + keep_alive_interval,
        keep_alive_interval,
    );
    keep_alive.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut we_choke = true;

    let outcome = loop {
        let deadline = last_useful + UNPRODUCTIVE_TIMEOUT;
        tokio::select! {
            _ = sleep_until(deadline) => break Ok(()),
            _ = keep_alive.tick() => {
                if write_tx.send(Message::KeepAlive).await.is_err() {
                    break Err(PeerError::ConnectionClosed);
                }
            }
            command = commands.recv() => match command {
                Some(PeerCommand::Request { index, begin, length }) => {
                    if write_tx.send(Message::Request { index, begin, length }).await.is_err() {
                        break Err(PeerError::ConnectionClosed);
                    }
                }
                Some(PeerCommand::Cancel { index, begin, length }) => {
                    if write_tx
                        .send(Message::Cancel { index, begin, length })
                        .await
                        .is_err()
                    {
                        break Err(PeerError::ConnectionClosed);
                    }
                }
                Some(PeerCommand::Have(index)) => {
                    if write_tx.send(Message::Have(index)).await.is_err() {
                        break Err(PeerError::ConnectionClosed);
                    }
                }
                Some(PeerCommand::Extended { extension_id, payload }) => {
                    if write_tx
                        .send(Message::Extended {
                            extension_id,
                            payload,
                        })
                        .await
                        .is_err()
                    {
                        break Err(PeerError::ConnectionClosed);
                    }
                }
                Some(PeerCommand::Choke) => {
                    we_choke = true;
                    if write_tx.send(Message::Choke).await.is_err() {
                        break Err(PeerError::ConnectionClosed);
                    }
                }
                Some(PeerCommand::Unchoke) => {
                    we_choke = false;
                    if write_tx.send(Message::Unchoke).await.is_err() {
                        break Err(PeerError::ConnectionClosed);
                    }
                }
                Some(PeerCommand::Stop) | None => break Ok(()),
            },
            message = reader.read_message() => match message? {
                Message::KeepAlive => {}
                Message::Choke => {
                    last_useful = TokioInstant::now();
                    let _ = events.send(PeerEvent::Choke { addr }).await;
                }
                Message::Unchoke => {
                    last_useful = TokioInstant::now();
                    let _ = events.send(PeerEvent::Unchoke { addr }).await;
                }
                Message::Bitfield(bitfield) => {
                    last_useful = TokioInstant::now();
                    let _ = events.send(PeerEvent::Bitfield { addr, bitfield }).await;
                }
                Message::Have(index) => {
                    last_useful = TokioInstant::now();
                    let _ = events.send(PeerEvent::Have { addr, index }).await;
                }
                Message::Interested => {
                    last_useful = TokioInstant::now();
                    let _ = events.send(PeerEvent::Interested { addr }).await;
                }
                Message::NotInterested => {
                    last_useful = TokioInstant::now();
                    let _ = events.send(PeerEvent::NotInterested { addr }).await;
                }
                Message::Piece { index, begin, block } => {
                    last_useful = TokioInstant::now();
                    let _ = events.send(PeerEvent::Block { addr, index, begin, block }).await;
                }
                Message::Request { index, begin, length } => {
                    last_useful = TokioInstant::now();
                    match decide_request(
                        we_choke,
                        ServeRequest { index, begin, length },
                        piece_count,
                        serving,
                        &have,
                    ) {
                        RequestDecision::Close => break Ok(()),
                        RequestDecision::Ignore => {}
                        RequestDecision::Serve => {
                            let request = ServeRequest { index, begin, length };
                            match serve_tx.try_send(request) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(_)) => {}
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    break Ok(());
                                }
                            }
                        }
                    }
                }
                Message::Cancel { index, begin, length } => {
                    last_useful = TokioInstant::now();
                    cancelled
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .insert((index, begin, length));
                }
                Message::Extended { extension_id, payload } => {
                    last_useful = TokioInstant::now();
                    let _ = events
                        .send(PeerEvent::Extended { addr, extension_id, payload })
                        .await;
                }
                Message::Unknown { .. } => {}
                Message::Port(port) => {
                    if crate::extensions::supports_dht(&dht.reserved) && port != 0 {
                        let _ = events.send(PeerEvent::Port { addr, port }).await;
                    }
                }
            },
        }
    };

    drop(serve_tx);
    serve_task.abort();
    drop(write_tx);
    let _ = writer_task.await;
    outcome
}

async fn writer_loop<W: AsyncWrite + Unpin>(
    mut writer: crate::peer::PeerWriteHalf<W>,
    mut rx: mpsc::Receiver<Message>,
) {
    while let Some(message) = rx.recv().await {
        if writer.write_message(&message).await.is_err() {
            break;
        }
    }
}

struct ServeRequest {
    index: u32,
    begin: u32,
    length: u32,
}

struct ServeContext {
    addr: SocketAddr,
    storage: Option<Arc<Storage>>,
    uploads: Arc<UploadBucket>,
    write_tx: mpsc::Sender<Message>,
    cancelled: Arc<Mutex<HashSet<(u32, u32, u32)>>>,
    events: mpsc::Sender<PeerEvent>,
}

async fn serve_loop(mut rx: mpsc::Receiver<ServeRequest>, ctx: ServeContext) {
    let ServeContext {
        addr,
        storage,
        uploads,
        write_tx,
        cancelled,
        events,
    } = ctx;
    while let Some(request) = rx.recv().await {
        let key = (request.index, request.begin, request.length);
        if is_cancelled(&cancelled, &key) {
            continue;
        }
        let Some(storage) = storage.clone() else {
            break;
        };
        let index = request.index;
        let begin = request.begin;
        let length = request.length;
        let read = spawn_blocking(move || {
            storage.read_block(index as usize, begin as usize, length as usize)
        })
        .await;
        if is_cancelled(&cancelled, &key) {
            continue;
        }
        match read {
            Ok(Ok(block)) => {
                uploads.acquire(block.len() as u64).await;
                if is_cancelled(&cancelled, &key) {
                    continue;
                }
                let sent = write_tx
                    .send(Message::Piece {
                        index: request.index,
                        begin: request.begin,
                        block,
                    })
                    .await;
                if sent.is_err() {
                    break;
                }
                let _ = events
                    .send(PeerEvent::Uploaded {
                        addr,
                        bytes: length as u64,
                    })
                    .await;
            }
            _ => break,
        }
    }
}

fn is_cancelled(cancelled: &Mutex<HashSet<(u32, u32, u32)>>, key: &(u32, u32, u32)) -> bool {
    cancelled
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(key)
}

enum RequestDecision {
    Serve,
    Ignore,
    Close,
}

fn decide_request(
    we_choke: bool,
    request: ServeRequest,
    piece_count: Option<usize>,
    serving: Option<(u32, u64)>,
    have: &HaveMap,
) -> RequestDecision {
    let (index, begin, length) = (request.index, request.begin, request.length);
    let Some((piece_length, total_length)) = serving else {
        return RequestDecision::Ignore;
    };
    let piece_count = match piece_count {
        Some(count) => count,
        None => return RequestDecision::Ignore,
    };
    if we_choke || length == 0 {
        return RequestDecision::Ignore;
    }
    if length as usize > MAX_TOLERATED_LENGTH {
        return RequestDecision::Close;
    }
    if length as usize > MAX_REQUEST_LENGTH {
        return RequestDecision::Ignore;
    }
    if index as usize >= piece_count {
        return RequestDecision::Ignore;
    }
    let Some(size) = piece_size(piece_length, total_length, index as usize) else {
        return RequestDecision::Ignore;
    };
    let begin = begin as usize;
    let length = length as usize;
    if begin >= size || length > size - begin {
        return RequestDecision::Ignore;
    }
    if !have
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(index as usize)
    {
        return RequestDecision::Ignore;
    }
    RequestDecision::Serve
}

fn piece_size(piece_length: u32, total_length: u64, index: usize) -> Option<usize> {
    let start = (index as u64).checked_mul(piece_length as u64)?;
    if start >= total_length {
        return None;
    }
    Some((total_length - start).min(piece_length as u64) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::BLOCK_SIZE;

    fn have_of(pieces: &[usize], piece_count: usize) -> HaveMap {
        let mut bitfield = Bitfield::new(piece_count);
        for &index in pieces {
            bitfield.set(index).unwrap();
        }
        Arc::new(RwLock::new(bitfield))
    }

    fn decide(
        we_choke: bool,
        index: u32,
        begin: u32,
        length: u32,
        have: &HaveMap,
    ) -> RequestDecision {
        decide_request(
            we_choke,
            ServeRequest {
                index,
                begin,
                length,
            },
            Some(3),
            Some((64 * 1024, 3 * 64 * 1024)),
            have,
        )
    }

    #[test]
    fn serves_valid_requests_for_verified_pieces() {
        let have = have_of(&[0], 3);
        assert!(matches!(
            decide(false, 0, 0, BLOCK_SIZE as u32, &have),
            RequestDecision::Serve
        ));
        assert!(matches!(
            decide(false, 0, BLOCK_SIZE as u32, BLOCK_SIZE as u32, &have),
            RequestDecision::Serve
        ));
    }

    #[test]
    fn ignores_requests_while_choked() {
        let have = have_of(&[0], 3);
        assert!(matches!(
            decide(true, 0, 0, BLOCK_SIZE as u32, &have),
            RequestDecision::Ignore
        ));
    }

    #[test]
    fn ignores_requests_for_unverified_pieces() {
        let have = have_of(&[0], 3);
        assert!(matches!(
            decide(false, 1, 0, BLOCK_SIZE as u32, &have),
            RequestDecision::Ignore
        ));
    }

    #[test]
    fn ignores_out_of_range_requests() {
        let have = have_of(&[0, 1, 2], 3);
        assert!(matches!(
            decide(false, 3, 0, BLOCK_SIZE as u32, &have),
            RequestDecision::Ignore
        ));
        assert!(matches!(
            decide(false, 0, 64 * 1024, BLOCK_SIZE as u32, &have),
            RequestDecision::Ignore
        ));
        assert!(matches!(
            decide(false, 0, 0, 0, &have),
            RequestDecision::Ignore
        ));
    }

    #[test]
    fn rejects_oversized_requests_by_closing() {
        let have = have_of(&[0], 3);
        assert!(matches!(
            decide(false, 0, 0, (MAX_TOLERATED_LENGTH + 1) as u32, &have),
            RequestDecision::Close
        ));
        assert!(matches!(
            decide(false, 0, 0, (MAX_REQUEST_LENGTH + 1) as u32, &have),
            RequestDecision::Ignore
        ));
    }
}
