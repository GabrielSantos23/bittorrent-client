use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{interval_at, sleep_until, timeout, Instant as TokioInstant, MissedTickBehavior};

use crate::error::PeerError;
use crate::peer::{Bitfield, Message, PeerConfig, PeerConnection};

pub trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + ?Sized> Stream for T {}

pub type BoxedStream = Box<dyn Stream>;

const UNPRODUCTIVE_TIMEOUT: Duration = Duration::from_secs(120);

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
    Have(u32),
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
    Block {
        addr: SocketAddr,
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Disconnected {
        addr: SocketAddr,
    },
}

pub(crate) struct PeerTask {
    pub addr: SocketAddr,
    pub info_hash: [u8; 20],
    pub our_peer_id: [u8; 20],
    pub piece_count: usize,
    pub config: PeerConfig,
    pub dial: Arc<dyn Dial>,
}

pub(crate) async fn run_peer_task(
    task: PeerTask,
    mut commands: mpsc::Receiver<PeerCommand>,
    events: mpsc::Sender<PeerEvent>,
) {
    let _ = serve(&task, &mut commands, &events).await;
    let _ = events
        .send(PeerEvent::Disconnected { addr: task.addr })
        .await;
}

async fn serve(
    task: &PeerTask,
    commands: &mut mpsc::Receiver<PeerCommand>,
    events: &mpsc::Sender<PeerEvent>,
) -> Result<(), PeerError> {
    let addr = task.addr;
    let stream = task.dial.dial(addr).await?;
    let mut conn = PeerConnection::connect_stream(
        stream,
        task.info_hash,
        task.our_peer_id,
        task.piece_count,
        task.config,
    )
    .await?;
    let _ = events
        .send(PeerEvent::Handshaken {
            addr,
            peer_id: conn.remote_peer_id(),
        })
        .await;
    conn.write_message(&Message::Interested).await?;
    let mut last_useful = TokioInstant::now();
    let mut keep_alive = interval_at(
        TokioInstant::now() + task.config.keep_alive_interval,
        task.config.keep_alive_interval,
    );
    keep_alive.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        let deadline = last_useful + UNPRODUCTIVE_TIMEOUT;
        tokio::select! {
            _ = sleep_until(deadline) => return Ok(()),
            _ = keep_alive.tick() => conn.send_keep_alive().await?,
            command = commands.recv() => match command {
                Some(PeerCommand::Request { index, begin, length }) => {
                    conn.write_message(&Message::Request { index, begin, length }).await?;
                }
                Some(PeerCommand::Have(index)) => {
                    conn.write_message(&Message::Have(index)).await?;
                }
                Some(PeerCommand::Stop) | None => return Ok(()),
            },
            message = conn.read_message() => match message? {
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
                Message::Piece { index, begin, block } => {
                    last_useful = TokioInstant::now();
                    let _ = events.send(PeerEvent::Block { addr, index, begin, block }).await;
                }
                _ => {}
            },
        }
    }
}
