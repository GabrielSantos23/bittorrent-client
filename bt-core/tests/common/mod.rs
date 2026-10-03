#![allow(dead_code)]

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bt_core::bencode::{self, Value};
use bt_core::engine::{BoxedStream, Dial};
use bt_core::error::PeerError;
use bt_core::metainfo::MetaInfo;
use bt_core::peer::{handshake, Bitfield, Handshake, Message, PeerConfig, PeerConnection};
use sha1::{Digest, Sha1};
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

pub const PIECE_LENGTH: usize = 16384;
pub const PIECE_COUNT: usize = 3;
pub const DATA_LENGTH: usize = PIECE_LENGTH * PIECE_COUNT;
pub const SEEDER_PEER_ID: [u8; 20] = *b"-SD0000-seeder000001";
pub const LEECHER_PEER_ID: [u8; 20] = *b"-LC0000-leecher00001";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeederKind {
    Good,
    CorruptOnce,
    Choking,
    NeverReads,
    Slow {
        delay: Duration,
        only_piece: Option<u32>,
    },
}

#[derive(Debug, Clone)]
pub struct SeederReport {
    pub addr: SocketAddr,
    pub blocks_served: usize,
    pub cancels_received: usize,
    pub last_piece_requests: usize,
}

#[derive(Debug, Clone)]
pub struct LeecherConfig {
    pub send_interested: bool,
    pub requests_after_unchoke: Vec<(u32, u32, u32)>,
    pub cancel_first: Option<(u32, u32, u32)>,
    pub deadline: Duration,
    pub request_blocks_of_all_pieces: bool,
}

impl Default for LeecherConfig {
    fn default() -> Self {
        LeecherConfig {
            send_interested: true,
            requests_after_unchoke: Vec::new(),
            cancel_first: None,
            deadline: Duration::from_secs(10),
            request_blocks_of_all_pieces: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct LeecherReport {
    pub saw_bitfield: bool,
    pub saw_unchoke: bool,
    pub saw_choke: bool,
    pub closed: bool,
    pub blocks: Vec<(u32, u32, Vec<u8>)>,
}

pub struct FakeLeecherDial {
    info_hash: [u8; 20],
    data: Arc<Vec<u8>>,
    piece_count: usize,
    config: LeecherConfig,
    reports: tokio::sync::mpsc::Sender<LeecherReport>,
}

impl FakeLeecherDial {
    pub fn new(
        info_hash: [u8; 20],
        data: Arc<Vec<u8>>,
        piece_count: usize,
        config: LeecherConfig,
    ) -> (FakeLeecherDial, tokio::sync::mpsc::Receiver<LeecherReport>) {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        (
            FakeLeecherDial {
                info_hash,
                data,
                piece_count,
                config,
                reports: tx,
            },
            rx,
        )
    }
}

impl Dial for FakeLeecherDial {
    fn dial(
        &self,
        _addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<BoxedStream>> + Send>> {
        let info_hash = self.info_hash;
        let data = self.data.clone();
        let piece_count = self.piece_count;
        let config = self.config.clone();
        let reports = self.reports.clone();
        Box::pin(async move {
            let (client_side, server_side) = duplex(256 * 1024);
            tokio::spawn(async move {
                let report =
                    run_leecher(Box::new(server_side), info_hash, data, piece_count, config).await;
                let _ = reports.send(report).await;
            });
            Ok(Box::new(client_side) as BoxedStream)
        })
    }
}

pub fn all_block_requests(data_length: usize, piece_count: usize) -> Vec<(u32, u32, u32)> {
    let mut requests = Vec::new();
    for index in 0..piece_count {
        let start = index * PIECE_LENGTH;
        let size = PIECE_LENGTH.min(data_length - start);
        let mut begin = 0;
        while begin < size {
            let length = BLOCK.min(size - begin);
            requests.push((index as u32, begin as u32, length as u32));
            begin += BLOCK;
        }
    }
    requests
}

const BLOCK: usize = 16 * 1024;

async fn send_requests(
    conn: &mut PeerConnection<BoxedStream>,
    report: &mut LeecherReport,
    config: &LeecherConfig,
    data: &Arc<Vec<u8>>,
    piece_count: usize,
) -> Option<usize> {
    let mut requests: Vec<(u32, u32, u32)> = Vec::new();
    if let Some(cancel) = config.cancel_first {
        requests.push(cancel);
    }
    if config.request_blocks_of_all_pieces {
        requests.extend(all_block_requests(data.len(), piece_count));
    }
    requests.extend(config.requests_after_unchoke.iter().copied());
    if let Some(cancel) = config.cancel_first {
        requests.retain(|request| (request.0, request.1, request.2) != cancel);
        if conn
            .write_message(&Message::Request {
                index: cancel.0,
                begin: cancel.1,
                length: cancel.2,
            })
            .await
            .is_err()
        {
            report.closed = true;
            return None;
        }
        if conn
            .write_message(&Message::Cancel {
                index: cancel.0,
                begin: cancel.1,
                length: cancel.2,
            })
            .await
            .is_err()
        {
            report.closed = true;
            return None;
        }
    }
    for (index, begin, length) in &requests {
        if conn
            .write_message(&Message::Request {
                index: *index,
                begin: *begin,
                length: *length,
            })
            .await
            .is_err()
        {
            report.closed = true;
            return None;
        }
    }
    Some(requests.len())
}

async fn run_leecher(
    stream: BoxedStream,
    info_hash: [u8; 20],
    data: Arc<Vec<u8>>,
    piece_count: usize,
    config: LeecherConfig,
) -> LeecherReport {
    let mut report = LeecherReport::default();
    let mut raw: BoxedStream = stream;
    let mut buffer = [0u8; 68];
    if raw.read_exact(&mut buffer).await.is_err() {
        report.closed = true;
        return report;
    }
    let remote = match handshake::decode(&buffer) {
        Ok(remote) => remote,
        Err(_) => {
            report.closed = true;
            return report;
        }
    };
    if remote.info_hash != info_hash {
        report.closed = true;
        return report;
    }
    let reply = handshake::encode(&Handshake {
        info_hash,
        reserved: [0; 8],
        peer_id: LEECHER_PEER_ID,
    });
    if raw.write_all(&reply).await.is_err() {
        report.closed = true;
        return report;
    }
    let config_for_conn = PeerConfig {
        read_timeout: Duration::from_secs(30),
        ..PeerConfig::default()
    };
    let mut conn = PeerConnection::new(raw, remote, piece_count, config_for_conn);
    if config.send_interested && conn.write_message(&Message::Interested).await.is_err() {
        report.closed = true;
        return report;
    }
    let mut requested = false;
    let mut expected: Option<usize> = None;
    if !config.send_interested {
        let sent = send_requests(&mut conn, &mut report, &config, &data, piece_count).await;
        if sent.is_none() {
            return report;
        }
    }
    let deadline = tokio::time::Instant::now() + config.deadline;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let message = match tokio::time::timeout(remaining, conn.read_message()).await {
            Ok(Ok(message)) => message,
            Ok(Err(PeerError::ConnectionClosed)) => {
                report.closed = true;
                break;
            }
            Ok(Err(_)) => break,
            Err(_) => break,
        };
        match message {
            Message::Bitfield(_) => report.saw_bitfield = true,
            Message::Unchoke => {
                report.saw_unchoke = true;
                if !requested {
                    requested = true;
                    expected =
                        send_requests(&mut conn, &mut report, &config, &data, piece_count).await;
                    if expected == Some(0) {
                        break;
                    }
                }
            }
            Message::Choke => report.saw_choke = true,
            Message::Piece {
                index,
                begin,
                block,
            } => {
                report.blocks.push((index, begin, block));
                if let Some(expected) = expected {
                    if report.blocks.len() >= expected {
                        return report;
                    }
                }
            }
            _ => {}
        }
    }
    report
}

pub struct FakeDial {
    info_hash: [u8; 20],
    data: Arc<Vec<u8>>,
    piece_count: usize,
    peers: Mutex<HashMap<SocketAddr, SeederKind>>,
    reports: Option<tokio::sync::mpsc::Sender<SeederReport>>,
}

impl FakeDial {
    pub fn new(
        info_hash: [u8; 20],
        data: Arc<Vec<u8>>,
        piece_count: usize,
        peers: Vec<(SocketAddr, SeederKind)>,
    ) -> FakeDial {
        FakeDial {
            info_hash,
            data,
            piece_count,
            peers: Mutex::new(peers.into_iter().collect()),
            reports: None,
        }
    }
}

impl FakeDial {
    pub fn with_reports(
        info_hash: [u8; 20],
        data: Arc<Vec<u8>>,
        piece_count: usize,
        peers: Vec<(SocketAddr, SeederKind)>,
    ) -> (FakeDial, tokio::sync::mpsc::Receiver<SeederReport>) {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let dial = FakeDial {
            info_hash,
            data,
            piece_count,
            peers: Mutex::new(peers.into_iter().collect()),
            reports: Some(tx),
        };
        (dial, rx)
    }
}

impl Dial for FakeDial {
    fn dial(
        &self,
        addr: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = std::io::Result<BoxedStream>> + Send>> {
        let kind = self.peers.lock().unwrap().get(&addr).copied();
        let info_hash = self.info_hash;
        let data = self.data.clone();
        let piece_count = self.piece_count;
        let reports = self.reports.clone();
        Box::pin(async move {
            let Some(kind) = kind else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "no fake peer registered",
                ));
            };
            let (client_side, server_side) = duplex(256 * 1024);
            tokio::spawn(run_seeder(
                Box::new(server_side),
                addr,
                info_hash,
                data,
                piece_count,
                kind,
                reports,
            ));
            Ok(Box::new(client_side) as BoxedStream)
        })
    }
}

pub fn torrent_bytes(data: &[u8]) -> Vec<u8> {
    let mut pieces = Vec::new();
    for chunk in data.chunks(PIECE_LENGTH) {
        let digest: [u8; 20] = Sha1::digest(chunk).into();
        pieces.extend_from_slice(&digest);
    }
    let mut info = std::collections::BTreeMap::new();
    info.insert(b"length".to_vec(), Value::Int(data.len() as i64));
    info.insert(b"name".to_vec(), Value::Bytes(b"e2e.bin".to_vec()));
    info.insert(b"piece length".to_vec(), Value::Int(PIECE_LENGTH as i64));
    info.insert(b"pieces".to_vec(), Value::Bytes(pieces));
    let mut root = std::collections::BTreeMap::new();
    root.insert(b"info".to_vec(), Value::Dict(info));
    bencode::encode(&Value::Dict(root))
}

pub fn torrent_meta(data: &[u8]) -> MetaInfo {
    let mut pieces = Vec::new();
    for chunk in data.chunks(PIECE_LENGTH) {
        let digest: [u8; 20] = Sha1::digest(chunk).into();
        pieces.extend_from_slice(&digest);
    }
    let mut info = std::collections::BTreeMap::new();
    info.insert(b"length".to_vec(), Value::Int(data.len() as i64));
    info.insert(b"name".to_vec(), Value::Bytes(b"e2e.bin".to_vec()));
    info.insert(b"piece length".to_vec(), Value::Int(PIECE_LENGTH as i64));
    info.insert(b"pieces".to_vec(), Value::Bytes(pieces));
    let mut root = std::collections::BTreeMap::new();
    root.insert(b"info".to_vec(), Value::Dict(info));
    let raw = bencode::encode(&Value::Dict(root));
    MetaInfo::from_bytes(&raw).unwrap()
}

pub fn test_data() -> Arc<Vec<u8>> {
    Arc::new((0..DATA_LENGTH).map(|i| (i % 251) as u8).collect())
}

pub fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("bt-core-{}-{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

async fn run_seeder(
    stream: BoxedStream,
    addr: SocketAddr,
    info_hash: [u8; 20],
    data: Arc<Vec<u8>>,
    piece_count: usize,
    kind: SeederKind,
    reports: Option<tokio::sync::mpsc::Sender<SeederReport>>,
) {
    let mut report = SeederReport {
        addr,
        blocks_served: 0,
        cancels_received: 0,
        last_piece_requests: 0,
    };
    serve_seeder(stream, info_hash, data, piece_count, kind, &mut report).await;
    if let Some(sender) = reports {
        let _ = sender.send(report).await;
    }
}

async fn serve_seeder(
    stream: BoxedStream,
    info_hash: [u8; 20],
    data: Arc<Vec<u8>>,
    piece_count: usize,
    kind: SeederKind,
    report: &mut SeederReport,
) {
    let mut conn = PeerConnection::connect_stream(
        stream,
        info_hash,
        SEEDER_PEER_ID,
        piece_count,
        PeerConfig::default(),
    )
    .await
    .unwrap();
    let mut bitfield = Bitfield::new(piece_count);
    for index in 0..piece_count {
        bitfield.set(index).unwrap();
    }
    conn.write_message(&Message::Bitfield(bitfield))
        .await
        .unwrap();
    conn.write_message(&Message::Unchoke).await.unwrap();
    if kind == SeederKind::NeverReads {
        std::future::pending::<()>().await;
    }
    let mut corrupted = false;
    let mut served = 0usize;
    struct PendingSend {
        deadline: tokio::time::Instant,
        message: Message,
        key: (u32, u32, u32),
    }
    let mut pending: Vec<PendingSend> = Vec::new();
    loop {
        let wake = async {
            let earliest = pending.iter().map(|send| send.deadline).min();
            match earliest {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };
        let message = tokio::select! {
            _ = wake => {
                let now = tokio::time::Instant::now();
                let mut due: Vec<(usize, Message)> = Vec::new();
                pending.retain(|send| {
                    if send.deadline <= now {
                        due.push((0, send.message.clone()));
                        false
                    } else {
                        true
                    }
                });
                for (_, message) in due {
                    report.blocks_served += 1;
                    if conn.write_message(&message).await.is_err() {
                        return;
                    }
                }
                continue;
            }
            read = conn.read_message() => match read {
                Ok(message) => message,
                Err(_) => return,
            },
        };
        match message {
            Message::Request {
                index,
                begin,
                length,
            } => {
                let start = index as usize * PIECE_LENGTH + begin as usize;
                let mut block = data[start..start + length as usize].to_vec();
                if index as usize == piece_count - 1 {
                    report.last_piece_requests += 1;
                }
                let slow_delay = match kind {
                    SeederKind::Slow { delay, only_piece } => {
                        if only_piece.is_none_or(|piece| piece == index) {
                            Some(delay)
                        } else {
                            None
                        }
                    }
                    _ => None,
                };
                if let Some(delay) = slow_delay {
                    pending.push(PendingSend {
                        deadline: tokio::time::Instant::now() + delay,
                        message: Message::Piece {
                            index,
                            begin,
                            block,
                        },
                        key: (index, begin, length),
                    });
                    continue;
                }
                match kind {
                    SeederKind::Good | SeederKind::NeverReads => {}
                    SeederKind::Slow { .. } => {}
                    SeederKind::CorruptOnce => {
                        if index == 1 && !corrupted {
                            block[0] ^= 0xFF;
                            corrupted = true;
                        }
                    }
                    SeederKind::Choking => {
                        if served >= 2 {
                            if conn.write_message(&Message::Choke).await.is_err() {
                                return;
                            }
                            continue;
                        }
                        served += 1;
                    }
                }
                report.blocks_served += 1;
                if conn
                    .write_message(&Message::Piece {
                        index,
                        begin,
                        block,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Message::Cancel {
                index,
                begin,
                length,
            } => {
                let before = pending.len();
                pending.retain(|send| send.key != (index, begin, length));
                if pending.len() != before {
                    report.cancels_received += 1;
                }
            }
            Message::Interested | Message::KeepAlive | Message::NotInterested => {}
            _ => {}
        }
    }
}
