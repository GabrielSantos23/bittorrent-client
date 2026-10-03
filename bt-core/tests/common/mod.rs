#![allow(dead_code)]

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use bt_core::bencode::{self, Value};
use bt_core::engine::{BoxedStream, Dial};
use bt_core::metainfo::MetaInfo;
use bt_core::peer::{Bitfield, Message, PeerConfig, PeerConnection};
use sha1::{Digest, Sha1};
use tokio::io::duplex;

pub const PIECE_LENGTH: usize = 16384;
pub const PIECE_COUNT: usize = 3;
pub const DATA_LENGTH: usize = PIECE_LENGTH * PIECE_COUNT;
pub const SEEDER_PEER_ID: [u8; 20] = *b"-SD0000-seeder000001";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeederKind {
    Good,
    CorruptOnce,
    Choking,
    NeverReads,
}

pub struct FakeDial {
    info_hash: [u8; 20],
    data: Arc<Vec<u8>>,
    piece_count: usize,
    peers: Mutex<HashMap<SocketAddr, SeederKind>>,
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
        }
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
                info_hash,
                data,
                piece_count,
                kind,
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
    info_hash: [u8; 20],
    data: Arc<Vec<u8>>,
    piece_count: usize,
    kind: SeederKind,
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
    loop {
        let Ok(message) = conn.read_message().await else {
            break;
        };
        match message {
            Message::Request {
                index,
                begin,
                length,
            } => {
                let start = index as usize * PIECE_LENGTH + begin as usize;
                let mut block = data[start..start + length as usize].to_vec();
                match kind {
                    SeederKind::Good | SeederKind::NeverReads => {}
                    SeederKind::CorruptOnce => {
                        if index == 1 && !corrupted {
                            block[0] ^= 0xFF;
                            corrupted = true;
                        }
                    }
                    SeederKind::Choking => {
                        if served >= 2 {
                            conn.write_message(&Message::Choke).await.unwrap();
                            continue;
                        }
                        served += 1;
                    }
                }
                conn.write_message(&Message::Piece {
                    index,
                    begin,
                    block,
                })
                .await
                .unwrap();
            }
            Message::Interested | Message::KeepAlive | Message::NotInterested => {}
            _ => {}
        }
    }
}
