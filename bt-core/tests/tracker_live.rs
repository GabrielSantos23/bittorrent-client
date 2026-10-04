use std::collections::BTreeMap;

use bt_core::bencode::{self, Value};
use bt_core::metainfo::MetaInfo;
use bt_core::peer_id;
use bt_core::tracker::{self, AnnounceRequest, Event};
use bt_core::tracker_udp::{UdpConfig, UdpTrackerClient};

const UDP_TRACKER_URL: &str = "udp://tracker.opentrackr.org:1337/announce";

fn debian_with_udp_announce() -> MetaInfo {
    let original = MetaInfo::from_bytes(FIXTURE).unwrap();
    let root = bencode::decode(FIXTURE).unwrap();
    let root = root.as_dict().unwrap();
    let info = root.get("info".as_bytes()).unwrap().clone();
    let mut tiers = vec![Value::List(vec![Value::Bytes(
        UDP_TRACKER_URL.as_bytes().to_vec(),
    )])];
    if let Some(existing) = root.get("announce-list".as_bytes()) {
        if let Some(items) = existing.as_list() {
            for tier in items {
                tiers.push(tier.clone());
            }
        }
    }
    let mut rebuilt = BTreeMap::new();
    rebuilt.insert(b"announce-list".to_vec(), Value::List(tiers));
    rebuilt.insert(b"info".to_vec(), info);
    let raw = bencode::encode(&Value::Dict(rebuilt));
    let meta = MetaInfo::from_bytes(&raw).unwrap();
    assert_eq!(meta.info_hash, original.info_hash);
    meta
}

#[tokio::test]
#[ignore = "requires internet access to a public UDP tracker"]
async fn announces_to_public_udp_tracker() {
    let meta = debian_with_udp_announce();
    let mut client = UdpTrackerClient::connect_tracker(UDP_TRACKER_URL, UdpConfig::default())
        .await
        .unwrap();
    let request = AnnounceRequest {
        info_hash: meta.info_hash,
        peer_id: *peer_id::session(),
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: meta.info.total_length().unwrap(),
        numwant: 50,
        event: Some(Event::Started),
    };
    let response = client.announce(&request, 50).await.unwrap();
    assert!(response.interval > 0);
    println!(
        "udp tracker answered: interval {} s, seeders {}, leechers {}, peers {}",
        response.interval,
        response.complete,
        response.incomplete,
        response.peers.len()
    );
}

const FIXTURE: &[u8] = include_bytes!("fixtures/debian-13.7.0-amd64-netinst.iso.torrent");

#[tokio::test]
#[ignore = "requires internet access to the live Debian tracker"]
async fn announces_to_debian_tracker() {
    let meta = MetaInfo::from_bytes(FIXTURE).unwrap();
    let request = AnnounceRequest {
        info_hash: meta.info_hash,
        peer_id: *peer_id::session(),
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: meta.info.total_length().unwrap(),
        numwant: 50,
        event: Some(Event::Started),
    };
    let client = tracker::http_client().unwrap();
    let outcome = tracker::announce(&client, &meta, &request).await.unwrap();
    assert_eq!(outcome.url, "http://bttracker.debian.org:6969/announce");
    assert!(outcome.response.interval > 0);
    assert!(!outcome.response.peers.is_empty());
}

#[tokio::test]
#[ignore = "requires internet access to the live Debian swarm"]
async fn magnet_metadata_and_download_from_live_swarm() {
    use bt_core::engine::{State, Torrent, TorrentOptions};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    const DEBIAN_HASH: &str = "7acf8fb590b2060dd9c3146ef770169d593433b0";
    let mut info_hash = [0u8; 20];
    for (index, chunk) in DEBIAN_HASH.as_bytes().chunks(2).enumerate() {
        info_hash[index] = u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap();
    }
    let magnet = crate_magnet(&info_hash);
    let dir = std::env::temp_dir().join(format!("bt-magnet-live-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let torrent = Torrent::spawn_from_magnet(
        magnet,
        dir.clone(),
        TorrentOptions {
            registry: Arc::new(bt_core::listener::Registry::default()),
            ..TorrentOptions::default()
        },
    )
    .await
    .unwrap();

    let mut stats = torrent.subscribe();
    let metadata_deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let snapshot = stats.borrow().clone();
        if snapshot.piece_count > 0 {
            println!(
                "metadata arrived: name {:?}, pieces {}, length {}",
                snapshot.name, snapshot.piece_count, snapshot.total_length
            );
            break;
        }
        println!("metadata diag: {:?}", snapshot.diag);
        assert!(
            Instant::now() < metadata_deadline,
            "metadata never arrived: {snapshot:?}"
        );
        assert!(stats.changed().await.is_ok());
    }
    let download_deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let snapshot = stats.borrow().clone();
        if matches!(snapshot.state, State::Downloading) && snapshot.verified_bytes > 0 {
            println!(
                "download started: name {:?}, verified {} bytes",
                snapshot.name, snapshot.verified_bytes
            );
            break;
        }
        assert!(
            Instant::now() < download_deadline,
            "download never started: {snapshot:?}"
        );
        assert!(stats.changed().await.is_ok());
    }
    torrent.stop().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

fn crate_magnet(info_hash: &[u8; 20]) -> bt_core::magnet::MagnetLink {
    bt_core::magnet::MagnetLink {
        info_hash: *info_hash,
        display_name: None,
        trackers: vec![
            "http://bttracker.debian.org:6969/announce".to_string(),
            "udp://tracker.opentrackr.org:1337/announce".to_string(),
        ],
        peers: Vec::new(),
    }
}

#[derive(Debug)]
struct ProbeReply {
    peer: std::net::SocketAddr,
    remote_ut_metadata_id: u8,
    remote_metadata_size: Option<u64>,
    remote_handshake_hex: String,
    reply_id: u8,
    reply_msg_type: Option<i64>,
    reply_piece: Option<u32>,
    reply_total_size: Option<u64>,
    reply_data_len: usize,
    reply_hex_prefix: String,
}

async fn probe_read_frame(
    stream: &mut tokio::net::TcpStream,
    deadline: std::time::Duration,
) -> Option<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut len_bytes = [0u8; 4];
    tokio::time::timeout(deadline, stream.read_exact(&mut len_bytes))
        .await
        .ok()?
        .ok()?;
    let len = u32::from_be_bytes(len_bytes) as usize;
    if len == 0 {
        return Some(Vec::new());
    }
    if len > 1024 * 1024 + 64 * 1024 {
        return None;
    }
    let mut frame = vec![0u8; len];
    tokio::time::timeout(deadline, stream.read_exact(&mut frame))
        .await
        .ok()?
        .ok()?;
    Some(frame)
}

fn probe_dict_int(dict: &std::collections::BTreeMap<Vec<u8>, Value>, key: &str) -> Option<i64> {
    dict.get(key.as_bytes()).and_then(Value::as_int)
}

#[derive(Debug, Clone, Copy)]
enum ProbeFailure {
    Connect,
    BaseHandshake,
    NoExtensionBit,
    NoRemoteHandshake,
    BadRemoteHandshake,
    NoReply,
    BadReply,
}

async fn probe_io<T>(
    io: impl std::future::Future<Output = std::io::Result<T>>,
    seconds: u64,
    failure: ProbeFailure,
) -> Result<T, ProbeFailure> {
    tokio::time::timeout(std::time::Duration::from_secs(seconds), io)
        .await
        .map_err(|_| failure)?
        .map_err(|_| failure)
}

async fn probe_exchange_with_peer(
    peer: std::net::SocketAddr,
    info_hash: [u8; 20],
) -> Result<ProbeReply, ProbeFailure> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    let mut stream =
        tokio::time::timeout(std::time::Duration::from_secs(3), TcpStream::connect(peer))
            .await
            .map_err(|_| ProbeFailure::Connect)?
            .map_err(|_| ProbeFailure::Connect)?;
    let mut reserved = [0u8; 8];
    reserved[5] |= 0x10;
    let mut handshake = [0u8; 68];
    handshake[0] = 19;
    handshake[1..20].copy_from_slice(b"BitTorrent protocol");
    handshake[20..28].copy_from_slice(&reserved);
    handshake[28..48].copy_from_slice(&info_hash);
    handshake[48..68].copy_from_slice(peer_id::session());
    probe_io(stream.write_all(&handshake), 5, ProbeFailure::BaseHandshake).await?;
    let mut reply = [0u8; 68];
    probe_io(
        stream.read_exact(&mut reply),
        5,
        ProbeFailure::BaseHandshake,
    )
    .await?;
    if &reply[1..20] != b"BitTorrent protocol" || reply[28..48] != info_hash {
        return Err(ProbeFailure::BaseHandshake);
    }
    if reply[20 + 5] & 0x10 == 0 {
        return Err(ProbeFailure::NoExtensionBit);
    }

    let mut m: std::collections::BTreeMap<Vec<u8>, Value> = std::collections::BTreeMap::new();
    m.insert(b"ut_metadata".to_vec(), Value::Int(1));
    let mut root: std::collections::BTreeMap<Vec<u8>, Value> = std::collections::BTreeMap::new();
    root.insert(b"m".to_vec(), Value::Dict(m));
    root.insert(b"v".to_vec(), Value::Bytes(b"bt-core-probe".to_vec()));
    let our_dict = bencode::encode(&Value::Dict(root));
    let mut our_wire = Vec::new();
    our_wire.extend_from_slice(&((2 + our_dict.len()) as u32).to_be_bytes());
    our_wire.push(20);
    our_wire.push(0);
    our_wire.extend_from_slice(&our_dict);
    probe_io(stream.write_all(&our_wire), 5, ProbeFailure::BaseHandshake).await?;

    let read_deadline = std::time::Duration::from_secs(10);
    let mut remote_ut_metadata_id = None;
    let mut remote_metadata_size = None;
    let mut remote_handshake_hex = String::new();
    for _ in 0..16 {
        let frame = match probe_read_frame(&mut stream, read_deadline).await {
            Some(frame) => frame,
            None => return Err(ProbeFailure::NoRemoteHandshake),
        };
        if frame.is_empty() || frame[0] != 20 || frame.len() < 2 {
            continue;
        }
        if frame[1] != 0 {
            continue;
        }
        let payload = &frame[2..];
        if payload.first() != Some(&b'd') {
            return Err(ProbeFailure::BadRemoteHandshake);
        }
        let Ok(value) = bencode::decode(payload) else {
            return Err(ProbeFailure::BadRemoteHandshake);
        };
        let Some(dict) = value.as_dict() else {
            return Err(ProbeFailure::BadRemoteHandshake);
        };
        remote_ut_metadata_id = dict
            .get(b"m".as_slice())
            .and_then(Value::as_dict)
            .and_then(|m| m.get(b"ut_metadata".as_slice()))
            .and_then(Value::as_int)
            .and_then(|id| u8::try_from(id).ok());
        remote_metadata_size = probe_dict_int(dict, "metadata_size").map(|size| size as u64);
        remote_handshake_hex = bt_core::hex::encode(payload);
        break;
    }
    let remote_id = remote_ut_metadata_id.ok_or(ProbeFailure::NoRemoteHandshake)?;

    let request_dict = b"d8:msg_typei0e5:piecei0ee";
    let mut request_wire = Vec::new();
    request_wire.extend_from_slice(&((2 + request_dict.len()) as u32).to_be_bytes());
    request_wire.push(20);
    request_wire.push(remote_id);
    request_wire.extend_from_slice(request_dict);
    probe_io(stream.write_all(&request_wire), 5, ProbeFailure::NoReply).await?;

    let reply_deadline = std::time::Duration::from_secs(20);
    for _ in 0..32 {
        let frame = match probe_read_frame(&mut stream, reply_deadline).await {
            Some(frame) => frame,
            None => return Err(ProbeFailure::NoReply),
        };
        if frame.is_empty() || frame[0] != 20 || frame.len() < 2 {
            continue;
        }
        if frame[1] == 0 {
            continue;
        }
        let payload = &frame[2..];
        if payload.first() != Some(&b'd') {
            return Err(ProbeFailure::BadReply);
        }
        let Ok((value, dict_end)) = bencode::decode_prefix(payload) else {
            return Err(ProbeFailure::BadReply);
        };
        let Some(dict) = value.as_dict() else {
            return Err(ProbeFailure::BadReply);
        };
        return Ok(ProbeReply {
            peer,
            remote_ut_metadata_id: remote_id,
            remote_metadata_size,
            remote_handshake_hex,
            reply_id: frame[1],
            reply_msg_type: probe_dict_int(dict, "msg_type"),
            reply_piece: probe_dict_int(dict, "piece").map(|piece| piece as u32),
            reply_total_size: probe_dict_int(dict, "total_size").map(|size| size as u64),
            reply_data_len: payload.len() - dict_end,
            reply_hex_prefix: bt_core::hex::encode(&payload[..payload.len().min(48)]),
        });
    }
    Err(ProbeFailure::NoReply)
}

#[tokio::test]
#[ignore = "requires internet access to a live swarm for a wire-level extension probe"]
async fn probe_live_extension_exchange() {
    let meta = debian_with_udp_announce();
    let mut client = UdpTrackerClient::connect_tracker(UDP_TRACKER_URL, UdpConfig::default())
        .await
        .unwrap();
    let request = AnnounceRequest {
        info_hash: meta.info_hash,
        peer_id: *peer_id::session(),
        port: 6881,
        uploaded: 0,
        downloaded: 0,
        left: meta.info.total_length().unwrap(),
        numwant: 50,
        event: Some(Event::Started),
    };
    let response = client.announce(&request, 50).await.unwrap();
    assert!(!response.peers.is_empty(), "tracker returned no peers");

    let mut failures: Vec<(std::net::SocketAddr, ProbeFailure)> = Vec::new();
    let mut replies: Vec<ProbeReply> = Vec::new();
    for peer in response.peers.iter().take(30) {
        match probe_exchange_with_peer(*peer, meta.info_hash).await {
            Ok(reply) => {
                replies.push(reply);
                break;
            }
            Err(reason) => failures.push((*peer, reason)),
        }
    }
    assert!(
        !replies.is_empty(),
        "no live peer completed a ut_metadata exchange; failures: {failures:?}"
    );
    let reply = &replies[0];
    assert_eq!(
        reply.reply_msg_type,
        Some(1),
        "expected a data reply; remote handshake hex {}",
        reply.remote_handshake_hex
    );
    assert_eq!(reply.reply_piece, Some(0));
    assert!(
        reply.reply_data_len == 16 * 1024
            || Some(reply.reply_data_len as u64) == reply.reply_total_size,
        "data reply must carry a full piece: {reply:?}"
    );
    println!(
        "probe reply: peer {}, reply_id {}, remote_ut_metadata_id {}, remote_metadata_size {:?}, reply_piece {:?}, reply_total_size {:?}, reply_data_len {}, hex_prefix {}",
        reply.peer,
        reply.reply_id,
        reply.remote_ut_metadata_id,
        reply.remote_metadata_size,
        reply.reply_piece,
        reply.reply_total_size,
        reply.reply_data_len,
        reply.reply_hex_prefix
    );
}
