mod common;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU16};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bt_core::dht::{AddressFilter, DhtOptions};
use bt_core::engine::{DhtIntegration, TorrentOptions};
use bt_core::metainfo::MetaInfo;
use common::temp_dir;

const FAKE_NODE_ID: [u8; 20] = *b"-BT0001-dhttestnode1";
const SERVICE_ID: [u8; 20] = *b"-BT0001-dhtservice01";

fn torrent_bytes(private: bool) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"d8:announce27:http://example.com/announce4:infod");
    bytes.extend_from_slice(b"6:lengthi16384e");
    bytes.extend_from_slice(b"4:name4:test12:piece lengthi16384e");
    bytes.extend_from_slice(b"6:pieces20:");
    bytes.extend_from_slice(&[0u8; 20]);
    if private {
        bytes.extend_from_slice(b"7:privatei1e");
    }
    bytes.extend_from_slice(b"ee");
    bytes
}

fn self_reply_planted(
    planted: &Mutex<Vec<([u8; 20], SocketAddr)>>,
    datagram: &[u8],
) -> Option<Vec<SocketAddr>> {
    if !datagram.windows(11).any(|window| window == b"9:get_peers") {
        return None;
    }
    let hash_pos = datagram
        .windows(14)
        .position(|window| window == b"9:info_hash20:")?
        + 14;
    let hash: [u8; 20] = datagram[hash_pos..hash_pos + 20].try_into().ok()?;
    let peers: Vec<SocketAddr> = planted
        .lock()
        .unwrap()
        .iter()
        .filter(|(planted_hash, _)| *planted_hash == hash)
        .map(|(_, addr)| *addr)
        .collect();
    (!peers.is_empty()).then_some(peers)
}

#[derive(Default)]
struct DhtObservations {
    queries: Vec<(String, Option<[u8; 20]>)>,
    pings: usize,
}

struct FakeDhtNode {
    socket: std::sync::Arc<tokio::net::UdpSocket>,
    observations: Arc<Mutex<DhtObservations>>,
}

impl FakeDhtNode {
    async fn spawn() -> FakeDhtNode {
        Self::spawn_with_peers(Vec::new()).await
    }

    async fn spawn_with_peers(planted: Vec<([u8; 20], SocketAddr)>) -> FakeDhtNode {
        let socket = std::sync::Arc::new(
            tokio::net::UdpSocket::bind((std::net::Ipv4Addr::new(127, 0, 0, 1), 0))
                .await
                .unwrap(),
        );
        let planted = Arc::new(Mutex::new(planted));
        let observations = Arc::new(Mutex::new(DhtObservations::default()));
        let task_socket = socket.clone();
        let task_observations = observations.clone();
        tokio::spawn(async move {
            let mut buffer = [0u8; 4096];
            loop {
                let (size, source) = match task_socket.recv_from(&mut buffer).await {
                    Ok(result) => result,
                    Err(_) => continue,
                };
                let datagram = &buffer[..size];
                let tid_pos = match datagram.windows(5).position(|window| window == b"1:t2:") {
                    Some(position) => position + 5,
                    None => continue,
                };
                let method = if datagram.windows(11).any(|w| w == b"9:get_peers") {
                    let hash_pos = match datagram.windows(14).position(|w| w == b"9:info_hash20:") {
                        Some(position) => position + 14,
                        None => continue,
                    };
                    let mut hash = [0u8; 20];
                    hash.copy_from_slice(&datagram[hash_pos..hash_pos + 20]);
                    ("get_peers", Some(hash))
                } else if datagram.windows(16).any(|w| w == b"13:announce_peer") {
                    ("announce_peer", None)
                } else if datagram.windows(6).any(|w| w == b"4:ping") {
                    ("ping", None)
                } else if datagram.windows(11).any(|w| w == b"9:find_node") {
                    ("find_node", None)
                } else {
                    continue;
                };
                if method.0 == "ping" {
                    task_observations.lock().unwrap().pings += 1;
                } else {
                    task_observations
                        .lock()
                        .unwrap()
                        .queries
                        .push((method.0.to_string(), method.1));
                }
                let mut reply = Vec::new();
                reply.extend_from_slice(b"d1:rd2:id20:");
                reply.extend_from_slice(&FAKE_NODE_ID);
                if let Some(peers) = self_reply_planted(&planted, datagram) {
                    reply.extend_from_slice(b"5:token2:TK6:valuesl");
                    for peer in peers {
                        let SocketAddr::V4(v4) = peer else {
                            continue;
                        };
                        reply.push(b'6');
                        reply.push(b':');
                        reply.extend_from_slice(&v4.ip().octets());
                        reply.extend_from_slice(&v4.port().to_be_bytes());
                    }
                    reply.push(b'e');
                }
                reply.extend_from_slice(b"e1:t2:");
                reply.extend_from_slice(&datagram[tid_pos..tid_pos + 2]);
                reply.extend_from_slice(b"1:y1:re");
                let _ = task_socket.send_to(&reply, source).await;
            }
        });
        FakeDhtNode {
            socket,
            observations,
        }
    }

    fn addr(&self) -> SocketAddr {
        self.socket.local_addr().unwrap()
    }

    fn queries_for(&self, info_hash: [u8; 20], method: &str) -> usize {
        self.observations
            .lock()
            .unwrap()
            .queries
            .iter()
            .filter(|(seen_method, hash)| *seen_method == method && *hash == Some(info_hash))
            .count()
    }

    fn pings(&self) -> usize {
        self.observations.lock().unwrap().pings
    }
}

struct FakeTcpPeer {
    observations: Arc<Mutex<TcpObservations>>,
    bound_addr: SocketAddr,
}

#[derive(Default)]
struct TcpObservations {
    dht_bit_in_handshake: Option<bool>,
    port_message_seen: Option<u16>,
}

impl FakeTcpPeer {
    async fn spawn(advertise_dht_bit: bool, send_port: Option<u16>) -> FakeTcpPeer {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::new(127, 0, 0, 1), 0))
            .await
            .unwrap();
        let bound_addr = listener.local_addr().unwrap();
        let observations = Arc::new(Mutex::new(TcpObservations::default()));
        let task_observations = observations.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let mut handshake = [0u8; 68];
                if tokio::io::AsyncReadExt::read_exact(&mut stream, &mut handshake)
                    .await
                    .is_err()
                {
                    continue;
                }
                let dht_bit = handshake[27] & 0x01 != 0;
                task_observations.lock().unwrap().dht_bit_in_handshake = Some(dht_bit);
                let mut reply = handshake.to_vec();
                reply[27] = if advertise_dht_bit { 0x01 } else { 0x00 };
                reply[48..68].copy_from_slice(b"-BT0001-fakepeerid00");
                if tokio::io::AsyncWriteExt::write_all(&mut stream, &reply)
                    .await
                    .is_err()
                {
                    continue;
                }
                if let Some(port) = send_port {
                    let frame = [0, 0, 0, 3, 9, (port >> 8) as u8, (port & 0xff) as u8];
                    let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, &frame).await;
                }
                loop {
                    let mut length = [0u8; 4];
                    if tokio::io::AsyncReadExt::read_exact(&mut stream, &mut length)
                        .await
                        .is_err()
                    {
                        break;
                    }
                    let length = u32::from_be_bytes(length) as usize;
                    if length == 0 {
                        continue;
                    }
                    let mut body = vec![0u8; length];
                    if tokio::io::AsyncReadExt::read_exact(&mut stream, &mut body)
                        .await
                        .is_err()
                    {
                        break;
                    }
                    if body[0] == 9 && body.len() == 3 {
                        let port = u16::from_be_bytes([body[1], body[2]]);
                        task_observations.lock().unwrap().port_message_seen = Some(port);
                    }
                    if body[0] == 3 {
                        break;
                    }
                }
            }
        });
        FakeTcpPeer {
            observations,
            bound_addr,
        }
    }

    fn addr(&self) -> SocketAddr {
        self.bound_addr
    }
}

async fn wait_for(condition: impl Fn() -> bool, seconds: u64, what: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(seconds);
    while !condition() {
        assert!(
            std::time::Instant::now() < deadline,
            "condition not met within {seconds}s: {what}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn integration(handle: bt_core::dht::DhtHandle, port: u16) -> DhtIntegration {
    DhtIntegration {
        handle,
        active: Arc::new(AtomicBool::new(true)),
        port: Arc::new(AtomicU16::new(port)),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_torrent_never_touches_dht() {
    let dht_node = FakeDhtNode::spawn().await;
    let integration_handle = bt_core::dht::spawn(
        DhtOptions::new(0, bt_core::dht::NodeId::from_bytes(SERVICE_ID))
            .with_bootstrap(vec![format!("127.0.0.1:{}", dht_node.addr().port())])
            .with_address_filter_for_tests(AddressFilter::permissive_for_tests()),
    );
    let status = integration_handle.status();
    wait_for(|| status.borrow().active, 5, "dht service became active").await;
    let tcp_peer = FakeTcpPeer::spawn(true, None).await;
    let meta = Arc::new(MetaInfo::from_bytes(&torrent_bytes(true)).unwrap());
    let info_hash = meta.info_hash;
    let options = TorrentOptions {
        bootstrap_peers: vec![tcp_peer.addr()],
        dht: Some(integration(
            integration_handle.clone(),
            dht_node.addr().port(),
        )),
        ..TorrentOptions::default()
    };
    let torrent = bt_core::engine::Torrent::spawn_with_options(
        (*meta).clone(),
        temp_dir("dht-private-out"),
        options,
    )
    .await
    .unwrap();
    wait_for(
        || {
            tcp_peer
                .observations
                .lock()
                .unwrap()
                .dht_bit_in_handshake
                .is_some()
        },
        10,
        "engine connected to the fake peer",
    )
    .await;
    assert_eq!(
        tcp_peer.observations.lock().unwrap().dht_bit_in_handshake,
        Some(false),
        "private torrents must not advertise the dht reserved bit"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        tcp_peer.observations.lock().unwrap().port_message_seen,
        None,
        "private torrents must not send port messages"
    );
    assert_eq!(dht_node.queries_for(info_hash, "get_peers"), 0);
    assert_eq!(dht_node.queries_for(info_hash, "announce_peer"), 0);
    let _ = torrent.stop().await;
    integration_handle.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dht_torrent_advertises_bit_sends_port_and_looks_up() {
    let dht_node = FakeDhtNode::spawn().await;
    let integration_handle = bt_core::dht::spawn(
        DhtOptions::new(0, bt_core::dht::NodeId::from_bytes(SERVICE_ID))
            .with_bootstrap(vec![format!("127.0.0.1:{}", dht_node.addr().port())])
            .with_address_filter_for_tests(AddressFilter::permissive_for_tests()),
    );
    let status = integration_handle.status();
    wait_for(|| status.borrow().active, 5, "dht service became active").await;
    let dht_port = status.borrow().port;
    let tcp_peer = FakeTcpPeer::spawn(true, None).await;
    let meta = Arc::new(MetaInfo::from_bytes(&torrent_bytes(false)).unwrap());
    let info_hash = meta.info_hash;
    let options = TorrentOptions {
        bootstrap_peers: vec![tcp_peer.addr()],
        dht: Some(integration(integration_handle.clone(), dht_port)),
        ..TorrentOptions::default()
    };
    let torrent = bt_core::engine::Torrent::spawn_with_options(
        (*meta).clone(),
        temp_dir("dht-open-out"),
        options,
    )
    .await
    .unwrap();
    wait_for(
        || {
            tcp_peer
                .observations
                .lock()
                .unwrap()
                .dht_bit_in_handshake
                .is_some()
        },
        10,
        "engine connected to the fake peer",
    )
    .await;
    assert_eq!(
        tcp_peer.observations.lock().unwrap().dht_bit_in_handshake,
        Some(true),
        "an active dht must advertise the reserved bit"
    );
    wait_for(
        || {
            tcp_peer
                .observations
                .lock()
                .unwrap()
                .port_message_seen
                .is_some()
        },
        10,
        "engine sent its port message",
    )
    .await;
    assert_eq!(
        tcp_peer.observations.lock().unwrap().port_message_seen,
        Some(dht_port),
        "the port message carries the bound dht udp port"
    );
    tokio::time::sleep(Duration::from_secs(3)).await;
    eprintln!("DEBUG status: {:?}", integration_handle.status().borrow());
    eprintln!(
        "DEBUG node queries: {:?}",
        dht_node.observations.lock().unwrap().queries
    );
    wait_for(
        || dht_node.queries_for(info_hash, "get_peers") > 0,
        10,
        "the torrent lookup reached the network",
    )
    .await;
    let _ = torrent.stop().await;
    integration_handle.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn port_message_from_peer_triggers_a_verification_ping() {
    let dht_node = FakeDhtNode::spawn().await;
    let integration_handle = bt_core::dht::spawn(
        DhtOptions::new(0, bt_core::dht::NodeId::from_bytes(SERVICE_ID))
            .with_bootstrap(vec![format!("127.0.0.1:{}", dht_node.addr().port())])
            .with_address_filter_for_tests(AddressFilter::permissive_for_tests()),
    );
    let status = integration_handle.status();
    wait_for(|| status.borrow().active, 5, "dht service became active").await;
    let dht_port = status.borrow().port;
    let tcp_peer = FakeTcpPeer::spawn(true, Some(dht_node.addr().port())).await;
    let meta = Arc::new(MetaInfo::from_bytes(&torrent_bytes(false)).unwrap());
    let options = TorrentOptions {
        bootstrap_peers: vec![tcp_peer.addr()],
        dht: Some(integration(integration_handle.clone(), dht_port)),
        ..TorrentOptions::default()
    };
    let torrent = bt_core::engine::Torrent::spawn_with_options(
        (*meta).clone(),
        temp_dir("dht-port-out"),
        options,
    )
    .await
    .unwrap();
    wait_for(
        || dht_node.pings() > 0,
        15,
        "the peer port announcement was verified with a ping",
    )
    .await;
    let _ = torrent.stop().await;
    integration_handle.shutdown();
}

#[allow(dead_code)]
fn unused(_: HashSet<u8>) {}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn engine_downloads_a_small_torrent_using_only_dht_peers() {
    let data = common::test_data();
    let meta = common::torrent_meta(&data);
    let fake_peer = SocketAddr::from(([10, 1, 1, 5], 7000));
    let dht_node = FakeDhtNode::spawn_with_peers(vec![(meta.info_hash, fake_peer)]).await;
    let integration_handle = bt_core::dht::spawn(
        DhtOptions::new(0, bt_core::dht::NodeId::from_bytes(SERVICE_ID))
            .with_bootstrap(vec![format!("127.0.0.1:{}", dht_node.addr().port())])
            .with_address_filter_for_tests(AddressFilter::permissive_for_tests()),
    );
    let status = integration_handle.status();
    wait_for(|| status.borrow().active, 5, "dht service became active").await;
    let dht_port = status.borrow().port;
    let dial = Arc::new(common::FakeDial::new(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![(fake_peer, common::SeederKind::Good)],
    ));
    let options = TorrentOptions {
        bootstrap_peers: Vec::new(),
        dial,
        dht: Some(integration(integration_handle.clone(), dht_port)),
        ..TorrentOptions::default()
    };
    let dir = temp_dir("dht-download-out");
    let torrent = bt_core::engine::Torrent::spawn_with_options(meta, dir.clone(), options)
        .await
        .unwrap();
    let stats = torrent.subscribe();
    let total = data.len() as u64;
    wait_for(
        || stats.borrow().verified_bytes >= total,
        30,
        "the torrent downloaded through the dht peer only",
    )
    .await;
    let _ = torrent.stop().await;
    integration_handle.shutdown();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn magnet_with_no_trackers_gets_metadata_through_dht() {
    let data = common::test_data();
    let meta = common::torrent_meta(&data);
    let root_value = bt_core::bencode::decode(&common::torrent_bytes(&data)).unwrap();
    let info_value = match &root_value {
        bt_core::bencode::Value::Dict(entries) => entries.get(&b"info".to_vec()).unwrap().clone(),
        _ => unreachable!(),
    };
    let info_dict = Arc::new(bt_core::bencode::encode(&info_value));
    let fake_peer = SocketAddr::from(([10, 1, 1, 6], 7001));
    let dht_node = FakeDhtNode::spawn_with_peers(vec![(meta.info_hash, fake_peer)]).await;
    let integration_handle = bt_core::dht::spawn(
        DhtOptions::new(0, bt_core::dht::NodeId::from_bytes(SERVICE_ID))
            .with_bootstrap(vec![format!("127.0.0.1:{}", dht_node.addr().port())])
            .with_address_filter_for_tests(AddressFilter::permissive_for_tests()),
    );
    let status = integration_handle.status();
    wait_for(|| status.borrow().active, 5, "dht service became active").await;
    let dht_port = status.borrow().port;
    let dial = Arc::new(common::FakeDial::with_metadata(
        meta.info_hash,
        data.clone(),
        meta.info.pieces.len(),
        vec![(fake_peer, common::SeederKind::Good)],
        info_dict,
    ));
    let magnet = bt_core::magnet::MagnetLink {
        info_hash: meta.info_hash,
        display_name: Some("e2e.bin".to_string()),
        trackers: Vec::new(),
        peers: Vec::new(),
    };
    let options = TorrentOptions {
        bootstrap_peers: Vec::new(),
        dial,
        dht: Some(integration(integration_handle.clone(), dht_port)),
        ..TorrentOptions::default()
    };
    let dir = temp_dir("dht-magnet-out");
    let torrent = bt_core::engine::Torrent::spawn_from_magnet(magnet, dir.clone(), options)
        .await
        .unwrap();
    let stats = torrent.subscribe();
    wait_for(
        || {
            matches!(
                stats.borrow().state,
                bt_core::engine::State::Downloading | bt_core::engine::State::Completed
            )
        },
        30,
        "metadata arrived through the dht peer",
    )
    .await;
    let total = data.len() as u64;
    wait_for(
        || stats.borrow().verified_bytes >= total,
        30,
        "the magnet downloaded through the dht peer only",
    )
    .await;
    assert!(
        !stats.borrow().dht_waiting,
        "the waiting flag clears once peers are connected"
    );
    let _ = torrent.stop().await;
    integration_handle.shutdown();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dht_state_persists_and_a_restart_reuses_the_node_id_and_table() {
    let dir = temp_dir("dht-persist");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("dht.json");
    let router = FakeDhtNode::spawn().await;
    let integration_handle = bt_core::dht::spawn(
        DhtOptions::new(0, bt_core::dht::NodeId::from_bytes(SERVICE_ID))
            .with_bootstrap(vec![format!("127.0.0.1:{}", router.addr().port())])
            .with_persist(Some(path.clone()), Vec::new())
            .with_address_filter_for_tests(AddressFilter::permissive_for_tests()),
    );
    let status = integration_handle.status();
    wait_for(|| status.borrow().active, 5, "dht service became active").await;
    integration_handle.persist_and_shutdown().await;
    let loaded = bt_core::dht::load_state(&path).unwrap();
    assert_eq!(loaded.node_id, bt_core::dht::NodeId::from_bytes(SERVICE_ID));

    let restarted = bt_core::dht::spawn(
        DhtOptions::new(0, loaded.node_id)
            .with_persist(Some(path.clone()), loaded.nodes)
            .with_address_filter_for_tests(AddressFilter::permissive_for_tests()),
    );
    let restarted_status = restarted.status();
    wait_for(
        || restarted_status.borrow().node_count >= 1,
        5,
        "the restored table is populated",
    )
    .await;
    restarted.shutdown();
    std::fs::remove_dir_all(dir).unwrap();
}
