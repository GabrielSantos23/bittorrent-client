use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Duration;

use serde::Serialize;
use tokio::net::UdpSocket;
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tokio::time::{Instant as TokioInstant, MissedTickBehavior};

use crate::dht::filter::AddressFilter;
use crate::dht::krpc::{self, KrpcMessage, NodeInfo, Query, Response, TransactionId};
use crate::dht::limiter::ResponseGate;
use crate::dht::node_id::{cmp_distance_to, NodeId, SystemRandom};
use crate::dht::store::PeerStore;
use crate::dht::table::{OfferOutcome, RoutingTable};
use crate::dht::tokens::TokenVault;

pub const DEFAULT_BOOTSTRAP_ROUTERS: &[&str] = &[
    "router.bittorrent.com:6881",
    "router.utorrent.com:6881",
    "dht.transmissionbt.com:6881",
];
pub const TRANSACTION_TIMEOUT_MS: u64 = 5_000;
pub const MAX_PENDING_QUERIES: usize = 128;
pub const MAX_NODES_PER_RESPONSE: usize = 8;
pub const MAX_PEERS_PER_RESPONSE: usize = 25;
const SWEEP_INTERVAL_MS: u64 = 1_000;
const MAX_FILL_QUERIES: u32 = 64;
const FILL_ALPHA: usize = 3;
const REFRESH_QUERIES_PER_SWEEP: usize = 2;
const REBOOTSTRAP_IDLE_MS: u64 = 30_000;
const MAX_BOOTSTRAP_TARGETS: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, ts_rs::TS)]
#[ts(export)]
pub struct DhtStatus {
    pub active: bool,
    pub port: u16,
    pub node_count: usize,
    pub error: Option<String>,
    pub datagrams_received: u64,
    pub datagrams_dropped: u64,
    pub responses_sent: u64,
    pub queries_sent: u64,
    pub responses_rate_limited: u64,
    pub transaction_timeouts: u64,
}

impl DhtStatus {
    pub fn inactive(port: u16, error: Option<String>) -> DhtStatus {
        DhtStatus {
            active: false,
            port,
            node_count: 0,
            error,
            datagrams_received: 0,
            datagrams_dropped: 0,
            responses_sent: 0,
            queries_sent: 0,
            responses_rate_limited: 0,
            transaction_timeouts: 0,
        }
    }
}

pub struct DhtOptions {
    pub port: u16,
    pub self_id: NodeId,
    pub bootstrap: Vec<String>,
    filter: AddressFilter,
}

impl DhtOptions {
    pub fn new(port: u16, self_id: NodeId) -> DhtOptions {
        DhtOptions {
            port,
            self_id,
            bootstrap: Vec::new(),
            filter: AddressFilter::strict(),
        }
    }

    pub fn with_bootstrap(mut self, bootstrap: Vec<String>) -> DhtOptions {
        self.bootstrap = bootstrap;
        self
    }

    #[doc(hidden)]
    pub fn with_address_filter_for_tests(mut self, filter: AddressFilter) -> DhtOptions {
        self.filter = filter;
        self
    }
}

pub struct DhtHandle {
    status: watch::Receiver<DhtStatus>,
    abort: AbortHandle,
}

impl DhtHandle {
    pub fn status(&self) -> watch::Receiver<DhtStatus> {
        self.status.clone()
    }

    pub fn shutdown(&self) {
        self.abort.abort();
    }
}

pub fn spawn(options: DhtOptions) -> DhtHandle {
    let (status_tx, status_rx) = watch::channel(DhtStatus::inactive(options.port, None));
    let task = tokio::spawn(run(options, status_tx));
    DhtHandle {
        status: status_rx,
        abort: task.abort_handle(),
    }
}

pub async fn bind(options: DhtOptions) -> Result<DhtHandle, std::io::Error> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, options.port)).await?;
    let (status_tx, status_rx) = watch::channel(DhtStatus::inactive(options.port, None));
    let task = tokio::spawn(run_bound(socket, options, status_tx));
    Ok(DhtHandle {
        status: status_rx,
        abort: task.abort_handle(),
    })
}

async fn run(options: DhtOptions, status: watch::Sender<DhtStatus>) {
    match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, options.port)).await {
        Ok(socket) => run_bound(socket, options, status).await,
        Err(err) => {
            let _ = status.send(DhtStatus::inactive(
                options.port,
                Some(format!("cannot bind DHT port {}: {err}", options.port)),
            ));
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PendingKind {
    Fill,
    Refresh,
}

struct PendingQuery {
    node_id: Option<NodeId>,
    sent_ms: u64,
    kind: PendingKind,
}

#[derive(Default)]
struct Counters {
    received: u64,
    dropped: u64,
    responses_sent: u64,
    queries_sent: u64,
    rate_limited: u64,
    timeouts: u64,
}

struct FillState {
    active: bool,
    queries_sent: u32,
    candidates: Vec<NodeInfo>,
}

struct NodeState {
    socket: UdpSocket,
    port: u16,
    self_id: NodeId,
    bootstrap: Vec<String>,
    filter: AddressFilter,
    table: RoutingTable,
    store: PeerStore,
    vault: TokenVault,
    gate: ResponseGate,
    pending: HashMap<(TransactionId, SocketAddrV4), PendingQuery>,
    queried: HashSet<SocketAddrV4>,
    fill: FillState,
    counters: Counters,
    last_bootstrap_ms: Option<u64>,
    start: TokioInstant,
    random: SystemRandom,
}

impl NodeState {
    fn now_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    async fn handle_datagram(&mut self, bytes: &[u8], source: SocketAddr) {
        self.counters.received = self.counters.received.saturating_add(1);
        if bytes.len() > krpc::MAX_DATAGRAM_SIZE {
            self.counters.dropped = self.counters.dropped.saturating_add(1);
            return;
        }
        let message = match krpc::decode(bytes) {
            Ok(message) => message,
            Err(_) => {
                self.counters.dropped = self.counters.dropped.saturating_add(1);
                return;
            }
        };
        let SocketAddr::V4(source) = source else {
            self.counters.dropped = self.counters.dropped.saturating_add(1);
            return;
        };
        match message {
            KrpcMessage::Query {
                transaction_id,
                requester,
                query,
            } => {
                self.handle_query(source, transaction_id, requester, query)
                    .await
            }
            KrpcMessage::Response {
                transaction_id,
                responder,
                response,
            } => {
                self.handle_response(source, transaction_id, responder, response)
                    .await
            }
            KrpcMessage::Error { transaction_id, .. } => {
                self.pending.remove(&(transaction_id, source));
            }
        }
    }

    async fn handle_query(
        &mut self,
        source: SocketAddrV4,
        transaction_id: TransactionId,
        requester: NodeId,
        query: Query,
    ) {
        let now = self.now_ms();
        if self.filter.allows(source) && requester != self.self_id {
            if let OfferOutcome::PingRequired { questioned } =
                self.table.offer(NodeInfo::new(requester, source), now)
            {
                if let Some(questioned_addr) = self.table.address_of(&questioned) {
                    self.send_query(
                        questioned_addr,
                        Query::Ping,
                        PendingKind::Refresh,
                        Some(questioned),
                    )
                    .await;
                }
            }
        }
        if !self.gate.try_acquire(*source.ip(), now) {
            self.counters.rate_limited = self.counters.rate_limited.saturating_add(1);
            return;
        }
        let response = match &query {
            Query::Ping => KrpcMessage::Response {
                transaction_id,
                responder: self.self_id,
                response: Response::default(),
            },
            Query::FindNode { target } => KrpcMessage::Response {
                transaction_id,
                responder: self.self_id,
                response: Response {
                    nodes: self.table.closest_nodes(target, MAX_NODES_PER_RESPONSE),
                    ..Response::default()
                },
            },
            Query::GetPeers { info_hash } => {
                let token = self.vault.token_for(*source.ip());
                let stored = self.store.peers(info_hash, now);
                let response = if stored.is_empty() {
                    Response {
                        nodes: self
                            .table
                            .closest_nodes(&NodeId::from_bytes(*info_hash), MAX_NODES_PER_RESPONSE),
                        token: Some(token),
                        ..Response::default()
                    }
                } else {
                    Response {
                        peers: stored
                            .into_iter()
                            .take(MAX_PEERS_PER_RESPONSE)
                            .map(SocketAddr::V4)
                            .collect(),
                        token: Some(token),
                        ..Response::default()
                    }
                };
                KrpcMessage::Response {
                    transaction_id,
                    responder: self.self_id,
                    response,
                }
            }
            Query::AnnouncePeer {
                info_hash,
                port,
                token,
                implied_port,
            } => {
                if !self.vault.validate(*source.ip(), token) {
                    let error = KrpcMessage::Error {
                        transaction_id,
                        code: 203,
                        message: "Bad token".to_string(),
                    };
                    self.send_datagram(&error, source).await;
                    self.counters.responses_sent = self.counters.responses_sent.saturating_add(1);
                    return;
                }
                let peer_port = if *implied_port { source.port() } else { *port };
                let peer = SocketAddrV4::new(*source.ip(), peer_port);
                if self.filter.allows(peer) {
                    self.store.insert(*info_hash, peer, now);
                }
                KrpcMessage::Response {
                    transaction_id,
                    responder: self.self_id,
                    response: Response::default(),
                }
            }
        };
        self.send_datagram(&response, source).await;
        self.counters.responses_sent = self.counters.responses_sent.saturating_add(1);
    }

    async fn handle_response(
        &mut self,
        source: SocketAddrV4,
        transaction_id: TransactionId,
        responder: NodeId,
        response: Response,
    ) {
        let Some(pending) = self.pending.remove(&(transaction_id, source)) else {
            return;
        };
        let now = self.now_ms();
        if self.filter.allows(source) && responder != self.self_id {
            self.table.offer(NodeInfo::new(responder, source), now);
        }
        for node in response.nodes {
            if !self.filter.allows(node.addr) || node.id == self.self_id {
                continue;
            }
            if !matches!(self.table.offer(node, now), OfferOutcome::Rejected { .. })
                && pending.kind == PendingKind::Fill
            {
                self.fill.candidates.push(node);
            }
        }
        if pending.kind == PendingKind::Fill {
            self.maybe_dispatch_fill().await;
        }
    }

    async fn maybe_dispatch_fill(&mut self) {
        if !self.fill.active {
            return;
        }
        self.fill
            .candidates
            .retain(|node| !self.queried.contains(&node.addr));
        let outstanding_fill = self
            .pending
            .values()
            .filter(|query| query.kind == PendingKind::Fill)
            .count();
        if self.fill.candidates.is_empty() && outstanding_fill == 0 {
            self.fill.active = false;
            return;
        }
        if self.fill.candidates.is_empty() || self.fill.queries_sent >= MAX_FILL_QUERIES {
            return;
        }
        self.fill
            .candidates
            .sort_by(|a, b| cmp_distance_to(&a.id, &b.id, &self.self_id));
        let budget = MAX_FILL_QUERIES.saturating_sub(self.fill.queries_sent) as usize;
        let batch = FILL_ALPHA.min(self.fill.candidates.len()).min(budget);
        let batch: Vec<NodeInfo> = self.fill.candidates.drain(..batch).collect();
        for node in batch {
            if self.queried.contains(&node.addr) {
                continue;
            }
            self.queried.insert(node.addr);
            self.send_query(
                node.addr,
                Query::FindNode {
                    target: self.self_id,
                },
                PendingKind::Fill,
                Some(node.id),
            )
            .await;
            self.fill.queries_sent = self.fill.queries_sent.saturating_add(1);
        }
    }

    async fn sweep(&mut self, status: &watch::Sender<DhtStatus>) {
        let now = self.now_ms();
        let expired: Vec<(TransactionId, SocketAddrV4)> = self
            .pending
            .iter()
            .filter(|(_, query)| now.saturating_sub(query.sent_ms) >= TRANSACTION_TIMEOUT_MS)
            .map(|(key, _)| *key)
            .collect();
        for key in expired {
            if let Some(query) = self.pending.remove(&key) {
                self.counters.timeouts = self.counters.timeouts.saturating_add(1);
                if let Some(node_id) = query.node_id {
                    self.table.note_failure(&node_id, now);
                }
            }
        }
        self.vault.rotate_if_due(now, &self.random);
        self.store.expire(now);
        if self.fill.active {
            self.maybe_dispatch_fill().await;
        } else if self.table.is_empty() {
            if now.saturating_sub(self.last_bootstrap_ms.unwrap_or(0)) >= REBOOTSTRAP_IDLE_MS
                && !self.bootstrap.is_empty()
            {
                self.bootstrap().await;
            }
        } else {
            self.refresh_stale_buckets(now).await;
        }
        self.publish(status);
    }

    async fn refresh_stale_buckets(&mut self, now: u64) {
        for bucket in self
            .table
            .stale_buckets(now)
            .into_iter()
            .take(REFRESH_QUERIES_PER_SWEEP)
        {
            let target = self.table.refresh_target(bucket, &self.random);
            for node in self.table.closest_nodes(&target, FILL_ALPHA) {
                self.send_query(
                    node.addr,
                    Query::FindNode { target },
                    PendingKind::Refresh,
                    Some(node.id),
                )
                .await;
            }
        }
    }

    async fn bootstrap(&mut self) {
        self.last_bootstrap_ms = Some(self.now_ms());
        let targets = resolve_bootstrap(&self.bootstrap).await;
        self.fill.active = true;
        for addr in targets.into_iter().take(MAX_BOOTSTRAP_TARGETS) {
            if !self.filter.allows(addr) || self.queried.contains(&addr) {
                continue;
            }
            self.queried.insert(addr);
            self.send_query(
                addr,
                Query::FindNode {
                    target: self.self_id,
                },
                PendingKind::Fill,
                None,
            )
            .await;
        }
    }

    async fn send_query(
        &mut self,
        destination: SocketAddrV4,
        query: Query,
        kind: PendingKind,
        node_id: Option<NodeId>,
    ) {
        let now = self.now_ms();
        if self.pending.len() >= MAX_PENDING_QUERIES {
            self.drop_one_pending(now);
            if self.pending.len() >= MAX_PENDING_QUERIES {
                return;
            }
        }
        let transaction_id = TransactionId::random(&self.random);
        let message = KrpcMessage::Query {
            transaction_id,
            requester: self.self_id,
            query,
        };
        self.pending.insert(
            (transaction_id, destination),
            PendingQuery {
                node_id,
                sent_ms: now,
                kind,
            },
        );
        self.send_datagram(&message, destination).await;
        self.counters.queries_sent = self.counters.queries_sent.saturating_add(1);
    }

    fn drop_one_pending(&mut self, now: u64) {
        if let Some(oldest) = self
            .pending
            .iter()
            .min_by_key(|(_, query)| query.sent_ms)
            .map(|(key, _)| *key)
        {
            if let Some(query) = self.pending.remove(&oldest) {
                self.counters.timeouts = self.counters.timeouts.saturating_add(1);
                if let Some(node_id) = query.node_id {
                    self.table.note_failure(&node_id, now);
                }
            }
        }
    }

    async fn send_datagram(&mut self, message: &KrpcMessage, destination: SocketAddrV4) {
        let Ok(bytes) = krpc::encode(message) else {
            self.counters.dropped = self.counters.dropped.saturating_add(1);
            return;
        };
        let _ = self.socket.send_to(&bytes, destination).await;
    }

    fn publish(&self, status: &watch::Sender<DhtStatus>) {
        let _ = status.send(DhtStatus {
            active: true,
            port: self.port,
            node_count: self.table.len(),
            error: None,
            datagrams_received: self.counters.received,
            datagrams_dropped: self.counters.dropped,
            responses_sent: self.counters.responses_sent,
            queries_sent: self.counters.queries_sent,
            responses_rate_limited: self.counters.rate_limited,
            transaction_timeouts: self.counters.timeouts,
        });
    }
}

async fn resolve_bootstrap(hosts: &[String]) -> Vec<SocketAddrV4> {
    let mut resolved = Vec::new();
    for host in hosts {
        if let Ok(mut addrs) = tokio::net::lookup_host(host.as_str()).await {
            if let Some(SocketAddr::V4(v4)) = addrs.next() {
                resolved.push(v4);
            }
        }
    }
    resolved.truncate(MAX_BOOTSTRAP_TARGETS);
    resolved
}

async fn run_bound(socket: UdpSocket, options: DhtOptions, status: watch::Sender<DhtStatus>) {
    let port = socket
        .local_addr()
        .map(|addr| addr.port())
        .unwrap_or(options.port);
    let start = TokioInstant::now();
    let mut node = NodeState {
        socket,
        port,
        self_id: options.self_id,
        bootstrap: options.bootstrap,
        filter: options.filter,
        table: RoutingTable::new(options.self_id, 0),
        store: PeerStore::default(),
        vault: TokenVault::new(0, &SystemRandom),
        gate: ResponseGate::new(0),
        pending: HashMap::new(),
        queried: HashSet::new(),
        fill: FillState {
            active: false,
            queries_sent: 0,
            candidates: Vec::new(),
        },
        counters: Counters::default(),
        last_bootstrap_ms: None,
        start,
        random: SystemRandom,
    };
    node.publish(&status);
    let mut sweep = tokio::time::interval(Duration::from_millis(SWEEP_INTERVAL_MS));
    sweep.set_missed_tick_behavior(MissedTickBehavior::Skip);
    if !node.bootstrap.is_empty() {
        node.bootstrap().await;
    }
    let mut buffer = [0u8; krpc::MAX_DATAGRAM_SIZE + 1];
    loop {
        tokio::select! {
            _ = status.closed() => break,
            received = node.socket.recv_from(&mut buffer) => match received {
                Ok((size, source)) => {
                    node.handle_datagram(&buffer[..size], source).await;
                }
                Err(err) if err.kind() == std::io::ErrorKind::ConnectionReset => {
                    node.counters.dropped = node.counters.dropped.saturating_add(1);
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
            _ = sweep.tick() => node.sweep(&status).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dht::filter::AddressFilter;
    use crate::dht::table::BUCKET_REFRESH_MS;
    use crate::dht::tokens::TOKEN_ROTATION_MS;

    mod wire {
        #[derive(Debug, Clone, PartialEq)]
        pub enum Value {
            Int(i64),
            Bytes(Vec<u8>),
            List(Vec<Value>),
            Dict(Vec<(Vec<u8>, Value)>),
        }

        pub fn encode(value: &Value) -> Vec<u8> {
            let mut out = Vec::new();
            write_value(value, &mut out);
            out
        }

        fn write_value(value: &Value, out: &mut Vec<u8>) {
            match value {
                Value::Int(number) => {
                    out.extend_from_slice(format!("i{number}e").as_bytes());
                }
                Value::Bytes(bytes) => {
                    out.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
                    out.extend_from_slice(bytes);
                }
                Value::List(items) => {
                    out.push(b'l');
                    for item in items {
                        write_value(item, out);
                    }
                    out.push(b'e');
                }
                Value::Dict(entries) => {
                    out.push(b'd');
                    for (key, item) in entries {
                        out.extend_from_slice(format!("{}:", key.len()).as_bytes());
                        out.extend_from_slice(key);
                        write_value(item, out);
                    }
                    out.push(b'e');
                }
            }
        }

        pub fn decode(bytes: &[u8]) -> Option<Value> {
            let (value, rest) = read_value(bytes)?;
            if rest.is_empty() {
                Some(value)
            } else {
                None
            }
        }

        fn read_value(bytes: &[u8]) -> Option<(Value, &[u8])> {
            match bytes.first()? {
                b'i' => {
                    let end = bytes.iter().position(|byte| *byte == b'e')?;
                    let number: i64 = std::str::from_utf8(&bytes[1..end]).ok()?.parse().ok()?;
                    Some((Value::Int(number), &bytes[end + 1..]))
                }
                b'l' => {
                    let mut items = Vec::new();
                    let mut rest = &bytes[1..];
                    loop {
                        if rest.first()? == &b'e' {
                            return Some((Value::List(items), &rest[1..]));
                        }
                        let (item, next) = read_value(rest)?;
                        items.push(item);
                        rest = next;
                    }
                }
                b'd' => {
                    let mut entries = Vec::new();
                    let mut rest = &bytes[1..];
                    loop {
                        if rest.first()? == &b'e' {
                            return Some((Value::Dict(entries), &rest[1..]));
                        }
                        let (key, next) = read_value(rest)?;
                        let Value::Bytes(key) = key else {
                            return None;
                        };
                        let (item, next) = read_value(next)?;
                        entries.push((key, item));
                        rest = next;
                    }
                }
                digit if digit.is_ascii_digit() => {
                    let colon = bytes.iter().position(|byte| *byte == b':')?;
                    let length: usize = std::str::from_utf8(&bytes[..colon]).ok()?.parse().ok()?;
                    let body = &bytes[colon + 1..];
                    if body.len() < length {
                        return None;
                    }
                    Some((Value::Bytes(body[..length].to_vec()), &body[length..]))
                }
                _ => None,
            }
        }

        pub fn bytes(raw: &[u8]) -> Value {
            Value::Bytes(raw.to_vec())
        }

        pub fn int(number: i64) -> Value {
            Value::Int(number)
        }

        pub fn dict(entries: Vec<(&[u8], Value)>) -> Value {
            Value::Dict(
                entries
                    .into_iter()
                    .map(|(key, value)| (key.to_vec(), value))
                    .collect(),
            )
        }

        pub fn get<'a>(value: &'a Value, key: &[u8]) -> Option<&'a Value> {
            let Value::Dict(entries) = value else {
                return None;
            };
            entries
                .iter()
                .find(|(entry, _)| entry.as_slice() == key)
                .map(|(_, item)| item)
        }
    }

    use wire::{bytes as b, dict, int, Value};

    struct FakeNode {
        socket: tokio::net::UdpSocket,
    }

    impl FakeNode {
        async fn bind(octet: u8) -> FakeNode {
            let socket =
                tokio::net::UdpSocket::bind((std::net::Ipv4Addr::new(127, 0, 0, octet), 0))
                    .await
                    .unwrap();
            FakeNode { socket }
        }

        fn addr(&self) -> SocketAddrV4 {
            match self.socket.local_addr().unwrap() {
                SocketAddr::V4(v4) => v4,
                SocketAddr::V6(_) => unreachable!("loopback bind is ipv4"),
            }
        }

        async fn send_raw(&self, destination: SocketAddr, datagram: &[u8]) {
            self.socket.send_to(datagram, destination).await.unwrap();
        }

        async fn recv_raw(&self, limit: Duration) -> Option<Vec<u8>> {
            let mut buffer = [0u8; 4096];
            match tokio::time::timeout(limit, self.socket.recv_from(&mut buffer)).await {
                Ok(Ok((size, _))) => Some(buffer[..size].to_vec()),
                _ => None,
            }
        }

        async fn next_datagram(&self) -> Vec<u8> {
            self.recv_raw(Duration::from_secs(5))
                .await
                .expect("fake node expected a datagram")
        }

        async fn expect_silence(&self, limit: Duration) {
            assert!(
                self.recv_raw(limit).await.is_none(),
                "fake node expected silence"
            );
        }
    }

    fn query_ping(tid: [u8; 2], id: &[u8; 20]) -> Vec<u8> {
        wire::encode(&dict(vec![
            (b"t", b(tid.as_slice())),
            (b"y", b(b"q")),
            (b"q", b(b"ping")),
            (b"a", dict(vec![(b"id", b(id))])),
        ]))
    }

    fn query_find_node(tid: [u8; 2], id: &[u8; 20], target: &[u8; 20]) -> Vec<u8> {
        wire::encode(&dict(vec![
            (b"t", b(tid.as_slice())),
            (b"y", b(b"q")),
            (b"q", b(b"find_node")),
            (
                b"a",
                dict(vec![(b"id", b(id)), (b"target", b(target.as_slice()))]),
            ),
        ]))
    }

    fn query_get_peers(tid: [u8; 2], id: &[u8; 20], info_hash: &[u8; 20]) -> Vec<u8> {
        wire::encode(&dict(vec![
            (b"t", b(tid.as_slice())),
            (b"y", b(b"q")),
            (b"q", b(b"get_peers")),
            (
                b"a",
                dict(vec![
                    (b"id", b(id)),
                    (b"info_hash", b(info_hash.as_slice())),
                ]),
            ),
        ]))
    }

    fn query_announce_peer(
        tid: [u8; 2],
        id: &[u8; 20],
        info_hash: &[u8; 20],
        port: u16,
        token: &[u8],
    ) -> Vec<u8> {
        wire::encode(&dict(vec![
            (b"t", b(tid.as_slice())),
            (b"y", b(b"q")),
            (b"q", b(b"announce_peer")),
            (
                b"a",
                dict(vec![
                    (b"id", b(id)),
                    (b"info_hash", b(info_hash.as_slice())),
                    (b"port", int(port as i64)),
                    (b"token", b(token)),
                    (b"implied_port", int(0)),
                ]),
            ),
        ]))
    }

    fn reply_response(
        tid: &[u8],
        responder: &[u8; 20],
        return_values: Vec<(&[u8], Value)>,
    ) -> Vec<u8> {
        wire::encode(&dict(vec![
            (b"t", b(tid)),
            (b"y", b(b"r")),
            (
                b"r",
                dict(
                    vec![(b"id" as &[u8], b(responder.as_slice()))]
                        .into_iter()
                        .chain(return_values)
                        .collect(),
                ),
            ),
        ]))
    }

    fn nodes_value(entries: &[(NodeId, SocketAddrV4)]) -> Value {
        let mut raw = Vec::new();
        for (id, addr) in entries {
            raw.extend_from_slice(id.as_bytes());
            raw.extend_from_slice(&addr.ip().octets());
            raw.extend_from_slice(&addr.port().to_be_bytes());
        }
        Value::Bytes(raw)
    }

    fn tid_of(datagram: &[u8]) -> Vec<u8> {
        let parsed = wire::decode(datagram).expect("fake node received unparseable datagram");
        match wire::get(&parsed, b"t").expect("datagram without t") {
            Value::Bytes(bytes) => bytes.clone(),
            _ => panic!("t is not bytes"),
        }
    }

    fn find_node_target(datagram: &[u8]) -> Option<[u8; 20]> {
        let parsed = wire::decode(datagram)?;
        let arguments = wire::get(&parsed, b"a")?;
        match wire::get(arguments, b"target")? {
            Value::Bytes(bytes) => bytes.as_slice().try_into().ok(),
            _ => None,
        }
    }

    fn is_find_node(datagram: &[u8]) -> bool {
        let parsed = match wire::decode(datagram) {
            Some(parsed) => parsed,
            None => return false,
        };
        matches!(wire::get(&parsed, b"q"), Some(Value::Bytes(method)) if method == b"find_node")
    }

    fn response_values(datagram: &[u8]) -> std::collections::HashMap<Vec<u8>, Value> {
        let parsed = wire::decode(datagram).expect("expected a parseable datagram");
        let return_values = wire::get(&parsed, b"r")
            .expect("expected a response")
            .clone();
        match return_values {
            Value::Dict(entries) => entries.into_iter().collect(),
            _ => panic!("r is not a dict"),
        }
    }

    fn is_error(datagram: &[u8]) -> bool {
        let parsed = match wire::decode(datagram) {
            Some(parsed) => parsed,
            None => return false,
        };
        matches!(wire::get(&parsed, b"y"), Some(Value::Bytes(direction)) if direction == b"e")
    }

    fn token_from(datagram: &[u8]) -> Vec<u8> {
        match response_values(datagram).get(b"token".as_slice()) {
            Some(Value::Bytes(token)) => token.clone(),
            _ => panic!("response carries no token"),
        }
    }

    fn peers_from(datagram: &[u8]) -> Vec<SocketAddrV4> {
        match response_values(datagram).get(b"values".as_slice()) {
            Some(Value::List(entries)) => entries
                .iter()
                .map(|entry| match entry {
                    Value::Bytes(raw) => {
                        assert_eq!(raw.len(), 6);
                        SocketAddrV4::new(
                            std::net::Ipv4Addr::new(raw[0], raw[1], raw[2], raw[3]),
                            u16::from_be_bytes([raw[4], raw[5]]),
                        )
                    }
                    _ => panic!("values entries must be bytes"),
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    fn nodes_from(datagram: &[u8]) -> Vec<(NodeId, SocketAddrV4)> {
        match response_values(datagram).get(b"nodes".as_slice()) {
            Some(Value::Bytes(raw)) => raw
                .chunks(26)
                .map(|chunk| {
                    let mut id = [0u8; 20];
                    id.copy_from_slice(&chunk[..20]);
                    (
                        NodeId::from_bytes(id),
                        SocketAddrV4::new(
                            std::net::Ipv4Addr::new(chunk[20], chunk[21], chunk[22], chunk[23]),
                            u16::from_be_bytes([chunk[24], chunk[25]]),
                        ),
                    )
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    async fn wait_for_status(
        mut rx: watch::Receiver<DhtStatus>,
        condition: impl Fn(&DhtStatus) -> bool,
        seconds: u64,
    ) -> DhtStatus {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(seconds);
        loop {
            let snapshot = rx.borrow().clone();
            if condition(&snapshot) {
                return snapshot;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "condition not met within {seconds}s: {snapshot:?}"
            );
            if rx.changed().await.is_err() {
                panic!("dht status channel closed");
            }
        }
    }

    fn permissive_options(port: u16, id: NodeId) -> DhtOptions {
        DhtOptions::new(port, id)
            .with_bootstrap(Vec::new())
            .with_address_filter_for_tests(AddressFilter::permissive_for_tests())
    }

    fn solid_id(seed: u8) -> NodeId {
        NodeId::from_bytes([seed; 20])
    }

    #[tokio::test]
    async fn bootstrap_against_a_fake_router_fills_the_table() {
        let router = FakeNode::bind(1).await;
        let node_a = FakeNode::bind(2).await;
        let node_b = FakeNode::bind(3).await;
        let handle = spawn(
            permissive_options(0, solid_id(1))
                .with_bootstrap(vec![format!("127.0.0.1:{}", router.addr().port())]),
        );
        let status = handle.status();
        let service = wait_for_status(status.clone(), |s| s.active, 5).await;
        let destination = SocketAddr::from(([127, 0, 0, 1], service.port));

        let request = router.next_datagram().await;
        assert!(
            is_find_node(&request),
            "bootstrap must start with find_node"
        );
        let tid = tid_of(&request);
        router
            .send_raw(
                destination,
                &reply_response(
                    &tid,
                    &[9u8; 20],
                    vec![(
                        b"nodes" as &[u8],
                        nodes_value(&[(solid_id(2), node_a.addr()), (solid_id(3), node_b.addr())]),
                    )],
                ),
            )
            .await;
        let request_a = node_a.next_datagram().await;
        assert!(is_find_node(&request_a));
        node_a
            .send_raw(
                destination,
                &reply_response(&tid_of(&request_a), &[2u8; 20], vec![]),
            )
            .await;
        let request_b = node_b.next_datagram().await;
        assert!(is_find_node(&request_b));
        node_b
            .send_raw(
                destination,
                &reply_response(&tid_of(&request_b), &[3u8; 20], vec![]),
            )
            .await;
        let filled = wait_for_status(status, |s| s.node_count == 3, 10).await;
        assert_eq!(filled.node_count, 3);
        handle.shutdown();
    }

    #[tokio::test]
    async fn replies_with_a_mismatched_transaction_id_are_ignored() {
        let node = FakeNode::bind(1).await;
        let handle = spawn(
            permissive_options(0, solid_id(1))
                .with_bootstrap(vec![format!("127.0.0.1:{}", node.addr().port())]),
        );
        let status = handle.status();
        let service = wait_for_status(status.clone(), |s| s.active, 5).await;
        let destination = SocketAddr::from(([127, 0, 0, 1], service.port));
        let request = node.next_datagram().await;
        let tid = tid_of(&request);
        let mut wrong = tid.clone();
        wrong[0] ^= 0xFF;
        node.send_raw(destination, &reply_response(&wrong, &[7u8; 20], vec![]))
            .await;
        node.expect_silence(Duration::from_millis(300)).await;
        assert_eq!(status.borrow().node_count, 0);
        node.send_raw(destination, &reply_response(&tid, &[7u8; 20], vec![]))
            .await;
        wait_for_status(status, |s| s.node_count == 1, 5).await;
        handle.shutdown();
    }

    #[tokio::test]
    async fn replies_from_the_wrong_address_are_ignored() {
        let node_a = FakeNode::bind(1).await;
        let node_b = FakeNode::bind(2).await;
        let handle = spawn(
            permissive_options(0, solid_id(1))
                .with_bootstrap(vec![format!("127.0.0.1:{}", node_a.addr().port())]),
        );
        let status = handle.status();
        let service = wait_for_status(status.clone(), |s| s.active, 5).await;
        let destination = SocketAddr::from(([127, 0, 0, 1], service.port));
        let request = node_a.next_datagram().await;
        let tid = tid_of(&request);
        node_b
            .send_raw(destination, &reply_response(&tid, &[8u8; 20], vec![]))
            .await;
        node_a.expect_silence(Duration::from_millis(300)).await;
        assert_eq!(
            status.borrow().node_count,
            0,
            "the impostor reply must be ignored"
        );
        node_a
            .send_raw(destination, &reply_response(&tid, &[7u8; 20], vec![]))
            .await;
        let learned = wait_for_status(status, |s| s.node_count == 1, 5).await;
        assert_eq!(learned.node_count, 1);
        handle.shutdown();
    }

    #[tokio::test]
    async fn malformed_and_oversized_datagrams_are_ignored() {
        let node = FakeNode::bind(1).await;
        let handle = spawn(permissive_options(0, solid_id(1)));
        let status = handle.status();
        let service = wait_for_status(status.clone(), |s| s.active, 5).await;
        let destination = SocketAddr::from(([127, 0, 0, 1], service.port));
        let valid = query_ping([0xAA, 0xBB], &[7u8; 20]);
        node.send_raw(destination, b"this is not bencode at all")
            .await;
        node.send_raw(destination, &valid[..valid.len() / 2]).await;
        node.send_raw(destination, &vec![b'd'; krpc::MAX_DATAGRAM_SIZE + 1])
            .await;
        node.expect_silence(Duration::from_millis(300)).await;
        node.send_raw(destination, &valid).await;
        let reply = node.next_datagram().await;
        assert!(!is_error(&reply), "a valid ping must still be answered");
        assert!(response_values(&reply).contains_key(b"id".as_slice()));
        handle.shutdown();
    }

    #[tokio::test(start_paused = true)]
    async fn announce_peer_requires_a_current_or_previous_secret_token() {
        let node = FakeNode::bind(1).await;
        let handle = spawn(permissive_options(0, solid_id(1)));
        let status = handle.status();
        let service = wait_for_status(status.clone(), |s| s.active, 5).await;
        let destination = SocketAddr::from(([127, 0, 0, 1], service.port));
        let info_hash = [0x42u8; 20];
        let requester = [7u8; 20];

        node.send_raw(
            destination,
            &query_get_peers([1, 1], &requester, &info_hash),
        )
        .await;
        let token = token_from(&node.next_datagram().await);
        node.send_raw(
            destination,
            &query_announce_peer([1, 2], &requester, &info_hash, 7001, &token),
        )
        .await;
        assert!(!is_error(&node.next_datagram().await));
        node.send_raw(
            destination,
            &query_announce_peer([1, 3], &requester, &info_hash, 7002, b"junk"),
        )
        .await;
        assert!(
            is_error(&node.next_datagram().await),
            "a wrong token must be rejected with an error"
        );
        node.send_raw(
            destination,
            &query_get_peers([1, 4], &requester, &info_hash),
        )
        .await;
        assert_eq!(
            peers_from(&node.next_datagram().await),
            vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7001)]
        );

        tokio::time::advance(Duration::from_millis(TOKEN_ROTATION_MS + 2_000)).await;
        node.send_raw(
            destination,
            &query_get_peers([2, 1], &requester, &info_hash),
        )
        .await;
        let fresh = token_from(&node.next_datagram().await);
        assert_ne!(fresh, token, "rotation must change the issued tokens");
        node.send_raw(
            destination,
            &query_announce_peer([2, 2], &requester, &info_hash, 7002, &token),
        )
        .await;
        assert!(
            !is_error(&node.next_datagram().await),
            "a token from the previous secret must stay valid for one interval"
        );

        tokio::time::advance(Duration::from_millis(TOKEN_ROTATION_MS + 2_000)).await;
        node.send_raw(
            destination,
            &query_announce_peer([3, 1], &requester, &info_hash, 7003, &token),
        )
        .await;
        assert!(
            is_error(&node.next_datagram().await),
            "a token older than one rotation must be expired"
        );
        node.send_raw(
            destination,
            &query_announce_peer([3, 2], &requester, &info_hash, 7003, &fresh),
        )
        .await;
        assert!(!is_error(&node.next_datagram().await));
        handle.shutdown();
    }

    #[tokio::test]
    async fn responses_are_rate_limited_per_ip_and_refill() {
        use crate::dht::limiter::PER_IP_BURST;

        let node = FakeNode::bind(1).await;
        let handle = spawn(permissive_options(0, solid_id(1)));
        let status = handle.status();
        let service = wait_for_status(status.clone(), |s| s.active, 5).await;
        let destination = SocketAddr::from(([127, 0, 0, 1], service.port));
        let requester = [7u8; 20];
        for index in 0..PER_IP_BURST as u8 {
            node.send_raw(destination, &query_ping([0x10, index], &requester))
                .await;
            assert!(
                !is_error(&node.next_datagram().await),
                "the burst must be answered"
            );
        }
        node.send_raw(destination, &query_ping([0x11, 0], &requester))
            .await;
        node.expect_silence(Duration::from_millis(300)).await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        node.send_raw(destination, &query_ping([0x12, 0], &requester))
            .await;
        assert!(
            !is_error(&node.next_datagram().await),
            "one token must have refilled"
        );
        let limited = wait_for_status(status, |s| s.responses_rate_limited >= 1, 5).await;
        assert!(limited.responses_rate_limited >= 1);
        handle.shutdown();
    }

    #[tokio::test]
    async fn concurrent_queries_all_get_responses() {
        let handle = spawn(permissive_options(0, solid_id(1)));
        let status = handle.status();
        let service = wait_for_status(status, |s| s.active, 5).await;
        let destination = SocketAddr::from(([127, 0, 0, 1], service.port));
        let mut joins = Vec::new();
        for index in 0..6u8 {
            let node = FakeNode::bind(index + 1).await;
            let datagram = query_ping([0x20, index], &[7u8; 20]);
            joins.push(tokio::spawn(async move {
                node.send_raw(destination, &datagram).await;
                !is_error(&node.next_datagram().await)
            }));
        }
        for join in joins {
            assert!(
                join.await.unwrap(),
                "every concurrent query must be answered"
            );
        }
        handle.shutdown();
    }

    #[tokio::test]
    async fn responses_never_return_more_than_eight_nodes() {
        let router = FakeNode::bind(1).await;
        let distant: Vec<(NodeId, SocketAddrV4)> = (0..12)
            .map(|index| {
                (
                    solid_id(index + 2),
                    SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, index + 2), 45990 + index as u16),
                )
            })
            .collect();
        let handle = spawn(
            permissive_options(0, solid_id(1))
                .with_bootstrap(vec![format!("127.0.0.1:{}", router.addr().port())]),
        );
        let status = handle.status();
        let service = wait_for_status(status.clone(), |s| s.active, 5).await;
        let destination = SocketAddr::from(([127, 0, 0, 1], service.port));
        let request = router.next_datagram().await;
        router
            .send_raw(
                destination,
                &reply_response(
                    &tid_of(&request),
                    &[0xEEu8; 20],
                    vec![(b"nodes" as &[u8], nodes_value(&distant))],
                ),
            )
            .await;
        wait_for_status(status.clone(), |s| s.node_count == 13, 10).await;
        let asker = FakeNode::bind(14).await;
        asker
            .send_raw(
                destination,
                &query_find_node([0x40, 0x01], &[7u8; 20], &[0x99u8; 20]),
            )
            .await;
        let nodes = nodes_from(&asker.next_datagram().await);
        assert_eq!(nodes.len(), MAX_NODES_PER_RESPONSE);
        handle.shutdown();
    }

    #[tokio::test(start_paused = true)]
    async fn transaction_timeouts_expire_and_late_replies_are_ignored() {
        let node = FakeNode::bind(1).await;
        let handle = spawn(
            permissive_options(0, solid_id(1))
                .with_bootstrap(vec![format!("127.0.0.1:{}", node.addr().port())]),
        );
        let status = handle.status();
        let service = wait_for_status(status.clone(), |s| s.active, 5).await;
        let destination = SocketAddr::from(([127, 0, 0, 1], service.port));
        let request = node.next_datagram().await;
        let tid = tid_of(&request);
        tokio::time::advance(Duration::from_millis(TRANSACTION_TIMEOUT_MS + 1_500)).await;
        let expired = wait_for_status(status.clone(), |s| s.transaction_timeouts >= 1, 5).await;
        assert!(expired.transaction_timeouts >= 1);
        node.send_raw(destination, &reply_response(&tid, &[7u8; 20], vec![]))
            .await;
        tokio::time::advance(Duration::from_millis(1_500)).await;
        assert_eq!(
            status.borrow().node_count,
            0,
            "a reply after the transaction timed out must be ignored"
        );
        handle.shutdown();
    }

    #[tokio::test]
    async fn production_construction_uses_the_strict_address_filter() {
        let node = FakeNode::bind(1).await;
        let handle = spawn(DhtOptions::new(0, solid_id(1)));
        let status = handle.status();
        let service = wait_for_status(status.clone(), |s| s.active, 5).await;
        let destination = SocketAddr::from(([127, 0, 0, 1], service.port));
        node.send_raw(destination, &query_ping([0x30, 0], &[7u8; 20]))
            .await;
        assert!(
            !is_error(&node.next_datagram().await),
            "queries are answered regardless of the address filter"
        );
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        assert_eq!(
            status.borrow().node_count,
            0,
            "a loopback source must never be learned by production construction"
        );
        handle.shutdown();

        let permissive = FakeNode::bind(2).await;
        let handle = spawn(permissive_options(0, solid_id(2)));
        let status = handle.status();
        let service = wait_for_status(status, |s| s.active, 5).await;
        let destination = SocketAddr::from(([127, 0, 0, 1], service.port));
        permissive
            .send_raw(destination, &query_ping([0x31, 0], &[8u8; 20]))
            .await;
        assert!(!is_error(&permissive.next_datagram().await));
        wait_for_status(handle.status(), |s| s.node_count == 1, 5).await;
        handle.shutdown();
    }

    #[tokio::test(start_paused = true)]
    async fn stale_empty_buckets_trigger_refresh_queries() {
        let router = FakeNode::bind(1).await;
        let node = FakeNode::bind(2).await;
        let handle = spawn(
            permissive_options(0, solid_id(1))
                .with_bootstrap(vec![format!("127.0.0.1:{}", router.addr().port())]),
        );
        let status = handle.status();
        let service = wait_for_status(status.clone(), |s| s.active, 5).await;
        let destination = SocketAddr::from(([127, 0, 0, 1], service.port));
        let bootstrap_request = router.next_datagram().await;
        let bootstrap_target = find_node_target(&bootstrap_request).expect("bootstrap find_node");
        router
            .send_raw(
                destination,
                &reply_response(
                    &tid_of(&bootstrap_request),
                    &[9u8; 20],
                    vec![(
                        b"nodes" as &[u8],
                        nodes_value(&[(solid_id(2), node.addr())]),
                    )],
                ),
            )
            .await;
        let fill_request = node.next_datagram().await;
        node.send_raw(
            destination,
            &reply_response(&tid_of(&fill_request), &[2u8; 20], vec![]),
        )
        .await;
        wait_for_status(status, |s| s.node_count == 2, 5).await;

        tokio::time::advance(Duration::from_millis(BUCKET_REFRESH_MS + 2_000)).await;
        let refresh_request = node.next_datagram().await;
        let refresh_target = find_node_target(&refresh_request).expect("refresh find_node");
        assert_ne!(
            refresh_target, bootstrap_target,
            "the refresh must probe a bucket range, not our own id"
        );
        handle.shutdown();
    }
}
