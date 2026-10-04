use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::AbortHandle;
use tokio::time::{Instant as TokioInstant, MissedTickBehavior};

use crate::dht::filter::AddressFilter;
use crate::dht::krpc::{self, KrpcMessage, NodeInfo, Query, Response, TransactionId};
use crate::dht::limiter::ResponseGate;
use crate::dht::node_id::{cmp_distance_to, NodeId, SystemRandom};
use crate::dht::schedule;
use crate::dht::state as dht_state;
use crate::dht::store::PeerStore;
use crate::dht::table::{OfferOutcome, RoutingTable, K};
use crate::dht::tokens::TokenVault;

pub const DEFAULT_BOOTSTRAP_ROUTERS: &[&str] = &[
    "router.bittorrent.com:6881",
    "router.utorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "212.129.33.59:6881",
    "87.98.162.88:6881",
];
pub const TRANSACTION_TIMEOUT_MS: u64 = 5_000;
pub const MAX_PENDING_QUERIES: usize = 128;
pub const MAX_NODES_PER_RESPONSE: usize = 8;
pub const MAX_PEERS_PER_RESPONSE: usize = 25;
pub const MAX_CONCURRENT_LOOKUPS: usize = 4;
pub const MAX_LOOKUP_QUERIES: u32 = 100;
pub const LOOKUP_TIME_LIMIT_MS: u64 = 20_000;
pub const MAX_LOOKUP_PEERS: usize = 128;
pub const MAX_VERIFICATIONS: usize = 32;
const SWEEP_INTERVAL_MS: u64 = 1_000;
const MAX_FILL_QUERIES: u32 = 64;
const FILL_ALPHA: usize = 3;
const LOOKUP_ALPHA: usize = 3;
const LOOKUP_CANDIDATES: usize = 8;
const LOOKUP_MAX_CANDIDATES: usize = 256;
const LOOKUP_MAINTENANCE_RESERVE: usize = 32;
const REFRESH_QUERIES_PER_SWEEP: usize = 2;
const REBOOTSTRAP_IDLE_MS: u64 = 30_000;
const PERSIST_INTERVAL_MS: u64 = 5 * 60 * 1000;
const MAX_BOOTSTRAP_TARGETS: usize = 8;
const COMMAND_CHANNEL_CAPACITY: usize = 64;
const LOOKUP_QUEUE_CAPACITY: usize = 32;

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
    pub lookups_completed: u64,
    pub announces_sent: u64,
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
            lookups_completed: 0,
            announces_sent: 0,
        }
    }
}

pub struct DhtOptions {
    pub port: u16,
    pub self_id: NodeId,
    pub bootstrap: Vec<String>,
    listener_active: Arc<AtomicBool>,
    announce_port: Arc<AtomicU16>,
    persist_path: Option<PathBuf>,
    restore_nodes: Vec<NodeInfo>,
    filter: AddressFilter,
}

impl DhtOptions {
    pub fn new(port: u16, self_id: NodeId) -> DhtOptions {
        DhtOptions {
            port,
            self_id,
            bootstrap: Vec::new(),
            listener_active: Arc::new(AtomicBool::new(false)),
            announce_port: Arc::new(AtomicU16::new(0)),
            persist_path: None,
            restore_nodes: Vec::new(),
            filter: AddressFilter::strict(),
        }
    }

    pub fn with_bootstrap(mut self, bootstrap: Vec<String>) -> DhtOptions {
        self.bootstrap = bootstrap;
        self
    }

    pub fn with_persist(
        mut self,
        persist_path: Option<PathBuf>,
        restore_nodes: Vec<NodeInfo>,
    ) -> DhtOptions {
        self.persist_path = persist_path;
        self.restore_nodes = restore_nodes;
        self
    }

    pub fn with_listener_state(
        mut self,
        listener_active: Arc<AtomicBool>,
        announce_port: Arc<AtomicU16>,
    ) -> DhtOptions {
        self.listener_active = listener_active;
        self.announce_port = announce_port;
        self
    }

    #[doc(hidden)]
    pub fn with_address_filter_for_tests(mut self, filter: AddressFilter) -> DhtOptions {
        self.filter = filter;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DhtPeers {
    pub info_hash: [u8; 20],
    pub peers: Vec<SocketAddr>,
}

#[derive(Debug)]
pub enum DhtCommand {
    Lookup {
        info_hash: [u8; 20],
        result: mpsc::Sender<DhtPeers>,
    },
    VerifyCandidate {
        addr: SocketAddrV4,
    },
    PersistAndShutdown {
        reply: oneshot::Sender<()>,
    },
}

#[derive(Clone)]
pub struct DhtHandle {
    status: watch::Receiver<DhtStatus>,
    commands: mpsc::Sender<DhtCommand>,
    abort: AbortHandle,
}

impl DhtHandle {
    pub fn status(&self) -> watch::Receiver<DhtStatus> {
        self.status.clone()
    }

    pub fn shutdown(&self) {
        self.abort.abort();
    }

    pub fn request_lookup(&self, info_hash: [u8; 20], result: mpsc::Sender<DhtPeers>) -> bool {
        self.commands
            .try_send(DhtCommand::Lookup { info_hash, result })
            .is_ok()
    }

    pub fn verify_candidate(&self, addr: SocketAddrV4) -> bool {
        self.commands
            .try_send(DhtCommand::VerifyCandidate { addr })
            .is_ok()
    }

    pub async fn persist_and_shutdown(&self) {
        let (reply, rx) = oneshot::channel();
        if self
            .commands
            .try_send(DhtCommand::PersistAndShutdown { reply })
            .is_ok()
        {
            let _ = tokio::time::timeout(Duration::from_secs(2), rx).await;
        }
        self.abort.abort();
    }
}

pub fn spawn(options: DhtOptions) -> DhtHandle {
    let (commands_tx, commands_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
    let (status_tx, status_rx) = watch::channel(DhtStatus::inactive(options.port, None));
    let task = tokio::spawn(run(options, status_tx, commands_rx));
    DhtHandle {
        status: status_rx,
        commands: commands_tx,
        abort: task.abort_handle(),
    }
}

pub async fn bind(options: DhtOptions) -> Result<DhtHandle, std::io::Error> {
    let (commands_tx, commands_rx) = mpsc::channel(COMMAND_CHANNEL_CAPACITY);
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, options.port)).await?;
    let (status_tx, status_rx) = watch::channel(DhtStatus::inactive(options.port, None));
    let task = tokio::spawn(run_bound(socket, options, status_tx, commands_rx));
    Ok(DhtHandle {
        status: status_rx,
        commands: commands_tx,
        abort: task.abort_handle(),
    })
}

async fn run(
    options: DhtOptions,
    status: watch::Sender<DhtStatus>,
    commands: mpsc::Receiver<DhtCommand>,
) {
    match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, options.port)).await {
        Ok(socket) => run_bound(socket, options, status, commands).await,
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
    Lookup { id: u32 },
    Announce,
    Verify,
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
    lookups_completed: u64,
    announces_sent: u64,
}

struct FillState {
    active: bool,
    queries_sent: u32,
    candidates: Vec<NodeInfo>,
}

struct Lookup {
    info_hash: [u8; 20],
    result: mpsc::Sender<DhtPeers>,
    queried: HashSet<SocketAddrV4>,
    failed: HashSet<SocketAddrV4>,
    candidates: Vec<NodeInfo>,
    responded: Vec<(NodeInfo, Option<Vec<u8>>)>,
    peers: Vec<SocketAddrV4>,
    queries_sent: u32,
    started_ms: u64,
}

impl Lookup {
    fn target(&self) -> NodeId {
        NodeId::from_bytes(self.info_hash)
    }

    fn is_finished(&self, now_ms: u64, outstanding_queries: usize) -> bool {
        if self.queries_sent >= MAX_LOOKUP_QUERIES {
            return true;
        }
        if now_ms.saturating_sub(self.started_ms) >= LOOKUP_TIME_LIMIT_MS {
            return true;
        }
        let mut closest: Vec<NodeInfo> = self.candidates.clone();
        closest.sort_by(|a, b| cmp_distance_to(&a.id, &b.id, &self.target()));
        closest.truncate(LOOKUP_CANDIDATES);
        let closest_resolved = !closest.is_empty()
            && closest.iter().all(|candidate| {
                self.queried.contains(&candidate.addr)
                    && (self
                        .responded
                        .iter()
                        .any(|(node, _)| node.addr == candidate.addr)
                        || self.failed.contains(&candidate.addr))
            });
        if closest_resolved {
            return true;
        }
        let unqueried = self
            .candidates
            .iter()
            .any(|candidate| !self.queried.contains(&candidate.addr));
        !unqueried && outstanding_queries == 0
    }

    fn closest_with_tokens(&self, count: usize) -> Vec<(SocketAddrV4, Vec<u8>)> {
        let mut responded = self.responded.clone();
        responded.sort_by(|a, b| cmp_distance_to(&a.0.id, &b.0.id, &self.target()));
        responded
            .into_iter()
            .filter_map(|(node, token)| token.map(|token| (node.addr, token)))
            .take(count)
            .collect()
    }
}

struct NodeState {
    socket: UdpSocket,
    port: u16,
    self_id: NodeId,
    bootstrap: Vec<String>,
    filter: AddressFilter,
    listener_active: Arc<AtomicBool>,
    announce_port: Arc<AtomicU16>,
    table: RoutingTable,
    store: PeerStore,
    vault: TokenVault,
    gate: ResponseGate,
    pending: HashMap<(TransactionId, SocketAddrV4), PendingQuery>,
    queried: HashSet<SocketAddrV4>,
    fill: FillState,
    lookups: HashMap<u32, Lookup>,
    lookup_queue: VecDeque<Lookup>,
    next_lookup_id: u32,
    announced_at: HashMap<[u8; 20], u64>,
    commands: mpsc::Receiver<DhtCommand>,
    commands_open: bool,
    counters: Counters,
    last_bootstrap_ms: Option<u64>,
    last_persist_ms: Option<u64>,
    persist_path: Option<PathBuf>,
    deferred_lookups: Vec<Lookup>,
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
        let learned: Vec<NodeInfo> = response
            .nodes
            .into_iter()
            .filter(|node| self.filter.allows(node.addr) && node.id != self.self_id)
            .collect();
        match pending.kind {
            PendingKind::Lookup { id } => {
                let peers: Vec<SocketAddrV4> = response
                    .peers
                    .into_iter()
                    .filter_map(|peer| match peer {
                        SocketAddr::V4(v4) if self.filter.allows(v4) => Some(v4),
                        _ => None,
                    })
                    .collect();
                self.absorb_lookup_response(id, source, responder, response.token, peers, learned);
                self.advance_lookup(id).await;
            }
            PendingKind::Fill => {
                self.fill.candidates.extend(
                    learned
                        .into_iter()
                        .filter(|node| !self.queried.contains(&node.addr)),
                );
                self.maybe_dispatch_fill().await;
            }
            _ => {}
        }
    }

    fn absorb_lookup_response(
        &mut self,
        id: u32,
        source: SocketAddrV4,
        responder: NodeId,
        token: Option<Vec<u8>>,
        peers: Vec<SocketAddrV4>,
        learned: Vec<NodeInfo>,
    ) {
        let Some(lookup) = self.lookups.get_mut(&id) else {
            return;
        };
        if let Some(token) = token {
            lookup
                .responded
                .push((NodeInfo::new(responder, source), Some(token)));
        } else {
            lookup
                .responded
                .push((NodeInfo::new(responder, source), None));
        }
        for peer in peers {
            if lookup.peers.len() >= MAX_LOOKUP_PEERS {
                break;
            }
            if !lookup.peers.contains(&peer) {
                lookup.peers.push(peer);
            }
        }
        for node in learned {
            if lookup.candidates.len() >= LOOKUP_MAX_CANDIDATES {
                break;
            }
            if lookup.queried.contains(&node.addr)
                || lookup
                    .candidates
                    .iter()
                    .any(|known| known.addr == node.addr)
            {
                continue;
            }
            lookup.candidates.push(node);
        }
    }

    fn outstanding_lookup_queries(&self, id: u32) -> usize {
        self.pending
            .values()
            .filter(|query| {
                matches!(query.kind, PendingKind::Lookup { id: pending_id } if pending_id == id)
            })
            .count()
    }

    async fn advance_lookup(&mut self, id: u32) {
        let now = self.now_ms();
        let outstanding = self.outstanding_lookup_queries(id);
        let finished = self
            .lookups
            .get(&id)
            .is_some_and(|lookup| lookup.is_finished(now, outstanding));
        if finished {
            self.finish_lookup(id).await;
            return;
        }
        self.dispatch_lookup(id).await;
    }

    async fn dispatch_lookup(&mut self, id: u32) {
        let now = self.now_ms();
        let targets: Vec<(SocketAddrV4, NodeId)> = {
            let Some(lookup) = self.lookups.get_mut(&id) else {
                return;
            };
            if lookup.queries_sent >= MAX_LOOKUP_QUERIES
                || now.saturating_sub(lookup.started_ms) >= LOOKUP_TIME_LIMIT_MS
            {
                return;
            }
            let room = (MAX_PENDING_QUERIES - LOOKUP_MAINTENANCE_RESERVE)
                .saturating_sub(self.pending.len());
            if room == 0 {
                return;
            }
            let target = lookup.target();
            lookup
                .candidates
                .sort_by(|a, b| cmp_distance_to(&a.id, &b.id, &target));
            let mut targets = Vec::new();
            for candidate in &lookup.candidates {
                if targets.len() >= LOOKUP_ALPHA.min(room) {
                    break;
                }
                if lookup.queried.contains(&candidate.addr) {
                    continue;
                }
                targets.push((candidate.addr, candidate.id));
            }
            for (addr, _) in &targets {
                lookup.queried.insert(*addr);
            }
            lookup.queries_sent += targets.len() as u32;
            targets
        };
        for (addr, node_id) in targets {
            let query = Query::GetPeers {
                info_hash: self
                    .lookups
                    .get(&id)
                    .map(|lookup| lookup.info_hash)
                    .unwrap_or([0u8; 20]),
            };
            self.send_query(addr, query, PendingKind::Lookup { id }, Some(node_id))
                .await;
        }
    }

    async fn finish_lookup(&mut self, id: u32) {
        let Some(lookup) = self.lookups.remove(&id) else {
            return;
        };
        self.counters.lookups_completed = self.counters.lookups_completed.saturating_add(1);
        let now = self.now_ms();
        let _ = lookup.result.try_send(DhtPeers {
            info_hash: lookup.info_hash,
            peers: lookup
                .peers
                .iter()
                .map(|peer| SocketAddr::V4(*peer))
                .collect(),
        });
        self.maybe_announce(lookup, now).await;
        self.start_queued_lookups().await;
    }

    async fn maybe_announce(&mut self, lookup: Lookup, now: u64) {
        if !self.listener_active.load(Ordering::Relaxed) {
            return;
        }
        if !schedule::should_announce_after_lookup(
            self.announced_at.get(&lookup.info_hash).copied(),
            now,
        ) {
            return;
        }
        let targets = lookup.closest_with_tokens(MAX_NODES_PER_RESPONSE);
        if targets.is_empty() {
            return;
        }
        self.announced_at.insert(lookup.info_hash, now);
        let port = self.announce_port.load(Ordering::Relaxed);
        for (addr, token) in targets {
            self.send_query(
                addr,
                Query::AnnouncePeer {
                    info_hash: lookup.info_hash,
                    port,
                    token,
                    implied_port: false,
                },
                PendingKind::Announce,
                None,
            )
            .await;
            self.counters.announces_sent = self.counters.announces_sent.saturating_add(1);
        }
    }

    async fn start_queued_lookups(&mut self) {
        while self.lookups.len() < MAX_CONCURRENT_LOOKUPS {
            let Some(lookup) = self.lookup_queue.pop_front() else {
                return;
            };
            self.start_lookup(lookup).await;
        }
    }

    async fn accept_lookup(&mut self, info_hash: [u8; 20], result: mpsc::Sender<DhtPeers>) {
        let lookup = Lookup {
            info_hash,
            result,
            queried: HashSet::new(),
            failed: HashSet::new(),
            candidates: self
                .table
                .closest_nodes(&NodeId::from_bytes(info_hash), LOOKUP_CANDIDATES),
            responded: Vec::new(),
            peers: Vec::new(),
            queries_sent: 0,
            started_ms: self.now_ms(),
        };
        if self.table.is_empty() {
            if self.deferred_lookups.len() >= LOOKUP_QUEUE_CAPACITY {
                let _ = lookup.result.try_send(DhtPeers {
                    info_hash,
                    peers: Vec::new(),
                });
                return;
            }
            self.deferred_lookups.push(lookup);
            return;
        }
        self.start_lookup(lookup).await;
    }

    async fn start_lookup(&mut self, lookup: Lookup) {
        if self.lookups.len() >= MAX_CONCURRENT_LOOKUPS {
            if self.lookup_queue.len() >= LOOKUP_QUEUE_CAPACITY {
                let _ = lookup.result.try_send(DhtPeers {
                    info_hash: lookup.info_hash,
                    peers: Vec::new(),
                });
                return;
            }
            self.lookup_queue.push_back(lookup);
            return;
        }
        let id = self.next_lookup_id;
        self.next_lookup_id = self.next_lookup_id.wrapping_add(1);
        let mut lookup = lookup;
        lookup.started_ms = self.now_ms();
        self.lookups.insert(id, lookup);
        self.dispatch_lookup(id).await;
    }

    fn outstanding_verifications(&self) -> usize {
        self.pending
            .values()
            .filter(|query| query.kind == PendingKind::Verify)
            .count()
    }

    async fn accept_verification(&mut self, addr: SocketAddrV4) {
        if !self.filter.allows(addr) || addr.port() == 0 {
            return;
        }
        if self.outstanding_verifications() >= MAX_VERIFICATIONS {
            return;
        }
        let already_outstanding = self.pending.iter().any(|((_, destination), query)| {
            query.kind == PendingKind::Verify && *destination == addr
        });
        if already_outstanding {
            return;
        }
        self.send_query(addr, Query::Ping, PendingKind::Verify, None)
            .await;
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
        let mut expired_lookups: Vec<u32> = Vec::new();
        for key in expired {
            if let Some(query) = self.pending.remove(&key) {
                self.counters.timeouts = self.counters.timeouts.saturating_add(1);
                if let Some(node_id) = query.node_id {
                    self.table.note_failure(&node_id, now);
                }
                if let PendingKind::Lookup { id } = query.kind {
                    if let Some(lookup) = self.lookups.get_mut(&id) {
                        lookup.failed.insert(key.1);
                    }
                    expired_lookups.push(id);
                }
            }
        }
        self.vault.rotate_if_due(now, &self.random);
        self.store.expire(now);
        if self
            .last_persist_ms
            .is_none_or(|last| now.saturating_sub(last) >= PERSIST_INTERVAL_MS)
        {
            self.persist();
            self.last_persist_ms = Some(now);
        }
        if !self.deferred_lookups.is_empty() && !self.table.is_empty() {
            for lookup in std::mem::take(&mut self.deferred_lookups) {
                self.start_lookup(lookup).await;
            }
        }
        if self.fill.active {
            self.maybe_dispatch_fill().await;
        } else {
            self.refresh_stale_buckets(now).await;
            if self.table.len() < K
                && now.saturating_sub(self.last_bootstrap_ms.unwrap_or(0)) >= REBOOTSTRAP_IDLE_MS
                && !self.bootstrap.is_empty()
            {
                self.bootstrap().await;
            }
        }
        let mut to_finish: Vec<u32> = Vec::new();
        for id in self.lookups.keys().copied().collect::<Vec<_>>() {
            let outstanding = self.outstanding_lookup_queries(id);
            if self
                .lookups
                .get(&id)
                .is_some_and(|lookup| lookup.is_finished(now, outstanding))
            {
                to_finish.push(id);
            }
        }
        for id in to_finish {
            self.finish_lookup(id).await;
        }
        for id in expired_lookups {
            if self.lookups.contains_key(&id) {
                self.dispatch_lookup(id).await;
            }
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
            if !self.filter.allows(addr) {
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

    fn persist(&self) {
        let Some(path) = &self.persist_path else {
            return;
        };
        let nodes: Vec<NodeInfo> = self
            .table
            .entries()
            .into_iter()
            .map(|entry| entry.info)
            .take(dht_state::MAX_PERSISTED_NODES)
            .collect();
        let _ = dht_state::save(path, &self.self_id, &nodes);
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
            lookups_completed: self.counters.lookups_completed,
            announces_sent: self.counters.announces_sent,
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

async fn run_bound(
    socket: UdpSocket,
    options: DhtOptions,
    status: watch::Sender<DhtStatus>,
    commands: mpsc::Receiver<DhtCommand>,
) {
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
        listener_active: options.listener_active,
        announce_port: options.announce_port,
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
        lookups: HashMap::new(),
        lookup_queue: VecDeque::new(),
        next_lookup_id: 1,
        announced_at: HashMap::new(),
        commands,
        commands_open: true,
        counters: Counters::default(),
        last_bootstrap_ms: None,
        last_persist_ms: None,
        persist_path: options.persist_path,
        deferred_lookups: Vec::new(),
        start,
        random: SystemRandom,
    };
    for node_info in options.restore_nodes {
        if node_info.id != node.self_id && node.filter.allows(node_info.addr) {
            node.table.offer(node_info, 0);
        }
    }
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
            command = node.commands.recv(), if node.commands_open => match command {
                Some(command) => match command {
                    DhtCommand::Lookup { info_hash, result } => {
                        node.accept_lookup(info_hash, result).await;
                    }
                    DhtCommand::VerifyCandidate { addr } => {
                        node.accept_verification(addr).await;
                    }
                    DhtCommand::PersistAndShutdown { reply } => {
                        node.persist();
                        let _ = reply.send(());
                        break;
                    }
                },
                None => node.commands_open = false,
            },
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

        async fn send_raw(&self, destination: impl Into<SocketAddr>, datagram: &[u8]) {
            self.socket
                .send_to(datagram, destination.into())
                .await
                .unwrap();
        }

        async fn recv_from_raw(&self, limit: Duration) -> Option<(Vec<u8>, SocketAddr)> {
            let mut buffer = [0u8; 4096];
            match tokio::time::timeout(limit, self.socket.recv_from(&mut buffer)).await {
                Ok(Ok((size, source))) => Some((buffer[..size].to_vec(), source)),
                _ => None,
            }
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

    fn query_announce_peer_implied(
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
                    (b"implied_port", int(1)),
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

    fn reply_error(tid: &[u8], code: i64, message: &str) -> Vec<u8> {
        wire::encode(&dict(vec![
            (b"t", b(tid)),
            (b"y", b(b"e")),
            (b"e", Value::List(vec![int(code), b(message.as_bytes())])),
        ]))
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

    #[derive(Clone, Copy, PartialEq)]
    enum NodeBehavior {
        Good,
        GoodNoToken,
        Garbage,
        InvalidAddresses,
        Silent,
        Oversized,
        Truncated,
    }

    type SwarmPeers = HashMap<[u8; 20], Vec<(std::net::Ipv4Addr, u16)>>;

    struct Swarm {
        peers: std::sync::Mutex<SwarmPeers>,
        queries: std::sync::Mutex<Vec<([u8; 20], &'static str)>>,
        secret: u8,
    }

    impl Swarm {
        fn new(secret: u8) -> Swarm {
            Swarm {
                peers: std::sync::Mutex::new(HashMap::new()),
                queries: std::sync::Mutex::new(Vec::new()),
                secret,
            }
        }

        fn token_for(&self, ip: std::net::Ipv4Addr) -> Vec<u8> {
            let octets = ip.octets();
            vec![
                octets[3],
                self.secret,
                octets[0] ^ self.secret,
                0x5A,
                octets[1],
                self.secret ^ 0x33,
                octets[2],
                0x3C,
            ]
        }

        fn plant(&self, info_hash: [u8; 20], peer: (std::net::Ipv4Addr, u16)) {
            self.peers
                .lock()
                .unwrap()
                .entry(info_hash)
                .or_default()
                .push(peer);
        }
    }

    struct SwarmNode {
        node: FakeNode,
        id: NodeId,
        behavior: NodeBehavior,
        neighbors: Arc<std::sync::Mutex<Vec<(NodeId, SocketAddrV4)>>>,
        swarm: Arc<Swarm>,
    }

    impl SwarmNode {
        fn addr(&self) -> SocketAddrV4 {
            self.node.addr()
        }

        fn spawn_responder(self) {
            tokio::spawn(async move {
                loop {
                    let Some((datagram, source)) =
                        self.node.recv_from_raw(Duration::from_secs(600)).await
                    else {
                        continue;
                    };
                    let SocketAddr::V4(source) = source else {
                        continue;
                    };
                    match self.behavior {
                        NodeBehavior::Silent => {}
                        NodeBehavior::Garbage => {
                            self.node.send_raw(source, &[0x07, 0x11, 0x22, 0x33]).await;
                        }
                        NodeBehavior::Oversized => {
                            self.node.send_raw(source, &vec![b'x'; 3000]).await;
                        }
                        NodeBehavior::Truncated => {
                            let reply =
                                reply_response(&[0x01, 0x02], &self.id.as_bytes().clone(), vec![]);
                            self.node.send_raw(source, &reply[..reply.len() / 2]).await;
                        }
                        NodeBehavior::InvalidAddresses => {
                            self.reply_invalid_addresses(&datagram, source).await;
                        }
                        NodeBehavior::Good => {
                            self.reply_good(&datagram, source, true).await;
                        }
                        NodeBehavior::GoodNoToken => {
                            self.reply_good(&datagram, source, false).await;
                        }
                    }
                }
            });
        }

        async fn reply_invalid_addresses(&self, datagram: &[u8], source: SocketAddrV4) {
            let Some(parsed) = wire::decode(datagram) else {
                return;
            };
            let Some(Value::Bytes(tid)) = wire::get(&parsed, b"t") else {
                return;
            };
            let bogus = [
                NodeId::from_bytes([0x51; 20]),
                NodeId::from_bytes([0x52; 20]),
            ];
            let addrs = [
                SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 6881),
                SocketAddrV4::new(Ipv4Addr::new(255, 255, 255, 255), 6881),
                SocketAddrV4::new(Ipv4Addr::new(10, 9, 9, 9), 0),
                SocketAddrV4::new(Ipv4Addr::new(224, 0, 0, 7), 6881),
            ];
            let entries: Vec<(NodeId, SocketAddrV4)> = bogus
                .iter()
                .enumerate()
                .map(|(index, id)| (*id, addrs[index % addrs.len()]))
                .collect();
            self.node
                .send_raw(
                    source,
                    &reply_response(
                        tid,
                        &self.id.as_bytes().clone(),
                        vec![(b"nodes" as &[u8], nodes_value(&entries))],
                    ),
                )
                .await;
        }

        async fn reply_good(&self, datagram: &[u8], source: SocketAddrV4, with_token: bool) {
            let Some(parsed) = wire::decode(datagram) else {
                return;
            };
            let Some(Value::Bytes(tid)) = wire::get(&parsed, b"t") else {
                return;
            };
            let Some(Value::Bytes(method)) = wire::get(&parsed, b"q") else {
                return;
            };
            let arguments = wire::get(&parsed, b"a")
                .cloned()
                .unwrap_or(Value::Dict(Vec::new()));
            match method.as_slice() {
                b"ping" | b"find_node" => {
                    let neighbors = self.neighbors.lock().unwrap().clone();
                    let nodes: Vec<(NodeId, SocketAddrV4)> = neighbors
                        .iter()
                        .filter(|(_, addr)| *addr != self.addr())
                        .take(8)
                        .cloned()
                        .collect();
                    let mut values = vec![];
                    if *method == b"find_node" {
                        values.push((b"nodes" as &[u8], nodes_value(&nodes)));
                    }
                    self.node
                        .send_raw(
                            source,
                            &reply_response(tid, &self.id.as_bytes().clone(), values),
                        )
                        .await;
                }
                b"get_peers" => {
                    let info_hash = match wire::get(&arguments, b"info_hash") {
                        Some(Value::Bytes(bytes)) => {
                            let mut hash = [0u8; 20];
                            hash.copy_from_slice(bytes);
                            hash
                        }
                        _ => return,
                    };
                    self.swarm
                        .queries
                        .lock()
                        .unwrap()
                        .push((info_hash, "get_peers"));
                    let token = self.swarm.token_for(*source.ip());
                    let stored = self
                        .swarm
                        .peers
                        .lock()
                        .unwrap()
                        .get(&info_hash)
                        .cloned()
                        .unwrap_or_default();
                    let mut values = vec![];
                    if stored.is_empty() {
                        let neighbors = self.neighbors.lock().unwrap().clone();
                        let nodes: Vec<(NodeId, SocketAddrV4)> = neighbors
                            .iter()
                            .filter(|(_, addr)| *addr != self.addr())
                            .take(8)
                            .cloned()
                            .collect();
                        values.push((b"nodes" as &[u8], nodes_value(&nodes)));
                    } else {
                        let entries = stored
                            .iter()
                            .map(|(ip, port)| {
                                let mut raw = Vec::new();
                                raw.extend_from_slice(&ip.octets());
                                raw.extend_from_slice(&port.to_be_bytes());
                                Value::Bytes(raw)
                            })
                            .collect();
                        values.push((b"values" as &[u8], Value::List(entries)));
                    }
                    if with_token {
                        values.push((b"token" as &[u8], Value::Bytes(token)));
                    }
                    self.node
                        .send_raw(
                            source,
                            &reply_response(tid, &self.id.as_bytes().clone(), values),
                        )
                        .await;
                }
                b"announce_peer" => {
                    let info_hash = match wire::get(&arguments, b"info_hash") {
                        Some(Value::Bytes(bytes)) => {
                            let mut hash = [0u8; 20];
                            hash.copy_from_slice(bytes);
                            hash
                        }
                        _ => return,
                    };
                    self.swarm
                        .queries
                        .lock()
                        .unwrap()
                        .push((info_hash, "announce_peer"));
                    let token_ok = match wire::get(&arguments, b"token") {
                        Some(Value::Bytes(token)) => *token == self.swarm.token_for(*source.ip()),
                        _ => false,
                    };
                    if !token_ok {
                        self.node
                            .send_raw(source, &reply_error(tid, 203, "Bad token"))
                            .await;
                        return;
                    }
                    let claimed_port = match wire::get(&arguments, b"port") {
                        Some(Value::Int(port)) => *port as u16,
                        _ => 0,
                    };
                    let implied =
                        matches!(wire::get(&arguments, b"implied_port"), Some(Value::Int(1)));
                    let port = if implied { source.port() } else { claimed_port };
                    self.swarm
                        .peers
                        .lock()
                        .unwrap()
                        .entry(info_hash)
                        .or_default()
                        .push((*source.ip(), port));
                    self.node
                        .send_raw(
                            source,
                            &reply_response(tid, &self.id.as_bytes().clone(), vec![]),
                        )
                        .await;
                }
                _ => {}
            }
        }
    }

    async fn swarm_network(
        sizes: &[(NodeBehavior, usize)],
        swarm: Arc<Swarm>,
    ) -> (Vec<SwarmNode>, SocketAddrV4) {
        let neighbors: Arc<std::sync::Mutex<Vec<(NodeId, SocketAddrV4)>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut nodes = Vec::new();
        let mut octet = 1u8;
        for (behavior, count) in sizes {
            for _ in 0..*count {
                let mut id_bytes = [0u8; 20];
                id_bytes[0] = 0x70;
                id_bytes[1] = octet;
                id_bytes[2] = 0x11;
                let node = SwarmNode {
                    node: FakeNode::bind(octet).await,
                    id: NodeId::from_bytes(id_bytes),
                    behavior: *behavior,
                    neighbors: neighbors.clone(),
                    swarm: swarm.clone(),
                };
                neighbors.lock().unwrap().push((node.id, node.addr()));
                nodes.push(node);
                octet += 1;
            }
        }
        let router = nodes[0].addr();
        (nodes, router)
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
        let mut distant: Vec<(NodeId, SocketAddrV4)> = Vec::new();
        for index in 0..12u8 {
            let node = FakeNode::bind(index + 2).await;
            let id = solid_id(index + 2);
            distant.push((id, node.addr()));
            spawn_id_responder(node, id);
        }
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

    fn spawn_id_responder(node: FakeNode, id: NodeId) {
        tokio::spawn(async move {
            loop {
                let Some((datagram, source)) = node.recv_from_raw(Duration::from_secs(600)).await
                else {
                    continue;
                };
                let SocketAddr::V4(source) = source else {
                    continue;
                };
                let Some(parsed) = wire::decode(&datagram) else {
                    continue;
                };
                let Some(Value::Bytes(tid)) = wire::get(&parsed, b"t") else {
                    continue;
                };
                node.send_raw(source, &reply_response(tid, &id.as_bytes().clone(), vec![]))
                    .await;
            }
        });
    }

    #[tokio::test]
    async fn lookup_on_a_simulated_network_converges_and_survives_hostile_nodes() {
        let swarm = Arc::new(Swarm::new(0xA1));
        let info_hash = [0x63u8; 20];
        let planted: Vec<SocketAddr> = (0..5u8)
            .map(|index| SocketAddr::from(([93, 1, 0, index], 6800 + index as u16)))
            .collect();
        for peer in &planted {
            let SocketAddr::V4(v4) = *peer else {
                panic!("planted peers are ipv4");
            };
            swarm.plant(info_hash, (*v4.ip(), v4.port()));
        }
        let (nodes, router) = swarm_network(
            &[
                (NodeBehavior::Good, 45),
                (NodeBehavior::GoodNoToken, 1),
                (NodeBehavior::Garbage, 1),
                (NodeBehavior::InvalidAddresses, 1),
                (NodeBehavior::Silent, 1),
                (NodeBehavior::Oversized, 1),
                (NodeBehavior::Truncated, 1),
            ],
            swarm.clone(),
        )
        .await;
        assert_eq!(nodes.len(), 51);
        for node in nodes {
            node.spawn_responder();
        }
        let handle = spawn(
            permissive_options(0, solid_id(1))
                .with_bootstrap(vec![format!("127.0.0.1:{}", router.port())]),
        );
        let status = handle.status();
        wait_for_status(status.clone(), |s| s.active, 5).await;
        wait_for_status(status.clone(), |s| s.node_count >= 8, 20).await;

        let (result_tx, mut result_rx) = mpsc::channel(1);
        assert!(handle.request_lookup(info_hash, result_tx));
        let outcome = tokio::time::timeout(Duration::from_secs(20), result_rx.recv())
            .await
            .expect("lookup outcome within timeout")
            .expect("outcome channel open");
        assert_eq!(outcome.info_hash, info_hash);
        let mut got = outcome.peers.clone();
        got.sort();
        let mut wanted = planted.clone();
        wanted.sort();
        assert_eq!(
            got, wanted,
            "the lookup must return exactly the planted peers"
        );
        wait_for_status(status, |s| s.lookups_completed >= 1, 5).await;
        handle.shutdown();
    }

    #[tokio::test]
    async fn announce_is_stored_then_found_by_a_second_service() {
        let swarm = Arc::new(Swarm::new(0xB2));
        let info_hash = [0x64u8; 20];
        let (nodes, router) = swarm_network(&[(NodeBehavior::Good, 6)], swarm.clone()).await;
        for node in nodes {
            node.spawn_responder();
        }
        let bootstrap = vec![format!("127.0.0.1:{}", router.port())];
        let listener_active = Arc::new(AtomicBool::new(true));
        let announce_port = Arc::new(AtomicU16::new(7777));
        let announcer = spawn(
            permissive_options(0, solid_id(1))
                .with_bootstrap(bootstrap.clone())
                .with_listener_state(listener_active, announce_port),
        );
        let status = announcer.status();
        wait_for_status(status, |s| s.node_count >= 2, 20).await;
        let (tx, mut announcer_rx) = mpsc::channel(1);
        assert!(announcer.request_lookup(info_hash, tx));
        let outcome = tokio::time::timeout(Duration::from_secs(20), announcer_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(outcome.peers.is_empty(), "nothing planted yet");
        wait_for_status(announcer.status(), |s| s.announces_sent >= 1, 20).await;

        let finder = spawn(permissive_options(0, solid_id(2)).with_bootstrap(bootstrap.clone()));
        wait_for_status(finder.status(), |s| s.node_count >= 2, 20).await;
        let (tx, mut finder_rx) = mpsc::channel(1);
        assert!(finder.request_lookup(info_hash, tx));
        let outcome = tokio::time::timeout(Duration::from_secs(20), finder_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            outcome.peers,
            vec![SocketAddr::from(([127, 0, 0, 1], 7777))],
            "the second service must discover the announced port"
        );
        announcer.shutdown();
        finder.shutdown();
    }

    #[tokio::test]
    async fn implied_port_one_uses_the_source_and_the_ip_is_always_the_packets_source() {
        let node = FakeNode::bind(1).await;
        let handle = spawn(permissive_options(0, solid_id(1)));
        let status = handle.status();
        let service = wait_for_status(status, |s| s.active, 5).await;
        let destination = SocketAddr::from(([127, 0, 0, 1], service.port));
        let info_hash = [0x44u8; 20];
        let requester = [7u8; 20];
        let source_port = node.addr().port();

        node.send_raw(
            destination,
            &query_get_peers([1, 1], &requester, &info_hash),
        )
        .await;
        let token = token_from(&node.next_datagram().await);
        let smuggled = wire::encode(&dict(vec![
            (b"t", b([1, 2].as_slice())),
            (b"y", b(b"q")),
            (b"q", b(b"announce_peer")),
            (
                b"a",
                dict(vec![
                    (b"id", b(requester.as_slice())),
                    (b"info_hash", b(info_hash.as_slice())),
                    (b"port", int(7001)),
                    (b"token", b(token.as_slice())),
                    (b"implied_port", int(0)),
                    (b"ip", b(&[6, 6, 6, 6])),
                ]),
            ),
        ]));
        node.send_raw(destination, &smuggled).await;
        assert!(!is_error(&node.next_datagram().await));
        node.send_raw(
            destination,
            &query_get_peers([1, 3], &requester, &info_hash),
        )
        .await;
        assert_eq!(
            peers_from(&node.next_datagram().await),
            vec![SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7001)],
            "implied_port 0 keeps the claimed port but the source ip"
        );

        node.send_raw(
            destination,
            &query_announce_peer_implied([2, 1], &requester, &info_hash, 9999, &token),
        )
        .await;
        assert!(!is_error(&node.next_datagram().await));
        node.send_raw(
            destination,
            &query_get_peers([2, 2], &requester, &info_hash),
        )
        .await;
        assert_eq!(
            peers_from(&node.next_datagram().await),
            vec![
                SocketAddrV4::new(Ipv4Addr::LOCALHOST, source_port),
                SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7001),
            ],
            "implied_port 1 replaces the claimed port with the packet source port"
        );
        handle.shutdown();
    }

    #[tokio::test]
    async fn port_candidates_are_capped_deduplicated_and_verified_before_insertion() {
        let answering = FakeNode::bind(1).await;
        let answering_id = solid_id(0x81);
        let answering_addr = answering.addr();
        spawn_id_responder(answering, answering_id);
        let mut candidates: Vec<FakeNode> = Vec::new();
        for octet in 2..42u8 {
            candidates.push(FakeNode::bind(octet).await);
        }
        let handle = spawn(permissive_options(0, solid_id(1)));
        let status = handle.status();
        wait_for_status(status.clone(), |s| s.active, 5).await;

        assert!(handle.verify_candidate(answering_addr));
        wait_for_status(status.clone(), |s| s.node_count == 1, 5).await;

        for candidate in candidates.iter().take(40) {
            assert!(handle.verify_candidate(candidate.addr()));
        }
        let mut pinged = 0;
        for candidate in candidates.iter().take(40) {
            if candidate
                .recv_raw(Duration::from_millis(400))
                .await
                .is_some()
            {
                pinged += 1;
            }
        }
        assert_eq!(pinged, 32, "at most 32 verifications may be outstanding");
        assert_eq!(status.borrow().node_count, 1);
        handle.shutdown();
    }

    #[tokio::test]
    async fn verification_pings_flow_while_lookups_hold_the_budget() {
        let router = FakeNode::bind(1).await;
        let mut silent_nodes: Vec<FakeNode> = Vec::new();
        let mut silent: Vec<(NodeId, SocketAddrV4)> = Vec::new();
        for octet in 2..32u8 {
            let mut id = [0u8; 20];
            id[0] = 0x60;
            id[1] = octet;
            let node = FakeNode::bind(octet).await;
            silent.push((NodeId::from_bytes(id), node.addr()));
            silent_nodes.push(node);
        }
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
                    vec![(b"nodes" as &[u8], nodes_value(&silent))],
                ),
            )
            .await;
        wait_for_status(status.clone(), |s| s.node_count >= 1, 10).await;
        for index in 0..4u8 {
            let mut hash = [0u8; 20];
            hash[0] = 0x71;
            hash[1] = index;
            let (tx, mut rx) = mpsc::channel(1);
            assert!(handle.request_lookup(hash, tx), "request {index} rejected");
            assert!(
                tokio::time::timeout(Duration::from_millis(2_000), rx.recv())
                    .await
                    .is_err(),
                "lookup {index} against silent nodes must stay in flight"
            );
        }
        let mut verifiers: Vec<FakeNode> = Vec::new();
        for octet in 40..56u8 {
            verifiers.push(FakeNode::bind(octet).await);
        }
        for verifier in &verifiers {
            assert!(handle.verify_candidate(verifier.addr()));
        }
        let mut pinged = 0;
        for verifier in &verifiers {
            if verifier
                .recv_raw(Duration::from_millis(500))
                .await
                .is_some()
            {
                pinged += 1;
            }
        }
        assert_eq!(
            pinged,
            verifiers.len(),
            "maintenance pings must not be starved"
        );
        handle.shutdown();
    }

    #[tokio::test]
    async fn tokens_are_bound_to_the_requester_ip() {
        let node_a = FakeNode::bind(1).await;
        let node_b = FakeNode::bind(2).await;
        let handle = spawn(permissive_options(0, solid_id(1)));
        let status = handle.status();
        let service = wait_for_status(status, |s| s.active, 5).await;
        let destination = SocketAddr::from(([127, 0, 0, 1], service.port));
        let info_hash = [0x45u8; 20];
        node_a
            .send_raw(
                destination,
                &query_get_peers([1, 1], &[7u8; 20], &info_hash),
            )
            .await;
        let token_a = token_from(&node_a.next_datagram().await);
        node_b
            .send_raw(
                destination,
                &query_announce_peer([1, 2], &[8u8; 20], &info_hash, 7100, &token_a),
            )
            .await;
        assert!(
            is_error(&node_b.next_datagram().await),
            "a token minted for another ip must be rejected"
        );
        node_b
            .send_raw(
                destination,
                &query_get_peers([1, 3], &[8u8; 20], &info_hash),
            )
            .await;
        assert!(peers_from(&node_b.next_datagram().await).is_empty());
        handle.shutdown();
    }
}
