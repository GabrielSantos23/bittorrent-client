use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde::Serialize;
use sha1::{Digest, Sha1};
use tokio::sync::{mpsc, watch};
use tokio::task::spawn_blocking;
use tokio::time::{sleep_until, Instant as TokioInstant, MissedTickBehavior};

use crate::error::TrackerError;
use crate::error::{EngineError, StorageError};
use crate::listener::{Incoming, Registry};
use crate::metainfo::MetaInfo;
use crate::peer::{Bitfield, PeerConfig};
use crate::peer_id;
use crate::ratelimit::{RateWindow, UploadBucket};
use crate::tracker::{self, AnnounceRequest, AnnounceResponse, Event};

use self::assembly::{BlockOutcome, PieceAssembler};
use self::peer_task::{HaveMap, PeerCommand, PeerEvent};
use self::picker::{pipeline_depth, PiecePicker};
use self::storage::Storage;

mod assembly;
mod peer_task;
mod picker;
mod storage;

pub use peer_task::{BoxedStream, Dial, TcpDial};
pub use storage::delete_torrent_files;

pub const BLOCK_SIZE: usize = assembly::BLOCK_SIZE;
const MAX_PEERS: usize = 50;
const MAX_PEERS_PER_IP: usize = 8;
const RANDOM_FIRST: usize = 4;
const MAX_ACTIVE_PIECES: usize = 25;
const STATS_INTERVAL: Duration = Duration::from_millis(500);
const CONNECT_INTERVAL: Duration = Duration::from_millis(500);
const CONNECT_BACKOFF: Duration = Duration::from_secs(30);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const REAP_INTERVAL: Duration = Duration::from_secs(1);
const BAN_STRIKES: u32 = 3;
const DEFAULT_ANNOUNCE_PORT: u16 = 6881;
const NUMWANT: u32 = 50;
const CHOKE_SLOTS: usize = 4;
const ANNOUNCE_BACKOFF_START: Duration = Duration::from_secs(15);
const ANNOUNCE_BACKOFF_CAP: Duration = Duration::from_secs(600);
const STOP_ANNOUNCE_WAIT: Duration = Duration::from_secs(3);
const TARGET_RTT: f64 = 1.5;
const RATE_WINDOW: Duration = Duration::from_secs(6);
const INITIAL_PIPELINE_DEPTH: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, ts_rs::TS)]
#[ts(export)]
pub enum State {
    Checking,
    Downloading,
    Paused,
    Completed,
    Stopped,
    Error,
    Seeding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, ts_rs::TS)]
#[ts(export)]
pub enum PeerDirection {
    Incoming,
    Outgoing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, ts_rs::TS)]
#[ts(export)]
pub enum TrackerState {
    Idle,
    Announcing,
    Ok,
    Error,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, ts_rs::TS)]
#[ts(export)]
pub struct TrackerStatus {
    pub url: String,
    pub state: TrackerState,
    #[ts(type = "number | null")]
    pub last_announce: Option<u64>,
    #[ts(type = "number")]
    pub seeders: u64,
    #[ts(type = "number")]
    pub leechers: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, ts_rs::TS)]
#[ts(export)]
pub struct PeerStats {
    pub addr: SocketAddr,
    pub client: String,
    pub rate: f64,
    pub up_rate: f64,
    pub choked: bool,
    pub unchoked: bool,
    pub direction: PeerDirection,
}

#[derive(Debug, Clone)]
pub struct Stats {
    pub state: State,
    pub name: String,
    pub total_length: u64,
    pub verified_bytes: u64,
    pub session_downloaded: u64,
    pub session_uploaded: u64,
    pub upload_rate: f64,
    pub ratio: f64,
    pub verified_pieces: usize,
    pub piece_count: usize,
    pub download_rate: f64,
    pub peer_count: usize,
    pub incoming_peers: usize,
    pub outgoing_peers: usize,
    pub peers: Vec<PeerStats>,
    pub trackers: Vec<TrackerStatus>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum EngineCommand {
    Start,
    Pause,
    Resume,
    Stop,
}

#[derive(Clone)]
pub struct TorrentOptions {
    pub bootstrap_peers: Vec<SocketAddr>,
    pub dial: Arc<dyn Dial>,
    pub listen_active: Arc<AtomicBool>,
    pub announce_port: Arc<AtomicU16>,
    pub uploads: Arc<UploadBucket>,
    pub registry: Arc<Registry>,
    pub choke_interval: Duration,
    pub optimistic_interval: Duration,
    pub peer_id: [u8; 20],
}

impl Default for TorrentOptions {
    fn default() -> Self {
        TorrentOptions {
            bootstrap_peers: Vec::new(),
            dial: Arc::new(TcpDial::new(PeerConfig::default().connect_timeout)),
            listen_active: Arc::new(AtomicBool::new(false)),
            announce_port: Arc::new(AtomicU16::new(DEFAULT_ANNOUNCE_PORT)),
            uploads: Arc::new(UploadBucket::new(0)),
            registry: Arc::new(Registry::default()),
            choke_interval: Duration::from_secs(10),
            optimistic_interval: Duration::from_secs(30),
            peer_id: *peer_id::session(),
        }
    }
}

pub struct Torrent {
    commands: mpsc::Sender<EngineCommand>,
    stats: watch::Receiver<Stats>,
}

impl Torrent {
    pub async fn spawn(meta: MetaInfo, output_dir: PathBuf) -> Result<Torrent, EngineError> {
        Torrent::spawn_with_options(meta, output_dir, TorrentOptions::default()).await
    }

    pub async fn spawn_with_dial(
        meta: MetaInfo,
        output_dir: PathBuf,
        bootstrap_peers: Vec<SocketAddr>,
        dial: Arc<dyn Dial>,
    ) -> Result<Torrent, EngineError> {
        let options = TorrentOptions {
            bootstrap_peers,
            dial,
            ..TorrentOptions::default()
        };
        Torrent::spawn_with_options(meta, output_dir, options).await
    }

    pub async fn spawn_with_options(
        meta: MetaInfo,
        output_dir: PathBuf,
        options: TorrentOptions,
    ) -> Result<Torrent, EngineError> {
        let meta = Arc::new(meta);
        let output_dir_clone = output_dir.clone();
        let storage = spawn_blocking({
            let meta = meta.clone();
            move || Storage::create(&meta, &output_dir_clone)
        })
        .await
        .map_err(|_| EngineError::Task)??;
        let storage = Arc::new(storage);
        let http = tracker::http_client()?;
        let (commands, command_rx) = mpsc::channel(16);
        let (events_tx, events) = mpsc::channel(1024);
        let (incoming_tx, incoming_rx) = mpsc::channel(8);
        let (announce_results_tx, announce_results) = mpsc::channel(64);
        let piece_count = meta.info.pieces.len();
        let total_length = storage.total_length();
        let name = meta.info.name.clone();
        let (stats_tx, stats_rx) = watch::channel(Stats {
            state: State::Checking,
            name,
            total_length,
            verified_bytes: 0,
            session_downloaded: 0,
            session_uploaded: 0,
            upload_rate: 0.0,
            ratio: 0.0,
            verified_pieces: 0,
            piece_count,
            download_rate: 0.0,
            peer_count: 0,
            incoming_peers: 0,
            outgoing_peers: 0,
            peers: Vec::new(),
            trackers: Vec::new(),
            error: None,
        });
        options.registry.register(meta.info_hash, incoming_tx);
        let engine = Engine::new(
            meta,
            storage,
            http,
            options,
            stats_tx,
            command_rx,
            events_tx,
            events,
            incoming_rx,
            announce_results,
            announce_results_tx,
        );
        tokio::spawn(engine.run());
        Ok(Torrent {
            commands,
            stats: stats_rx,
        })
    }

    pub fn subscribe(&self) -> watch::Receiver<Stats> {
        self.stats.clone()
    }

    pub async fn start(&self) -> Result<(), EngineError> {
        self.send(EngineCommand::Start).await
    }

    pub async fn pause(&self) -> Result<(), EngineError> {
        self.send(EngineCommand::Pause).await
    }

    pub async fn resume(&self) -> Result<(), EngineError> {
        self.send(EngineCommand::Resume).await
    }

    pub async fn stop(&self) -> Result<(), EngineError> {
        self.send(EngineCommand::Stop).await
    }

    async fn send(&self, command: EngineCommand) -> Result<(), EngineError> {
        self.commands
            .send(command)
            .await
            .map_err(|_| EngineError::Closed)
    }
}

struct PeerHandle {
    client: String,
    peer_id: [u8; 20],
    commands: mpsc::Sender<PeerCommand>,
    bitfield: Option<Bitfield>,
    choked: bool,
    interested: bool,
    we_unchoked: bool,
    direction: PeerDirection,
    in_flight: usize,
    received_bytes: u64,
    uploaded_bytes: u64,
    rate_base: u64,
    upload_rate_base: u64,
    window_down: u64,
    window_up: u64,
    depth: usize,
    rate_window: RateWindow,
}

struct Engine {
    meta: Arc<MetaInfo>,
    storage: Arc<Storage>,
    dial: Arc<dyn Dial>,
    http: reqwest::Client,
    our_peer_id: [u8; 20],
    listen_active: Arc<AtomicBool>,
    announce_port: Arc<AtomicU16>,
    stats_tx: watch::Sender<Stats>,
    commands: mpsc::Receiver<EngineCommand>,
    events_tx: mpsc::Sender<PeerEvent>,
    events: mpsc::Receiver<PeerEvent>,
    incoming_rx: mpsc::Receiver<Incoming>,
    registry: Arc<Registry>,
    uploads: Arc<UploadBucket>,
    have_map: HaveMap,
    state: State,
    choke_interval: Duration,
    optimistic_interval: Duration,
    optimistic_peer: Option<SocketAddr>,
    optimistic_cursor: usize,
    total_length: u64,
    picker: PiecePicker,
    assembler: PieceAssembler,
    peers: HashMap<SocketAddr, PeerHandle>,
    queue: VecDeque<SocketAddr>,
    known: HashSet<SocketAddr>,
    banned: HashSet<SocketAddr>,
    backoff: HashMap<SocketAddr, TokioInstant>,
    deferred: HashMap<SocketAddr, TokioInstant>,
    strikes: HashMap<SocketAddr, u32>,
    session_downloaded: u64,
    session_uploaded: u64,
    verified_bytes: u64,
    error: Option<String>,
    last_rate: (TokioInstant, u64, u64),
    trackers: Vec<TrackerRuntime>,
    announce_results: mpsc::Receiver<TrackerOutcome>,
    announce_results_tx: mpsc::Sender<TrackerOutcome>,
    primary_tier: Option<usize>,
    pending_pause: bool,
}

struct TrackerRuntime {
    id: usize,
    url: String,
    tier: usize,
    active: bool,
    state: TrackerState,
    last_announce: Option<u64>,
    seeders: u64,
    leechers: u64,
    last_error: Option<String>,
    next_announce: TokioInstant,
    backoff: Duration,
    pending_event: Option<Event>,
}

struct TrackerOutcome {
    id: usize,
    result: Result<AnnounceResponse, TrackerError>,
}

fn shuffle_tier(urls: &[String], rng: &mut impl rand::Rng) -> Vec<String> {
    let mut shuffled: Vec<String> = urls.to_vec();
    for index in (1..shuffled.len()).rev() {
        let swap = rng.random_range(0..=index);
        shuffled.swap(index, swap);
    }
    shuffled
}

fn build_trackers(meta: &MetaInfo) -> Vec<TrackerRuntime> {
    let mut rng = rand::rng();
    let mut tiers: Vec<Vec<String>> = meta.announce_list.clone();
    if let Some(announce) = &meta.announce {
        tiers.push(vec![announce.clone()]);
    }
    let mut trackers = Vec::new();
    let mut id = 0usize;
    for (tier, tier_urls) in tiers.iter().enumerate() {
        let mut first_active = false;
        for (position, url) in shuffle_tier(tier_urls, &mut rng).iter().enumerate() {
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                continue;
            }
            if trackers
                .iter()
                .any(|tracker: &TrackerRuntime| tracker.url == *url)
            {
                continue;
            }
            let active = position == 0 && !first_active;
            first_active |= active;
            trackers.push(TrackerRuntime {
                id,
                url: url.clone(),
                tier,
                active,
                state: TrackerState::Idle,
                last_announce: None,
                seeders: 0,
                leechers: 0,
                last_error: None,
                next_announce: TokioInstant::now(),
                backoff: ANNOUNCE_BACKOFF_START,
                pending_event: Some(Event::Started),
            });
            id += 1;
        }
    }
    trackers
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|span| span.as_secs())
        .unwrap_or(0)
}

impl Engine {
    #[allow(clippy::too_many_arguments)]
    fn new(
        meta: Arc<MetaInfo>,
        storage: Arc<Storage>,
        http: reqwest::Client,
        options: TorrentOptions,
        stats_tx: watch::Sender<Stats>,
        commands: mpsc::Receiver<EngineCommand>,
        events_tx: mpsc::Sender<PeerEvent>,
        events: mpsc::Receiver<PeerEvent>,
        incoming_rx: mpsc::Receiver<Incoming>,
        announce_results: mpsc::Receiver<TrackerOutcome>,
        announce_results_tx: mpsc::Sender<TrackerOutcome>,
    ) -> Engine {
        let piece_count = meta.info.pieces.len();
        let total_length = storage.total_length();
        let mut queue = VecDeque::new();
        let mut known = HashSet::new();
        for addr in &options.bootstrap_peers {
            known.insert(*addr);
            queue.push_back(*addr);
        }
        let have_map: HaveMap = Arc::new(RwLock::new(Bitfield::new(piece_count)));
        let trackers = build_trackers(&meta);
        let TorrentOptions {
            dial,
            listen_active,
            announce_port,
            uploads,
            registry,
            choke_interval,
            optimistic_interval,
            peer_id: our_peer_id,
            ..
        } = options;
        Engine {
            picker: PiecePicker::new(
                piece_count,
                meta.info.piece_length,
                total_length,
                RANDOM_FIRST,
                MAX_ACTIVE_PIECES,
            ),
            assembler: PieceAssembler::new(meta.info.piece_length, total_length),
            meta,
            storage,
            dial,
            http,
            our_peer_id,
            stats_tx,
            commands,
            events_tx,
            events,
            incoming_rx,
            registry,
            uploads,
            have_map,
            state: State::Checking,
            listen_active,
            announce_port,
            choke_interval,
            optimistic_interval,
            optimistic_peer: None,
            optimistic_cursor: 0,
            total_length,
            trackers,
            announce_results,
            announce_results_tx,
            primary_tier: None,
            peers: HashMap::new(),
            queue,
            known,
            banned: HashSet::new(),
            backoff: HashMap::new(),
            deferred: HashMap::new(),
            strikes: HashMap::new(),
            session_downloaded: 0,
            session_uploaded: 0,
            verified_bytes: 0,
            error: None,
            last_rate: (TokioInstant::now(), 0, 0),
            pending_pause: false,
        }
    }

    async fn run(mut self) {
        let mut stats_tick = tokio::time::interval(STATS_INTERVAL);
        let mut reap_tick = tokio::time::interval(REAP_INTERVAL);
        let mut connect_tick = tokio::time::interval(CONNECT_INTERVAL);
        let mut choke_tick = tokio::time::interval(self.choke_interval);
        let mut optimistic_tick = tokio::time::interval(self.optimistic_interval);
        for tick in [
            &mut stats_tick,
            &mut reap_tick,
            &mut connect_tick,
            &mut choke_tick,
            &mut optimistic_tick,
        ] {
            tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        }
        self.run_check().await;
        loop {
            let announce_at = self.earliest_announce();
            let announce_tick = async move {
                match announce_at {
                    Some(at) => sleep_until(at).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                command = self.commands.recv() => match command {
                    Some(command) => self.handle_command(command).await,
                    None => break,
                },
                outcome = self.announce_results.recv() => {
                    if let Some(outcome) = outcome {
                        self.handle_tracker_result(outcome);
                    }
                },
                event = self.events.recv() => match event {
                    Some(event) => self.handle_event(event).await,
                    None => break,
                },
                incoming = self.incoming_rx.recv() => {
                    if let Some(incoming) = incoming {
                        self.accept_incoming(incoming);
                    }
                },
                _ = announce_tick => self.run_announce().await,
                _ = reap_tick.tick() => self.reap_stale_requests().await,
                _ = stats_tick.tick() => self.tick_stats(),
                _ = connect_tick.tick() => self.try_connect(),
                _ = choke_tick.tick() => self.apply_choke(false),
                _ = optimistic_tick.tick() => self.apply_choke(true),
            }
        }
        self.registry.unregister(&self.meta.info_hash);
    }

    async fn handle_command(&mut self, command: EngineCommand) {
        match command {
            EngineCommand::Start => {
                if self.state == State::Stopped {
                    self.run_check().await;
                }
            }
            EngineCommand::Pause => self.pause().await,
            EngineCommand::Resume => self.resume(),
            EngineCommand::Stop => self.stop().await,
        }
    }

    async fn run_check(&mut self) {
        self.state = State::Checking;
        self.publish();
        let storage = self.storage.clone();
        let meta = self.meta.clone();
        let progress = self.stats_tx.clone();
        let have = spawn_blocking(move || {
            storage::recheck(&storage, &meta.info.pieces, |done| {
                progress.send_modify(|stats| stats.verified_pieces = done);
            })
        })
        .await;
        match have {
            Ok(have) => {
                *self
                    .have_map
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = have.clone();
                self.picker.set_have(&have);
                let mut verified = 0u64;
                for index in 0..self.meta.info.pieces.len() {
                    if have.get(index) {
                        verified += self.piece_size(index) as u64;
                    }
                }
                self.verified_bytes = verified;
                self.error = None;
                if self.picker.is_complete() {
                    self.state = self.completed_state();
                    self.queue_event(Event::Completed);
                    self.wake_trackers(TokioInstant::now() + Duration::from_secs(1));
                } else {
                    self.state = State::Downloading;
                    self.queue_event(Event::Started);
                    self.wake_trackers(TokioInstant::now());
                }
                if self.pending_pause {
                    self.pending_pause = false;
                    self.state = State::Paused;
                }
            }
            Err(_) => {
                self.error = Some("recheck failed".to_string());
                self.state = State::Error;
            }
        }
        self.publish();
    }

    fn completed_state(&self) -> State {
        if self.listen_active.load(Ordering::Relaxed) {
            State::Seeding
        } else {
            State::Completed
        }
    }

    async fn pause(&mut self) {
        match self.state {
            State::Checking => self.pending_pause = true,
            State::Downloading | State::Completed | State::Seeding => {
                self.disconnect_all().await;
                self.backoff.clear();
                self.state = State::Paused;
                self.publish();
            }
            _ => {}
        }
    }

    fn resume(&mut self) {
        if self.state == State::Paused {
            self.state = if self.picker.is_complete() {
                self.completed_state()
            } else {
                State::Downloading
            };
            self.queue_event(Event::Started);
            self.wake_trackers(TokioInstant::now());
            self.publish();
        }
    }

    async fn stop(&mut self) {
        self.disconnect_all().await;
        let mut waits = tokio::task::JoinSet::new();
        for tracker in &self.trackers {
            let request = AnnounceRequest {
                info_hash: self.meta.info_hash,
                peer_id: self.our_peer_id,
                port: self.announce_port.load(Ordering::Relaxed),
                uploaded: self.session_uploaded,
                downloaded: self.session_downloaded,
                left: self.total_length - self.verified_bytes,
                numwant: 0,
                event: Some(Event::Stopped),
            };
            let http = self.http.clone();
            let url = tracker.url.clone();
            waits.spawn(async move {
                let _ = tracker::http_announce(&http, &url, &request).await;
            });
        }
        let _ = tokio::time::timeout(STOP_ANNOUNCE_WAIT, waits.join_all()).await;
        self.state = State::Stopped;
        self.publish();
    }

    fn earliest_announce(&self) -> Option<TokioInstant> {
        if !matches!(
            self.state,
            State::Downloading | State::Completed | State::Seeding
        ) {
            return None;
        }
        self.trackers
            .iter()
            .filter(|tracker| tracker.active)
            .map(|tracker| tracker.next_announce)
            .min()
    }

    async fn run_announce(&mut self) {
        if !matches!(
            self.state,
            State::Downloading | State::Completed | State::Seeding
        ) {
            return;
        }
        let now = TokioInstant::now();
        let mut due: Vec<(usize, String, AnnounceRequest)> = Vec::new();
        for tracker in &mut self.trackers {
            if !tracker.active || tracker.next_announce > now {
                continue;
            }
            let event = tracker.pending_event.take();
            let request = AnnounceRequest {
                info_hash: self.meta.info_hash,
                peer_id: self.our_peer_id,
                port: self.announce_port.load(Ordering::Relaxed),
                uploaded: self.session_uploaded,
                downloaded: self.session_downloaded,
                left: self.total_length - self.verified_bytes,
                numwant: NUMWANT,
                event,
            };
            due.push((tracker.id, tracker.url.clone(), request));
            tracker.state = TrackerState::Announcing;
            tracker.next_announce = now + tracker.backoff;
        }
        let due_count = due.len();
        for (id, url, request) in due {
            self.spawn_announce(&url, id, request);
        }
        if due_count > 0 {
            self.publish();
        }
    }

    fn spawn_announce(&self, url: &str, id: usize, request: AnnounceRequest) {
        let http = self.http.clone();
        let url = url.to_string();
        let tx = self.announce_results_tx.clone();
        tokio::spawn(async move {
            let result = tracker::http_announce(&http, &url, &request).await;
            let _ = tx.send(TrackerOutcome { id, result }).await;
        });
    }

    fn handle_tracker_result(&mut self, outcome: TrackerOutcome) {
        let now = TokioInstant::now();
        let Some(index) = self
            .trackers
            .iter()
            .position(|tracker| tracker.id == outcome.id)
        else {
            return;
        };
        let tier = self.trackers[index].tier;
        match outcome.result {
            Ok(response) => {
                let tracker = &mut self.trackers[index];
                tracker.state = TrackerState::Ok;
                tracker.last_announce = Some(unix_now());
                tracker.seeders = response.complete;
                tracker.leechers = response.incomplete;
                tracker.last_error = None;
                tracker.backoff = ANNOUNCE_BACKOFF_START;
                let wait = response
                    .interval
                    .max(response.min_interval.unwrap_or(0))
                    .max(1);
                tracker.next_announce = now + Duration::from_secs(wait);
                for addr in &response.peers {
                    self.push_peer(*addr);
                }
                self.promote_to_tier_front(index);
                if self.primary_tier.is_none() {
                    self.primary_tier = Some(tier);
                }
                if self.primary_tier == Some(tier) {
                    for tracker in &mut self.trackers {
                        if tracker.tier == tier {
                            tracker.active = true;
                        }
                    }
                }
            }
            Err(err) => {
                let tracker = &mut self.trackers[index];
                tracker.state = TrackerState::Error;
                tracker.last_error = Some(err.to_string());
                tracker.next_announce = now + tracker.backoff;
                tracker.backoff = (tracker.backoff * 2).min(ANNOUNCE_BACKOFF_CAP);
                let next: Vec<usize> = self
                    .trackers
                    .iter()
                    .filter(|t| t.tier == tier && !t.active)
                    .map(|t| t.id)
                    .collect();
                if let Some(next_id) = next.first() {
                    if let Some(t) = self.trackers.iter_mut().find(|t| t.id == *next_id) {
                        t.active = true;
                    }
                }
            }
        }
        self.publish();
    }

    fn promote_to_tier_front(&mut self, index: usize) {
        let tier = self.trackers[index].tier;
        let start = self
            .trackers
            .iter()
            .position(|tracker| tracker.tier == tier)
            .unwrap_or(index);
        if index > start {
            let tracker = self.trackers.remove(index);
            self.trackers.insert(start, tracker);
        }
    }

    fn queue_event(&mut self, event: Event) {
        for tracker in &mut self.trackers {
            tracker.pending_event = Some(event);
        }
    }

    fn wake_trackers(&mut self, at: TokioInstant) {
        for tracker in &mut self.trackers {
            if tracker.active && tracker.next_announce > at {
                tracker.next_announce = at;
            }
        }
    }

    fn push_peer(&mut self, addr: SocketAddr) {
        if self.banned.contains(&addr)
            || self.known.contains(&addr)
            || self.peers.contains_key(&addr)
        {
            return;
        }
        self.known.insert(addr);
        self.queue.push_back(addr);
    }

    fn try_connect(&mut self) {
        if !matches!(
            self.state,
            State::Downloading | State::Completed | State::Seeding
        ) {
            return;
        }
        while self.peers.len() < MAX_PEERS {
            let Some(addr) = self.pop_eligible() else {
                break;
            };
            self.spawn_peer(addr);
        }
    }

    fn pop_eligible(&mut self) -> Option<SocketAddr> {
        let now = TokioInstant::now();
        let mut position = 0;
        while position < self.queue.len() {
            let addr = self.queue[position];
            if self.banned.contains(&addr) {
                self.queue.remove(position);
                self.known.remove(&addr);
                continue;
            }
            if self.peers.contains_key(&addr) {
                self.queue.remove(position);
                continue;
            }
            if self.backoff.get(&addr).is_some_and(|next| now < *next) {
                position += 1;
                continue;
            }
            self.queue.remove(position);
            return Some(addr);
        }
        None
    }

    fn spawn_peer(&mut self, addr: SocketAddr) {
        let (commands, command_rx) = mpsc::channel(64);
        self.peers.insert(
            addr,
            PeerHandle {
                client: String::new(),
                peer_id: [0; 20],
                commands,
                bitfield: None,
                choked: true,
                interested: false,
                we_unchoked: false,
                direction: PeerDirection::Outgoing,
                in_flight: 0,
                received_bytes: 0,
                uploaded_bytes: 0,
                rate_base: 0,
                upload_rate_base: 0,
                window_down: 0,
                window_up: 0,
                depth: INITIAL_PIPELINE_DEPTH,
                rate_window: RateWindow::new(RATE_WINDOW),
            },
        );
        let task = peer_task::PeerTask {
            addr,
            info_hash: self.meta.info_hash,
            our_peer_id: self.our_peer_id,
            piece_count: self.meta.info.pieces.len(),
            piece_length: self.meta.info.piece_length,
            total_length: self.total_length,
            config: PeerConfig::default(),
            dial: self.dial.clone(),
            have: self.have_map.clone(),
            storage: self.storage.clone(),
            uploads: self.uploads.clone(),
        };
        tokio::spawn(peer_task::run_peer_task(
            task,
            command_rx,
            self.events_tx.clone(),
        ));
    }

    fn accept_incoming(&mut self, incoming: Incoming) {
        let addr = incoming.addr;
        if self.banned.contains(&addr)
            || self.peers.contains_key(&addr)
            || self.peers.len() >= MAX_PEERS
        {
            return;
        }
        let ip = addr.ip();
        if self.peers.keys().filter(|a| a.ip() == ip).count() >= MAX_PEERS_PER_IP {
            return;
        }
        let (commands, command_rx) = mpsc::channel(64);
        self.peers.insert(
            addr,
            PeerHandle {
                client: String::new(),
                peer_id: [0; 20],
                commands,
                bitfield: None,
                choked: true,
                interested: false,
                we_unchoked: false,
                direction: PeerDirection::Incoming,
                in_flight: 0,
                received_bytes: 0,
                uploaded_bytes: 0,
                rate_base: 0,
                upload_rate_base: 0,
                window_down: 0,
                window_up: 0,
                depth: INITIAL_PIPELINE_DEPTH,
                rate_window: RateWindow::new(RATE_WINDOW),
            },
        );
        let task = peer_task::IncomingPeer {
            addr,
            piece_count: self.meta.info.pieces.len(),
            piece_length: self.meta.info.piece_length,
            total_length: self.total_length,
            config: PeerConfig::default(),
            remote: incoming.remote,
            stream: incoming.stream,
            have: self.have_map.clone(),
            storage: self.storage.clone(),
            uploads: self.uploads.clone(),
        };
        tokio::spawn(peer_task::run_incoming_peer_task(
            task,
            command_rx,
            self.events_tx.clone(),
        ));
    }

    async fn handle_event(&mut self, event: PeerEvent) {
        match event {
            PeerEvent::Handshaken { addr, peer_id } => {
                let duplicate = self
                    .peers
                    .iter()
                    .any(|(other, handle)| *other != addr && handle.peer_id == peer_id);
                let incoming = self
                    .peers
                    .get(&addr)
                    .is_some_and(|handle| handle.direction == PeerDirection::Incoming);
                if duplicate && incoming {
                    if let Some(handle) = self.peers.remove(&addr) {
                        let _ = handle.commands.try_send(PeerCommand::Stop);
                    }
                } else if let Some(handle) = self.peers.get_mut(&addr) {
                    handle.peer_id = peer_id;
                    handle.client = peer_id::client_name(&peer_id);
                }
                self.publish();
            }
            PeerEvent::Bitfield { addr, bitfield } => {
                self.picker.add_peer(&bitfield);
                if let Some(handle) = self.peers.get_mut(&addr) {
                    handle.bitfield = Some(bitfield);
                }
                self.refill(addr).await;
            }
            PeerEvent::Have { addr, index } => {
                self.picker.add_have(index as usize);
                if let Some(handle) = self.peers.get_mut(&addr) {
                    let bitfield = handle
                        .bitfield
                        .get_or_insert_with(|| Bitfield::new(self.meta.info.pieces.len()));
                    let _ = bitfield.set(index as usize);
                }
                self.refill(addr).await;
            }
            PeerEvent::Choke { addr } => {
                if let Some(handle) = self.peers.get_mut(&addr) {
                    handle.choked = true;
                    handle.in_flight = 0;
                }
                self.picker.return_blocks(addr);
                self.refill_all().await;
            }
            PeerEvent::Unchoke { addr } => {
                if let Some(handle) = self.peers.get_mut(&addr) {
                    handle.choked = false;
                }
                self.refill(addr).await;
            }
            PeerEvent::Interested { addr } => {
                if let Some(handle) = self.peers.get_mut(&addr) {
                    handle.interested = true;
                }
                self.publish();
            }
            PeerEvent::NotInterested { addr } => {
                if let Some(handle) = self.peers.get_mut(&addr) {
                    handle.interested = false;
                }
                self.publish();
            }
            PeerEvent::Block {
                addr,
                index,
                begin,
                block,
            } => self.handle_block(addr, index, begin, block).await,
            PeerEvent::Uploaded { addr, bytes } => {
                self.session_uploaded += bytes;
                if let Some(handle) = self.peers.get_mut(&addr) {
                    handle.uploaded_bytes += bytes;
                    handle.window_up += bytes;
                }
                self.publish();
            }
            PeerEvent::Disconnected { addr } => {
                let mut direction = None;
                if let Some(handle) = self.peers.remove(&addr) {
                    direction = Some(handle.direction);
                    if let Some(bitfield) = &handle.bitfield {
                        self.picker.remove_peer(bitfield);
                    }
                    self.picker.return_blocks(addr);
                }
                if direction == Some(PeerDirection::Outgoing) {
                    if self.state == State::Downloading {
                        self.backoff
                            .insert(addr, TokioInstant::now() + CONNECT_BACKOFF);
                    }
                    self.known.remove(&addr);
                    self.push_peer(addr);
                }
                self.deferred.remove(&addr);
                self.refill_all().await;
                self.publish();
            }
        }
    }

    async fn handle_block(&mut self, addr: SocketAddr, index: u32, begin: u32, block: Vec<u8>) {
        if self.state != State::Downloading {
            return;
        }
        let index = index as usize;
        let begin = begin as usize;
        if index >= self.meta.info.pieces.len() {
            return;
        }
        match self.assembler.write_block(index, begin, &block) {
            BlockOutcome::Accepted | BlockOutcome::Completed => {
                if let Some(handle) = self.peers.get_mut(&addr) {
                    handle.in_flight = handle.in_flight.saturating_sub(1);
                    handle.received_bytes += block.len() as u64;
                    handle.window_down += block.len() as u64;
                }
                self.session_downloaded += block.len() as u64;
                let cancel_targets = self.picker.block_received(index, begin, addr);
                let size = self.piece_size(index);
                let length = BLOCK_SIZE.min(size.saturating_sub(begin)) as u32;
                for target in cancel_targets {
                    if let Some(handle) = self.peers.get(&target) {
                        let _ = handle.commands.try_send(PeerCommand::Cancel {
                            index: index as u32,
                            begin: begin as u32,
                            length,
                        });
                    }
                }
            }
            _ => return,
        }
        if self.assembler.is_complete(index) {
            self.finish_piece(index).await;
        } else {
            self.refill(addr).await;
        }
    }

    async fn finish_piece(&mut self, index: usize) {
        let Some(data) = self.assembler.take(index) else {
            return;
        };
        let storage = self.storage.clone();
        let expected = self.meta.info.pieces[index];
        let outcome = spawn_blocking(move || {
            let digest: [u8; 20] = Sha1::digest(&data).into();
            if digest != expected {
                return PieceWriteOutcome::HashMismatch;
            }
            match storage.write_piece(index, &data) {
                Ok(()) => PieceWriteOutcome::Verified,
                Err(err) => PieceWriteOutcome::Failed(err),
            }
        })
        .await;
        match outcome {
            Ok(PieceWriteOutcome::Verified) => {
                self.picker.mark_have(index);
                let _ = self
                    .have_map
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .set(index);
                self.verified_bytes += self.piece_size(index) as u64;
                if self.picker.is_complete() {
                    self.state = self.completed_state();
                    self.queue_event(Event::Completed);
                    self.wake_trackers(TokioInstant::now() + Duration::from_secs(1));
                    self.disconnect_all().await;
                } else {
                    self.broadcast_have(index);
                }
            }
            Ok(PieceWriteOutcome::HashMismatch) => {
                for contributor in self.picker.contributors(index) {
                    self.strike(contributor).await;
                }
                self.picker.requeue_piece(index);
                self.refill_all().await;
            }
            Ok(PieceWriteOutcome::Failed(err)) => {
                self.error = Some(err.to_string());
                self.state = State::Error;
            }
            Err(_) => self.state = State::Error,
        }
        self.publish();
    }

    async fn strike(&mut self, addr: SocketAddr) {
        if !register_strike(&mut self.strikes, &mut self.banned, addr) {
            return;
        }
        if let Some(handle) = self.peers.remove(&addr) {
            if let Some(bitfield) = &handle.bitfield {
                self.picker.remove_peer(bitfield);
            }
            self.picker.return_blocks(addr);
            let _ = handle.commands.try_send(PeerCommand::Stop);
        }
        self.refill_all().await;
    }

    fn broadcast_have(&mut self, index: usize) {
        for handle in self.peers.values() {
            let _ = handle.commands.try_send(PeerCommand::Have(index as u32));
        }
    }

    async fn refill(&mut self, addr: SocketAddr) {
        if self.state != State::Downloading {
            return;
        }
        if self
            .deferred
            .get(&addr)
            .is_some_and(|until| TokioInstant::now() < *until)
        {
            return;
        }
        loop {
            let depth = match self.peers.get(&addr) {
                Some(handle) => handle.depth,
                None => return,
            };
            let bitfield = match self.peers.get(&addr) {
                Some(handle) => match handle.bitfield.as_ref() {
                    Some(bitfield) if !handle.choked && handle.in_flight < depth => bitfield,
                    _ => return,
                },
                None => return,
            };
            let next = match self.picker.next_block(addr, bitfield) {
                Some(next) => Some(next),
                None => {
                    if !self.picker.is_endgame() {
                        None
                    } else {
                        self.picker.next_endgame_block(addr, bitfield)
                    }
                }
            };
            let Some((index, begin, length)) = next else {
                return;
            };
            if !self.assembler.is_open(index) {
                self.assembler.open(index);
            }
            let sent = {
                let Some(handle) = self.peers.get(&addr) else {
                    return;
                };
                dispatch_request(&handle.commands, &mut self.picker, index, begin, length)
            };
            if !sent {
                return;
            }
            if let Some(handle) = self.peers.get_mut(&addr) {
                handle.in_flight += 1;
            }
        }
    }

    async fn refill_all(&mut self) {
        let addrs: Vec<SocketAddr> = self.peers.keys().copied().collect();
        for addr in addrs {
            self.refill(addr).await;
        }
    }

    async fn disconnect_all(&mut self) {
        for handle in self.peers.values() {
            let _ = handle.commands.try_send(PeerCommand::Stop);
        }
    }

    async fn reap_stale_requests(&mut self) {
        let reaped = self.picker.reap_stale(REQUEST_TIMEOUT, TokioInstant::now());
        if reaped.is_empty() {
            return;
        }
        let now = TokioInstant::now();
        for (peer, count) in reaped {
            self.deferred.insert(peer, now + REQUEST_TIMEOUT);
            if let Some(handle) = self.peers.get_mut(&peer) {
                handle.in_flight = handle.in_flight.saturating_sub(count);
            }
        }
        self.refill_all().await;
    }

    fn apply_choke(&mut self, rotate: bool) {
        self.reevaluate_completed_state();
        if self.peers.is_empty() {
            return;
        }
        let seeding = matches!(self.state, State::Seeding | State::Completed);
        let candidates: Vec<ChokeCandidate> = self
            .peers
            .iter()
            .map(|(addr, handle)| ChokeCandidate {
                addr: *addr,
                interested: handle.interested,
                down: handle.window_down,
                up: handle.window_up,
            })
            .collect();
        if rotate {
            let (next, cursor) =
                rotate_optimistic(&candidates, CHOKE_SLOTS, seeding, self.optimistic_cursor);
            self.optimistic_peer = next;
            self.optimistic_cursor = cursor;
        }
        if let Some(optimistic) = self.optimistic_peer {
            if !self.peers.contains_key(&optimistic) {
                self.optimistic_peer = None;
            }
        }
        let unchoke = decide_choke(&candidates, CHOKE_SLOTS, seeding, self.optimistic_peer);
        for (addr, handle) in self.peers.iter_mut() {
            let want_unchoke = unchoke.contains(addr);
            if want_unchoke != handle.we_unchoked {
                let command = if want_unchoke {
                    PeerCommand::Unchoke
                } else {
                    PeerCommand::Choke
                };
                let _ = handle.commands.try_send(command);
                handle.we_unchoked = want_unchoke;
            }
        }
        if !rotate {
            for handle in self.peers.values_mut() {
                handle.window_down = 0;
                handle.window_up = 0;
            }
        }
    }

    fn reevaluate_completed_state(&mut self) {
        if self.picker.is_complete() && matches!(self.state, State::Completed | State::Seeding) {
            let want = self.completed_state();
            if want != self.state {
                self.state = want;
                self.publish();
            }
        }
    }

    fn piece_size(&self, index: usize) -> usize {
        self.storage.piece_size(index)
    }

    fn tick_stats(&mut self) {
        let now = TokioInstant::now();
        for handle in self.peers.values_mut() {
            handle.rate_base = handle.received_bytes;
            handle.upload_rate_base = handle.uploaded_bytes;
            handle.rate_window.push(now, handle.received_bytes);
            handle.depth = pipeline_depth(handle.rate_window.rate(), TARGET_RTT, BLOCK_SIZE);
        }
        self.last_rate = (now, self.session_downloaded, self.session_uploaded);
        self.publish();
    }

    fn current_stats(&self) -> Stats {
        let now = TokioInstant::now();
        let dt = (now - self.last_rate.0).as_secs_f64().max(0.001);
        let peers: Vec<PeerStats> = self
            .peers
            .iter()
            .map(|(addr, handle)| PeerStats {
                addr: *addr,
                client: handle.client.clone(),
                rate: (handle.received_bytes - handle.rate_base) as f64 / dt,
                up_rate: (handle.uploaded_bytes - handle.upload_rate_base) as f64 / dt,
                choked: handle.choked,
                unchoked: handle.we_unchoked,
                direction: handle.direction,
            })
            .collect();
        let incoming_peers = peers
            .iter()
            .filter(|peer| peer.direction == PeerDirection::Incoming)
            .count();
        Stats {
            state: self.state,
            name: self.meta.info.name.clone(),
            total_length: self.storage.total_length(),
            verified_bytes: self.verified_bytes,
            session_downloaded: self.session_downloaded,
            session_uploaded: self.session_uploaded,
            upload_rate: (self.session_uploaded - self.last_rate.2) as f64 / dt,
            ratio: if self.total_length > 0 {
                self.session_uploaded as f64 / self.total_length as f64
            } else {
                0.0
            },
            verified_pieces: self.picker.have().count(),
            piece_count: self.meta.info.pieces.len(),
            download_rate: (self.session_downloaded - self.last_rate.1) as f64 / dt,
            peer_count: self.peers.len(),
            incoming_peers,
            outgoing_peers: peers.len() - incoming_peers,
            peers,
            trackers: self
                .trackers
                .iter()
                .map(|tracker| TrackerStatus {
                    url: tracker.url.clone(),
                    state: tracker.state,
                    last_announce: tracker.last_announce,
                    seeders: tracker.seeders,
                    leechers: tracker.leechers,
                    last_error: tracker.last_error.clone(),
                })
                .collect(),
            error: self.error.clone(),
        }
    }

    fn publish(&self) {
        let _ = self.stats_tx.send(self.current_stats());
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ChokeCandidate {
    pub addr: SocketAddr,
    pub interested: bool,
    pub down: u64,
    pub up: u64,
}

fn choke_metric(candidate: &ChokeCandidate, seeding: bool) -> u64 {
    if seeding {
        candidate.up
    } else {
        candidate.down
    }
}

fn ranked_interested(candidates: &[ChokeCandidate], seeding: bool) -> Vec<SocketAddr> {
    let mut interested: Vec<&ChokeCandidate> = candidates.iter().filter(|c| c.interested).collect();
    interested.sort_by(|a, b| {
        choke_metric(b, seeding)
            .cmp(&choke_metric(a, seeding))
            .then_with(|| a.addr.cmp(&b.addr))
    });
    interested.into_iter().map(|c| c.addr).collect()
}

fn decide_choke(
    candidates: &[ChokeCandidate],
    slots: usize,
    seeding: bool,
    optimistic: Option<SocketAddr>,
) -> Vec<SocketAddr> {
    let ranked = ranked_interested(candidates, seeding);
    let mut unchoke: Vec<SocketAddr> = ranked.iter().take(slots).copied().collect();
    if let Some(optimistic) = optimistic {
        if !unchoke.contains(&optimistic) {
            unchoke.push(optimistic);
        }
    }
    unchoke
}

fn rotate_optimistic(
    candidates: &[ChokeCandidate],
    slots: usize,
    seeding: bool,
    cursor: usize,
) -> (Option<SocketAddr>, usize) {
    let ranked = ranked_interested(candidates, seeding);
    let mut pool: Vec<SocketAddr> = ranked.into_iter().skip(slots).collect();
    pool.sort();
    if pool.is_empty() {
        return (None, cursor);
    }
    (Some(pool[cursor % pool.len()]), cursor + 1)
}

enum PieceWriteOutcome {
    Verified,
    HashMismatch,
    Failed(StorageError),
}

fn dispatch_request(
    commands: &mpsc::Sender<PeerCommand>,
    picker: &mut PiecePicker,
    index: usize,
    begin: usize,
    length: usize,
) -> bool {
    let command = PeerCommand::Request {
        index: index as u32,
        begin: begin as u32,
        length: length as u32,
    };
    match commands.try_send(command) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_)) => {
            picker.requeue_block(index, begin);
            false
        }
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    }
}

fn register_strike(
    strikes: &mut HashMap<SocketAddr, u32>,
    banned: &mut HashSet<SocketAddr>,
    addr: SocketAddr,
) -> bool {
    let count = strikes.entry(addr).or_insert(0);
    *count += 1;
    if *count >= BAN_STRIKES {
        strikes.remove(&addr);
        banned.insert(addr);
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    fn addr(host: &str) -> SocketAddr {
        format!("{host}:1").parse().unwrap()
    }

    fn candidate(host: &str, interested: bool, down: u64, up: u64) -> ChokeCandidate {
        ChokeCandidate {
            addr: addr(host),
            interested,
            down,
            up,
        }
    }

    #[test]
    fn bans_peer_after_three_strikes() {
        let addr: SocketAddr = "1.2.3.4:1".parse().unwrap();
        let mut strikes = HashMap::new();
        let mut banned = HashSet::new();
        assert!(!register_strike(&mut strikes, &mut banned, addr));
        assert!(!banned.contains(&addr));
        assert!(!register_strike(&mut strikes, &mut banned, addr));
        assert!(register_strike(&mut strikes, &mut banned, addr));
        assert!(banned.contains(&addr));
        assert!(!strikes.contains_key(&addr));
    }

    #[test]
    fn bans_peers_independently() {
        let one: SocketAddr = "1.2.3.4:1".parse().unwrap();
        let two: SocketAddr = "5.6.7.8:1".parse().unwrap();
        let mut strikes = HashMap::new();
        let mut banned = HashSet::new();
        for _ in 0..3 {
            register_strike(&mut strikes, &mut banned, one);
        }
        register_strike(&mut strikes, &mut banned, two);
        assert!(banned.contains(&one));
        assert!(!banned.contains(&two));
        assert_eq!(strikes.get(&two), Some(&1));
    }

    #[test]
    fn choking_picks_top_downloaders_while_downloading() {
        let candidates = vec![
            candidate("1.1.1.1", true, 100, 0),
            candidate("2.2.2.2", true, 900, 0),
            candidate("3.3.3.3", true, 500, 0),
            candidate("4.4.4.4", true, 700, 0),
            candidate("5.5.5.5", true, 300, 0),
            candidate("6.6.6.6", false, 999, 0),
        ];
        let unchoke = decide_choke(&candidates, 4, false, None);
        assert_eq!(unchoke.len(), 4);
        assert!(unchoke.contains(&addr("2.2.2.2")));
        assert!(unchoke.contains(&addr("4.4.4.4")));
        assert!(unchoke.contains(&addr("3.3.3.3")));
        assert!(unchoke.contains(&addr("5.5.5.5")));
        assert!(!unchoke.contains(&addr("1.1.1.1")));
        assert!(!unchoke.contains(&addr("6.6.6.6")));
    }

    #[test]
    fn choking_picks_top_upload_recipients_while_seeding() {
        let candidates = vec![
            candidate("1.1.1.1", true, 0, 100),
            candidate("2.2.2.2", true, 0, 900),
            candidate("3.3.3.3", true, 0, 500),
            candidate("4.4.4.4", true, 0, 700),
            candidate("5.5.5.5", true, 0, 300),
        ];
        let unchoke = decide_choke(&candidates, 4, true, None);
        assert_eq!(unchoke.len(), 4);
        assert!(unchoke.contains(&addr("2.2.2.2")));
        assert!(unchoke.contains(&addr("4.4.4.4")));
        assert!(unchoke.contains(&addr("3.3.3.3")));
        assert!(unchoke.contains(&addr("5.5.5.5")));
        assert!(!unchoke.contains(&addr("1.1.1.1")));
    }

    #[test]
    fn only_interested_peers_get_unchoked() {
        let candidates = vec![
            candidate("1.1.1.1", false, 100, 100),
            candidate("2.2.2.2", false, 100, 100),
        ];
        assert!(decide_choke(&candidates, 4, false, None).is_empty());
    }

    #[test]
    fn optimistic_slot_is_added_and_kept() {
        let candidates = vec![
            candidate("1.1.1.1", true, 900, 0),
            candidate("2.2.2.2", true, 800, 0),
            candidate("3.3.3.3", true, 700, 0),
            candidate("4.4.4.4", true, 600, 0),
            candidate("5.5.5.5", true, 500, 0),
            candidate("6.6.6.6", true, 400, 0),
        ];
        let (optimistic, cursor) = rotate_optimistic(&candidates, 4, false, 0);
        assert_eq!(optimistic, Some(addr("5.5.5.5")));
        assert_eq!(cursor, 1);
        let unchoke = decide_choke(&candidates, 4, false, optimistic);
        assert_eq!(unchoke.len(), 5);
        assert!(unchoke.contains(&addr("5.5.5.5")));
        let (next, _) = rotate_optimistic(&candidates, 4, false, cursor);
        assert_eq!(next, Some(addr("6.6.6.6")));
    }

    #[test]
    fn shuffle_tier_is_a_permutation() {
        let urls: Vec<String> = [
            "http://a/announce",
            "http://b/announce",
            "http://c/announce",
            "http://d/announce",
        ]
        .iter()
        .map(|url| url.to_string())
        .collect();
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);
        let shuffled = shuffle_tier(&urls, &mut rng);
        let mut sorted = shuffled.clone();
        sorted.sort();
        let mut expected = urls.clone();
        expected.sort();
        assert_eq!(sorted, expected);
        assert_eq!(shuffled.len(), urls.len());
    }

    #[test]
    fn optimistic_rotation_wraps_and_survives_empty_pool() {
        let candidates = vec![
            candidate("1.1.1.1", true, 900, 0),
            candidate("2.2.2.2", true, 800, 0),
            candidate("3.3.3.3", true, 700, 0),
            candidate("4.4.4.4", true, 600, 0),
        ];
        let (optimistic, cursor) = rotate_optimistic(&candidates, 4, false, 0);
        assert_eq!(optimistic, None);
        assert_eq!(cursor, 0);
        let mut sparse = candidates.clone();
        sparse.push(candidate("5.5.5.5", true, 1, 0));
        let (first, cursor) = rotate_optimistic(&sparse, 4, false, 0);
        assert_eq!(first, Some(addr("5.5.5.5")));
        let (second, _) = rotate_optimistic(&sparse, 4, false, cursor);
        assert_eq!(second, Some(addr("5.5.5.5")));
    }
}
