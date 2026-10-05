use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::net::{SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::pin::Pin;
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
use crate::tracker_udp::{UdpConfig, UdpTrackerClient};

use self::assembly::{BlockOutcome, PieceAssembler};
use self::peer_task::{HaveMap, PeerCommand, PeerEvent};
use self::picker::{pipeline_depth, PiecePicker};
use self::storage::Storage;

mod assembly;
mod peer_task;
mod picker;
mod resume;
mod storage;

pub use peer_task::{BoxedStream, Dial, TcpDial};
pub use resume::remove_snapshot;
pub use storage::{delete_torrent_files, FaultOp, FaultyFs, FileBackend, RealFs};

pub const BLOCK_SIZE: usize = assembly::BLOCK_SIZE;
const MAX_PEERS: usize = 50;
const MAX_PEERS_PER_IP: usize = 8;
const QUEUE_CAP: usize = 2000;
const KNOWN_CAP: usize = 10000;
const RANDOM_FIRST: usize = 4;
const MAX_ACTIVE_PIECES: usize = 25;
const STATS_INTERVAL: Duration = Duration::from_millis(500);
const RESUME_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(30);
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
const METADATA_TICK: Duration = Duration::from_millis(500);
const METADATA_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_IN_FLIGHT_METADATA: usize = 8;
const MAX_METADATA_ASSEMBLY_FAILURES: u32 = 8;
const TARGET_RTT: f64 = 1.5;
const RATE_WINDOW: Duration = Duration::from_secs(6);
const INITIAL_PIPELINE_DEPTH: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, ts_rs::TS)]
#[ts(export)]
pub enum FilePriority {
    Skip,
    Normal,
    High,
}

impl FilePriority {
    pub fn is_skip(self) -> bool {
        matches!(self, FilePriority::Skip)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, ts_rs::TS)]
#[ts(export)]
pub enum State {
    Checking,
    FetchingMetadata,
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
    /// Sum of the lengths of files that are not skipped.
    pub wanted_bytes: u64,
    /// Bytes of verified pieces that fall inside non-skipped files.
    pub verified_wanted_bytes: u64,
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
    pub files: Vec<FileStats>,
    pub metadata_progress: Option<MetadataProgress>,
    pub diag: MetadataDiag,
    pub error: Option<String>,
    /// Whether the current error can plausibly be fixed by the user and
    /// retried (disk full, permissions, missing directory...).
    pub error_retryable: bool,
    pub dht_waiting: bool,
    pub resumed_from_saved_state: bool,
    pub startup_pieces_hashed: usize,
    pub resume_fallback: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, ts_rs::TS)]
#[ts(export)]
pub struct FileStats {
    pub path: String,
    #[ts(type = "number")]
    pub length: u64,
    pub priority: FilePriority,
    /// Bytes of verified pieces that fall inside this file. Boundary pieces
    /// count for every file they cover.
    #[ts(type = "number")]
    pub verified_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, ts_rs::TS)]
#[ts(export)]
pub struct MetadataProgress {
    pub received: u32,
    pub total: Option<u32>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MetadataDiag {
    pub peers_connected: u32,
    pub extension_handshakes: u32,
    pub extension_handshake_decode_errors: u32,
    pub ut_metadata_advertisers: u32,
    pub metadata_sizes: BTreeMap<u64, u32>,
    pub metadata_requests_sent: u32,
    pub metadata_data_received: u32,
    pub metadata_rejects_received: u32,
    pub peer_close_reasons: BTreeMap<String, u32>,
}

#[derive(Debug)]
pub enum EngineCommand {
    Start,
    Pause,
    Resume,
    Stop,
    SetFilePriorities(Vec<(usize, FilePriority)>),
    ForceRecheck,
}

#[derive(Clone)]
pub struct DhtIntegration {
    pub handle: crate::dht::DhtHandle,
    pub active: Arc<AtomicBool>,
    pub port: Arc<AtomicU16>,
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
    pub dht: Option<DhtIntegration>,
    pub resume_dir: Option<PathBuf>,
    /// Sparse `(file index, priority)` pairs; unlisted files are Normal.
    pub file_priorities: Vec<(usize, FilePriority)>,
    /// Magnets only: pause as soon as the metadata arrives so the file list
    /// can be shown and priorities chosen before any data is downloaded.
    pub pause_after_metadata: bool,
    /// Filesystem boundary; production uses `RealFs`, tests may inject
    /// `FaultyFs` to simulate disk errors.
    pub fs: Arc<dyn FileBackend>,
    /// Overrides the free space check performed before a download starts.
    pub skip_free_space_check: bool,
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
            dht: None,
            resume_dir: None,
            file_priorities: Vec::new(),
            pause_after_metadata: false,
            fs: Arc::new(storage::RealFs),
            skip_free_space_check: false,
        }
    }
}

pub struct Torrent {
    commands: mpsc::Sender<EngineCommand>,
    stats: watch::Receiver<Stats>,
    metadata: watch::Receiver<Option<Arc<Vec<u8>>>>,
}

enum StartupVerification {
    FullRecheck(Option<String>),
    Resume(self::resume::ResumeStartup),
}

impl Torrent {
    pub fn subscribe_metadata(&self) -> watch::Receiver<Option<Arc<Vec<u8>>>> {
        self.metadata.clone()
    }

    pub async fn set_file_priorities(
        &self,
        priorities: Vec<(usize, FilePriority)>,
    ) -> Result<(), EngineError> {
        self.commands
            .send(EngineCommand::SetFilePriorities(priorities))
            .await
            .map_err(|_| EngineError::Closed)
    }

    pub async fn force_recheck(&self) -> Result<(), EngineError> {
        self.commands
            .send(EngineCommand::ForceRecheck)
            .await
            .map_err(|_| EngineError::Closed)
    }
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

    pub async fn spawn_from_magnet(
        link: crate::magnet::MagnetLink,
        output_dir: PathBuf,
        options: TorrentOptions,
    ) -> Result<Torrent, EngineError> {
        let raw_metainfo: Arc<std::sync::Mutex<Vec<u8>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let (commands, command_rx) = mpsc::channel(16);
        let (events_tx, events) = mpsc::channel(1024);
        let (incoming_tx, incoming_rx) = mpsc::channel(8);
        let (announce_results_tx, announce_results) = mpsc::channel(64);
        let (metadata_tx, metadata_rx) = watch::channel(None);
        let id = crate::hex::encode(&link.info_hash);
        let name = link.display_name.clone().unwrap_or_else(|| id.clone());
        let no_source = link.trackers.is_empty() && link.peers.is_empty();
        let (stats_tx, stats_rx) = watch::channel(Stats {
            state: State::FetchingMetadata,
            name,
            total_length: 0,
            verified_bytes: 0,
            wanted_bytes: 0,
            verified_wanted_bytes: 0,
            session_downloaded: 0,
            session_uploaded: 0,
            upload_rate: 0.0,
            ratio: 0.0,
            verified_pieces: 0,
            piece_count: 0,
            download_rate: 0.0,
            peer_count: 0,
            incoming_peers: 0,
            outgoing_peers: 0,
            peers: Vec::new(),
            trackers: Vec::new(),
            files: Vec::new(),
            metadata_progress: Some(MetadataProgress {
                received: 0,
                total: None,
            }),
            diag: MetadataDiag::default(),
            error_retryable: false,
            error: if no_source {
                Some(
                    "no peer source available: the magnet has no trackers and no x.pe peers"
                        .to_string(),
                )
            } else {
                None
            },
            dht_waiting: false,
            resumed_from_saved_state: false,
            startup_pieces_hashed: 0,
            resume_fallback: None,
        });
        options.registry.register(link.info_hash, incoming_tx);
        let mut bootstrap_peers = options.bootstrap_peers.clone();
        for peer in &link.peers {
            if let Ok(mut addrs) = tokio::net::lookup_host((peer.host.as_str(), peer.port)).await {
                bootstrap_peers.extend(&mut addrs);
            }
        }
        let mut options = options;
        options.bootstrap_peers = bootstrap_peers;
        let engine = Engine::new_from_magnet(
            link,
            output_dir,
            options,
            stats_tx,
            command_rx,
            events_tx,
            events,
            incoming_rx,
            announce_results,
            announce_results_tx,
            raw_metainfo,
            metadata_tx,
        );
        tokio::spawn(engine.run());
        Ok(Torrent {
            commands,
            stats: stats_rx,
            metadata: metadata_rx,
        })
    }

    pub async fn spawn_with_options(
        meta: MetaInfo,
        output_dir: PathBuf,
        options: TorrentOptions,
    ) -> Result<Torrent, EngineError> {
        let meta = Arc::new(meta);
        let output_dir_clone = output_dir.clone();
        let priorities = padded_priorities(&options.file_priorities, file_count_of(&meta));
        let priorities_for_storage = priorities.clone();
        if !options.skip_free_space_check {
            let meta_for_space = meta.clone();
            let dir_for_space = output_dir.clone();
            let priorities_for_space = priorities.clone();
            let fs = options.fs.clone();
            spawn_blocking(move || {
                Storage::check_free_space(
                    &meta_for_space,
                    &dir_for_space,
                    &priorities_for_space,
                    fs.as_ref(),
                )
            })
            .await
            .map_err(|_| EngineError::Task)??;
        }
        let fs = options.fs.clone();
        let storage = spawn_blocking({
            let meta = meta.clone();
            move || Storage::create_with_fs(&meta, &output_dir_clone, &priorities_for_storage, fs)
        })
        .await
        .map_err(|_| EngineError::Task)??;
        let storage = Arc::new(storage);
        let startup = match &options.resume_dir {
            Some(dir) => {
                let dir = dir.clone();
                let meta_for_plan = meta.clone();
                let storage_for_plan = storage.clone();
                match spawn_blocking(move || {
                    self::resume::startup_plan(&dir, &meta_for_plan, &storage_for_plan)
                })
                .await
                {
                    Ok(Ok(plan)) => StartupVerification::Resume(plan),
                    Ok(Err(err)) => StartupVerification::FullRecheck(Some(err.to_string())),
                    Err(_) => StartupVerification::FullRecheck(Some(
                        "resume planning task failed".to_string(),
                    )),
                }
            }
            None => StartupVerification::FullRecheck(None),
        };
        let http = tracker::http_client()?;
        let (commands, command_rx) = mpsc::channel(16);
        let (events_tx, events) = mpsc::channel(1024);
        let (incoming_tx, incoming_rx) = mpsc::channel(8);
        let (announce_results_tx, announce_results) = mpsc::channel(64);
        let (metadata_tx, metadata_rx) = watch::channel(None);
        let raw_metainfo = Arc::new(std::sync::Mutex::new(meta.raw.clone()));
        let piece_count = meta.info.pieces.len();
        let total_length = storage.total_length();
        let name = meta.info.name.clone();
        let resumed = matches!(&startup, StartupVerification::Resume(_));
        let initial_state = match &startup {
            StartupVerification::Resume(plan) => {
                if plan.overlaps.is_empty() {
                    if plan.trusted.is_complete() && options.listen_active.load(Ordering::Relaxed) {
                        State::Seeding
                    } else if plan.trusted.is_complete() {
                        State::Completed
                    } else {
                        State::Downloading
                    }
                } else {
                    State::Checking
                }
            }
            StartupVerification::FullRecheck(_) => State::Checking,
        };
        let initial_verified = match &startup {
            StartupVerification::Resume(plan) => {
                self::resume::verified_length(&storage, &plan.trusted)
            }
            StartupVerification::FullRecheck(_) => 0,
        };
        let initial_verified_pieces = match &startup {
            StartupVerification::Resume(plan) => plan.trusted.count(),
            StartupVerification::FullRecheck(_) => 0,
        };
        let (stats_tx, stats_rx) = watch::channel(Stats {
            state: initial_state,
            name,
            total_length,
            verified_bytes: initial_verified,
            wanted_bytes: 0,
            verified_wanted_bytes: 0,
            session_downloaded: 0,
            session_uploaded: 0,
            upload_rate: 0.0,
            ratio: 0.0,
            verified_pieces: initial_verified_pieces,
            piece_count,
            download_rate: 0.0,
            peer_count: 0,
            incoming_peers: 0,
            outgoing_peers: 0,
            peers: Vec::new(),
            trackers: Vec::new(),
            files: Vec::new(),
            metadata_progress: None,
            diag: MetadataDiag::default(),
            error_retryable: false,
            error: None,
            dht_waiting: false,
            resumed_from_saved_state: resumed,
            startup_pieces_hashed: 0,
            resume_fallback: match &startup {
                StartupVerification::FullRecheck(reason) => reason.clone(),
                StartupVerification::Resume(_) => None,
            },
        });
        options.registry.register(meta.info_hash, incoming_tx);
        let engine = Engine::new(
            meta,
            storage,
            raw_metainfo,
            None,
            metadata_tx.clone(),
            http,
            options,
            stats_tx,
            command_rx,
            events_tx,
            events,
            incoming_rx,
            announce_results,
            announce_results_tx,
            startup,
        );
        tokio::spawn(engine.run());
        Ok(Torrent {
            commands,
            stats: stats_rx,
            metadata: metadata_rx,
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
    extensions: Option<crate::extensions::ExtensionHandshake>,
    metadata_trusted: bool,
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

struct PendingMetadata {
    info_hash: [u8; 20],
    display_name: Option<String>,
    output_dir: PathBuf,
    size: Option<u64>,
    pieces: HashMap<u32, Vec<u8>>,
    in_flight: HashMap<u32, TokioInstant>,
    round_robin: usize,
    contributors: HashSet<SocketAddr>,
    failed_assemblies: u32,
    file_priorities: Vec<(usize, FilePriority)>,
}

impl PendingMetadata {
    fn total_pieces(&self) -> Option<u32> {
        self.size
            .map(|size| size.div_ceil(crate::extensions::METADATA_PIECE_SIZE as u64) as u32)
    }

    fn received_of_total(&self) -> MetadataProgress {
        MetadataProgress {
            received: self.pieces.len() as u32,
            total: self.total_pieces(),
        }
    }
}

struct Engine {
    meta: Option<Arc<MetaInfo>>,
    storage: Option<Arc<Storage>>,
    pending: Option<PendingMetadata>,
    metadata_tx: watch::Sender<Option<Arc<Vec<u8>>>>,
    raw_metainfo: Arc<std::sync::Mutex<Vec<u8>>>,
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
    picker: Option<PiecePicker>,
    spare_picker: PiecePicker,
    assembler: Option<PieceAssembler>,
    peers: HashMap<SocketAddr, PeerHandle>,
    backlog: PeerBacklog,
    banned: HashSet<SocketAddr>,
    backoff: HashMap<SocketAddr, TokioInstant>,
    deferred: HashMap<SocketAddr, TokioInstant>,
    strikes: HashMap<SocketAddr, u32>,
    session_downloaded: u64,
    session_uploaded: u64,
    verified_bytes: u64,
    file_priorities: Vec<FilePriority>,
    file_verified: Vec<u64>,
    pause_after_metadata: bool,
    fs: Arc<dyn FileBackend>,
    skip_free_space_check: bool,
    error: Option<String>,
    error_retryable: bool,
    failed_piece: Option<usize>,
    last_rate: (TokioInstant, u64, u64),
    trackers: Vec<TrackerRuntime>,
    announce_results: mpsc::Receiver<TrackerOutcome>,
    announce_results_tx: mpsc::Sender<TrackerOutcome>,
    primary_tier: Option<usize>,
    pending_pause: bool,
    diag: MetadataDiag,
    dht: Option<DhtIntegration>,
    dht_results: mpsc::Receiver<crate::dht::DhtPeers>,
    dht_results_tx: mpsc::Sender<crate::dht::DhtPeers>,
    dht_lookup_in_flight: bool,
    last_dht_lookup_ms: Option<u64>,
    resume_dir: Option<PathBuf>,
    startup: Option<StartupVerification>,
    resumed_from_saved_state: bool,
    startup_pieces_hashed: usize,
    resume_fallback: Option<String>,
    resume_dirty: bool,
    start: TokioInstant,
}

#[derive(Clone)]
enum TrackerTransport {
    Http,
    Udp(Arc<tokio::sync::OnceCell<Arc<tokio::sync::Mutex<UdpTrackerClient>>>>),
}

#[derive(Default)]
struct PeerBacklog {
    queue: VecDeque<SocketAddr>,
    known: HashSet<SocketAddr>,
    order: VecDeque<SocketAddr>,
}

impl PeerBacklog {
    fn push(&mut self, addr: SocketAddr) {
        if self.known.contains(&addr) {
            return;
        }
        while self.order.len() >= KNOWN_CAP {
            match self.order.pop_front() {
                Some(oldest) => {
                    self.known.remove(&oldest);
                }
                None => break,
            }
        }
        while self.queue.len() >= QUEUE_CAP {
            self.queue.pop_front();
        }
        self.known.insert(addr);
        self.order.push_back(addr);
        self.queue.push_back(addr);
    }

    fn remove(&mut self, addr: &SocketAddr) {
        self.known.remove(addr);
        self.order.retain(|known| known != addr);
        self.queue.retain(|queued| queued != addr);
    }

    #[cfg(test)]
    fn is_known(&self, addr: &SocketAddr) -> bool {
        self.known.contains(addr)
    }
}

struct TrackerRuntime {
    id: usize,
    url: String,
    tier: usize,
    transport: TrackerTransport,
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
    let mut tiers: Vec<Vec<String>> = meta.announce_list.clone();
    if let Some(announce) = &meta.announce {
        tiers.push(vec![announce.clone()]);
    }
    build_trackers_from_tiers(tiers)
}

fn build_trackers_from_tiers(tiers: Vec<Vec<String>>) -> Vec<TrackerRuntime> {
    let mut rng = rand::rng();
    let mut trackers = Vec::new();
    let mut id = 0usize;
    for (tier, tier_urls) in tiers.iter().enumerate() {
        let mut first_active = false;
        for (position, url) in shuffle_tier(tier_urls, &mut rng).iter().enumerate() {
            let transport = if url.starts_with("http://") || url.starts_with("https://") {
                TrackerTransport::Http
            } else if url.starts_with("udp://") {
                TrackerTransport::Udp(Arc::new(tokio::sync::OnceCell::new()))
            } else {
                continue;
            };
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
                transport,
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

fn metadata_info_and_size(raw: &[u8]) -> Result<(Vec<u8>, u64), crate::error::MetaInfoError> {
    let root = crate::bencode::decode(raw)?;
    let dict = root
        .as_dict()
        .ok_or(crate::error::MetaInfoError::NotADictionary)?;
    let info = dict
        .get(&b"info".to_vec())
        .ok_or(crate::error::MetaInfoError::MissingInfo)?;
    let mut bytes = Vec::new();
    crate::bencode::encode_into(info, &mut bytes);
    let size = bytes.len() as u64;
    Ok((bytes, size))
}

pub fn file_count_of(meta: &MetaInfo) -> usize {
    match &meta.info.content {
        crate::metainfo::Content::Single { .. } => 1,
        crate::metainfo::Content::Multi { files } => files.len(),
    }
}

/// Expands sparse `(index, priority)` pairs into a per-file vector, ignoring
/// out-of-range indices. Unlisted files are Normal.
pub fn padded_priorities(pairs: &[(usize, FilePriority)], file_count: usize) -> Vec<FilePriority> {
    let mut priorities = vec![FilePriority::Normal; file_count];
    for (index, priority) in pairs {
        if *index < file_count {
            priorities[*index] = *priority;
        }
    }
    priorities
}

/// A piece is wanted when it overlaps at least one non-skipped file; it is in
/// the High class when it overlaps at least one High file.
pub fn piece_wanted_map(
    meta: &MetaInfo,
    storage: &Storage,
    priorities: &[FilePriority],
) -> (Bitfield, Bitfield) {
    let piece_count = meta.info.pieces.len();
    let mut wanted = Bitfield::new(piece_count);
    let mut high = Bitfield::new(piece_count);
    for index in 0..piece_count {
        for (file, _) in storage.piece_file_spans(index) {
            match priorities
                .get(file)
                .copied()
                .unwrap_or(FilePriority::Normal)
            {
                FilePriority::Skip => {}
                FilePriority::Normal => {
                    let _ = wanted.set(index);
                }
                FilePriority::High => {
                    let _ = wanted.set(index);
                    let _ = high.set(index);
                }
            }
        }
    }
    (wanted, high)
}

/// Per-file verified bytes: every verified piece contributes the bytes it
/// covers in each file, so boundary pieces count for all files they span.
pub fn file_verified_of(storage: &Storage, have: &Bitfield) -> Vec<u64> {
    let mut verified = vec![0u64; storage.file_lengths().len()];
    for index in 0..have.piece_count() {
        if !have.get(index) {
            continue;
        }
        for (file, bytes) in storage.piece_file_spans(index) {
            if let Some(slot) = verified.get_mut(file) {
                *slot += bytes as u64;
            }
        }
    }
    verified
}

impl Engine {
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    fn new_from_magnet(
        link: crate::magnet::MagnetLink,
        output_dir: PathBuf,
        options: TorrentOptions,
        stats_tx: watch::Sender<Stats>,
        commands: mpsc::Receiver<EngineCommand>,
        events_tx: mpsc::Sender<PeerEvent>,
        events: mpsc::Receiver<PeerEvent>,
        incoming_rx: mpsc::Receiver<Incoming>,
        announce_results: mpsc::Receiver<TrackerOutcome>,
        announce_results_tx: mpsc::Sender<TrackerOutcome>,
        raw_metainfo: Arc<std::sync::Mutex<Vec<u8>>>,
        metadata_tx: watch::Sender<Option<Arc<Vec<u8>>>>,
    ) -> Engine {
        let info_hash = link.info_hash;
        let mut backlog = PeerBacklog::default();
        for addr in options.bootstrap_peers {
            backlog.push(addr);
        }
        let have_map: HaveMap = Arc::new(RwLock::new(Bitfield::new(0)));
        let tiers: Vec<Vec<String>> = if link.trackers.is_empty() {
            Vec::new()
        } else {
            vec![link.trackers.clone()]
        };
        let trackers = build_trackers_from_tiers(tiers);
        let TorrentOptions {
            dial,
            listen_active,
            announce_port,
            uploads,
            registry,
            choke_interval,
            optimistic_interval,
            peer_id: our_peer_id,
            dht,
            resume_dir,
            file_priorities,
            pause_after_metadata,
            fs,
            skip_free_space_check,
            ..
        } = options;
        let (dht_results_tx, dht_results) = mpsc::channel(16);
        let pending = PendingMetadata {
            info_hash,
            display_name: link.display_name,
            output_dir,
            size: None,
            pieces: HashMap::new(),
            in_flight: HashMap::new(),
            round_robin: 0,
            contributors: HashSet::new(),
            failed_assemblies: 0,
            file_priorities,
        };
        Engine {
            picker: None,
            spare_picker: PiecePicker::new(0, 16384, 0, 0, 1),
            assembler: None,
            meta: None,
            pending: Some(pending),
            metadata_tx,
            raw_metainfo,
            storage: None,
            dial,
            http: tracker::http_client().unwrap_or_else(|_| reqwest::Client::new()),
            our_peer_id,
            listen_active,
            announce_port,
            primary_tier: None,
            trackers,
            stats_tx,
            commands,
            events_tx,
            events,
            incoming_rx,
            announce_results,
            announce_results_tx,
            registry,
            uploads,
            have_map,
            state: State::FetchingMetadata,
            choke_interval,
            optimistic_interval,
            optimistic_peer: None,
            optimistic_cursor: 0,
            total_length: 0,
            peers: HashMap::new(),
            backlog,
            banned: HashSet::new(),
            backoff: HashMap::new(),
            deferred: HashMap::new(),
            strikes: HashMap::new(),
            session_downloaded: 0,
            session_uploaded: 0,
            verified_bytes: 0,
            file_priorities: Vec::new(),
            file_verified: Vec::new(),
            pause_after_metadata,
            fs,
            skip_free_space_check,
            error: None,
            error_retryable: false,
            failed_piece: None,
            last_rate: (TokioInstant::now(), 0, 0),
            pending_pause: false,
            diag: MetadataDiag::default(),
            dht,
            dht_results,
            dht_results_tx,
            dht_lookup_in_flight: false,
            last_dht_lookup_ms: None,
            resume_dir,
            startup: None,
            resumed_from_saved_state: false,
            startup_pieces_hashed: 0,
            resume_fallback: None,
            resume_dirty: false,
            start: TokioInstant::now(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        meta: Arc<MetaInfo>,
        storage: Arc<Storage>,
        raw_metainfo: Arc<std::sync::Mutex<Vec<u8>>>,
        pending: Option<PendingMetadata>,
        metadata_tx: watch::Sender<Option<Arc<Vec<u8>>>>,
        http: reqwest::Client,
        options: TorrentOptions,
        stats_tx: watch::Sender<Stats>,
        commands: mpsc::Receiver<EngineCommand>,
        events_tx: mpsc::Sender<PeerEvent>,
        events: mpsc::Receiver<PeerEvent>,
        incoming_rx: mpsc::Receiver<Incoming>,
        announce_results: mpsc::Receiver<TrackerOutcome>,
        announce_results_tx: mpsc::Sender<TrackerOutcome>,
        startup: StartupVerification,
    ) -> Engine {
        let piece_count = meta.info.pieces.len();
        let total_length = storage.total_length();
        let file_priorities = padded_priorities(&options.file_priorities, file_count_of(&meta));
        let (wanted, high) = piece_wanted_map(&meta, &storage, &file_priorities);
        let mut picker = PiecePicker::new(
            piece_count,
            meta.info.piece_length,
            total_length,
            RANDOM_FIRST,
            MAX_ACTIVE_PIECES,
        );
        picker.set_wanted(&wanted, &high);
        let mut backlog = PeerBacklog::default();
        for addr in options.bootstrap_peers {
            backlog.push(addr);
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
            dht,
            resume_dir,
            pause_after_metadata,
            fs,
            skip_free_space_check,
            ..
        } = options;
        let (dht_results_tx, dht_results) = mpsc::channel(16);
        let (resumed_from_saved_state, resume_fallback) = match &startup {
            StartupVerification::Resume(_) => (true, None),
            StartupVerification::FullRecheck(reason) => (false, reason.clone()),
        };
        Engine {
            spare_picker: PiecePicker::new(0, 16384, 0, 0, 1),
            picker: Some(picker),
            assembler: Some(PieceAssembler::new(meta.info.piece_length, total_length)),
            meta: Some(meta),
            pending,
            metadata_tx,
            raw_metainfo,
            storage: Some(storage),
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
            backlog,
            banned: HashSet::new(),
            backoff: HashMap::new(),
            deferred: HashMap::new(),
            strikes: HashMap::new(),
            session_downloaded: 0,
            session_uploaded: 0,
            verified_bytes: 0,
            file_priorities,
            file_verified: Vec::new(),
            pause_after_metadata,
            fs,
            skip_free_space_check,
            error: None,
            error_retryable: false,
            failed_piece: None,
            last_rate: (TokioInstant::now(), 0, 0),
            pending_pause: false,
            diag: MetadataDiag::default(),
            dht,
            dht_results,
            dht_results_tx,
            dht_lookup_in_flight: false,
            last_dht_lookup_ms: None,
            resume_dir,
            startup: Some(startup),
            resumed_from_saved_state,
            startup_pieces_hashed: 0,
            resume_fallback,
            resume_dirty: false,
            start: TokioInstant::now(),
        }
    }

    async fn run(mut self) {
        let mut stats_tick = tokio::time::interval(STATS_INTERVAL);
        let mut reap_tick = tokio::time::interval(REAP_INTERVAL);
        let mut connect_tick = tokio::time::interval(CONNECT_INTERVAL);
        let mut choke_tick = tokio::time::interval(self.choke_interval);
        let mut optimistic_tick = tokio::time::interval(self.optimistic_interval);
        let mut metadata_tick = tokio::time::interval(METADATA_TICK);
        let mut resume_tick = tokio::time::interval(RESUME_SNAPSHOT_INTERVAL);
        for tick in [
            &mut stats_tick,
            &mut reap_tick,
            &mut connect_tick,
            &mut choke_tick,
            &mut optimistic_tick,
            &mut metadata_tick,
            &mut resume_tick,
        ] {
            tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        }
        match self.startup.take() {
            Some(StartupVerification::Resume(plan)) => self.run_resume(plan).await,
            Some(StartupVerification::FullRecheck(_)) => self.run_check().await,
            None => self.initial_check().await,
        }
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
                peers = self.dht_results.recv() => {
                    if let Some(peers) = peers {
                        self.handle_dht_peers(peers);
                    }
                },
                _ = announce_tick => self.run_announce().await,
                _ = reap_tick.tick() => self.reap_stale_requests().await,
                _ = stats_tick.tick() => self.tick_stats(),
                _ = connect_tick.tick() => {
                    self.try_connect();
                    self.maybe_start_dht_lookup();
                },
                _ = choke_tick.tick() => self.apply_choke(false),
                _ = optimistic_tick.tick() => self.apply_choke(true),
                _ = metadata_tick.tick() => self.metadata_tick().await,
                _ = resume_tick.tick() => {
                    if self.resume_dirty {
                        self.write_resume_snapshot().await;
                    }
                },
            }
        }
        self.write_resume_snapshot().await;
        self.registry.unregister(&self.info_hash());
    }

    async fn initial_check(&mut self) {
        match self.plan_resume().await {
            Some(startup) => self.run_resume(startup).await,
            None => self.run_check().await,
        }
    }

    async fn plan_resume(&mut self) -> Option<self::resume::ResumeStartup> {
        let dir = self.resume_dir.clone()?;
        let meta = self.meta.clone()?;
        let storage = self.storage.clone()?;
        match spawn_blocking(move || self::resume::startup_plan(&dir, &meta, &storage)).await {
            Ok(Ok(plan)) => Some(plan),
            Ok(Err(err)) => {
                self.resumed_from_saved_state = false;
                self.resume_fallback = Some(err.to_string());
                None
            }
            Err(_) => {
                self.resumed_from_saved_state = false;
                self.resume_fallback = Some("resume planning task failed".to_string());
                None
            }
        }
    }

    async fn run_resume(&mut self, startup: self::resume::ResumeStartup) {
        let Some(storage) = self.storage.clone() else {
            return;
        };
        let Some(meta) = self.meta.clone() else {
            return;
        };
        self.verified_bytes = self::resume::verified_length(&storage, &startup.trusted);
        {
            let mut have = self
                .have_map
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *have = startup.trusted.clone();
        }
        self.file_verified = file_verified_of(&storage, &startup.trusted);
        self.picker().set_have(&startup.trusted);
        self.error = None;
        if startup.overlaps.is_empty() {
            self.state = if startup.trusted.is_complete() {
                self.completed_state()
            } else {
                State::Downloading
            };
        } else {
            self.state = State::Checking;
        }
        self.publish();
        let self::resume::ResumeStartup {
            sample, overlaps, ..
        } = startup;
        let hashed_count = sample.len() + overlaps.len();
        let verify_overlaps = overlaps.clone();
        let verification = spawn_blocking(move || {
            let sample_failed = self::resume::verify_pieces(&storage, &meta.info.pieces, &sample);
            let overlap_failed =
                self::resume::verify_pieces(&storage, &meta.info.pieces, &verify_overlaps);
            (sample_failed, overlap_failed)
        })
        .await;
        match verification {
            Ok((sample_failed, overlap_failed)) => {
                self.startup_pieces_hashed += hashed_count;
                if !sample_failed.is_empty() {
                    self.resume_fallback = Some("resume sample verification failed".to_string());
                    self.run_check().await;
                    return;
                }
                let mut pieces_restored = 0u64;
                for index in overlaps {
                    if overlap_failed.contains(&index) {
                        continue;
                    }
                    let _ = self
                        .have_map
                        .write()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .set(index);
                    self.picker().mark_have(index);
                    pieces_restored += self.piece_size(index) as u64;
                }
                self.verified_bytes += pieces_restored;
                if !overlap_failed.is_empty() {
                    self.resume_dirty = true;
                    self.write_resume_snapshot().await;
                }
            }
            Err(_) => {
                self.resume_fallback = Some("resume verification task failed".to_string());
                self.run_check().await;
                return;
            }
        }
        if self.state == State::Paused {
            self.publish();
            return;
        }
        self.finish_initial_verification().await;
    }

    fn picker(&mut self) -> &mut PiecePicker {
        match self.picker.as_mut() {
            Some(picker) => picker,
            None => {
                self.error = Some("picker unavailable".to_string());
                &mut self.spare_picker
            }
        }
    }

    fn piece_size(&self, index: usize) -> usize {
        match &self.storage {
            Some(storage) => storage.piece_size(index),
            None => 0,
        }
    }

    fn info_hash(&self) -> [u8; 20] {
        match &self.meta {
            Some(meta) => meta.info_hash,
            None => self
                .pending
                .as_ref()
                .map(|pending| pending.info_hash)
                .unwrap_or([0; 20]),
        }
    }

    fn display_name(&self) -> String {
        match &self.meta {
            Some(meta) => meta.info.name.clone(),
            None => self
                .pending
                .as_ref()
                .and_then(|pending| pending.display_name.clone())
                .unwrap_or_else(|| crate::hex::encode(&self.info_hash())),
        }
    }

    async fn handle_command(&mut self, command: EngineCommand) {
        match command {
            EngineCommand::Start => {
                if self.state == State::Stopped {
                    self.initial_check().await;
                }
            }
            EngineCommand::Pause => self.pause().await,
            EngineCommand::Resume => self.resume().await,
            EngineCommand::Stop => self.stop().await,
            EngineCommand::SetFilePriorities(priorities) => {
                self.apply_file_priorities(priorities).await;
            }
            EngineCommand::ForceRecheck => self.force_recheck().await,
        }
    }

    /// Applies a priority change at runtime: pieces that are no longer wanted
    /// have their in-flight blocks cancelled and assembler buffers dropped,
    /// newly wanted pieces are requested immediately without a re-check, and
    /// files leaving the skipped set are created so resume fingerprints exist.
    async fn apply_file_priorities(&mut self, pairs: Vec<(usize, FilePriority)>) {
        let Some(meta) = self.meta.clone() else {
            // Metadata has not arrived yet; remember the request for the
            // upgrade to the real torrent.
            if let Some(pending) = self.pending.as_mut() {
                for (index, priority) in pairs {
                    pending
                        .file_priorities
                        .retain(|(existing, _)| *existing != index);
                    pending.file_priorities.push((index, priority));
                }
            }
            return;
        };
        let file_count = file_count_of(&meta);
        let mut priorities = self.file_priorities.clone();
        if priorities.len() != file_count {
            priorities = padded_priorities(&pairs, file_count);
        }
        let mut changed_to_wanted = Vec::new();
        for (index, priority) in &pairs {
            if *index >= file_count {
                continue;
            }
            if priorities[*index].is_skip() && !priority.is_skip() {
                changed_to_wanted.push(*index);
            }
            priorities[*index] = *priority;
        }
        self.file_priorities = priorities;
        let Some(storage) = self.storage.clone() else {
            return;
        };
        let skipped: Vec<bool> = self.file_priorities.iter().map(|p| p.is_skip()).collect();
        storage.set_skipped(&skipped);
        if !changed_to_wanted.is_empty() {
            let _ = spawn_blocking({
                let storage = storage.clone();
                let indices = changed_to_wanted.clone();
                move || storage.ensure_files_exist(&indices)
            })
            .await;
        }
        let (wanted, high) = piece_wanted_map(&meta, &storage, &self.file_priorities);
        let (cancels, dropped) = self.picker().set_wanted(&wanted, &high);
        if let Some(assembler) = self.assembler.as_mut() {
            assembler.drop_pieces(&dropped);
        }
        for (peer, index, begin) in cancels {
            let length = BLOCK_SIZE.min(self.piece_size(index).saturating_sub(begin)) as u32;
            if let Some(handle) = self.peers.get_mut(&peer) {
                let _ = handle.commands.try_send(PeerCommand::Cancel {
                    index: index as u32,
                    begin: begin as u32,
                    length,
                });
                handle.in_flight = handle.in_flight.saturating_sub(1);
            }
        }
        self.resume_dirty = true;
        let was_running = matches!(
            self.state,
            State::Downloading | State::Completed | State::Seeding
        );
        if was_running {
            if self.picker().is_complete() {
                self.state = self.completed_state();
                if self
                    .have_map
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .count()
                    == meta.info.pieces.len()
                {
                    self.queue_event(Event::Completed);
                } else {
                    self.queue_event(Event::Started);
                }
                self.wake_trackers(TokioInstant::now() + Duration::from_secs(1));
                self.disconnect_all().await;
                self.write_resume_snapshot().await;
            } else if matches!(self.state, State::Completed | State::Seeding) {
                // Newly wanted pieces exist; go back to downloading.
                self.state = State::Downloading;
                self.queue_event(Event::Started);
                self.wake_trackers(TokioInstant::now());
            }
        }
        self.refill_all().await;
        self.publish();
    }

    async fn run_check(&mut self) {
        let Some(storage) = self.storage.clone() else {
            return;
        };
        let Some(meta) = self.meta.clone() else {
            return;
        };
        self.state = State::Checking;
        self.publish();
        self.startup_pieces_hashed += meta.info.pieces.len();
        let progress = self.stats_tx.clone();
        let have = spawn_blocking(move || {
            storage::recheck(&storage, &meta.info.pieces, |done| {
                progress.send_modify(|stats| stats.verified_pieces = done);
            })
        })
        .await;
        let meta = match &self.meta {
            Some(meta) => meta.clone(),
            None => return,
        };
        match have {
            Ok(have) => {
                *self
                    .have_map
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = have.clone();
                // A recheck may run on a picker that already has state (force
                // recheck), so pieces that no longer verify must be unmarked.
                self.picker().reset_have(&have);
                let mut verified = 0u64;
                for index in 0..meta.info.pieces.len() {
                    if have.get(index) {
                        verified += self.piece_size(index) as u64;
                    }
                }
                self.verified_bytes = verified;
                self.file_verified = self
                    .storage
                    .as_ref()
                    .map(|storage| file_verified_of(storage, &have))
                    .unwrap_or_default();
                self.error = None;
                self.finish_initial_verification().await;
            }
            Err(_) => {
                self.error = Some("recheck failed".to_string());
                self.state = State::Error;
            }
        }
        self.publish();
    }

    async fn finish_initial_verification(&mut self) {
        let complete = self.picker().is_complete();
        let fully_verified = self
            .have_map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .count()
            == self
                .meta
                .as_ref()
                .map(|meta| meta.info.pieces.len())
                .unwrap_or(0);
        if complete {
            self.state = self.completed_state();
            if fully_verified {
                self.queue_event(Event::Completed);
            } else {
                self.queue_event(Event::Started);
            }
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
        if complete {
            self.write_resume_snapshot().await;
        }
        self.publish();
    }

    async fn write_resume_snapshot(&mut self) {
        let Some(dir) = self.resume_dir.clone() else {
            return;
        };
        let Some(storage) = self.storage.clone() else {
            return;
        };
        let Some(meta) = self.meta.clone() else {
            return;
        };
        let have = self
            .have_map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if have.count() == 0 {
            return;
        }
        let info_hash_hex = crate::hex::encode(&meta.info_hash);
        let piece_count = meta.info.pieces.len();
        let path = self::resume::snapshot_path(&dir, &info_hash_hex);
        let outcome = spawn_blocking(move || {
            storage.sync_dirty()?;
            // Missing files are recorded as `None`: startup planning ignores
            // them for skipped files and forces re-verification for files
            // that should exist.
            let files = self::resume::current_fingerprints(&storage.file_paths());
            let snapshot = self::resume::ResumeSnapshot {
                version: self::resume::RESUME_FORMAT_VERSION,
                info_hash: info_hash_hex,
                piece_count,
                bitfield: have.as_raw().to_vec(),
                files,
            };
            self::resume::write_snapshot(&path, &snapshot)
        })
        .await;
        if let Ok(Ok(())) = outcome {
            self.resume_dirty = false;
        }
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
            State::Checking | State::FetchingMetadata => self.pending_pause = true,
            State::Downloading | State::Completed | State::Seeding => {
                self.disconnect_all().await;
                self.backoff.clear();
                self.state = State::Paused;
                self.publish();
                self.write_resume_snapshot().await;
            }
            _ => {}
        }
    }

    async fn resume(&mut self) {
        match self.state {
            State::Paused => {
                self.state = if self
                    .picker
                    .as_ref()
                    .is_some_and(|picker| picker.is_complete())
                {
                    self.completed_state()
                } else {
                    State::Downloading
                };
                self.queue_event(Event::Started);
                self.wake_trackers(TokioInstant::now());
                self.publish();
            }
            State::Error => {
                // Retry after a storage fault: recreate the directories the
                // download writes into (never deleting anything), requeue the
                // piece whose write failed and start requesting again.
                self.error = None;
                self.error_retryable = false;
                if let Some(storage) = self.storage.clone() {
                    match spawn_blocking(move || storage.recreate_directories()).await {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => {
                            // The retry cannot even prepare the directories;
                            // report it and stay in the Error state.
                            self.error = Some(err.to_string());
                            self.error_retryable = err.retryable();
                            self.publish();
                            return;
                        }
                        Err(_) => {
                            self.error = Some("directory recreation task failed".to_string());
                            self.error_retryable = true;
                            self.publish();
                            return;
                        }
                    }
                }
                // Blocks in flight were silently dropped while the engine was
                // in the Error state, so their picker slots are stale: claim
                // them all back, like a choke would, before requesting again.
                if let Some(picker) = self.picker.as_mut() {
                    let peers: Vec<SocketAddr> = self.peers.keys().copied().collect();
                    for peer in peers {
                        picker.return_blocks(peer);
                    }
                }
                for handle in self.peers.values_mut() {
                    handle.in_flight = 0;
                }
                self.deferred.clear();
                if let Some(index) = self.failed_piece.take() {
                    self.picker().requeue_piece(index);
                }
                self.state = if self
                    .picker
                    .as_ref()
                    .is_some_and(|picker| picker.is_complete())
                {
                    self.completed_state()
                } else {
                    State::Downloading
                };
                self.queue_event(Event::Started);
                self.wake_trackers(TokioInstant::now());
                self.refill_all().await;
                self.publish();
            }
            _ => {}
        }
    }

    /// Re-verifies every piece from disk. Also rewrites the resume snapshot,
    /// or removes it when nothing survives the recheck, so a stale snapshot
    /// cannot resurrect pieces that no longer verify.
    async fn force_recheck(&mut self) {
        let Some(meta) = self.meta.clone() else {
            return;
        };
        self.disconnect_all().await;
        let resume_dir = self.resume_dir.clone();
        let info_hash_hex = crate::hex::encode(&meta.info_hash);
        self.state = State::Checking;
        self.publish();
        self.run_check().await;
        if self.state == State::Error {
            return;
        }
        let have_count = self
            .have_map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .count();
        if let Some(dir) = resume_dir {
            if have_count == 0 {
                self::resume::remove_snapshot(&dir, &info_hash_hex);
            } else {
                self.resume_dirty = true;
                self.write_resume_snapshot().await;
            }
        }
    }

    async fn stop(&mut self) {
        self.disconnect_all().await;
        let mut waits = tokio::task::JoinSet::new();
        for tracker in &self.trackers {
            let request = AnnounceRequest {
                info_hash: self.info_hash(),
                peer_id: self.our_peer_id,
                port: self.announce_port.load(Ordering::Relaxed),
                uploaded: self.session_uploaded,
                downloaded: self.session_downloaded,
                left: self.left_for_announce(),
                numwant: 0,
                event: Some(Event::Stopped),
            };
            let future = self.announce_future(
                tracker.url.clone(),
                tracker.transport.clone(),
                tracker.id,
                request,
            );
            waits.spawn(async move {
                let _ = future.await;
            });
        }
        let _ = tokio::time::timeout(STOP_ANNOUNCE_WAIT, waits.join_all()).await;
        self.write_resume_snapshot().await;
        self.state = State::Stopped;
        self.publish();
    }

    fn earliest_announce(&self) -> Option<TokioInstant> {
        if !matches!(
            self.state,
            State::FetchingMetadata | State::Downloading | State::Completed | State::Seeding
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
            State::FetchingMetadata | State::Downloading | State::Completed | State::Seeding
        ) {
            return;
        }
        let now = TokioInstant::now();
        let left = self.left_for_announce();
        let info_hash = self.info_hash();
        let our_peer_id = self.our_peer_id;
        let announce_port = self.announce_port.load(Ordering::Relaxed);
        let uploaded = self.session_uploaded;
        let downloaded = self.session_downloaded;
        let mut due: Vec<(usize, String, TrackerTransport, AnnounceRequest)> = Vec::new();
        for tracker in &mut self.trackers {
            if !tracker.active || tracker.next_announce > now {
                continue;
            }
            let event = tracker.pending_event.take();
            let request = AnnounceRequest {
                info_hash,
                peer_id: our_peer_id,
                port: announce_port,
                uploaded,
                downloaded,
                left,
                numwant: NUMWANT,
                event,
            };
            due.push((
                tracker.id,
                tracker.url.clone(),
                tracker.transport.clone(),
                request,
            ));
            tracker.state = TrackerState::Announcing;
            tracker.next_announce = now + tracker.backoff;
        }
        let due_count = due.len();
        for (id, url, transport, request) in due {
            self.spawn_announce(&url, transport, id, request);
        }
        if due_count > 0 {
            self.publish();
        }
    }

    fn announce_future(
        &self,
        url: String,
        transport: TrackerTransport,
        id: usize,
        request: AnnounceRequest,
    ) -> Pin<Box<dyn Future<Output = TrackerOutcome> + Send>> {
        match transport {
            TrackerTransport::Http => {
                let http = self.http.clone();
                Box::pin(async move {
                    let result = tracker::http_announce(&http, &url, &request).await;
                    TrackerOutcome { id, result }
                })
            }
            TrackerTransport::Udp(cell) => Box::pin(async move {
                let client = cell
                    .get_or_try_init(|| async {
                        UdpTrackerClient::connect_tracker(&url, UdpConfig::default())
                            .await
                            .map(|client| Arc::new(tokio::sync::Mutex::new(client)))
                    })
                    .await;
                let result = match client {
                    Ok(client) => client.lock().await.announce(&request, -1).await,
                    Err(err) => Err(err),
                };
                TrackerOutcome { id, result }
            }),
        }
    }

    fn extension_handshake_payload(&self) -> Option<Vec<u8>> {
        match (&self.meta, &self.pending) {
            (Some(_), _) => {
                let raw = self
                    .raw_metainfo
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                let Ok((_, size)) = metadata_info_and_size(&raw) else {
                    return None;
                };
                Some(crate::extensions::encode_extension_handshake(
                    &crate::extensions::ExtensionHandshake::with_metadata_size(size),
                ))
            }
            (None, Some(pending)) => Some(crate::extensions::encode_extension_handshake(
                &crate::extensions::ExtensionHandshake {
                    ut_metadata: Some(1),
                    metadata_size: pending.size,
                    client: Some(concat!("bt-core/", env!("CARGO_PKG_VERSION")).to_string()),
                },
            )),
            (None, None) => None,
        }
    }

    async fn metadata_tick(&mut self) {
        let Some(pending) = self.pending.as_mut() else {
            return;
        };
        let now = TokioInstant::now();
        let expired: Vec<u32> = pending
            .in_flight
            .iter()
            .filter(|(_, sent_at)| now.duration_since(**sent_at) >= METADATA_REQUEST_TIMEOUT)
            .map(|(piece, _)| *piece)
            .collect();
        for piece in &expired {
            pending.pieces.remove(piece);
            pending.in_flight.remove(piece);
        }

        let candidates: Vec<SocketAddr> = self
            .peers
            .iter()
            .filter(|(_, handle)| {
                handle.metadata_trusted
                    && handle
                        .extensions
                        .as_ref()
                        .is_some_and(|ext| ext.ut_metadata.is_some())
            })
            .map(|(addr, _)| *addr)
            .collect();

        let Some(pending) = self.pending.as_mut() else {
            return;
        };
        let target_pieces: Vec<u32> = match pending.total_pieces() {
            Some(total) => (0..total).collect(),
            None => vec![0],
        };
        if candidates.is_empty() {
            return;
        }

        let mut in_flight = pending.in_flight.len();
        let mut round_robin = pending.round_robin;
        for piece in target_pieces {
            if in_flight >= MAX_IN_FLIGHT_METADATA {
                break;
            }
            if pending.pieces.contains_key(&piece) || pending.in_flight.contains_key(&piece) {
                continue;
            }
            let addr = candidates[round_robin % candidates.len()];
            round_robin += 1;
            let Some(extension_id) = self
                .peers
                .get(&addr)
                .and_then(|handle| handle.extensions.as_ref().and_then(|ext| ext.ut_metadata))
            else {
                continue;
            };
            let payload =
                crate::extensions::encode_ut_metadata(&crate::extensions::UtMetadata::Request {
                    piece,
                });
            if let Some(handle) = self.peers.get(&addr) {
                if handle
                    .commands
                    .try_send(PeerCommand::Extended {
                        extension_id,
                        payload,
                    })
                    .is_ok()
                {
                    self.diag.metadata_requests_sent += 1;
                    pending.in_flight.insert(piece, now);
                    in_flight += 1;
                }
            }
        }
        pending.round_robin = round_robin;
    }

    async fn handle_extended_event(&mut self, addr: SocketAddr, extension_id: u8, payload: &[u8]) {
        if extension_id == crate::extensions::EXTENSION_HANDSHAKE_ID {
            let handshake = match crate::extensions::decode_extension_handshake(payload) {
                Ok(handshake) => handshake,
                Err(_) => {
                    self.diag.extension_handshake_decode_errors += 1;
                    return;
                }
            };
            self.diag.extension_handshakes += 1;
            if handshake.ut_metadata.is_some() {
                self.diag.ut_metadata_advertisers += 1;
            }
            if let Some(size) = handshake.metadata_size {
                *self.diag.metadata_sizes.entry(size).or_default() += 1;
            }
            if let Some(handle) = self.peers.get_mut(&addr) {
                handle.extensions = Some(handshake.clone());
            }
            if let Some(pending) = &self.pending {
                let advertised = handshake.metadata_size;
                if let Some(advertised) = advertised {
                    let valid =
                        advertised > 0 && advertised <= crate::extensions::MAX_METADATA_SIZE;
                    let consistent = pending.size.is_none_or(|locked| locked == advertised);
                    if !valid || !consistent {
                        if let Some(handle) = self.peers.get_mut(&addr) {
                            handle.metadata_trusted = false;
                        }
                    }
                }
            }
            return;
        }
        if extension_id != crate::extensions::LOCAL_UT_METADATA_ID {
            return;
        }
        let message = match crate::extensions::decode_ut_metadata(payload) {
            Ok(message) => message,
            Err(_) => return,
        };
        match message {
            crate::extensions::UtMetadata::Request { piece } => {
                self.serve_metadata_piece(addr, piece).await;
            }
            crate::extensions::UtMetadata::Reject { piece } => {
                self.diag.metadata_rejects_received += 1;
                if let Some(pending) = &mut self.pending {
                    pending.in_flight.remove(&piece);
                }
            }
            crate::extensions::UtMetadata::Data {
                piece,
                total_size,
                data,
            } => {
                self.diag.metadata_data_received += 1;
                self.accept_metadata_piece(addr, piece, total_size, data)
                    .await;
            }
        }
    }

    async fn serve_metadata_piece(&mut self, addr: SocketAddr, piece: u32) {
        let raw = self
            .raw_metainfo
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let Ok((info_bytes, total_size)) = metadata_info_and_size(&raw) else {
            return;
        };
        let total_pieces =
            total_size.div_ceil(crate::extensions::METADATA_PIECE_SIZE as u64) as u32;
        let Some(extension_id) = self
            .peers
            .get(&addr)
            .and_then(|handle| handle.extensions.as_ref().and_then(|ext| ext.ut_metadata))
        else {
            return;
        };
        let payload = if piece < total_pieces {
            let start = piece as usize * crate::extensions::METADATA_PIECE_SIZE;
            let mut end = (start + crate::extensions::METADATA_PIECE_SIZE).min(total_size as usize);
            if end < start {
                end = start;
            }
            crate::extensions::encode_ut_metadata(&crate::extensions::UtMetadata::Data {
                piece,
                total_size,
                data: info_bytes[start..end].to_vec(),
            })
        } else {
            crate::extensions::encode_ut_metadata(&crate::extensions::UtMetadata::Reject { piece })
        };
        if let Some(handle) = self.peers.get(&addr) {
            let _ = handle.commands.try_send(PeerCommand::Extended {
                extension_id,
                payload,
            });
        }
    }

    async fn accept_metadata_piece(
        &mut self,
        addr: SocketAddr,
        piece: u32,
        total_size: u64,
        data: Vec<u8>,
    ) {
        let Some(pending) = self.pending.as_mut() else {
            return;
        };
        {
            if let Some(locked) = pending.size {
                if locked != total_size {
                    if let Some(handle) = self.peers.get_mut(&addr) {
                        handle.metadata_trusted = false;
                    }
                    pending.in_flight.remove(&piece);
                    return;
                }
            } else {
                if total_size == 0
                    || total_size > crate::extensions::MAX_METADATA_SIZE
                    || data.len() > crate::extensions::METADATA_PIECE_SIZE
                {
                    return;
                }
                pending.size = Some(total_size);
            }
            let total_pieces = pending
                .size
                .unwrap_or(total_size)
                .div_ceil(crate::extensions::METADATA_PIECE_SIZE as u64)
                as u32;
            if piece >= total_pieces
                || pending.pieces.contains_key(&piece)
                || data.len() != crate::extensions::metadata_piece_len(total_size, piece)
            {
                return;
            }
            pending.pieces.insert(piece, data);
            pending.in_flight.remove(&piece);
            pending.contributors.insert(addr);
            if pending.pieces.len() != total_pieces as usize {
                return;
            }
        }
        let Some(pending) = self.pending.as_ref() else {
            return;
        };
        let info_hash = pending.info_hash;
        let output_dir = pending.output_dir.clone();
        let mut assembled = Vec::with_capacity(pending.size.unwrap_or(0) as usize);
        let Some(total) = pending.total_pieces() else {
            return;
        };
        for piece in 0..total {
            match pending.pieces.get(&piece) {
                Some(data) => assembled.extend_from_slice(data),
                None => return,
            }
        }
        let contributors: HashSet<SocketAddr> = pending.contributors.iter().copied().collect();
        if crate::extensions::sha1(&assembled) != info_hash {
            let mut ban_contributors = false;
            {
                let Some(pending) = self.pending.as_mut() else {
                    return;
                };
                pending.failed_assemblies = pending.failed_assemblies.saturating_add(1);
                if pending.failed_assemblies >= MAX_METADATA_ASSEMBLY_FAILURES {
                    ban_contributors = true;
                }
                pending.size = None;
                pending.pieces.clear();
                pending.in_flight.clear();
                pending.contributors.clear();
                pending.round_robin = pending.round_robin.wrapping_add(1);
            }
            if ban_contributors {
                for contributor in contributors {
                    self.strike(contributor).await;
                }
            }
            return;
        }
        self.upgrade_with_metadata(assembled, output_dir).await;
    }

    async fn upgrade_with_metadata(&mut self, info_dict: Vec<u8>, output_dir: PathBuf) {
        let trackers: Vec<Vec<String>> = self
            .trackers
            .iter()
            .map(|tracker| vec![tracker.url.clone()])
            .collect();
        let mut root = std::collections::BTreeMap::new();
        let Ok(info) = crate::bencode::decode(&info_dict) else {
            return;
        };
        root.insert(b"info".to_vec(), info);
        root.insert(
            b"announce-list".to_vec(),
            crate::bencode::Value::List(
                trackers
                    .iter()
                    .map(|tier| {
                        crate::bencode::Value::List(
                            tier.iter()
                                .map(|url| crate::bencode::Value::Bytes(url.clone().into_bytes()))
                                .collect(),
                        )
                    })
                    .collect(),
            ),
        );
        let raw = crate::bencode::encode(&crate::bencode::Value::Dict(root));
        let Ok(meta) = MetaInfo::from_bytes(&raw) else {
            return;
        };
        if meta.info_hash != self.info_hash() {
            return;
        }
        let meta = Arc::new(meta);
        let output = output_dir.clone();
        let storage_meta = meta.clone();
        let file_count = file_count_of(&meta);
        let file_priorities = {
            let pairs = self
                .pending
                .as_ref()
                .map(|pending| pending.file_priorities.clone())
                .unwrap_or_default();
            padded_priorities(&pairs, file_count_of(&meta))
        };
        if !self.skip_free_space_check {
            let meta_for_space = meta.clone();
            let dir_for_space = output.clone();
            let priorities_for_space = file_priorities.clone();
            let fs = self.fs.clone();
            match tokio::task::spawn_blocking(move || {
                Storage::check_free_space(
                    &meta_for_space,
                    &dir_for_space,
                    &priorities_for_space,
                    fs.as_ref(),
                )
            })
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    self.error = Some(err.to_string());
                    self.error_retryable = err.retryable();
                    self.state = State::Error;
                    self.publish();
                    return;
                }
                Err(_) => {
                    self.error = Some("free space check failed".to_string());
                    self.error_retryable = true;
                    self.state = State::Error;
                    self.publish();
                    return;
                }
            }
        }
        let priorities_for_storage = file_priorities.clone();
        let fs = self.fs.clone();
        let storage = match tokio::task::spawn_blocking(move || {
            Storage::create_with_fs(&storage_meta, &output, &priorities_for_storage, fs)
        })
        .await
        {
            Ok(Ok(storage)) => Arc::new(storage),
            Ok(Err(err)) => {
                self.error = Some(err.to_string());
                self.error_retryable = err.retryable();
                self.state = State::Error;
                self.publish();
                return;
            }
            Err(_) => {
                self.error = Some("storage creation failed".to_string());
                self.error_retryable = false;
                self.state = State::Error;
                self.publish();
                return;
            }
        };
        let piece_count = meta.info.pieces.len();
        let total_length = storage.total_length();
        self.total_length = total_length;
        let (wanted, high) = piece_wanted_map(&meta, &storage, &file_priorities);
        let mut picker = PiecePicker::new(
            piece_count,
            meta.info.piece_length,
            total_length,
            RANDOM_FIRST,
            MAX_ACTIVE_PIECES,
        );
        picker.set_wanted(&wanted, &high);
        self.picker = Some(picker);
        self.assembler = Some(PieceAssembler::new(meta.info.piece_length, total_length));
        self.meta = Some(meta);
        self.storage = Some(storage);
        self.file_priorities = file_priorities;
        self.file_verified = vec![0; file_count];
        self.raw_metainfo = Arc::new(std::sync::Mutex::new(raw.clone()));
        self.pending = None;
        let _ = self.metadata_tx.send(Some(Arc::new(raw)));
        self.initial_check().await;
        if self.pause_after_metadata && !matches!(self.state, State::Paused) {
            // Stop before any data is downloaded so the UI can show the file
            // list and let the user choose priorities first.
            self.state = State::Paused;
            self.disconnect_all().await;
            self.publish();
        }
        self.refill_all().await;
        let targets: Vec<(u8, mpsc::Sender<PeerCommand>)> = self
            .peers
            .values()
            .filter_map(|handle| {
                let extension_id = handle.extensions.as_ref().and_then(|ext| ext.ut_metadata)?;
                Some((extension_id, handle.commands.clone()))
            })
            .collect();
        if let Some(payload) = self.extension_handshake_payload() {
            for (_, commands) in targets {
                let _ = commands.try_send(PeerCommand::Extended {
                    extension_id: crate::extensions::EXTENSION_HANDSHAKE_ID,
                    payload: payload.clone(),
                });
            }
        }
        self.try_connect();
    }

    fn left_for_announce(&self) -> u64 {
        match &self.pending {
            Some(pending) => pending.size.unwrap_or(1),
            None => self.total_length - self.verified_bytes,
        }
    }

    fn spawn_announce(
        &self,
        url: &str,
        transport: TrackerTransport,
        id: usize,
        request: AnnounceRequest,
    ) {
        let future = self.announce_future(url.to_string(), transport, id, request);
        let tx = self.announce_results_tx.clone();
        tokio::spawn(async move {
            let outcome = future.await;
            let _ = tx.send(outcome).await;
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
                    if tracker::is_valid_peer_address(*addr) {
                        self.push_peer(*addr);
                    }
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
        if self.banned.contains(&addr) || self.peers.contains_key(&addr) {
            return;
        }
        self.backlog.push(addr);
    }

    fn try_connect(&mut self) {
        if !matches!(
            self.state,
            State::FetchingMetadata | State::Downloading | State::Completed | State::Seeding
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
        while position < self.backlog.queue.len() {
            let addr = self.backlog.queue[position];
            if self.banned.contains(&addr) {
                self.backlog.remove(&addr);
                continue;
            }
            if self.peers.contains_key(&addr) {
                self.backlog.queue.remove(position);
                continue;
            }
            if self.backoff.get(&addr).is_some_and(|next| now < *next) {
                position += 1;
                continue;
            }
            self.backlog.queue.remove(position);
            return Some(addr);
        }
        None
    }

    fn is_private(&self) -> bool {
        self.meta.as_ref().is_some_and(|meta| meta.info.private)
    }

    fn now_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    fn dht_handshake(&self) -> peer_task::DhtHandshake {
        let mut handshake = peer_task::DhtHandshake::default();
        if let Some(dht) = &self.dht {
            if dht.active.load(Ordering::Relaxed) && !self.is_private() {
                handshake.reserved = crate::extensions::with_dht_bit(handshake.reserved);
                handshake.our_port = Some(dht.port.load(Ordering::Relaxed));
            }
        }
        handshake
    }

    fn maybe_start_dht_lookup(&mut self) {
        let Some(dht) = self.dht.clone() else {
            return;
        };
        let schedule = crate::dht::LookupSchedule {
            enabled: dht.active.load(Ordering::Relaxed),
            private: self.is_private(),
            fetching_metadata: matches!(self.state, State::FetchingMetadata),
            connected_peers: self.peers.len(),
            lookup_in_flight: self.dht_lookup_in_flight,
            last_lookup_ms: self.last_dht_lookup_ms,
        };
        let now_ms = self.now_ms();
        if !crate::dht::should_lookup(&schedule, now_ms) {
            return;
        }
        if dht
            .handle
            .request_lookup(self.info_hash(), self.dht_results_tx.clone())
        {
            self.dht_lookup_in_flight = true;
            self.last_dht_lookup_ms = Some(now_ms);
        }
    }

    fn handle_dht_peers(&mut self, peers: crate::dht::DhtPeers) {
        self.dht_lookup_in_flight = false;
        if peers.info_hash != self.info_hash() {
            return;
        }
        for addr in peers.peers {
            if crate::tracker::is_valid_peer_address(addr) {
                self.backlog.push(addr);
            }
        }
    }

    fn spawn_peer(&mut self, addr: SocketAddr) {
        let (commands, command_rx) = mpsc::channel(64);
        self.peers.insert(
            addr,
            PeerHandle {
                client: String::new(),
                peer_id: [0; 20],
                commands,
                extensions: None,
                metadata_trusted: true,
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
            info_hash: self.info_hash(),
            our_peer_id: self.our_peer_id,
            piece_count: self.meta.as_ref().map(|meta| meta.info.pieces.len()),
            config: PeerConfig::default(),
            dial: self.dial.clone(),
            extension_handshake: self.extension_handshake_payload(),
            have: self.have_map.clone(),
            storage: self.storage.clone(),
            uploads: self.uploads.clone(),
            dht: self.dht_handshake(),
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
                extensions: None,
                metadata_trusted: true,
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
            piece_count: self.meta.as_ref().map(|meta| meta.info.pieces.len()),
            config: PeerConfig::default(),
            remote: incoming.remote,
            stream: incoming.stream,
            extension_handshake: self.extension_handshake_payload(),
            have: self.have_map.clone(),
            storage: self.storage.clone(),
            uploads: self.uploads.clone(),
            dht: self.dht_handshake(),
        };
        tokio::spawn(peer_task::run_incoming_peer_task(
            task,
            command_rx,
            self.events_tx.clone(),
        ));
    }

    async fn handle_event(&mut self, event: PeerEvent) {
        match event {
            PeerEvent::Port { addr, port } => {
                if self.is_private() || port == 0 {
                    return;
                }
                let SocketAddr::V4(v4) = addr else {
                    return;
                };
                if let Some(dht) = &self.dht {
                    let _ = dht
                        .handle
                        .verify_candidate(SocketAddrV4::new(*v4.ip(), port));
                }
            }
            PeerEvent::Handshaken { addr, peer_id } => {
                self.diag.peers_connected += 1;
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
                if let Some(picker) = self.picker.as_mut() {
                    picker.add_peer(&bitfield);
                }
                if let Some(handle) = self.peers.get_mut(&addr) {
                    handle.bitfield = Some(bitfield);
                }
                self.refill(addr).await;
            }
            PeerEvent::Have { addr, index } => {
                if let Some(picker) = self.picker.as_mut() {
                    picker.add_have(index as usize);
                }
                if let Some(handle) = self.peers.get_mut(&addr) {
                    if let Some(meta) = &self.meta {
                        let bitfield = handle
                            .bitfield
                            .get_or_insert_with(|| Bitfield::new(meta.info.pieces.len()));
                        let _ = bitfield.set(index as usize);
                    }
                }
                self.refill(addr).await;
            }
            PeerEvent::Choke { addr } => {
                if let Some(handle) = self.peers.get_mut(&addr) {
                    handle.choked = true;
                    handle.in_flight = 0;
                }
                if let Some(picker) = self.picker.as_mut() {
                    picker.return_blocks(addr);
                }
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
            PeerEvent::Extended {
                addr,
                extension_id,
                payload,
            } => {
                self.handle_extended_event(addr, extension_id, &payload)
                    .await;
            }
            PeerEvent::Disconnected { addr, reason } => {
                let label = reason.unwrap_or_else(|| "unknown".to_string());
                let engine_initiated = label == "clean";
                *self.diag.peer_close_reasons.entry(label).or_default() += 1;
                let mut direction = None;
                if let Some(handle) = self.peers.remove(&addr) {
                    direction = Some(handle.direction);
                    if let Some(picker) = self.picker.as_mut() {
                        if let Some(bitfield) = &handle.bitfield {
                            picker.remove_peer(bitfield);
                        }
                        picker.return_blocks(addr);
                    }
                }
                if direction == Some(PeerDirection::Outgoing) {
                    // A "clean" close is the engine's own doing (Stop);
                    // only failures the peer caused earn a connect backoff.
                    if self.state == State::Downloading && !engine_initiated {
                        self.backoff
                            .insert(addr, TokioInstant::now() + CONNECT_BACKOFF);
                    }
                    self.backlog.remove(&addr);
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
        let Some(meta) = &self.meta else {
            return;
        };
        if index >= meta.info.pieces.len() {
            return;
        }
        let Some(assembler) = self.assembler.as_mut() else {
            return;
        };
        let Some(picker) = self.picker.as_mut() else {
            return;
        };
        let piece_size = self
            .storage
            .as_ref()
            .map(|storage| storage.piece_size(index))
            .unwrap_or(0);
        match assembler.write_block(index, begin, &block) {
            BlockOutcome::Accepted | BlockOutcome::Completed => {
                if let Some(handle) = self.peers.get_mut(&addr) {
                    handle.in_flight = handle.in_flight.saturating_sub(1);
                    handle.received_bytes += block.len() as u64;
                    handle.window_down += block.len() as u64;
                }
                self.session_downloaded += block.len() as u64;
                let cancel_targets = picker.block_received(index, begin, addr);
                let length = BLOCK_SIZE.min(piece_size.saturating_sub(begin)) as u32;
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
        let complete = assembler.is_complete(index);
        if complete {
            self.finish_piece(index).await;
        } else {
            self.refill(addr).await;
        }
    }

    async fn finish_piece(&mut self, index: usize) {
        let Some(data) = self.assembler.as_mut().and_then(|asm| asm.take(index)) else {
            return;
        };
        let Some(meta) = &self.meta else {
            return;
        };
        let piece_count = meta.info.pieces.len();
        let Some(storage) = self.storage.clone() else {
            return;
        };
        let expected = meta.info.pieces[index];
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
                self.picker().mark_have(index);
                let _ = self
                    .have_map
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .set(index);
                self.verified_bytes += self.piece_size(index) as u64;
                for (file, bytes) in self
                    .storage
                    .as_ref()
                    .map(|storage| storage.piece_file_spans(index))
                    .unwrap_or_default()
                {
                    if let Some(slot) = self.file_verified.get_mut(file) {
                        *slot += bytes as u64;
                    }
                }
                self.resume_dirty = true;
                if self.picker().is_complete() {
                    self.state = self.completed_state();
                    // The completed event is only honest once the entire
                    // torrent is verified: we cannot serve the skipped parts,
                    // so announcing completion early would misrepresent us.
                    if self
                        .have_map
                        .read()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .count()
                        == piece_count
                    {
                        self.queue_event(Event::Completed);
                    }
                    self.wake_trackers(TokioInstant::now() + Duration::from_secs(1));
                    self.disconnect_all().await;
                    self.write_resume_snapshot().await;
                } else {
                    self.broadcast_have(index);
                }
            }
            Ok(PieceWriteOutcome::HashMismatch) => {
                for contributor in self.picker().contributors(index) {
                    self.strike(contributor).await;
                }
                self.picker().requeue_piece(index);
                self.refill_all().await;
            }
            Ok(PieceWriteOutcome::Failed(err)) => {
                // Disk-related write failures are not peer strikes and not
                // hash failures: the data came back fine, the disk said no.
                self.error = Some(err.to_string());
                self.error_retryable = err.retryable();
                self.failed_piece = Some(index);
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
            if let Some(picker) = self.picker.as_mut() {
                if let Some(bitfield) = &handle.bitfield {
                    picker.remove_peer(bitfield);
                }
                picker.return_blocks(addr);
            }
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
            let Some((picker, assembler)) = self.picker.as_mut().zip(self.assembler.as_mut())
            else {
                return;
            };
            let next = match picker.next_block(addr, bitfield) {
                Some(next) => Some(next),
                None => {
                    if !picker.is_endgame() {
                        None
                    } else {
                        picker.next_endgame_block(addr, bitfield)
                    }
                }
            };
            let Some((index, begin, length)) = next else {
                return;
            };
            if !assembler.is_open(index) {
                assembler.open(index);
            }
            let sent = {
                let Some(handle) = self.peers.get(&addr) else {
                    return;
                };
                dispatch_request(&handle.commands, picker, index, begin, length)
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
        let Some(picker) = self.picker.as_mut() else {
            return;
        };
        let reaped = picker.reap_stale(REQUEST_TIMEOUT, TokioInstant::now());
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
        if self
            .picker
            .as_ref()
            .is_some_and(|picker| picker.is_complete())
            && matches!(self.state, State::Completed | State::Seeding)
        {
            let want = self.completed_state();
            if want != self.state {
                self.state = want;
                self.publish();
            }
        }
    }

    fn tick_stats(&mut self) {
        // Publish BEFORE rotating the baselines: rotating first made every
        // tick publish an exactly-zero rate (bytes minus themselves over the
        // window), and since block arrivals do not publish, those zero
        // snapshots were almost all the UI ever received.
        self.publish();
        let now = TokioInstant::now();
        for handle in self.peers.values_mut() {
            handle.rate_base = handle.received_bytes;
            handle.upload_rate_base = handle.uploaded_bytes;
            handle.rate_window.push(now, handle.received_bytes);
            handle.depth = pipeline_depth(handle.rate_window.rate(), TARGET_RTT, BLOCK_SIZE);
        }
        self.last_rate = (now, self.session_downloaded, self.session_uploaded);
    }

    fn file_reports(&self) -> Vec<FileStats> {
        let Some(meta) = self.meta.as_ref() else {
            return Vec::new();
        };
        let entries: Vec<(String, u64)> = match &meta.info.content {
            crate::metainfo::Content::Single { length } => vec![(meta.info.name.clone(), *length)],
            crate::metainfo::Content::Multi { files } => files
                .iter()
                .map(|file| {
                    let mut path = PathBuf::new();
                    for segment in &file.path {
                        path.push(segment);
                    }
                    (path.to_string_lossy().into_owned(), file.length)
                })
                .collect(),
        };
        entries
            .into_iter()
            .enumerate()
            .map(|(index, (path, length))| FileStats {
                path,
                length,
                priority: self
                    .file_priorities
                    .get(index)
                    .copied()
                    .unwrap_or(FilePriority::Normal),
                verified_bytes: self.file_verified.get(index).copied().unwrap_or(0),
            })
            .collect()
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
        let files = self.file_reports();
        let wanted_bytes: u64 = files
            .iter()
            .filter(|file| !file.priority.is_skip())
            .map(|file| file.length)
            .sum();
        let verified_wanted_bytes: u64 = files
            .iter()
            .filter(|file| !file.priority.is_skip())
            .map(|file| file.verified_bytes)
            .sum();
        Stats {
            state: self.state,
            name: self.display_name(),
            total_length: self
                .storage
                .as_ref()
                .map(|storage| storage.total_length())
                .unwrap_or(self.total_length),
            verified_bytes: self.verified_bytes,
            wanted_bytes,
            verified_wanted_bytes,
            session_downloaded: self.session_downloaded,
            session_uploaded: self.session_uploaded,
            upload_rate: (self.session_uploaded - self.last_rate.2) as f64 / dt,
            ratio: if self.total_length > 0 {
                self.session_uploaded as f64 / self.total_length as f64
            } else {
                0.0
            },
            verified_pieces: self
                .picker
                .as_ref()
                .map(|picker| picker.have().count())
                .unwrap_or(0),
            piece_count: self
                .meta
                .as_ref()
                .map(|meta| meta.info.pieces.len())
                .unwrap_or(0),
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
            files,
            metadata_progress: self
                .pending
                .as_ref()
                .map(|pending| pending.received_of_total()),
            diag: self.diag.clone(),
            error: self.error.clone(),
            error_retryable: self.error_retryable,
            dht_waiting: self.dht_waiting(),
            resumed_from_saved_state: self.resumed_from_saved_state,
            startup_pieces_hashed: self.startup_pieces_hashed,
            resume_fallback: self.resume_fallback.clone(),
        }
    }

    fn dht_waiting(&self) -> bool {
        let Some(dht) = &self.dht else {
            return false;
        };
        if !dht.active.load(Ordering::Relaxed) || !self.peers.is_empty() {
            return false;
        }
        matches!(
            self.state,
            State::FetchingMetadata | State::Downloading | State::Checking
        ) && self.last_dht_lookup_ms.is_some()
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

    fn stats_engine(dir: &std::path::Path) -> (Engine, watch::Receiver<Stats>) {
        let raw = boundary_torrent_bytes();
        let meta = Arc::new(MetaInfo::from_bytes(&raw).unwrap());
        let priorities = vec![FilePriority::Normal; 3];
        let storage = Arc::new(Storage::create(&meta, dir, &priorities).unwrap());
        let (stats_tx, stats_rx) = watch::channel(Stats {
            state: State::Downloading,
            name: "test".to_string(),
            total_length: 0,
            verified_bytes: 0,
            wanted_bytes: 0,
            verified_wanted_bytes: 0,
            session_downloaded: 0,
            session_uploaded: 0,
            upload_rate: 0.0,
            ratio: 0.0,
            verified_pieces: 0,
            piece_count: 0,
            download_rate: 0.0,
            peer_count: 0,
            incoming_peers: 0,
            outgoing_peers: 0,
            peers: Vec::new(),
            trackers: Vec::new(),
            files: Vec::new(),
            metadata_progress: None,
            diag: MetadataDiag::default(),
            error_retryable: false,
            error: None,
            dht_waiting: false,
            resumed_from_saved_state: false,
            startup_pieces_hashed: 0,
            resume_fallback: None,
        });
        let (commands_tx, commands_rx) = mpsc::channel(16);
        let (events_tx, events_rx) = mpsc::channel(1024);
        let (incoming_tx, incoming_rx) = mpsc::channel(8);
        let (announce_tx, announce_rx) = mpsc::channel(64);
        let (_metadata_tx, _metadata_rx) = watch::channel(None::<Arc<Vec<u8>>>);
        let engine = Engine::new(
            meta,
            storage,
            Arc::new(std::sync::Mutex::new(Vec::new())),
            None,
            watch::channel(None).0,
            tracker::http_client().unwrap_or_else(|_| reqwest::Client::new()),
            TorrentOptions::default(),
            stats_tx,
            commands_rx,
            events_tx,
            events_rx,
            incoming_rx,
            announce_rx,
            mpsc::channel(64).0,
            StartupVerification::FullRecheck(None),
        );
        // Silence unused-variable warnings for the senders this test never
        // drives; the engine only needs them to exist.
        let _ = (commands_tx, incoming_tx, announce_tx);
        (engine, stats_rx)
    }

    #[test]
    fn stats_ticks_publish_the_window_rate_before_resetting_the_baselines() {
        let dir = prios_temp_dir("stats-rate");
        let (mut engine, stats_rx) = stats_engine(&dir);

        // Bytes arrived since spawn: the tick must publish the window rate,
        // not zero.
        engine.session_downloaded = 10_000;
        engine.session_uploaded = 2_000;
        engine.tick_stats();
        let snapshot = stats_rx.borrow().clone();
        assert!(
            snapshot.download_rate > 0.0,
            "a tick with bytes in the window must publish a non-zero download rate"
        );
        assert!(snapshot.upload_rate > 0.0);

        // An idle tick (no new bytes) correctly reports zero.
        engine.tick_stats();
        let snapshot = stats_rx.borrow().clone();
        assert_eq!(snapshot.download_rate, 0.0);
        assert_eq!(snapshot.upload_rate, 0.0);

        // New bytes in the next window are visible again.
        engine.session_downloaded += 5_000;
        engine.tick_stats();
        let snapshot = stats_rx.borrow().clone();
        assert!(snapshot.download_rate > 0.0);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    fn boundary_torrent_bytes() -> Vec<u8> {
        // dir/a.txt (5 B), dir/sub/b.bin (3 B), dir/c.txt (4 B); piece length 4.
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d4:infod5:filesl");
        raw.extend_from_slice(b"d6:lengthi5e4:pathl5:a.txtee");
        raw.extend_from_slice(b"d6:lengthi3e4:pathl3:sub5:b.binee");
        raw.extend_from_slice(b"d6:lengthi4e4:pathl5:c.txtee");
        raw.extend_from_slice(b"e4:name3:dir12:piece lengthi4e6:pieces");
        let hashes: Vec<[u8; 20]> = vec![[1u8; 20], [2u8; 20], [3u8; 20]];
        raw.extend_from_slice((hashes.len() * 20).to_string().as_bytes());
        raw.push(b':');
        for hash in &hashes {
            raw.extend_from_slice(hash);
        }
        raw.extend_from_slice(b"ee");
        raw
    }

    fn prios_temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("bt-core-prios-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn per_file_verified_bytes_count_boundary_pieces_for_every_file() {
        let raw = boundary_torrent_bytes();
        let meta = MetaInfo::from_bytes(&raw).unwrap();
        let dir = prios_temp_dir("verified");
        let priorities = vec![FilePriority::Normal; 3];
        let storage = Storage::create(&meta, &dir, &priorities).unwrap();
        let mut have = Bitfield::new(3);
        // Piece 1 (bytes 4..8) covers a.txt's last byte and all of b.bin.
        have.set(1).unwrap();
        assert_eq!(file_verified_of(&storage, &have), vec![1, 3, 0]);
        let mut have = Bitfield::new(3);
        have.set(0).unwrap();
        have.set(2).unwrap();
        // Piece 0 is a.txt only; piece 2 is c.txt only.
        assert_eq!(file_verified_of(&storage, &have), vec![4, 0, 4]);
        let mut have = Bitfield::new(3);
        have.set(0).unwrap();
        have.set(1).unwrap();
        have.set(2).unwrap();
        assert_eq!(file_verified_of(&storage, &have), vec![5, 3, 4]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn wanted_map_marks_pieces_overlapping_non_skipped_files() {
        let raw = boundary_torrent_bytes();
        let meta = MetaInfo::from_bytes(&raw).unwrap();
        let dir = prios_temp_dir("wanted");
        let all_normal = vec![FilePriority::Normal; 3];
        let storage = Storage::create(&meta, &dir, &all_normal).unwrap();
        // a.txt skipped, b.bin High, c.txt Normal:
        // piece 0 (a only) unwanted; piece 1 (a+b) wanted+high; piece 2 (c) wanted.
        let priorities = vec![FilePriority::Skip, FilePriority::High, FilePriority::Normal];
        let (wanted, high) = piece_wanted_map(&meta, &storage, &priorities);
        assert!(!wanted.get(0));
        assert!(wanted.get(1));
        assert!(wanted.get(2));
        assert!(high.get(1));
        assert!(!high.get(2));
        std::fs::remove_dir_all(&dir).unwrap();
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

    fn backlog_addr(port: u16) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, 1], port))
    }

    #[test]
    fn backlog_ignores_duplicates() {
        let mut backlog = PeerBacklog::default();
        backlog.push(backlog_addr(1));
        backlog.push(backlog_addr(1));
        assert_eq!(backlog.queue.len(), 1);
        assert_eq!(backlog.known.len(), 1);
    }

    #[test]
    fn backlog_evicts_oldest_beyond_the_known_cap() {
        let mut backlog = PeerBacklog::default();
        for port in 0..(KNOWN_CAP + 2) as u16 {
            backlog.push(backlog_addr(port));
        }
        assert_eq!(backlog.known.len(), KNOWN_CAP);
        assert!(!backlog.is_known(&backlog_addr(0)));
        assert!(!backlog.is_known(&backlog_addr(1)));
        assert!(backlog.is_known(&backlog_addr(2)));
    }

    #[test]
    fn backlog_queue_is_capped() {
        let mut backlog = PeerBacklog::default();
        for port in 0..(QUEUE_CAP + 2) as u16 {
            backlog.push(backlog_addr(port));
        }
        assert_eq!(backlog.queue.len(), QUEUE_CAP);
        assert_eq!(backlog.queue.front().copied(), Some(backlog_addr(2)));
    }

    #[test]
    fn backlog_remove_drops_everywhere() {
        let mut backlog = PeerBacklog::default();
        backlog.push(backlog_addr(1));
        backlog.push(backlog_addr(2));
        backlog.remove(&backlog_addr(1));
        assert!(!backlog.is_known(&backlog_addr(1)));
        assert_eq!(backlog.queue.len(), 1);
        assert_eq!(backlog.order.len(), 1);
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
