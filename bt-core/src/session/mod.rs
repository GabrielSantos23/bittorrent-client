mod persist;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::spawn_blocking;

use crate::dht::{DhtHandle, DhtOptions, DhtStatus};
use crate::dht::{NodeId, SystemRandom};
use crate::engine::{PeerStats, State, Torrent, TrackerStatus};
use crate::listener::{self, Listener, ListenerOptions, ListenerStatus, Registry};
use crate::ratelimit::UploadBucket;

use self::persist::{PersistedTorrent, SessionFile};
use crate::error::SessionError;
use crate::hex;
use crate::metainfo::MetaInfo;
use crate::peer_id;

const PUBLISH_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub struct SessionOptions {
    pub listen_port: u16,
    pub upload_limit_bps: u64,
    pub choke_interval: Duration,
    pub optimistic_interval: Duration,
    pub dial: Arc<dyn crate::engine::Dial>,
    pub bootstrap_peers: Vec<std::net::SocketAddr>,
    pub dht_enabled: bool,
    pub dht_port: u16,
    pub dht_bootstrap: Vec<String>,
}

impl SessionOptions {
    pub fn new(listen_port: u16, upload_limit_bps: u64) -> SessionOptions {
        let connect_timeout = crate::peer::PeerConfig::default().connect_timeout;
        SessionOptions {
            listen_port,
            upload_limit_bps,
            choke_interval: Duration::from_secs(10),
            optimistic_interval: Duration::from_secs(30),
            dial: Arc::new(crate::engine::TcpDial::new(connect_timeout)),
            bootstrap_peers: Vec::new(),
            dht_enabled: true,
            dht_port: listen_port,
            dht_bootstrap: Vec::new(),
        }
    }

    pub fn with_dht_bootstrap(mut self, bootstrap: Vec<String>) -> SessionOptions {
        self.dht_bootstrap = bootstrap;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, ts_rs::TS)]
#[ts(export)]
pub struct TorrentSummary {
    pub id: String,
    pub name: String,
    pub state: State,
    #[ts(type = "number")]
    pub total_length: u64,
    #[ts(type = "number")]
    pub verified_bytes: u64,
    pub progress: f64,
    pub download_rate: f64,
    #[ts(type = "number")]
    pub session_uploaded: u64,
    pub upload_rate: f64,
    pub ratio: f64,
    #[ts(type = "number | null")]
    pub eta_seconds: Option<u64>,
    pub peer_count: usize,
    pub output_dir: PathBuf,
    pub error: Option<String>,
    pub dht_waiting: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, ts_rs::TS)]
#[ts(export)]
pub struct FileSummary {
    pub path: String,
    #[ts(type = "number")]
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, ts_rs::TS)]
#[ts(export)]
pub struct TorrentDetail {
    pub id: String,
    pub info_hash: String,
    pub peers: Vec<PeerStats>,
    pub files: Vec<FileSummary>,
    pub trackers: Vec<TrackerStatus>,
    pub comment: Option<String>,
    pub output_dir: PathBuf,
    #[ts(type = "number")]
    pub session_uploaded: u64,
    pub upload_rate: f64,
    pub ratio: f64,
}

#[derive(Debug)]
enum SessionCommand {
    Add {
        bytes: Vec<u8>,
        output_dir: PathBuf,
        paused: bool,
        reply: oneshot::Sender<Result<String, SessionError>>,
    },
    AddMagnet {
        uri: String,
        output_dir: PathBuf,
        paused: bool,
        reply: oneshot::Sender<Result<String, SessionError>>,
    },
    Pause {
        id: String,
        reply: oneshot::Sender<Result<(), SessionError>>,
    },
    Resume {
        id: String,
        reply: oneshot::Sender<Result<(), SessionError>>,
    },
    Remove {
        id: String,
        delete_files: bool,
        reply: oneshot::Sender<Result<(), SessionError>>,
    },
    Detail {
        id: String,
        reply: oneshot::Sender<Option<TorrentDetail>>,
    },
    SetUploadLimit {
        bps: u64,
        reply: oneshot::Sender<Result<(), SessionError>>,
    },
    SetListenPort {
        port: u16,
        reply: oneshot::Sender<Result<(), SessionError>>,
    },
    SetDht {
        enabled: bool,
        port: u16,
        reply: oneshot::Sender<Result<(), SessionError>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

#[derive(Clone)]
pub struct Session {
    commands: mpsc::Sender<SessionCommand>,
    summaries: watch::Receiver<Vec<TorrentSummary>>,
    listener_status: watch::Receiver<ListenerStatus>,
    dht_status: watch::Receiver<DhtStatus>,
    restore_errors: Arc<Vec<String>>,
}

fn spawn_dht_status_forwarder(
    status: Option<watch::Receiver<DhtStatus>>,
    status_tx: watch::Sender<DhtStatus>,
    configured_port: u16,
    dht_active: Arc<AtomicBool>,
    dht_port: Arc<AtomicU16>,
) -> tokio::task::AbortHandle {
    let task = tokio::spawn(async move {
        let Some(mut status) = status else {
            dht_active.store(false, Ordering::Relaxed);
            let _ = status_tx.send(DhtStatus::inactive(configured_port, None));
            return;
        };
        loop {
            let snapshot = status.borrow().clone();
            dht_active.store(snapshot.active, Ordering::Relaxed);
            if snapshot.active {
                dht_port.store(snapshot.port, Ordering::Relaxed);
            }
            let _ = status_tx.send(snapshot);
            if status.changed().await.is_err() {
                break;
            }
        }
    });
    task.abort_handle()
}

fn initial_listener_status(port: u16) -> ListenerStatus {
    ListenerStatus {
        active: false,
        port,
        error: None,
    }
}

fn spawn_status_forwarder(
    listener: &Listener,
    status_tx: watch::Sender<ListenerStatus>,
    configured_port: u16,
    listen_active: Arc<AtomicBool>,
    announce_port: Arc<AtomicU16>,
) -> tokio::task::AbortHandle {
    let mut status = listener.status();
    let task = tokio::spawn(async move {
        loop {
            let snapshot = status.borrow().clone();
            listen_active.store(snapshot.active, Ordering::Relaxed);
            if snapshot.active {
                announce_port.store(snapshot.port, Ordering::Relaxed);
            } else {
                announce_port.store(configured_port, Ordering::Relaxed);
            }
            let _ = status_tx.send(snapshot);
            if status.changed().await.is_err() {
                break;
            }
        }
    });
    task.abort_handle()
}

impl Session {
    pub async fn spawn(persistence: Option<PathBuf>) -> Result<Session, SessionError> {
        Session::spawn_with_options(
            persistence,
            SessionOptions::new(listener::DEFAULT_LISTEN_PORT, 0),
        )
        .await
    }

    pub async fn spawn_with_dial(
        persistence: Option<PathBuf>,
        dial: Arc<dyn crate::engine::Dial>,
        bootstrap_peers: Vec<std::net::SocketAddr>,
    ) -> Result<Session, SessionError> {
        let options = SessionOptions {
            dial,
            bootstrap_peers,
            ..SessionOptions::new(listener::DEFAULT_LISTEN_PORT, 0)
        };
        Session::spawn_with_options(persistence, options).await
    }

    pub async fn spawn_with_options(
        persistence: Option<PathBuf>,
        options: SessionOptions,
    ) -> Result<Session, SessionError> {
        let registry = Arc::new(Registry::default());
        let (status_tx, listener_status) =
            watch::channel(initial_listener_status(options.listen_port));
        let listen_active = Arc::new(AtomicBool::new(false));
        let announce_port = Arc::new(AtomicU16::new(options.listen_port));
        let (dht_status_tx, dht_status) =
            watch::channel(DhtStatus::inactive(options.dht_port, None));
        let dht_persist_path = persistence.as_ref().map(|dir| dir.join("dht.json"));
        let (dht_node_id, dht_restore_nodes, dht_report) = match &dht_persist_path {
            Some(path) => match crate::dht::load_state(path) {
                Ok(loaded) => {
                    let report = (loaded.dropped > 0)
                        .then(|| format!("DHT state: {} invalid entries dropped", loaded.dropped));
                    (loaded.node_id, loaded.nodes, report)
                }
                Err(crate::dht::LoadError::Missing) => {
                    (NodeId::random(&SystemRandom), Vec::new(), None)
                }
                Err(crate::dht::LoadError::Corrupt(err)) => (
                    NodeId::random(&SystemRandom),
                    Vec::new(),
                    Some(format!("corrupt DHT state, starting fresh: {err}")),
                ),
            },
            None => (NodeId::random(&SystemRandom), Vec::new(), None),
        };
        let dht_active = Arc::new(AtomicBool::new(false));
        let dht_port = Arc::new(AtomicU16::new(options.dht_port));
        let (dht, dht_forwarder) = if options.dht_enabled {
            let handle = crate::dht::spawn(
                DhtOptions::new(options.dht_port, dht_node_id)
                    .with_bootstrap(options.dht_bootstrap.clone())
                    .with_persist(dht_persist_path.clone(), dht_restore_nodes)
                    .with_listener_state(listen_active.clone(), announce_port.clone()),
            );
            let forwarder = spawn_dht_status_forwarder(
                Some(handle.status()),
                dht_status_tx.clone(),
                options.dht_port,
                dht_active.clone(),
                dht_port.clone(),
            );
            (Some(handle), forwarder)
        } else {
            (
                None,
                spawn_dht_status_forwarder(
                    None,
                    dht_status_tx.clone(),
                    options.dht_port,
                    dht_active.clone(),
                    dht_port.clone(),
                ),
            )
        };
        let listener = listener::spawn(
            ListenerOptions {
                port: options.listen_port,
                our_peer_id: *peer_id::session(),
                dht_active: dht_active.clone(),
                ..ListenerOptions::default()
            },
            registry.clone(),
        );
        let listener_forwarder = spawn_status_forwarder(
            &listener,
            status_tx.clone(),
            options.listen_port,
            listen_active.clone(),
            announce_port.clone(),
        );
        let wiring = EngineWiring {
            listen_active,
            announce_port,
            uploads: Arc::new(UploadBucket::new(options.upload_limit_bps)),
            registry,
            choke_interval: options.choke_interval,
            optimistic_interval: options.optimistic_interval,
            dial: options.dial,
            bootstrap_peers: options.bootstrap_peers,
            peer_id: *peer_id::session(),
            dht: dht.as_ref().map(|handle| crate::engine::DhtIntegration {
                handle: handle.clone(),
                active: dht_active.clone(),
                port: dht_port.clone(),
            }),
            resume_dir: persistence.clone(),
        };
        let mut restore_errors = Vec::new();
        let mut restored = Vec::new();
        let mut pending_magnets: Vec<(String, PathBuf, bool)> = Vec::new();
        let persisted_file;
        if let Some(data_dir) = &persistence {
            std::fs::create_dir_all(data_dir)?;
            let (loaded, errors) = persist::load(data_dir);
            persisted_file = loaded;
            restore_errors.extend(errors);
            for entry in &persisted_file.torrents {
                if let Some(uri) = &entry.magnet {
                    pending_magnets.push((uri.clone(), entry.output_dir.clone(), entry.paused));
                }
            }
            for entry in persisted_file.torrents {
                if entry.magnet.is_some() {
                    continue;
                }
                let metainfo_path = data_dir.join(&entry.file);
                let restored_entry = match std::fs::read(&metainfo_path) {
                    Ok(bytes) => match MetaInfo::from_bytes(&bytes) {
                        Ok(meta) => {
                            let id = hex::encode(&meta.info_hash);
                            if id == entry.id {
                                Some((meta, entry.output_dir.clone(), entry.paused))
                            } else {
                                restore_errors.push(format!(
                                    "entry {} has a mismatching info hash",
                                    entry.id
                                ));
                                None
                            }
                        }
                        Err(err) => {
                            restore_errors.push(format!("corrupt metainfo {}: {err}", entry.file));
                            None
                        }
                    },
                    Err(err) => {
                        restore_errors.push(format!(
                            "unreadable metainfo {}: {err}",
                            metainfo_path.display()
                        ));
                        None
                    }
                };
                if let Some((meta, output_dir, paused)) = restored_entry {
                    restored.push((Arc::new(meta), output_dir, paused));
                }
            }
        }
        let (commands, command_rx) = mpsc::channel(32);
        let mut torrents = HashMap::new();
        let mut order = Vec::new();
        let mut initial = Vec::new();
        for (meta, output_dir, paused) in restored {
            let id = hex::encode(&meta.info_hash);
            if torrents.contains_key(&id) {
                restore_errors.push(format!("duplicate restored torrent {id}"));
                continue;
            }
            let entry = spawn_entry(meta, output_dir, paused, &wiring).await?;
            initial.push(make_summary(&id, &entry));
            order.push(id.clone());
            torrents.insert(id, entry);
        }
        for (uri, output_dir, paused) in pending_magnets {
            let Ok(link) = crate::magnet::parse(&uri) else {
                restore_errors.push(format!("bad magnet: {uri}"));
                continue;
            };
            let id = hex::encode(&link.info_hash);
            if torrents.contains_key(&id) {
                restore_errors.push(format!("duplicate restored magnet {id}"));
                continue;
            }
            let entry = spawn_magnet_entry(link, uri.clone(), output_dir, paused, &wiring).await?;
            initial.push(make_summary(&id, &entry));
            order.push(id.clone());
            torrents.insert(id, entry);
        }
        if let Some(report) = dht_report {
            restore_errors.push(report);
        }
        let restore_errors = Arc::new(restore_errors);
        let (summaries_tx, summaries_rx) = watch::channel(initial.clone());
        let actor = SessionActor {
            persistence,
            wiring,
            torrents,
            order,
            summaries_tx,
            last: initial,
            commands: command_rx,
            listener,
            status_tx,
            listener_forwarder,
            dht,
            dht_forwarder,
            dht_status_tx,
            dht_bootstrap: options.dht_bootstrap,
            dht_node_id,
            dht_active,
            dht_port,
            dht_persist_path,
        };
        tokio::spawn(actor.run());
        Ok(Session {
            commands,
            summaries: summaries_rx,
            listener_status,
            dht_status,
            restore_errors,
        })
    }

    pub fn listener_status(&self) -> watch::Receiver<ListenerStatus> {
        self.listener_status.clone()
    }

    pub fn dht_status(&self) -> watch::Receiver<DhtStatus> {
        self.dht_status.clone()
    }

    pub async fn set_dht(&self, enabled: bool, port: u16) -> Result<(), SessionError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(SessionCommand::SetDht {
                enabled,
                port,
                reply,
            })
            .await
            .map_err(|_| SessionError::Closed)?;
        rx.await.map_err(|_| SessionError::Closed)?
    }

    pub async fn set_upload_limit(&self, bps: u64) -> Result<(), SessionError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(SessionCommand::SetUploadLimit { bps, reply })
            .await
            .map_err(|_| SessionError::Closed)?;
        rx.await.map_err(|_| SessionError::Closed)?
    }

    pub async fn set_listen_port(&self, port: u16) -> Result<(), SessionError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(SessionCommand::SetListenPort { port, reply })
            .await
            .map_err(|_| SessionError::Closed)?;
        rx.await.map_err(|_| SessionError::Closed)?
    }

    pub fn restore_errors(&self) -> &[String] {
        &self.restore_errors
    }

    pub fn subscribe(&self) -> watch::Receiver<Vec<TorrentSummary>> {
        self.summaries.clone()
    }

    pub async fn list(&self) -> Vec<TorrentSummary> {
        self.summaries.borrow().clone()
    }

    pub async fn add_torrent(
        &self,
        bytes: &[u8],
        output_dir: PathBuf,
    ) -> Result<String, SessionError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(SessionCommand::Add {
                bytes: bytes.to_vec(),
                output_dir,
                paused: false,
                reply,
            })
            .await
            .map_err(|_| SessionError::Closed)?;
        rx.await.map_err(|_| SessionError::Closed)?
    }

    pub async fn add_magnet(&self, uri: &str, output_dir: PathBuf) -> Result<String, SessionError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(SessionCommand::AddMagnet {
                uri: uri.to_string(),
                output_dir,
                paused: false,
                reply,
            })
            .await
            .map_err(|_| SessionError::Closed)?;
        rx.await.map_err(|_| SessionError::Closed)?
    }

    pub async fn pause(&self, id: &str) -> Result<(), SessionError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(SessionCommand::Pause {
                id: id.to_string(),
                reply,
            })
            .await
            .map_err(|_| SessionError::Closed)?;
        rx.await.map_err(|_| SessionError::Closed)?
    }

    pub async fn resume(&self, id: &str) -> Result<(), SessionError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(SessionCommand::Resume {
                id: id.to_string(),
                reply,
            })
            .await
            .map_err(|_| SessionError::Closed)?;
        rx.await.map_err(|_| SessionError::Closed)?
    }

    pub async fn remove(&self, id: &str, delete_files: bool) -> Result<(), SessionError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(SessionCommand::Remove {
                id: id.to_string(),
                delete_files,
                reply,
            })
            .await
            .map_err(|_| SessionError::Closed)?;
        rx.await.map_err(|_| SessionError::Closed)?
    }

    pub async fn detail(&self, id: &str) -> Option<TorrentDetail> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(SessionCommand::Detail {
                id: id.to_string(),
                reply,
            })
            .await
            .ok()?;
        rx.await.ok().flatten()
    }

    pub async fn shutdown(&self) -> Result<(), SessionError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(SessionCommand::Shutdown { reply })
            .await
            .map_err(|_| SessionError::Closed)?;
        rx.await.map_err(|_| SessionError::Closed)
    }
}

struct SessionTorrent {
    handle: Torrent,
    meta: Arc<MetaInfo>,
    output_dir: PathBuf,
    paused: bool,
    magnet: Option<String>,
    stats: watch::Receiver<crate::engine::Stats>,
}

#[derive(Clone)]
struct EngineWiring {
    listen_active: Arc<AtomicBool>,
    announce_port: Arc<AtomicU16>,
    uploads: Arc<UploadBucket>,
    registry: Arc<Registry>,
    choke_interval: Duration,
    optimistic_interval: Duration,
    dial: Arc<dyn crate::engine::Dial>,
    bootstrap_peers: Vec<std::net::SocketAddr>,
    peer_id: [u8; 20],
    dht: Option<crate::engine::DhtIntegration>,
    resume_dir: Option<PathBuf>,
}

struct SessionActor {
    persistence: Option<PathBuf>,
    wiring: EngineWiring,
    torrents: HashMap<String, SessionTorrent>,
    order: Vec<String>,
    summaries_tx: watch::Sender<Vec<TorrentSummary>>,
    last: Vec<TorrentSummary>,
    commands: mpsc::Receiver<SessionCommand>,
    listener: Listener,
    status_tx: watch::Sender<ListenerStatus>,
    listener_forwarder: tokio::task::AbortHandle,
    dht: Option<DhtHandle>,
    dht_forwarder: tokio::task::AbortHandle,
    dht_status_tx: watch::Sender<DhtStatus>,
    dht_bootstrap: Vec<String>,
    dht_node_id: NodeId,
    dht_active: Arc<AtomicBool>,
    dht_port: Arc<AtomicU16>,
    dht_persist_path: Option<PathBuf>,
}

impl SessionActor {
    async fn rebind_listener(&mut self, port: u16) -> Result<(), String> {
        let listener = listener::bind(
            ListenerOptions {
                port,
                our_peer_id: self.wiring.peer_id,
                dht_active: self.dht_active.clone(),
                ..ListenerOptions::default()
            },
            self.wiring.registry.clone(),
        )
        .await?;
        self.listener.shutdown();
        self.listener_forwarder.abort();
        self.listener_forwarder = spawn_status_forwarder(
            &listener,
            self.status_tx.clone(),
            port,
            self.wiring.listen_active.clone(),
            self.wiring.announce_port.clone(),
        );
        self.listener = listener;
        Ok(())
    }

    async fn rebind_dht(&mut self, enabled: bool, port: u16) -> Result<(), SessionError> {
        if !enabled {
            if let Some(dht) = self.dht.take() {
                dht.shutdown();
            }
            self.dht_forwarder.abort();
            self.dht_forwarder = spawn_dht_status_forwarder(
                None,
                self.dht_status_tx.clone(),
                port,
                self.dht_active.clone(),
                self.dht_port.clone(),
            );
            self.wiring.dht = None;
            return Ok(());
        }
        self.dht_forwarder.abort();
        if let Some(old) = self.dht.take() {
            old.persist_and_shutdown().await;
        }
        let (node_id, restore_nodes) = match &self.dht_persist_path {
            Some(path) => match crate::dht::load_state(path) {
                Ok(loaded) => (loaded.node_id, loaded.nodes),
                Err(_) => (self.dht_node_id, Vec::new()),
            },
            None => (self.dht_node_id, Vec::new()),
        };
        let options = DhtOptions::new(port, node_id)
            .with_bootstrap(self.dht_bootstrap.clone())
            .with_persist(self.dht_persist_path.clone(), restore_nodes)
            .with_listener_state(
                self.wiring.listen_active.clone(),
                self.wiring.announce_port.clone(),
            );
        let handle = crate::dht::bind(options)
            .await
            .map_err(|err| SessionError::Dht(format!("cannot bind DHT port {port}: {err}")))?;
        self.dht_forwarder = spawn_dht_status_forwarder(
            Some(handle.status()),
            self.dht_status_tx.clone(),
            port,
            self.dht_active.clone(),
            self.dht_port.clone(),
        );
        self.wiring.dht = Some(crate::engine::DhtIntegration {
            handle: handle.clone(),
            active: self.dht_active.clone(),
            port: self.dht_port.clone(),
        });
        self.dht = Some(handle);
        Ok(())
    }
}

impl SessionActor {
    async fn run(mut self) {
        self.publish();
        let mut tick = tokio::time::interval(PUBLISH_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                command = self.commands.recv() => match command {
                    Some(command) => {
                        if self.handle(command).await {
                            return;
                        }
                    }
                    None => return,
                },
                _ = tick.tick() => self.publish(),
            }
        }
    }

    async fn handle(&mut self, command: SessionCommand) -> bool {
        match command {
            SessionCommand::Add {
                bytes,
                output_dir,
                paused,
                reply,
            } => {
                let result = self.add(bytes, output_dir, paused).await;
                let _ = reply.send(result);
            }
            SessionCommand::AddMagnet {
                uri,
                output_dir,
                paused,
                reply,
            } => {
                let result = self.add_magnet(&uri, output_dir, paused).await;
                let _ = reply.send(result);
            }
            SessionCommand::Pause { id, reply } => {
                let result = self.set_paused(&id, true).await;
                let _ = reply.send(result);
            }
            SessionCommand::Resume { id, reply } => {
                let result = self.set_paused(&id, false).await;
                let _ = reply.send(result);
            }
            SessionCommand::Remove {
                id,
                delete_files,
                reply,
            } => {
                let result = self.remove(&id, delete_files).await;
                let _ = reply.send(result);
            }
            SessionCommand::Detail { id, reply } => {
                let _ = reply.send(self.detail(&id));
            }
            SessionCommand::SetUploadLimit { bps, reply } => {
                self.wiring.uploads.set_limit(bps);
                let _ = reply.send(Ok(()));
            }
            SessionCommand::SetListenPort { port, reply } => {
                let result = self
                    .rebind_listener(port)
                    .await
                    .map_err(SessionError::Listen);
                let _ = reply.send(result);
            }
            SessionCommand::SetDht {
                enabled,
                port,
                reply,
            } => {
                let result = self.rebind_dht(enabled, port).await;
                let _ = reply.send(result);
            }
            SessionCommand::Shutdown { reply } => {
                self.shutdown().await;
                let _ = reply.send(());
                return true;
            }
        }
        false
    }

    async fn add(
        &mut self,
        bytes: Vec<u8>,
        output_dir: PathBuf,
        paused: bool,
    ) -> Result<String, SessionError> {
        let meta = Arc::new(MetaInfo::from_bytes(&bytes)?);
        let id = hex::encode(&meta.info_hash);
        if self.torrents.contains_key(&id) {
            return Err(SessionError::Duplicate(id));
        }
        let entry = spawn_entry(meta, output_dir, paused, &self.wiring).await?;
        if let Some(data_dir) = &self.persistence {
            persist::write_metainfo(data_dir, &id, &bytes)?;
        }
        self.order.push(id.clone());
        self.torrents.insert(id.clone(), entry);
        self.persist();
        self.publish();
        Ok(id)
    }

    async fn add_magnet(
        &mut self,
        uri: &str,
        output_dir: PathBuf,
        paused: bool,
    ) -> Result<String, SessionError> {
        let link = crate::magnet::parse(uri)?;
        let id = hex::encode(&link.info_hash);
        if self.torrents.contains_key(&id) {
            return Err(SessionError::Duplicate(id));
        }
        let entry = spawn_magnet_entry(
            link,
            uri.to_string(),
            output_dir.clone(),
            paused,
            &self.wiring,
        )
        .await?;
        self.order.push(id.clone());
        self.torrents.insert(id.clone(), entry);
        self.persist();
        self.publish();
        Ok(id)
    }

    async fn set_paused(&mut self, id: &str, paused: bool) -> Result<(), SessionError> {
        match self.torrents.get_mut(id) {
            Some(entry) => {
                if paused {
                    entry.handle.pause().await?;
                } else {
                    entry.handle.resume().await?;
                }
                entry.paused = paused;
            }
            None => return Err(SessionError::Unknown(id.to_string())),
        }
        self.persist();
        self.publish();
        Ok(())
    }

    async fn remove(&mut self, id: &str, delete_files: bool) -> Result<(), SessionError> {
        let entry = self
            .torrents
            .remove(id)
            .ok_or_else(|| SessionError::Unknown(id.to_string()))?;
        self.order.retain(|existing| existing != id);
        entry.handle.stop().await?;
        if delete_files {
            let meta = entry.meta.clone();
            let output_dir = entry.output_dir.clone();
            spawn_blocking(move || crate::engine::delete_torrent_files(&meta, &output_dir))
                .await
                .map_err(|_| SessionError::Closed)??;
        }
        if let Some(data_dir) = &self.persistence {
            let _ = persist::remove_metainfo(data_dir, id);
            let _ = crate::engine::remove_snapshot(data_dir, id);
        }
        self.persist();
        self.publish();
        Ok(())
    }

    fn detail(&self, id: &str) -> Option<TorrentDetail> {
        let entry = self.torrents.get(id)?;
        let stats = entry.stats.borrow().clone();
        let mut files = Vec::new();
        match &entry.meta.info.content {
            crate::metainfo::Content::Single { length } => {
                files.push(FileSummary {
                    path: entry.meta.info.name.clone(),
                    length: *length,
                });
            }
            crate::metainfo::Content::Multi { files: entries } => {
                for file in entries {
                    let mut path = PathBuf::new();
                    for segment in &file.path {
                        path.push(segment);
                    }
                    files.push(FileSummary {
                        path: path.to_string_lossy().into_owned(),
                        length: file.length,
                    });
                }
            }
        }
        Some(TorrentDetail {
            id: id.to_string(),
            info_hash: id.to_string(),
            peers: stats.peers,
            files,
            trackers: stats.trackers,
            comment: entry.meta.comment.clone(),
            output_dir: entry.output_dir.clone(),
            session_uploaded: stats.session_uploaded,
            upload_rate: stats.upload_rate,
            ratio: stats.ratio,
        })
    }

    async fn shutdown(&mut self) {
        let ids = self.order.clone();
        for id in &ids {
            if let Some(entry) = self.torrents.get(id) {
                let _ = entry.handle.stop().await;
            }
        }
        self.persist();
        self.listener.shutdown();
        if let Some(dht) = self.dht.take() {
            dht.persist_and_shutdown().await;
        }
        let _ = self.summaries_tx.send(Vec::new());
    }

    fn persist(&self) {
        if let Some(data_dir) = &self.persistence {
            let file = SessionFile {
                torrents: self
                    .order
                    .iter()
                    .filter_map(|id| {
                        self.torrents.get(id).map(|entry| PersistedTorrent {
                            id: id.clone(),
                            file: format!("{id}.torrent"),
                            output_dir: entry.output_dir.clone(),
                            paused: entry.paused,
                            magnet: entry.magnet.clone(),
                        })
                    })
                    .collect(),
            };
            let _ = persist::save(data_dir, &file);
        }
    }

    fn publish(&mut self) {
        let summaries: Vec<TorrentSummary> = self
            .order
            .iter()
            .filter_map(|id| self.torrents.get(id).map(|entry| make_summary(id, entry)))
            .collect();
        if summaries != self.last {
            self.last = summaries.clone();
            let _ = self.summaries_tx.send(summaries);
        }
    }
}

async fn spawn_magnet_entry(
    link: crate::magnet::MagnetLink,
    uri: String,
    output_dir: PathBuf,
    paused: bool,
    wiring: &EngineWiring,
) -> Result<SessionTorrent, SessionError> {
    let options = crate::engine::TorrentOptions {
        bootstrap_peers: wiring.bootstrap_peers.clone(),
        dial: wiring.dial.clone(),
        listen_active: wiring.listen_active.clone(),
        announce_port: wiring.announce_port.clone(),
        uploads: wiring.uploads.clone(),
        registry: wiring.registry.clone(),
        choke_interval: wiring.choke_interval,
        optimistic_interval: wiring.optimistic_interval,
        peer_id: wiring.peer_id,
        dht: wiring.dht.clone(),
        resume_dir: wiring.resume_dir.clone(),
    };
    let handle = Torrent::spawn_from_magnet(link, output_dir.clone(), options).await?;
    let stats = handle.subscribe();
    if paused {
        handle.pause().await?;
    }
    let mut info = std::collections::BTreeMap::new();
    info.insert(b"length".to_vec(), crate::bencode::Value::Int(1));
    info.insert(
        b"name".to_vec(),
        crate::bencode::Value::Bytes(b"a".to_vec()),
    );
    info.insert(b"piece length".to_vec(), crate::bencode::Value::Int(16384));
    info.insert(
        b"pieces".to_vec(),
        crate::bencode::Value::Bytes(vec![0u8; 20]),
    );
    let mut root = std::collections::BTreeMap::new();
    root.insert(b"info".to_vec(), crate::bencode::Value::Dict(info));
    let placeholder_bytes = crate::bencode::encode(&crate::bencode::Value::Dict(root));
    #[allow(clippy::expect_used)]
    let placeholder_meta =
        MetaInfo::from_bytes(&placeholder_bytes).expect("statically valid placeholder");
    Ok(SessionTorrent {
        handle,
        meta: Arc::new(placeholder_meta),
        output_dir,
        paused,
        magnet: Some(uri),
        stats,
    })
}

async fn spawn_entry(
    meta: Arc<MetaInfo>,
    output_dir: PathBuf,
    paused: bool,
    wiring: &EngineWiring,
) -> Result<SessionTorrent, SessionError> {
    let options = crate::engine::TorrentOptions {
        bootstrap_peers: wiring.bootstrap_peers.clone(),
        dial: wiring.dial.clone(),
        listen_active: wiring.listen_active.clone(),
        announce_port: wiring.announce_port.clone(),
        uploads: wiring.uploads.clone(),
        registry: wiring.registry.clone(),
        choke_interval: wiring.choke_interval,
        optimistic_interval: wiring.optimistic_interval,
        peer_id: wiring.peer_id,
        dht: wiring.dht.clone(),
        resume_dir: wiring.resume_dir.clone(),
    };
    let handle = Torrent::spawn_with_options((*meta).clone(), output_dir.clone(), options).await?;
    let stats = handle.subscribe();
    if paused {
        handle.pause().await?;
    }
    Ok(SessionTorrent {
        handle,
        meta,
        output_dir,
        paused,
        magnet: None,
        stats,
    })
}

fn make_summary(id: &str, entry: &SessionTorrent) -> TorrentSummary {
    let stats = entry.stats.borrow().clone();
    let progress = if stats.total_length > 0 {
        stats.verified_bytes as f64 / stats.total_length as f64
    } else {
        0.0
    };
    let remaining = stats.total_length.saturating_sub(stats.verified_bytes);
    let eta_seconds = if stats.download_rate > 1.0 && remaining > 0 {
        Some((remaining as f64 / stats.download_rate) as u64)
    } else {
        None
    };
    TorrentSummary {
        id: id.to_string(),
        name: stats.name,
        state: stats.state,
        total_length: stats.total_length,
        verified_bytes: stats.verified_bytes,
        progress,
        download_rate: stats.download_rate,
        session_uploaded: stats.session_uploaded,
        upload_rate: stats.upload_rate,
        ratio: stats.ratio,
        eta_seconds,
        peer_count: stats.peer_count,
        output_dir: entry.output_dir.clone(),
        error: stats.error,
        dht_waiting: stats.dht_waiting,
    }
}
