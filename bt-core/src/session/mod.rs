mod persist;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::spawn_blocking;

use crate::engine::{PeerStats, State, Torrent};
use crate::listener::{self, Listener, ListenerOptions, ListenerStatus, Registry};
use crate::ratelimit::UploadBucket;

use self::persist::{PersistedTorrent, SessionFile};
use crate::error::SessionError;
use crate::hex;
use crate::metainfo::MetaInfo;

const PUBLISH_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Clone)]
pub struct SessionOptions {
    pub listen_port: u16,
    pub upload_limit_bps: u64,
    pub choke_interval: Duration,
    pub optimistic_interval: Duration,
    pub dial: Arc<dyn crate::engine::Dial>,
    pub bootstrap_peers: Vec<std::net::SocketAddr>,
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
        }
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
    pub trackers: Vec<String>,
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
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

#[derive(Clone)]
pub struct Session {
    commands: mpsc::Sender<SessionCommand>,
    summaries: watch::Receiver<Vec<TorrentSummary>>,
    listener_status: watch::Receiver<ListenerStatus>,
    restore_errors: Arc<Vec<String>>,
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
        let listener = listener::spawn(
            ListenerOptions {
                port: options.listen_port,
                ..ListenerOptions::default()
            },
            registry.clone(),
        );
        let listener_status = listener.status();
        let snapshot = listener_status.borrow().clone();
        let wiring = EngineWiring {
            listen_active: snapshot.active,
            announce_port: if snapshot.active {
                snapshot.port
            } else {
                options.listen_port
            },
            uploads: Arc::new(UploadBucket::new(options.upload_limit_bps)),
            registry,
            choke_interval: options.choke_interval,
            optimistic_interval: options.optimistic_interval,
            dial: options.dial,
            bootstrap_peers: options.bootstrap_peers,
        };
        let mut restore_errors = Vec::new();
        let mut restored = Vec::new();
        if let Some(data_dir) = &persistence {
            std::fs::create_dir_all(data_dir)?;
            let (file, errors) = persist::load(data_dir);
            restore_errors.extend(errors);
            for entry in file.torrents {
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
        };
        tokio::spawn(actor.run());
        Ok(Session {
            commands,
            summaries: summaries_rx,
            listener_status,
            restore_errors,
        })
    }

    pub fn listener_status(&self) -> watch::Receiver<ListenerStatus> {
        self.listener_status.clone()
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
    stats: watch::Receiver<crate::engine::Stats>,
}

#[derive(Clone)]
struct EngineWiring {
    listen_active: bool,
    announce_port: u16,
    uploads: Arc<UploadBucket>,
    registry: Arc<Registry>,
    choke_interval: Duration,
    optimistic_interval: Duration,
    dial: Arc<dyn crate::engine::Dial>,
    bootstrap_peers: Vec<std::net::SocketAddr>,
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
        let mut trackers = Vec::new();
        if let Some(announce) = &entry.meta.announce {
            trackers.push(announce.clone());
        }
        for tier in &entry.meta.announce_list {
            trackers.extend(tier.iter().cloned());
        }
        Some(TorrentDetail {
            id: id.to_string(),
            info_hash: id.to_string(),
            peers: stats.peers,
            files,
            trackers,
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

async fn spawn_entry(
    meta: Arc<MetaInfo>,
    output_dir: PathBuf,
    paused: bool,
    wiring: &EngineWiring,
) -> Result<SessionTorrent, SessionError> {
    let options = crate::engine::TorrentOptions {
        bootstrap_peers: wiring.bootstrap_peers.clone(),
        dial: wiring.dial.clone(),
        listen_active: wiring.listen_active,
        announce_port: wiring.announce_port,
        uploads: wiring.uploads.clone(),
        registry: wiring.registry.clone(),
        choke_interval: wiring.choke_interval,
        optimistic_interval: wiring.optimistic_interval,
        ..crate::engine::TorrentOptions::default()
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
    }
}
