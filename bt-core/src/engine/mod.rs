use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use sha1::{Digest, Sha1};
use tokio::sync::{mpsc, watch};
use tokio::task::spawn_blocking;
use tokio::time::{sleep_until, Instant as TokioInstant, MissedTickBehavior};

use crate::error::{EngineError, StorageError};
use crate::metainfo::MetaInfo;
use crate::peer::{Bitfield, PeerConfig};
use crate::peer_id;
use crate::tracker::{self, AnnounceRequest, Event};

use self::assembly::{BlockOutcome, PieceAssembler};
use self::peer_task::{PeerCommand, PeerEvent};
use self::picker::PiecePicker;
use self::storage::Storage;

mod assembly;
mod peer_task;
mod picker;
mod storage;

pub use peer_task::{BoxedStream, Dial, TcpDial};
pub use storage::delete_torrent_files;

pub const BLOCK_SIZE: usize = assembly::BLOCK_SIZE;
const MAX_PEERS: usize = 50;
const REFILL_LIMIT: usize = 8;
const RANDOM_FIRST: usize = 4;
const MAX_ACTIVE_PIECES: usize = 25;
const STATS_INTERVAL: Duration = Duration::from_millis(500);
const CONNECT_INTERVAL: Duration = Duration::from_millis(500);
const ANNOUNCE_RETRY: Duration = Duration::from_secs(15);
const CONNECT_BACKOFF: Duration = Duration::from_secs(30);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const REAP_INTERVAL: Duration = Duration::from_secs(1);
const BAN_STRIKES: u32 = 3;
const DEFAULT_PORT: u16 = 6881;
const NUMWANT: u32 = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, ts_rs::TS)]
#[ts(export)]
pub enum State {
    Checking,
    Downloading,
    Paused,
    Completed,
    Stopped,
    Error,
}

#[derive(Debug, Clone, PartialEq, Serialize, ts_rs::TS)]
#[ts(export)]
pub struct PeerStats {
    pub addr: SocketAddr,
    pub client: String,
    pub rate: f64,
    pub choked: bool,
}

#[derive(Debug, Clone)]
pub struct Stats {
    pub state: State,
    pub name: String,
    pub total_length: u64,
    pub verified_bytes: u64,
    pub session_downloaded: u64,
    pub session_uploaded: u64,
    pub verified_pieces: usize,
    pub piece_count: usize,
    pub download_rate: f64,
    pub peer_count: usize,
    pub peers: Vec<PeerStats>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub enum EngineCommand {
    Start,
    Pause,
    Resume,
    Stop,
}

pub struct Torrent {
    commands: mpsc::Sender<EngineCommand>,
    stats: watch::Receiver<Stats>,
}

impl Torrent {
    pub async fn spawn(meta: MetaInfo, output_dir: PathBuf) -> Result<Torrent, EngineError> {
        let connect_timeout = PeerConfig::default().connect_timeout;
        Torrent::spawn_with_dial(
            meta,
            output_dir,
            Vec::new(),
            Arc::new(TcpDial::new(connect_timeout)),
        )
        .await
    }

    pub async fn spawn_with_dial(
        meta: MetaInfo,
        output_dir: PathBuf,
        bootstrap_peers: Vec<SocketAddr>,
        dial: Arc<dyn Dial>,
    ) -> Result<Torrent, EngineError> {
        let meta = Arc::new(meta);
        let storage = spawn_blocking({
            let meta = meta.clone();
            let output_dir = output_dir.clone();
            move || Storage::create(&meta, &output_dir)
        })
        .await
        .map_err(|_| EngineError::Task)??;
        let storage = Arc::new(storage);
        let http = tracker::http_client()?;
        let (commands, command_rx) = mpsc::channel(16);
        let (events_tx, events) = mpsc::channel(1024);
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
            verified_pieces: 0,
            piece_count,
            download_rate: 0.0,
            peer_count: 0,
            peers: Vec::new(),
            error: None,
        });
        let engine = Engine::new(
            meta,
            storage,
            bootstrap_peers,
            dial,
            http,
            stats_tx,
            command_rx,
            events_tx,
            events,
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
    commands: mpsc::Sender<PeerCommand>,
    bitfield: Option<Bitfield>,
    choked: bool,
    in_flight: usize,
    received_bytes: u64,
    rate_base: u64,
}

struct Engine {
    meta: Arc<MetaInfo>,
    storage: Arc<Storage>,
    dial: Arc<dyn Dial>,
    http: reqwest::Client,
    stats_tx: watch::Sender<Stats>,
    commands: mpsc::Receiver<EngineCommand>,
    events_tx: mpsc::Sender<PeerEvent>,
    events: mpsc::Receiver<PeerEvent>,
    state: State,
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
    last_rate: (TokioInstant, u64),
    announce_at: Option<TokioInstant>,
    announce_event: Option<Event>,
    pending_pause: bool,
}

impl Engine {
    #[allow(clippy::too_many_arguments)]
    fn new(
        meta: Arc<MetaInfo>,
        storage: Arc<Storage>,
        bootstrap_peers: Vec<SocketAddr>,
        dial: Arc<dyn Dial>,
        http: reqwest::Client,
        stats_tx: watch::Sender<Stats>,
        commands: mpsc::Receiver<EngineCommand>,
        events_tx: mpsc::Sender<PeerEvent>,
        events: mpsc::Receiver<PeerEvent>,
    ) -> Engine {
        let piece_count = meta.info.pieces.len();
        let total_length = storage.total_length();
        let mut queue = VecDeque::new();
        let mut known = HashSet::new();
        for addr in bootstrap_peers {
            known.insert(addr);
            queue.push_back(addr);
        }
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
            stats_tx,
            commands,
            events_tx,
            events,
            state: State::Checking,
            total_length,
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
            last_rate: (TokioInstant::now(), 0),
            announce_at: None,
            announce_event: None,
            pending_pause: false,
        }
    }

    async fn run(mut self) {
        let mut stats_tick = tokio::time::interval(STATS_INTERVAL);
        let mut reap_tick = tokio::time::interval(REAP_INTERVAL);
        let mut connect_tick = tokio::time::interval(CONNECT_INTERVAL);
        for tick in [&mut stats_tick, &mut reap_tick, &mut connect_tick] {
            tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        }
        self.run_check().await;
        loop {
            let announce_at = self.announce_at;
            let announce_tick = async move {
                match announce_at {
                    Some(at) => sleep_until(at).await,
                    None => std::future::pending::<()>().await,
                }
            };
            tokio::select! {
                command = self.commands.recv() => match command {
                    Some(command) => self.handle_command(command).await,
                    None => return,
                },
                event = self.events.recv() => match event {
                    Some(event) => self.handle_event(event).await,
                    None => return,
                },
                _ = announce_tick => self.run_announce().await,
                _ = reap_tick.tick() => self.reap_stale_requests().await,
                _ = stats_tick.tick() => self.tick_stats(),
                _ = connect_tick.tick() => self.try_connect(),
            }
        }
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
                    self.state = State::Completed;
                    self.announce_event = Some(Event::Completed);
                    self.announce_at = Some(TokioInstant::now() + Duration::from_secs(1));
                } else {
                    self.state = State::Downloading;
                    self.announce_event = Some(Event::Started);
                    self.announce_at = Some(TokioInstant::now());
                }
                if self.pending_pause {
                    self.pending_pause = false;
                    self.announce_at = None;
                    self.announce_event = None;
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

    async fn pause(&mut self) {
        match self.state {
            State::Checking => self.pending_pause = true,
            State::Downloading | State::Completed => {
                self.disconnect_all().await;
                self.backoff.clear();
                self.announce_at = None;
                self.state = State::Paused;
                self.publish();
            }
            _ => {}
        }
    }

    fn resume(&mut self) {
        if self.state == State::Paused {
            self.state = if self.picker.is_complete() {
                State::Completed
            } else {
                State::Downloading
            };
            self.announce_event = Some(Event::Started);
            self.announce_at = Some(TokioInstant::now());
            self.publish();
        }
    }

    async fn stop(&mut self) {
        self.disconnect_all().await;
        self.announce_at = None;
        let request = AnnounceRequest {
            info_hash: self.meta.info_hash,
            peer_id: *peer_id::session(),
            port: DEFAULT_PORT,
            uploaded: 0,
            downloaded: self.session_downloaded,
            left: self.total_length - self.verified_bytes,
            numwant: 0,
            event: Some(Event::Stopped),
        };
        let _ = tracker::announce(&self.http, &self.meta, &request).await;
        self.state = State::Stopped;
        self.publish();
    }

    async fn run_announce(&mut self) {
        if !matches!(self.state, State::Downloading | State::Completed) {
            self.announce_at = None;
            return;
        }
        let event = self.announce_event.take();
        let request = AnnounceRequest {
            info_hash: self.meta.info_hash,
            peer_id: *peer_id::session(),
            port: DEFAULT_PORT,
            uploaded: 0,
            downloaded: self.session_downloaded,
            left: self.total_length - self.verified_bytes,
            numwant: NUMWANT,
            event,
        };
        match tracker::announce(&self.http, &self.meta, &request).await {
            Ok(outcome) => {
                for addr in outcome.response.peers {
                    self.push_peer(addr);
                }
                let wait = outcome
                    .response
                    .interval
                    .max(outcome.response.min_interval.unwrap_or(0));
                self.announce_at = Some(TokioInstant::now() + Duration::from_secs(wait));
            }
            Err(_) => {
                self.announce_at = Some(TokioInstant::now() + ANNOUNCE_RETRY);
            }
        }
        self.publish();
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
        if self.state != State::Downloading {
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
                commands,
                bitfield: None,
                choked: true,
                in_flight: 0,
                received_bytes: 0,
                rate_base: 0,
            },
        );
        tokio::spawn(peer_task::run_peer_task(
            peer_task::PeerTask {
                addr,
                info_hash: self.meta.info_hash,
                our_peer_id: *peer_id::session(),
                piece_count: self.meta.info.pieces.len(),
                config: PeerConfig::default(),
                dial: self.dial.clone(),
            },
            command_rx,
            self.events_tx.clone(),
        ));
    }

    async fn handle_event(&mut self, event: PeerEvent) {
        match event {
            PeerEvent::Handshaken { addr, peer_id } => {
                if let Some(handle) = self.peers.get_mut(&addr) {
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
            PeerEvent::Block {
                addr,
                index,
                begin,
                block,
            } => self.handle_block(addr, index, begin, block).await,
            PeerEvent::Disconnected { addr } => {
                if let Some(handle) = self.peers.remove(&addr) {
                    if let Some(bitfield) = &handle.bitfield {
                        self.picker.remove_peer(bitfield);
                    }
                    self.picker.return_blocks(addr);
                }
                if self.state == State::Downloading {
                    self.backoff
                        .insert(addr, TokioInstant::now() + CONNECT_BACKOFF);
                }
                self.deferred.remove(&addr);
                self.known.remove(&addr);
                self.push_peer(addr);
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
                }
                self.session_downloaded += block.len() as u64;
                self.picker.block_received(index, begin, addr);
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
                self.verified_bytes += self.piece_size(index) as u64;
                if self.picker.is_complete() {
                    self.state = State::Completed;
                    self.announce_event = Some(Event::Completed);
                    self.announce_at = Some(TokioInstant::now() + Duration::from_secs(1));
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
            let bitfield = match self.peers.get(&addr) {
                Some(handle) => match handle.bitfield.as_ref() {
                    Some(bitfield) if !handle.choked && handle.in_flight < REFILL_LIMIT => bitfield,
                    _ => return,
                },
                None => return,
            };
            let Some((index, begin, length)) = self.picker.next_block(addr, bitfield) else {
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
        for addr in self.peers.keys().copied().collect::<Vec<_>>() {
            self.picker.return_blocks(addr);
            if let Some(bitfield) = self
                .peers
                .get(&addr)
                .and_then(|handle| handle.bitfield.as_ref())
            {
                self.picker.remove_peer(bitfield);
            }
            if let Some(handle) = self.peers.remove(&addr) {
                let _ = handle.commands.try_send(PeerCommand::Stop);
            }
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

    fn piece_size(&self, index: usize) -> usize {
        self.storage.piece_size(index)
    }

    fn tick_stats(&mut self) {
        for handle in self.peers.values_mut() {
            handle.rate_base = handle.received_bytes;
        }
        self.last_rate = (TokioInstant::now(), self.session_downloaded);
        self.publish();
    }

    fn current_stats(&self) -> Stats {
        let now = TokioInstant::now();
        let dt = (now - self.last_rate.0).as_secs_f64().max(0.001);
        let peers = self
            .peers
            .iter()
            .map(|(addr, handle)| PeerStats {
                addr: *addr,
                client: handle.client.clone(),
                rate: (handle.received_bytes - handle.rate_base) as f64 / dt,
                choked: handle.choked,
            })
            .collect();
        Stats {
            state: self.state,
            name: self.meta.info.name.clone(),
            total_length: self.storage.total_length(),
            verified_bytes: self.verified_bytes,
            session_downloaded: self.session_downloaded,
            session_uploaded: self.session_uploaded,
            verified_pieces: self.picker.have().count(),
            piece_count: self.meta.info.pieces.len(),
            download_rate: (self.session_downloaded - self.last_rate.1) as f64 / dt,
            peer_count: self.peers.len(),
            peers,
            error: self.error.clone(),
        }
    }

    fn publish(&self) {
        let _ = self.stats_tx.send(self.current_stats());
    }
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
}
