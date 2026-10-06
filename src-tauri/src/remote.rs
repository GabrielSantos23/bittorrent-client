use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{Html, IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::{watch, Mutex as AsyncMutex, Notify};
use ts_rs::TS;

use bt_core::engine::FilePriority;
use bt_core::session::{MagnetOptions, Session, TorrentDetail, TorrentSummary};

use crate::settings::{save_settings, Settings};

// The phone page is built from ui/remote.html + ui/src/remote/ with
// `bun run build:remote` (vite.config.remote.ts inlines JS/CSS/fonts into one
// self-contained file) and committed, so cargo builds never depend on the
// node toolchain. Rebuild after editing the React sources.
const MOBILE_PAGE: &str = include_str!("../ui/dist-remote/remote.html");
const TOKEN_LEN: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, TS)]
#[ts(export)]
pub struct RemoteStatus {
    pub running: bool,
    pub connected: bool,
    pub url: Option<String>,
    pub token: String,
    #[ts(type = "number")]
    pub port: u16,
}

#[derive(Clone)]
struct RemoteShared {
    token: Arc<Mutex<String>>,
    status_tx: watch::Sender<RemoteStatus>,
    session: Session,
    settings: Arc<Mutex<Settings>>,
}

fn lock<T>(guard: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    guard
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn build_url(ip: Ipv4Addr, port: u16, token: &str) -> String {
    format!("http://{ip}:{port}/?token={token}")
}

async fn require_token(
    State(shared): State<RemoteShared>,
    request: Request,
    next: Next,
) -> Response {
    let provided = request
        .uri()
        .query()
        .and_then(|query| {
            query.split('&').find_map(|pair| {
                let (key, value) = pair.split_once('=')?;
                (key == "token").then(|| value.to_string())
            })
        })
        .unwrap_or_default();
    let expected = lock(&shared.token).clone();
    if provided != expected {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "invalid or missing token" })),
        )
            .into_response();
    }
    mark_connected(&shared);
    next.run(request).await
}

fn mark_connected(shared: &RemoteShared) {
    let mut status = shared.status_tx.borrow().clone();
    if status.running && !status.connected {
        status.connected = true;
        let _ = shared.status_tx.send(status);
    }
}

async fn index_page() -> Html<&'static str> {
    Html(MOBILE_PAGE)
}

async fn api_status() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "app": "BitTorrent Client",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

async fn api_torrents(State(shared): State<RemoteShared>) -> Json<Vec<TorrentSummary>> {
    Json(shared.session.list().await)
}

/// Full detail for one torrent; the phone page uses `files` for the file
/// tree dialog.
async fn api_detail(
    State(shared): State<RemoteShared>,
    Path(id): Path<String>,
) -> Result<Json<TorrentDetail>, Response> {
    let detail = shared
        .session
        .detail(&id)
        .await
        .ok_or_else(|| "unknown torrent".to_string())
        .map_err(request_error)?;
    Ok(Json(detail))
}

fn request_error<E: std::fmt::Display>(err: E) -> Response {
    (StatusCode::BAD_REQUEST, err.to_string()).into_response()
}

async fn api_pause(
    State(shared): State<RemoteShared>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, Response> {
    shared.session.pause(&id).await.map_err(request_error)?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

async fn api_resume(
    State(shared): State<RemoteShared>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, Response> {
    shared.session.resume(&id).await.map_err(request_error)?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

#[derive(Deserialize)]
struct RemoveQuery {
    delete_files: Option<bool>,
}

async fn api_remove(
    State(shared): State<RemoteShared>,
    Path(id): Path<String>,
    Query(query): Query<RemoveQuery>,
) -> Result<Json<serde_json::Value>, Response> {
    shared
        .session
        .remove(&id, query.delete_files.unwrap_or(false))
        .await
        .map_err(request_error)?;
    Ok(Json(serde_json::json!({ "ok": true })))
}

#[derive(Deserialize)]
struct MagnetBody {
    uri: String,
}

/// Per-file download priorities sent by the phone's file tree (Skip excludes
/// a file from the download, like the desktop's add-dialog checkboxes).
#[derive(Deserialize)]
struct PrioritiesBody {
    priorities: Vec<(u32, FilePriority)>,
}

async fn api_priorities(
    State(shared): State<RemoteShared>,
    Path(id): Path<String>,
    Json(body): Json<PrioritiesBody>,
) -> Result<Json<serde_json::Value>, Response> {
    let priorities = body
        .priorities
        .into_iter()
        .map(|(index, priority)| (index as usize, priority))
        .collect();
    shared
        .session
        .set_file_priorities(&id, priorities)
        .await
        .map_err(request_error)?;
    Ok(Json(serde_json::json!({ "ok": true })))
}
async fn api_magnet(
    State(shared): State<RemoteShared>,
    Json(body): Json<MagnetBody>,
) -> Result<Json<serde_json::Value>, Response> {
    if !body.uri.starts_with("magnet:") {
        return Err((StatusCode::BAD_REQUEST, "not a magnet URI".to_string()).into_response());
    }
    let download_dir = lock(&shared.settings).download_dir.clone();
    let options = MagnetOptions {
        paused: false,
        pause_after_metadata: false,
        file_priorities: Vec::new(),
        skip_free_space_check: false,
        stop_after_complete: false,
    };
    let id = shared
        .session
        .add_magnet(&body.uri, download_dir, options)
        .await
        .map_err(request_error)?;
    Ok(Json(serde_json::json!({ "id": id })))
}

fn router(shared: RemoteShared) -> Router {
    Router::new()
        .route("/", get(index_page))
        .route("/api/status", get(api_status))
        .route("/api/torrents", get(api_torrents))
        .route("/api/torrents/{id}", get(api_detail))
        .route("/api/torrents/{id}/pause", post(api_pause))
        .route("/api/torrents/{id}/resume", post(api_resume))
        .route("/api/torrents/{id}/remove", post(api_remove))
        .route("/api/torrents/{id}/priorities", post(api_priorities))
        .route("/api/magnet", post(api_magnet))
        .layer(middleware::from_fn_with_state(
            shared.clone(),
            require_token,
        ))
        .with_state(shared)
}

struct RunningServer {
    task: tokio::task::JoinHandle<()>,
    shutdown: Arc<Notify>,
    ip: Ipv4Addr,
}

pub struct RemoteHandle {
    session: Session,
    settings: Arc<Mutex<Settings>>,
    settings_path: PathBuf,
    token: Arc<Mutex<String>>,
    status_tx: watch::Sender<RemoteStatus>,
    status_rx: watch::Receiver<RemoteStatus>,
    server: AsyncMutex<Option<RunningServer>>,
}

impl RemoteHandle {
    pub fn new(session: Session, settings: Arc<Mutex<Settings>>, settings_path: PathBuf) -> Self {
        let (remote_token, remote_port) = {
            let guard = lock(&settings);
            (guard.remote_token.clone(), guard.remote_port)
        };
        let (status_tx, status_rx) = watch::channel(RemoteStatus {
            running: false,
            connected: false,
            url: None,
            token: remote_token.clone(),
            port: remote_port,
        });
        Self {
            session,
            settings,
            settings_path,
            token: Arc::new(Mutex::new(remote_token)),
            status_tx,
            status_rx,
            server: AsyncMutex::new(None),
        }
    }

    pub fn status(&self) -> RemoteStatus {
        self.status_rx.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<RemoteStatus> {
        self.status_tx.subscribe()
    }

    pub async fn start(&self) -> Result<RemoteStatus, String> {
        let mut running = self.server.lock().await;
        let current = self.status();
        if current.running {
            return Ok(current);
        }
        if let Some(previous) = running.take() {
            previous.shutdown.notify_one();
            let _ = previous.task.await;
        }
        let token = self.ensure_token()?;
        let ip = lan_ip().ok_or_else(|| {
            "no LAN network connection found — connect to a Wi-Fi or ethernet network".to_string()
        })?;
        let configured_port = lock(&self.settings).remote_port;
        let listener = match TcpListener::bind((Ipv4Addr::UNSPECIFIED, configured_port)).await {
            Ok(listener) => listener,
            Err(_) => TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0))
                .await
                .map_err(|err| format!("cannot bind remote server: {err}"))?,
        };
        let port = listener
            .local_addr()
            .map_err(|err| format!("cannot read remote server port: {err}"))?
            .port();
        let shared = RemoteShared {
            token: self.token.clone(),
            status_tx: self.status_tx.clone(),
            session: self.session.clone(),
            settings: self.settings.clone(),
        };
        let shutdown = Arc::new(Notify::new());
        let serve_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, router(shared))
                .with_graceful_shutdown(async move { serve_shutdown.notified().await })
                .await;
        });
        *running = Some(RunningServer { task, shutdown, ip });
        let status = RemoteStatus {
            running: true,
            connected: false,
            url: Some(build_url(ip, port, &token)),
            token,
            port,
        };
        let _ = self.status_tx.send(status.clone());
        Ok(status)
    }

    pub async fn stop(&self) -> RemoteStatus {
        let mut running = self.server.lock().await;
        if let Some(server) = running.take() {
            server.shutdown.notify_one();
            let _ = server.task.await;
        }
        let mut status = self.status();
        status.running = false;
        status.connected = false;
        status.url = None;
        let _ = self.status_tx.send(status.clone());
        status
    }

    pub async fn refresh_token(&self) -> Result<RemoteStatus, String> {
        let token = generate_token();
        self.set_token(&token)?;
        let mut status = self.status();
        status.token = token.clone();
        let running = self.server.lock().await;
        if let Some(server) = running.as_ref() {
            status.url = Some(build_url(server.ip, status.port, &token));
            status.connected = false;
        }
        let _ = self.status_tx.send(status.clone());
        Ok(status)
    }

    fn ensure_token(&self) -> Result<String, String> {
        let current = lock(&self.token).clone();
        if !current.is_empty() {
            return Ok(current);
        }
        let token = generate_token();
        self.set_token(&token)?;
        Ok(token)
    }

    fn set_token(&self, token: &str) -> Result<(), String> {
        *lock(&self.token) = token.to_string();
        let snapshot = {
            let mut settings = lock(&self.settings);
            settings.remote_token = token.to_string();
            settings.clone()
        };
        save_settings(&self.settings_path, &snapshot).map_err(|err| err.to_string())
    }
}

fn lan_ip() -> Option<Ipv4Addr> {
    let addrs = if_addrs::get_if_addrs().ok()?;
    let candidates: Vec<IpAddr> = addrs.into_iter().map(|iface| iface.ip()).collect();
    pick_lan_ip(&candidates)
}

/// Picks the address a phone on the same network would reach: private
/// ranges (RFC1918) win over public ones, otherwise the first usable IPv4.
fn pick_lan_ip(candidates: &[IpAddr]) -> Option<Ipv4Addr> {
    let mut best: Option<(u8, Ipv4Addr)> = None;
    for candidate in candidates {
        let IpAddr::V4(ip) = candidate else {
            continue;
        };
        if ip.is_loopback()
            || ip.is_unspecified()
            || ip.is_link_local()
            || ip.is_broadcast()
            || ip.is_documentation()
        {
            continue;
        }
        let rank = if ip.is_private() { 2 } else { 1 };
        if best.is_none() || rank > best.unwrap().0 {
            best = Some((rank, *ip));
        }
    }
    best.map(|(_, ip)| ip)
}

fn generate_token() -> String {
    use rand::distr::{Alphanumeric, SampleString};
    Alphanumeric.sample_string(&mut rand::rng(), TOKEN_LEN)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(value: &str) -> IpAddr {
        value.parse().unwrap()
    }

    #[test]
    fn pick_lan_ip_prefers_private_ranges() {
        let candidates = vec![
            ip("8.8.8.8"),
            ip("10.0.0.2"),
            ip("169.254.1.1"),
            ip("192.168.1.20"),
        ];
        assert_eq!(pick_lan_ip(&candidates), Some(Ipv4Addr::new(10, 0, 0, 2)));
    }

    #[test]
    fn pick_lan_ip_skips_loopback_and_link_local() {
        let candidates = vec![ip("127.0.0.1"), ip("169.254.1.1"), ip("::1")];
        assert_eq!(pick_lan_ip(&candidates), None);
    }

    #[test]
    fn pick_lan_ip_falls_back_to_the_first_public_address() {
        let candidates = vec![ip("8.8.8.8"), ip("9.9.9.9")];
        assert_eq!(pick_lan_ip(&candidates), Some(Ipv4Addr::new(8, 8, 8, 8)));
    }

    #[test]
    fn tokens_are_long_unique_and_alphanumeric() {
        let first = generate_token();
        let second = generate_token();
        assert_eq!(first.len(), TOKEN_LEN);
        assert!(first.chars().all(|c| c.is_ascii_alphanumeric()));
        assert_ne!(first, second);
    }

    fn unique_dir(name: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "bt-app-remote-{}-{}-{}",
            name,
            std::process::id(),
            id
        ))
    }

    async fn test_handle(name: &str) -> (RemoteHandle, PathBuf) {
        let session = bt_core::session::Session::spawn_with_options(
            None,
            bt_core::session::SessionOptions::new(0, 0).with_dht_bootstrap(Vec::new()),
        )
        .await
        .unwrap();
        let dir = unique_dir(name);
        std::fs::create_dir_all(&dir).unwrap();
        let mut settings = crate::settings::default_settings(&dir);
        settings.remote_port = 0;
        let settings = Arc::new(Mutex::new(settings));
        (
            RemoteHandle::new(session, settings, dir.join("settings.json")),
            dir,
        )
    }

    /// Rewrites the advertised LAN URL to loopback so HTTP assertions stay
    /// hermetic regardless of which interface the machine advertised.
    fn loopback(url: &str) -> String {
        let rest = url.strip_prefix("http://").unwrap_or(url);
        let (authority, tail) = rest.split_once('/').unwrap_or((rest, ""));
        let port = authority.rsplit(':').next().unwrap_or("");
        let query = tail.split_once('?').map(|(_, q)| q).unwrap_or("");
        format!("http://127.0.0.1:{port}/?{query}")
    }

    #[tokio::test]
    async fn serves_page_and_rejects_bad_tokens() {
        let (handle, dir) = test_handle("http").await;
        let status = handle.start().await.unwrap();
        assert!(status.running);
        assert!(!status.connected);
        let url = loopback(&status.url.clone().unwrap());
        assert!(url.contains("token="));
        let base = url
            .split('?')
            .next()
            .unwrap()
            .trim_end_matches('/')
            .to_string();

        let client = reqwest::Client::new();
        let page = client.get(&url).send().await.unwrap();
        assert_eq!(page.status().as_u16(), 200);
        assert!(page.text().await.unwrap().contains("BitTorrent Client"));

        let rejected = client
            .get(format!("{base}/?token=not-the-token"))
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status().as_u16(), 401);

        let missing = client.get(&base).send().await.unwrap();
        assert_eq!(missing.status().as_u16(), 401);

        let torrents = client
            .get(format!("{}/api/torrents?token={}", base, status.token))
            .send()
            .await
            .unwrap();
        assert_eq!(torrents.status().as_u16(), 200);
        let body: serde_json::Value =
            serde_json::from_str(&torrents.text().await.unwrap()).unwrap();
        assert_eq!(body, serde_json::json!([]));

        let app_info = client
            .get(format!("{}/api/status?token={}", base, status.token))
            .send()
            .await
            .unwrap();
        assert_eq!(app_info.status().as_u16(), 200);

        let unknown_detail = client
            .get(format!(
                "{}/api/torrents/not-a-real-id?token={}",
                base, status.token
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(unknown_detail.status().as_u16(), 400);

        let unknown_priorities = client
            .post(format!(
                "{}/api/torrents/not-a-real-id/priorities?token={}",
                base, status.token
            ))
            .header("Content-Type", "application/json")
            .body(r#"{"priorities":[[0,"Skip"]]}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(unknown_priorities.status().as_u16(), 400);

        // the first authenticated request pairs the phone
        assert!(handle.status().connected);

        let persisted = crate::settings::load_settings(&dir.join("settings.json")).unwrap();
        assert_eq!(persisted.remote_token, status.token);

        handle.stop().await;
        assert!(!handle.status().running);
        assert!(client.get(&url).send().await.is_err());
    }

    #[tokio::test]
    async fn refreshing_the_token_invalidates_the_old_link() {
        let (handle, _dir) = test_handle("refresh").await;
        let status = handle.start().await.unwrap();
        let old_url = loopback(&status.url.clone().unwrap());
        let client = reqwest::Client::new();

        let refreshed = handle.refresh_token().await.unwrap();
        assert_ne!(refreshed.token, status.token);
        assert!(refreshed.running);
        assert!(!refreshed.connected);

        let old_link = client.get(&old_url).send().await.unwrap();
        assert_eq!(old_link.status().as_u16(), 401);
        let new_url = loopback(&refreshed.url.clone().unwrap());
        let new_link = client.get(&new_url).send().await.unwrap();
        assert_eq!(new_link.status().as_u16(), 200);

        handle.stop().await;
    }

    #[tokio::test]
    async fn start_is_idempotent_and_stop_without_start_is_harmless() {
        let (handle, _dir) = test_handle("lifecycle").await;
        assert!(!handle.status().running);
        handle.stop().await;

        let first = handle.start().await.unwrap();
        let second = handle.start().await.unwrap();
        assert_eq!(first.url, second.url);
        assert!(second.running);
        handle.stop().await;
        assert!(!handle.stop().await.running);
    }

    #[test]
    fn magnet_body_parsing_matches_the_mobile_page() {
        let payload = r#"{"uri":"magnet:?xt=urn:btih:abc"}"#;
        let parsed: MagnetBody = serde_json::from_str(payload).unwrap();
        assert_eq!(parsed.uri, "magnet:?xt=urn:btih:abc");
    }
}
