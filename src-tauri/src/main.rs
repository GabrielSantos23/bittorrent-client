#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod remote;
mod settings;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bt_core::dht::DhtStatus;
use bt_core::engine::{FilePriority, State};
use bt_core::listener::ListenerStatus;
use bt_core::session::{
    AddOptions, MagnetOptions, Session, SessionOptions, TorrentDetail, TorrentSummary,
};
use remote::RemoteStatus;
use settings::{default_settings, load_settings, save_settings, Settings};
use tauri::{AppHandle, Emitter, Manager};
use tokio::sync::watch;

const MAX_TORRENT_FILE_BYTES: u64 = 10 * 1024 * 1024;

pub struct AppState {
    session: Session,
    settings: Arc<Mutex<Settings>>,
    data_dir: PathBuf,
    selected_tx: watch::Sender<Option<String>>,
    summaries: watch::Receiver<Vec<TorrentSummary>>,
    remote: remote::RemoteHandle,
}

fn lock_settings(state: &AppState) -> std::sync::MutexGuard<'_, Settings> {
    match state.settings.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn read_torrent_file(path: &str) -> Result<Vec<u8>, String> {
    let metadata =
        std::fs::metadata(path).map_err(|err| format!("cannot access torrent file: {err}"))?;
    if !metadata.is_file() {
        return Err("torrent path is not a regular file".to_string());
    }
    if metadata.len() > MAX_TORRENT_FILE_BYTES {
        return Err("torrent file is larger than 10 MiB".to_string());
    }
    std::fs::read(path).map_err(|err| format!("cannot read torrent file: {err}"))
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, ts_rs::TS)]
#[ts(export)]
pub struct TorrentFileEntry {
    pub index: usize,
    pub path: Vec<String>,
    #[ts(type = "number")]
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, ts_rs::TS)]
#[ts(export)]
pub struct TorrentInspection {
    pub name: String,
    #[ts(type = "number")]
    pub total_length: u64,
    pub files: Vec<TorrentFileEntry>,
}

#[tauri::command]
async fn inspect_torrent(path: String) -> Result<TorrentInspection, String> {
    let bytes = read_torrent_file(&path)?;
    let meta = bt_core::metainfo::MetaInfo::from_bytes(&bytes)
        .map_err(|err| format!("invalid torrent file: {err}"))?;
    let total_length = meta
        .info
        .total_length()
        .map_err(|err| format!("invalid torrent file: {err}"))?;
    let files = match &meta.info.content {
        bt_core::metainfo::Content::Single { length } => vec![TorrentFileEntry {
            index: 0,
            path: vec![meta.info.name.clone()],
            length: *length,
        }],
        bt_core::metainfo::Content::Multi { files: entries } => entries
            .iter()
            .enumerate()
            .map(|(index, file)| TorrentFileEntry {
                index,
                path: file.path.clone(),
                length: file.length,
            })
            .collect(),
    };
    Ok(TorrentInspection {
        name: meta.info.name,
        total_length,
        files,
    })
}

#[tauri::command]
async fn add_torrent(
    state: tauri::State<'_, AppState>,
    path: String,
    paused: Option<bool>,
    file_priorities: Option<Vec<(u32, FilePriority)>>,
    skip_free_space_check: Option<bool>,
) -> Result<String, String> {
    let bytes = read_torrent_file(&path)?;
    bt_core::metainfo::MetaInfo::from_bytes(&bytes)
        .map_err(|err| format!("invalid torrent file: {err}"))?;
    let download_dir = lock_settings(&state).download_dir.clone();
    let options = AddOptions {
        paused: paused.unwrap_or(false),
        file_priorities: file_priorities
            .unwrap_or_default()
            .into_iter()
            .map(|(index, priority)| (index as usize, priority))
            .collect(),
        skip_free_space_check: skip_free_space_check.unwrap_or(false),
        stop_after_complete: false,
    };
    state
        .session
        .add_torrent(&bytes, download_dir, options)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn add_magnet(
    state: tauri::State<'_, AppState>,
    uri: String,
    paused: Option<bool>,
    pause_after_metadata: Option<bool>,
    skip_free_space_check: Option<bool>,
) -> Result<String, String> {
    let download_dir = lock_settings(&state).download_dir.clone();
    let options = MagnetOptions {
        paused: paused.unwrap_or(false),
        pause_after_metadata: pause_after_metadata.unwrap_or(false),
        file_priorities: Vec::new(),
        skip_free_space_check: skip_free_space_check.unwrap_or(false),
        stop_after_complete: false,
    };
    state
        .session
        .add_magnet(&uri, download_dir, options)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn set_file_priorities(
    state: tauri::State<'_, AppState>,
    id: String,
    priorities: Vec<(u32, FilePriority)>,
) -> Result<(), String> {
    let priorities = priorities
        .into_iter()
        .map(|(index, priority)| (index as usize, priority))
        .collect();
    state
        .session
        .set_file_priorities(&id, priorities)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn force_recheck(state: tauri::State<'_, AppState>, id: String) -> Result<(), String> {
    state
        .session
        .force_recheck(&id)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn list_torrents(state: tauri::State<'_, AppState>) -> Result<Vec<TorrentSummary>, String> {
    Ok(state.session.list().await)
}

fn listener_snapshot(session: &Session) -> ListenerStatus {
    session.listener_status().borrow().clone()
}

fn dht_snapshot(session: &Session) -> DhtStatus {
    session.dht_status().borrow().clone()
}

#[tauri::command]
async fn get_listener_status(state: tauri::State<'_, AppState>) -> Result<ListenerStatus, String> {
    Ok(listener_snapshot(&state.session))
}

#[tauri::command]
async fn get_dht_status(state: tauri::State<'_, AppState>) -> Result<DhtStatus, String> {
    Ok(dht_snapshot(&state.session))
}

#[tauri::command]
async fn pause_torrent(state: tauri::State<'_, AppState>, id: String) -> Result<(), String> {
    state
        .session
        .pause(&id)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn resume_torrent(state: tauri::State<'_, AppState>, id: String) -> Result<(), String> {
    state
        .session
        .resume(&id)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn remove_torrent(
    state: tauri::State<'_, AppState>,
    id: String,
    delete_files: bool,
) -> Result<(), String> {
    state
        .session
        .remove(&id, delete_files)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn select_torrent(
    state: tauri::State<'_, AppState>,
    id: Option<String>,
) -> Result<(), String> {
    state.selected_tx.send(id).map_err(|err| err.to_string())
}

#[tauri::command]
async fn get_settings(state: tauri::State<'_, AppState>) -> Result<Settings, String> {
    Ok(lock_settings(&state).clone())
}

#[tauri::command]
async fn set_settings(
    state: tauri::State<'_, AppState>,
    download_dir: String,
    listen_port: u16,
    upload_limit_bps: u64,
    dht_enabled: bool,
    dht_port: u16,
) -> Result<(), String> {
    let (remote_token, remote_port) = {
        let guard = lock_settings(&state);
        (guard.remote_token.clone(), guard.remote_port)
    };
    let settings = Settings {
        download_dir: PathBuf::from(download_dir),
        listen_port,
        upload_limit_bps,
        dht_enabled,
        dht_port,
        remote_token,
        remote_port,
    };
    let path = state.data_dir.join("settings.json");
    state
        .session
        .set_upload_limit(upload_limit_bps)
        .await
        .map_err(|err| err.to_string())?;
    state
        .session
        .set_listen_port(listen_port)
        .await
        .map_err(|err| err.to_string())?;
    state
        .session
        .set_dht(dht_enabled, dht_port)
        .await
        .map_err(|err| err.to_string())?;
    save_settings(&path, &settings).map_err(|err| err.to_string())?;
    *lock_settings(&state) = settings;
    Ok(())
}

#[tauri::command]
async fn open_output_dir(state: tauri::State<'_, AppState>, id: String) -> Result<(), String> {
    let detail = state
        .session
        .detail(&id)
        .await
        .ok_or_else(|| "unknown torrent".to_string())?;
    tauri_plugin_opener::open_path(detail.output_dir, None::<&str>).map_err(|err| err.to_string())
}

#[tauri::command]
async fn remote_start(state: tauri::State<'_, AppState>) -> Result<RemoteStatus, String> {
    state.remote.start().await
}

#[tauri::command]
async fn remote_stop(state: tauri::State<'_, AppState>) -> Result<RemoteStatus, String> {
    Ok(state.remote.stop().await)
}

#[tauri::command]
async fn remote_status(state: tauri::State<'_, AppState>) -> Result<RemoteStatus, String> {
    Ok(state.remote.status())
}

#[tauri::command]
async fn remote_refresh_token(state: tauri::State<'_, AppState>) -> Result<RemoteStatus, String> {
    state.remote.refresh_token().await
}

#[tauri::command]
async fn set_stop_after_complete(
    state: tauri::State<'_, AppState>,
    id: String,
    stop: bool,
) -> Result<(), String> {
    state
        .session
        .set_stop_after_complete(&id, stop)
        .await
        .map_err(|err| err.to_string())
}

fn spawn_summary_events(app: AppHandle, mut summaries: watch::Receiver<Vec<TorrentSummary>>) {
    tauri::async_runtime::spawn(async move {
        loop {
            if summaries.changed().await.is_err() {
                break;
            }
            let snapshot = summaries.borrow().clone();
            let _ = app.emit("session://summaries", &snapshot);
        }
    });
}

fn spawn_detail_events(
    app: AppHandle,
    session: Session,
    mut selected: watch::Receiver<Option<String>>,
) {
    tauri::async_runtime::spawn(async move {
        let mut last: Option<TorrentDetail> = None;
        loop {
            let current = selected.borrow().clone();
            let detail = match current {
                Some(id) => session.detail(&id).await,
                None => None,
            };
            if detail != last {
                let _ = app.emit("torrent://detail", &detail);
                last = detail;
            }
            tokio::select! {
                _ = selected.changed() => {}
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
        }
    });
}

fn spawn_listener_events(app: AppHandle, mut status: watch::Receiver<ListenerStatus>) {
    tauri::async_runtime::spawn(async move {
        loop {
            let snapshot = status.borrow().clone();
            let _ = app.emit("session://listener", &snapshot);
            if status.changed().await.is_err() {
                break;
            }
        }
    });
}

fn spawn_dht_events(app: AppHandle, mut status: watch::Receiver<DhtStatus>) {
    tauri::async_runtime::spawn(async move {
        loop {
            let snapshot = status.borrow().clone();
            let _ = app.emit("session://dht", &snapshot);
            if status.changed().await.is_err() {
                break;
            }
        }
    });
}

fn spawn_remote_events(app: AppHandle, mut status: watch::Receiver<RemoteStatus>) {
    tauri::async_runtime::spawn(async move {
        loop {
            let snapshot = status.borrow().clone();
            let _ = app.emit("remote://status", &snapshot);
            if status.changed().await.is_err() {
                break;
            }
        }
    });
}

fn build_tray(app: &tauri::App) -> Result<(), Box<dyn std::error::Error>> {
    let show = tauri::menu::MenuItem::with_id(app, "show", "Show", true, None::<&str>)?;
    let quit = tauri::menu::MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;
    let menu = tauri::menu::Menu::with_items(app, &[&show, &quit])?;
    let icon = app
        .default_window_icon()
        .cloned()
        .ok_or("missing default window icon")?;
    tauri::tray::TrayIconBuilder::with_id("main-tray")
        .icon(icon)
        .tooltip("BitTorrent Client")
        .menu(&menu)
        .on_menu_event(|app, event| match event.id().as_ref() {
            "show" => {
                if let Some(window) = app.get_webview_window("main") {
                    let _ = window.show();
                    let _ = window.set_focus();
                }
            }
            "quit" => {
                let app = app.clone();
                tauri::async_runtime::spawn(async move {
                    let _ = app.state::<AppState>().session.shutdown().await;
                    app.exit(0);
                });
            }
            _ => {}
        })
        .build(app)?;
    Ok(())
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_process::init())
        .setup(|app| {
            let data_dir = app.path().app_data_dir()?;
            std::fs::create_dir_all(&data_dir)?;
            let settings = load_settings(&data_dir.join("settings.json"))
                .unwrap_or_else(|| default_settings(&data_dir));
            std::fs::create_dir_all(&settings.download_dir)?;
            let session = tauri::async_runtime::block_on(Session::spawn_with_options(
                Some(data_dir.join("session")),
                SessionOptions::new(settings.listen_port, settings.upload_limit_bps)
                    .with_dht_bootstrap(
                        bt_core::dht::DEFAULT_BOOTSTRAP_ROUTERS
                            .iter()
                            .map(|host| host.to_string())
                            .collect(),
                    ),
            ))?;
            let summaries = session.subscribe();
            let (selected_tx, selected_rx) = watch::channel(None);
            let settings = Arc::new(Mutex::new(settings));
            let remote = remote::RemoteHandle::new(
                session.clone(),
                settings.clone(),
                data_dir.join("settings.json"),
            );
            app.manage(AppState {
                session: session.clone(),
                settings,
                data_dir,
                selected_tx,
                summaries: summaries.clone(),
                remote,
            });

            let handle = app.handle().clone();
            spawn_summary_events(handle, summaries);

            let handle = app.handle().clone();
            spawn_detail_events(handle, session.clone(), selected_rx);

            let handle = app.handle().clone();
            spawn_listener_events(handle, session.listener_status());

            let handle = app.handle().clone();
            spawn_dht_events(handle, session.dht_status());

            let handle = app.handle().clone();
            spawn_remote_events(handle, app.state::<AppState>().remote.subscribe());

            build_tray(app)?;
            Ok(())
        })
        .on_window_event(|window, event| {
            if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                let app = window.app_handle();
                let active = app
                    .state::<AppState>()
                    .summaries
                    .borrow()
                    .iter()
                    .any(|summary| matches!(summary.state, State::Checking | State::Downloading));
                if active {
                    api.prevent_close();
                    let _ = window.hide();
                }
            }
        })
        .invoke_handler(tauri::generate_handler![
            add_torrent,
            add_magnet,
            inspect_torrent,
            set_file_priorities,
            force_recheck,
            list_torrents,
            get_listener_status,
            get_dht_status,
            pause_torrent,
            resume_torrent,
            remove_torrent,
            select_torrent,
            get_settings,
            set_settings,
            open_output_dir,
            set_stop_after_complete,
            remote_start,
            remote_stop,
            remote_status,
            remote_refresh_token
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn wait_for(mut read: impl FnMut() -> Option<ListenerStatus>) -> ListenerStatus {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = read() {
                return status;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "listener status never settled"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    #[tokio::test]
    async fn listener_snapshot_reports_the_bound_port() {
        let session = Session::spawn_with_options(
            None,
            SessionOptions::new(0, 0).with_dht_bootstrap(Vec::new()),
        )
        .await
        .unwrap();
        let status = wait_for(|| {
            let snapshot = listener_snapshot(&session);
            snapshot.active.then_some(snapshot)
        })
        .await;
        assert!(status.port > 0);
        assert!(status.error.is_none());
    }

    #[tokio::test]
    async fn dht_snapshot_reports_the_running_service() {
        let session = Session::spawn_with_options(
            None,
            SessionOptions::new(0, 0).with_dht_bootstrap(Vec::new()),
        )
        .await
        .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let snapshot = dht_snapshot(&session);
            if snapshot.active {
                assert!(snapshot.port > 0);
                assert!(snapshot.error.is_none());
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "dht status never became active"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    #[tokio::test]
    async fn dht_snapshot_reports_inactive_when_disabled() {
        let options = SessionOptions {
            dht_enabled: false,
            ..SessionOptions::new(0, 0)
        };
        let session = Session::spawn_with_options(None, options).await.unwrap();
        let snapshot = dht_snapshot(&session);
        assert!(!snapshot.active);
        assert_eq!(snapshot.node_count, 0);
    }
}
