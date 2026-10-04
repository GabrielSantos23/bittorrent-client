#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod settings;

use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use bt_core::engine::State;
use bt_core::listener::ListenerStatus;
use bt_core::session::{Session, SessionOptions, TorrentDetail, TorrentSummary};
use settings::{default_settings, load_settings, save_settings, Settings};
use tauri::{AppHandle, Emitter, Manager};
use tokio::sync::watch;

const MAX_TORRENT_FILE_BYTES: u64 = 10 * 1024 * 1024;

pub struct AppState {
    session: Session,
    settings: Mutex<Settings>,
    data_dir: PathBuf,
    selected_tx: watch::Sender<Option<String>>,
    summaries: watch::Receiver<Vec<TorrentSummary>>,
}

fn lock_settings(state: &AppState) -> std::sync::MutexGuard<'_, Settings> {
    match state.settings.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[tauri::command]
async fn add_torrent(state: tauri::State<'_, AppState>, path: String) -> Result<String, String> {
    let path = PathBuf::from(path);
    let metadata =
        std::fs::metadata(&path).map_err(|err| format!("cannot access torrent file: {err}"))?;
    if !metadata.is_file() {
        return Err("torrent path is not a regular file".to_string());
    }
    if metadata.len() > MAX_TORRENT_FILE_BYTES {
        return Err("torrent file is larger than 10 MiB".to_string());
    }
    let bytes = std::fs::read(&path).map_err(|err| format!("cannot read torrent file: {err}"))?;
    bt_core::metainfo::MetaInfo::from_bytes(&bytes)
        .map_err(|err| format!("invalid torrent file: {err}"))?;
    let download_dir = lock_settings(&state).download_dir.clone();
    state
        .session
        .add_torrent(&bytes, download_dir)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn add_magnet(state: tauri::State<'_, AppState>, uri: String) -> Result<String, String> {
    let download_dir = lock_settings(&state).download_dir.clone();
    state
        .session
        .add_magnet(&uri, download_dir)
        .await
        .map_err(|err| err.to_string())
}

#[tauri::command]
async fn list_torrents(state: tauri::State<'_, AppState>) -> Result<Vec<TorrentSummary>, String> {
    Ok(state.session.list().await)
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
    let settings = Settings {
        download_dir: PathBuf::from(download_dir),
        listen_port,
        upload_limit_bps,
        dht_enabled,
        dht_port,
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
            app.manage(AppState {
                session: session.clone(),
                settings: Mutex::new(settings),
                data_dir,
                selected_tx,
                summaries: summaries.clone(),
            });

            let handle = app.handle().clone();
            spawn_summary_events(handle, summaries);

            let handle = app.handle().clone();
            spawn_detail_events(handle, session.clone(), selected_rx);

            let handle = app.handle().clone();
            spawn_listener_events(handle, session.listener_status());

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
            list_torrents,
            pause_torrent,
            resume_torrent,
            remove_torrent,
            select_torrent,
            get_settings,
            set_settings,
            open_output_dir
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
