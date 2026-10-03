use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use ts_rs::TS;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct Settings {
    pub download_dir: PathBuf,
}

pub fn default_settings(data_dir: &Path) -> Settings {
    Settings {
        download_dir: data_dir.join("downloads"),
    }
}

pub fn load_settings(path: &Path) -> Option<Settings> {
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

pub fn save_settings(path: &Path, settings: &Settings) -> std::io::Result<()> {
    let temp = temp_sibling(path);
    let text = serde_json::to_string_pretty(settings)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    std::fs::write(&temp, text)?;
    std::fs::rename(&temp, path)
}

fn temp_sibling(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".tmp");
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn unique_dir(name: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "bt-app-settings-{}-{}-{}",
            name,
            std::process::id(),
            id
        ))
    }

    fn sample() -> Settings {
        Settings {
            download_dir: PathBuf::from("D:\\downloads"),
        }
    }

    #[test]
    fn round_trips_through_disk() {
        let dir = unique_dir("roundtrip");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("settings.json");
        save_settings(&path, &sample()).unwrap();
        assert_eq!(load_settings(&path), Some(sample()));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn missing_file_loads_to_none() {
        let dir = unique_dir("missing");
        std::fs::create_dir_all(&dir).unwrap();
        assert_eq!(load_settings(&dir.join("settings.json")), None);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn corrupt_file_loads_to_none() {
        let dir = unique_dir("corrupt");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("settings.json");
        std::fs::write(&path, "{not json at all").unwrap();
        assert_eq!(load_settings(&path), None);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn atomic_write_leaves_no_temp_file() {
        let dir = unique_dir("atomic");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("settings.json");
        save_settings(&path, &sample()).unwrap();
        let entries: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries, vec!["settings.json".to_string()]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn defaults_live_inside_the_data_dir() {
        let dir = unique_dir("defaults");
        let settings = default_settings(&dir);
        assert_eq!(settings.download_dir, dir.join("downloads"));
    }
}
