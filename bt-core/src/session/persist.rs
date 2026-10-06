use crate::engine::FilePriority;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct SessionFile {
    #[serde(default)]
    pub torrents: Vec<PersistedTorrent>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistedTorrent {
    pub id: String,
    pub file: String,
    pub output_dir: PathBuf,
    pub paused: bool,
    #[serde(default)]
    pub magnet: Option<String>,
    /// Sparse `(file index, priority)` pairs chosen for this torrent.
    #[serde(default)]
    pub file_priorities: Vec<(usize, FilePriority)>,
    #[serde(default)]
    pub pause_after_metadata: bool,
    /// Pause automatically once the download finishes (stop seeding).
    #[serde(default)]
    pub stop_after_complete: bool,
}

pub(crate) fn load(data_dir: &Path) -> (SessionFile, Vec<String>) {
    let mut errors = Vec::new();
    let path = data_dir.join("session.json");
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return (SessionFile::default(), errors);
        }
        Err(err) => {
            errors.push(format!("unreadable session.json: {err}"));
            return (SessionFile::default(), errors);
        }
    };
    match serde_json::from_str::<SessionFile>(&text) {
        Ok(file) => (file, errors),
        Err(err) => {
            errors.push(format!("corrupt session.json: {err}"));
            (SessionFile::default(), errors)
        }
    }
}

pub(crate) fn save(data_dir: &Path, file: &SessionFile) -> Result<(), std::io::Error> {
    let temp = data_dir.join("session.json.tmp");
    let text = serde_json::to_string_pretty(file).unwrap_or_default();
    fs::write(&temp, text)?;
    fs::rename(&temp, data_dir.join("session.json"))
}

pub(crate) fn write_metainfo(
    data_dir: &Path,
    id: &str,
    bytes: &[u8],
) -> Result<(), std::io::Error> {
    let temp = data_dir.join(format!("{id}.torrent.tmp"));
    fs::write(&temp, bytes)?;
    fs::rename(&temp, data_dir.join(format!("{id}.torrent")))
}

pub(crate) fn remove_metainfo(data_dir: &Path, id: &str) -> Result<(), std::io::Error> {
    fs::remove_file(data_dir.join(format!("{id}.torrent")))
}
