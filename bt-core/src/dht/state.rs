use std::net::SocketAddrV4;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::dht::krpc::NodeInfo;
use crate::dht::node_id::NodeId;
use crate::hex;

pub const MAX_PERSISTED_NODES: usize = 200;
pub const MAX_NODES_PER_IP: usize = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedNode {
    id: String,
    ip: [u8; 4],
    port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PersistedFile {
    version: u32,
    node_id: String,
    nodes: Vec<PersistedNode>,
}

const VERSION: u32 = 1;

#[derive(Debug, PartialEq, Eq)]
pub struct LoadedState {
    pub node_id: NodeId,
    pub nodes: Vec<NodeInfo>,
    pub dropped: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum LoadError {
    Missing,
    Corrupt(String),
}

pub fn save(path: &Path, node_id: &NodeId, nodes: &[NodeInfo]) -> std::io::Result<()> {
    let file = PersistedFile {
        version: VERSION,
        node_id: hex::encode(node_id.as_bytes()),
        nodes: nodes
            .iter()
            .map(|node| PersistedNode {
                id: hex::encode(node.id.as_bytes()),
                ip: node.addr.ip().octets(),
                port: node.addr.port(),
            })
            .collect(),
    };
    let bytes = serde_json::to_vec(&file)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    let temp = path.with_extension("json.tmp");
    std::fs::write(&temp, bytes)?;
    std::fs::rename(&temp, path)
}

pub fn load(path: &Path) -> Result<LoadedState, LoadError> {
    let bytes = std::fs::read(path).map_err(|_| LoadError::Missing)?;
    let file: PersistedFile =
        serde_json::from_slice(&bytes).map_err(|err| LoadError::Corrupt(err.to_string()))?;
    if file.version != VERSION {
        return Err(LoadError::Corrupt(format!(
            "unsupported version {}",
            file.version
        )));
    }
    let node_id =
        decode_fixed_20(&file.node_id).ok_or_else(|| LoadError::Corrupt("bad node id".into()))?;
    let mut nodes = Vec::new();
    let mut seen_ids = std::collections::HashSet::new();
    let mut per_ip: std::collections::HashMap<std::net::Ipv4Addr, usize> =
        std::collections::HashMap::new();
    let mut dropped = 0usize;
    for node in &file.nodes {
        if nodes.len() >= MAX_PERSISTED_NODES {
            dropped += 1;
            continue;
        }
        let Some(id) = decode_fixed_20(&node.id) else {
            dropped += 1;
            continue;
        };
        let addr = SocketAddrV4::new(node.ip.into(), node.port);
        if !seen_ids.insert(id) {
            dropped += 1;
            continue;
        }
        let count = per_ip.entry(*addr.ip()).or_insert(0);
        if *count >= MAX_NODES_PER_IP {
            dropped += 1;
            continue;
        }
        *count += 1;
        nodes.push(NodeInfo::new(id, addr));
    }
    Ok(LoadedState {
        node_id,
        nodes,
        dropped,
    })
}

fn decode_fixed_20(text: &str) -> Option<NodeId> {
    let bytes = hex::decode(text).ok()?;
    if bytes.len() != 20 {
        return None;
    }
    let mut id = [0u8; 20];
    id.copy_from_slice(&bytes);
    Some(NodeId::from_bytes(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn node(seed: u8, ip: [u8; 4], port: u16) -> NodeInfo {
        let mut id = [0u8; 20];
        id[0] = seed;
        NodeInfo::new(NodeId::from_bytes(id), SocketAddrV4::new(ip.into(), port))
    }

    fn written(tmp: &Path, node_id: &NodeId, nodes: &[NodeInfo]) -> std::path::PathBuf {
        let path = tmp.join("dht.json");
        save(&path, node_id, nodes).unwrap();
        path
    }

    #[test]
    fn round_trips_node_id_and_nodes() {
        let dir = tempfile_dir();
        let node_id = NodeId::from_bytes([7; 20]);
        let nodes = vec![node(1, [93, 1, 1, 1], 6881), node(2, [93, 1, 1, 2], 6882)];
        let path = written(&dir, &node_id, &nodes);
        let loaded = load(&path).unwrap();
        assert_eq!(loaded.node_id, node_id);
        assert_eq!(loaded.nodes, nodes);
        assert_eq!(loaded.dropped, 0);
        cleanup(&dir);
    }

    #[test]
    fn corrupt_file_is_an_error() {
        let dir = tempfile_dir();
        let path = dir.join("dht.json");
        std::fs::write(&path, b"{not json").unwrap();
        assert!(matches!(load(&path), Err(LoadError::Corrupt(_))));
        cleanup(&dir);
    }

    #[test]
    fn missing_file_is_missing_not_corrupt() {
        let dir = tempfile_dir();
        assert_eq!(load(&dir.join("dht.json")), Err(LoadError::Missing));
        cleanup(&dir);
    }

    #[test]
    fn duplicate_ids_and_unparseable_entries_are_dropped() {
        let dir = tempfile_dir();
        let node_id = NodeId::from_bytes([7; 20]);
        let good = node(3, [93, 1, 1, 9], 6881);
        let other = node(4, [93, 1, 1, 10], 6882);
        let nodes = vec![good, good, other];
        let path = written(&dir, &node_id, &nodes);
        let loaded = load(&path).unwrap();
        assert_eq!(loaded.nodes, vec![good, other]);
        assert_eq!(loaded.dropped, 1);
        cleanup(&dir);
    }

    #[test]
    fn more_than_two_nodes_per_ip_are_dropped() {
        let dir = tempfile_dir();
        let node_id = NodeId::from_bytes([7; 20]);
        let nodes = vec![
            node(1, [10, 0, 0, 1], 6881),
            node(2, [10, 0, 0, 1], 6882),
            node(3, [10, 0, 0, 1], 6883),
            node(4, [10, 0, 0, 2], 6884),
        ];
        let path = written(&dir, &node_id, &nodes);
        let loaded = load(&path).unwrap();
        assert_eq!(loaded.nodes.len(), 3);
        assert_eq!(loaded.dropped, 1);
        cleanup(&dir);
    }

    #[test]
    fn the_node_cap_bounds_what_survives() {
        let dir = tempfile_dir();
        let node_id = NodeId::from_bytes([7; 20]);
        let nodes: Vec<NodeInfo> = (0..MAX_PERSISTED_NODES + 50)
            .map(|index| {
                node(
                    (index % 250 + 1) as u8,
                    [93, (index / 250) as u8, 0, (index % 250 + 1) as u8],
                    6881,
                )
            })
            .collect();
        let path = written(&dir, &node_id, &nodes);
        let loaded = load(&path).unwrap();
        assert_eq!(loaded.nodes.len(), MAX_PERSISTED_NODES);
        cleanup(&dir);
    }

    #[test]
    fn atomic_write_leaves_no_temp_file() {
        let dir = tempfile_dir();
        let node_id = NodeId::from_bytes([7; 20]);
        let path = written(&dir, &node_id, &[node(1, [93, 1, 1, 1], 6881)]);
        save(&path, &node_id, &[]).unwrap();
        let loaded = load(&path).unwrap();
        assert!(loaded.nodes.is_empty());
        let entries: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries, vec!["dht.json".to_string()]);
        cleanup(&dir);
    }

    fn tempfile_dir() -> std::path::PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("bt-dht-state-{}-{}", std::process::id(), id));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn cleanup(dir: &Path) {
        std::fs::remove_dir_all(dir).unwrap();
    }
}
