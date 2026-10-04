use std::collections::{HashMap, VecDeque};
use std::net::SocketAddrV4;

pub const PEER_TTL_MS: u64 = 30 * 60 * 1000;
pub const MAX_PEERS_PER_INFO_HASH: usize = 100;
pub const MAX_INFO_HASHES: usize = 2048;
pub const MAX_TOTAL_PEERS: usize = 8192;

#[derive(Default)]
pub struct PeerStore {
    hashes: HashMap<[u8; 20], HashMap<SocketAddrV4, u64>>,
    order: VecDeque<[u8; 20]>,
    total: usize,
}

impl PeerStore {
    pub fn insert(&mut self, info_hash: [u8; 20], addr: SocketAddrV4, now_ms: u64) -> bool {
        self.expire_hash(&info_hash, now_ms);
        self.evict_for_room();
        let hash = self.hashes.entry(info_hash).or_default();
        let was_empty = hash.is_empty();
        let is_new = hash.insert(addr, now_ms).is_none();
        if is_new {
            self.total = self.total.saturating_add(1);
        }
        if hash.len() > MAX_PEERS_PER_INFO_HASH {
            if let Some(oldest) = hash
                .iter()
                .min_by_key(|(_, seen)| **seen)
                .map(|(entry, _)| *entry)
            {
                hash.remove(&oldest);
                self.total = self.total.saturating_sub(1);
            }
        }
        if was_empty {
            self.order.push_back(info_hash);
        } else if !is_new {
            self.order.retain(|seen| *seen != info_hash);
            self.order.push_back(info_hash);
        }
        is_new
    }

    pub fn peers(&mut self, info_hash: &[u8; 20], now_ms: u64) -> Vec<SocketAddrV4> {
        self.expire_hash(info_hash, now_ms);
        let mut entries: Vec<(SocketAddrV4, u64)> = match self.hashes.get(info_hash) {
            Some(hash) => hash.iter().map(|(addr, seen)| (*addr, *seen)).collect(),
            None => return Vec::new(),
        };
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        self.order.retain(|seen| *seen != *info_hash);
        self.order.push_back(*info_hash);
        entries.into_iter().map(|(addr, _)| addr).collect()
    }

    pub fn expire(&mut self, now_ms: u64) -> usize {
        let mut expired = 0usize;
        for info_hash in self.hashes.keys().copied().collect::<Vec<_>>() {
            expired += self.expire_hash(&info_hash, now_ms);
        }
        expired
    }

    #[cfg(test)]
    pub fn total(&self) -> usize {
        self.total
    }

    pub fn info_hash_count(&self) -> usize {
        self.hashes.len()
    }

    fn expire_hash(&mut self, info_hash: &[u8; 20], now_ms: u64) -> usize {
        let Some(hash) = self.hashes.get_mut(info_hash) else {
            return 0;
        };
        let before = hash.len();
        hash.retain(|_, seen| now_ms.saturating_sub(*seen) < PEER_TTL_MS);
        let removed = before - hash.len();
        if hash.is_empty() {
            self.hashes.remove(info_hash);
        }
        self.total = self.total.saturating_sub(removed);
        if removed > 0 && self.hashes.contains_key(info_hash) {
            self.order.retain(|seen| seen != info_hash);
            self.order.push_back(*info_hash);
        }
        removed
    }

    fn evict_for_room(&mut self) {
        while self.info_hash_count() >= MAX_INFO_HASHES || self.total >= MAX_TOTAL_PEERS {
            let Some(victim) = self.order.pop_front() else {
                return;
            };
            if let Some(hash) = self.hashes.remove(&victim) {
                self.total = self.total.saturating_sub(hash.len());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn addr(last: u8, port: u16) -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, last), port)
    }

    fn hash16(seed: u16) -> [u8; 20] {
        let mut id = [0u8; 20];
        id[0] = (seed >> 8) as u8;
        id[1] = seed as u8;
        id
    }

    #[test]
    fn inserted_peers_come_back_newest_first() {
        let mut store = PeerStore::default();
        assert!(store.insert([1; 20], addr(1, 6881), 100));
        assert!(store.insert([1; 20], addr(2, 6882), 200));
        assert!(store.insert([1; 20], addr(3, 6883), 300));
        assert_eq!(
            store.peers(&[1; 20], 400),
            vec![addr(3, 6883), addr(2, 6882), addr(1, 6881)]
        );
        assert!(!store.insert([1; 20], addr(3, 6883), 350));
        assert_eq!(store.total(), 3);
    }

    #[test]
    fn peers_expire_after_thirty_minutes() {
        let mut store = PeerStore::default();
        store.insert([1; 20], addr(1, 6881), 100);
        assert_eq!(store.peers(&[1; 20], 100 + PEER_TTL_MS - 1).len(), 1);
        assert_eq!(store.peers(&[1; 20], 100 + PEER_TTL_MS).len(), 0);
        assert_eq!(store.total(), 0);
        assert!(store.insert([1; 20], addr(2, 6882), 100 + PEER_TTL_MS + 1));
        assert_eq!(store.total(), 1);
    }

    #[test]
    fn expire_sweeps_every_info_hash() {
        let mut store = PeerStore::default();
        for seed in 0..5u16 {
            store.insert(hash16(seed), addr(seed as u8, 6881), 0);
        }
        assert_eq!(store.expire(PEER_TTL_MS), 5);
        assert_eq!(store.info_hash_count(), 0);
        assert_eq!(store.expire(PEER_TTL_MS + 1), 0);
    }

    #[test]
    fn a_renewed_announce_slides_the_expiry() {
        let mut store = PeerStore::default();
        store.insert([1; 20], addr(1, 6881), 0);
        store.insert([1; 20], addr(1, 6881), PEER_TTL_MS - 1_000);
        assert_eq!(
            store.peers(&[1; 20], PEER_TTL_MS + 500).len(),
            1,
            "a renewed announce keeps the peer alive for another interval"
        );
    }

    #[test]
    fn per_info_hash_cap_evicts_the_oldest_entry() {
        let mut store = PeerStore::default();
        for index in 0..MAX_PEERS_PER_INFO_HASH as u16 {
            assert!(store.insert([1; 20], addr(index as u8, 6881 + index), index as u64));
        }
        assert_eq!(store.total(), MAX_PEERS_PER_INFO_HASH);
        assert!(store.insert([1; 20], addr(200, 7000), MAX_PEERS_PER_INFO_HASH as u64));
        assert_eq!(store.total(), MAX_PEERS_PER_INFO_HASH);
        let peers = store.peers(&[1; 20], MAX_PEERS_PER_INFO_HASH as u64 + 1);
        assert_eq!(peers.len(), MAX_PEERS_PER_INFO_HASH);
        assert!(
            !peers.contains(&addr(0, 6881)),
            "the oldest peer must be evicted"
        );
        assert!(peers.contains(&addr(200, 7000)));
    }

    #[test]
    fn the_info_hash_cap_evicts_the_least_recently_used_hash() {
        let mut store = PeerStore::default();
        for seed in 0..MAX_INFO_HASHES as u16 {
            store.insert(hash16(seed), addr(seed as u8, 6881), seed as u64);
        }
        assert_eq!(store.info_hash_count(), MAX_INFO_HASHES);
        assert_eq!(
            store.peers(&hash16(0), MAX_INFO_HASHES as u64 + 1),
            vec![addr(0, 6881)]
        );
        store.insert([0xFF; 20], addr(254, 6881), MAX_INFO_HASHES as u64 + 2);
        assert_eq!(store.info_hash_count(), MAX_INFO_HASHES);
        assert!(
            store
                .peers(&hash16(1), MAX_INFO_HASHES as u64 + 3)
                .is_empty(),
            "the least recently used info hash must be evicted first"
        );
        assert_eq!(
            store.peers(&hash16(0), MAX_INFO_HASHES as u64 + 4),
            vec![addr(0, 6881)]
        );
    }

    #[test]
    fn the_global_cap_bounds_total_entries() {
        let mut store = PeerStore::default();
        let per_hash = 4usize;
        let hash_count = MAX_TOTAL_PEERS / per_hash;
        for seed in 0..hash_count as u16 {
            for index in 0..per_hash as u16 {
                store.insert(
                    hash16(seed),
                    addr((index + 1) as u8, 6881 + index),
                    (seed as u64) * 10_000 + index as u64,
                );
            }
        }
        assert!(store.total() <= MAX_TOTAL_PEERS);
        assert!(store.info_hash_count() <= MAX_INFO_HASHES);
        store.insert([0xAA; 20], addr(1, 6881), MAX_TOTAL_PEERS as u64 + 1);
        assert!(store.total() <= MAX_TOTAL_PEERS);
        assert_eq!(
            store.peers(&[0xAA; 20], MAX_TOTAL_PEERS as u64 + 2),
            vec![addr(1, 6881)]
        );
        assert!(
            store
                .peers(&hash16(0), MAX_TOTAL_PEERS as u64 + 3)
                .is_empty(),
            "the oldest info hash gave up its peers for the new one"
        );
    }
}
