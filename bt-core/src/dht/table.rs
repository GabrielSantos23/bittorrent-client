use std::net::{Ipv4Addr, SocketAddrV4};

use crate::dht::krpc::NodeInfo;
use crate::dht::node_id::{
    bucket_index, cmp_distance_to, distance, NodeId, RandomBytes, NODE_ID_LENGTH,
};

pub const K: usize = 8;
pub const BUCKET_COUNT: usize = 160;
pub const GOOD_WINDOW_MS: u64 = 15 * 60 * 1000;
pub const BUCKET_REFRESH_MS: u64 = 15 * 60 * 1000;
pub const MAX_FAILED_QUERIES: u32 = 2;
pub const MAX_NODES_PER_IP: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Health {
    Good,
    Questionable,
    Bad,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NodeEntry {
    pub info: NodeInfo,
    pub last_responded_ms: u64,
    pub failed_queries: u32,
}

impl NodeEntry {
    pub fn health(&self, now_ms: u64) -> Health {
        if self.failed_queries >= MAX_FAILED_QUERIES {
            Health::Bad
        } else if now_ms.saturating_sub(self.last_responded_ms) >= GOOD_WINDOW_MS {
            Health::Questionable
        } else {
            Health::Good
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OfferOutcome {
    Accepted,
    Updated,
    AcceptedReplacingBad { evicted: NodeId },
    PingRequired { questioned: NodeId },
    Rejected { reason: RejectReason },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    InvalidAddress,
    OwnId,
    BucketFull,
    EvictionAlreadyPending,
    IpLimitPerBucket,
    IpLimitPerTable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseOutcome {
    Refreshed,
    PendingDiscarded { discarded: NodeInfo },
    UnknownNode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureOutcome {
    Noted { failures: u32 },
    BecameBad { replaced_by: Option<NodeInfo> },
    UnknownNode,
}

#[derive(Debug, Default)]
struct Bucket {
    nodes: Vec<NodeEntry>,
    pending: Option<NodeInfo>,
    last_refresh_ms: Option<u64>,
}

impl Bucket {
    fn position_of(&self, id: &NodeId) -> Option<usize> {
        self.nodes.iter().position(|entry| entry.info.id == *id)
    }
}

#[derive(Debug)]
pub struct RoutingTable {
    self_id: NodeId,
    buckets: Vec<Bucket>,
}

impl RoutingTable {
    pub fn new(self_id: NodeId, started_ms: u64) -> RoutingTable {
        RoutingTable {
            self_id,
            buckets: (0..BUCKET_COUNT)
                .map(|_| Bucket {
                    nodes: Vec::new(),
                    pending: None,
                    last_refresh_ms: Some(started_ms),
                })
                .collect(),
        }
    }

    pub fn self_id(&self) -> &NodeId {
        &self.self_id
    }

    pub fn len(&self) -> usize {
        self.buckets.iter().map(|bucket| bucket.nodes.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn offer(&mut self, candidate: NodeInfo, now_ms: u64) -> OfferOutcome {
        if !is_valid_address(&candidate.addr) {
            return OfferOutcome::Rejected {
                reason: RejectReason::InvalidAddress,
            };
        }
        let Some(bucket_index) = bucket_index(&distance(&candidate.id, &self.self_id)) else {
            return OfferOutcome::Rejected {
                reason: RejectReason::OwnId,
            };
        };
        let ip = *candidate.addr.ip();
        {
            let bucket = &mut self.buckets[usize::from(bucket_index)];
            if let Some(position) = bucket.position_of(&candidate.id) {
                let mut entry = bucket.nodes.remove(position);
                entry.info.addr = candidate.addr;
                entry.last_responded_ms = now_ms;
                entry.failed_queries = 0;
                bucket.nodes.push(entry);
                bucket.last_refresh_ms = Some(now_ms);
                return OfferOutcome::Updated;
            }
            if bucket.pending.is_some() {
                return OfferOutcome::Rejected {
                    reason: RejectReason::EvictionAlreadyPending,
                };
            }
            if bucket.nodes.iter().any(|entry| *entry.info.addr.ip() == ip) {
                return OfferOutcome::Rejected {
                    reason: RejectReason::IpLimitPerBucket,
                };
            }
        }
        if self.nodes_per_ip(ip) >= MAX_NODES_PER_IP {
            return OfferOutcome::Rejected {
                reason: RejectReason::IpLimitPerTable,
            };
        }
        let bucket = &mut self.buckets[usize::from(bucket_index)];
        if bucket.nodes.len() < K {
            bucket.nodes.push(NodeEntry {
                info: candidate,
                last_responded_ms: now_ms,
                failed_queries: 0,
            });
            bucket.last_refresh_ms = Some(now_ms);
            return OfferOutcome::Accepted;
        }
        if let Some(position) = bucket
            .nodes
            .iter()
            .position(|entry| entry.health(now_ms) == Health::Bad)
        {
            let evicted = bucket.nodes.remove(position).info.id;
            bucket.nodes.push(NodeEntry {
                info: candidate,
                last_responded_ms: now_ms,
                failed_queries: 0,
            });
            bucket.last_refresh_ms = Some(now_ms);
            return OfferOutcome::AcceptedReplacingBad { evicted };
        }
        let questionable = bucket
            .nodes
            .iter()
            .position(|entry| entry.health(now_ms) == Health::Questionable);
        match questionable {
            Some(position) => {
                let questioned = bucket.nodes[position].info.id;
                bucket.pending = Some(candidate);
                OfferOutcome::PingRequired { questioned }
            }
            None => OfferOutcome::Rejected {
                reason: RejectReason::BucketFull,
            },
        }
    }

    pub fn note_response(&mut self, id: &NodeId, now_ms: u64) -> ResponseOutcome {
        let Some(bucket_index) = self.bucket_of(id) else {
            return ResponseOutcome::UnknownNode;
        };
        let bucket = &mut self.buckets[usize::from(bucket_index)];
        let Some(position) = bucket.position_of(id) else {
            return ResponseOutcome::UnknownNode;
        };
        let mut entry = bucket.nodes.remove(position);
        entry.last_responded_ms = now_ms;
        entry.failed_queries = 0;
        bucket.nodes.push(entry);
        bucket.last_refresh_ms = Some(now_ms);
        match bucket.pending.take() {
            Some(discarded) => ResponseOutcome::PendingDiscarded { discarded },
            None => ResponseOutcome::Refreshed,
        }
    }

    pub fn note_failure(&mut self, id: &NodeId, now_ms: u64) -> FailureOutcome {
        let Some(bucket_index) = self.bucket_of(id) else {
            return FailureOutcome::UnknownNode;
        };
        let bucket = &mut self.buckets[usize::from(bucket_index)];
        let Some(position) = bucket.position_of(id) else {
            return FailureOutcome::UnknownNode;
        };
        bucket.nodes[position].failed_queries += 1;
        if bucket.nodes[position].failed_queries < MAX_FAILED_QUERIES {
            return FailureOutcome::Noted {
                failures: bucket.nodes[position].failed_queries,
            };
        }
        let pending = bucket.pending.take();
        if let Some(candidate) = pending {
            bucket.nodes.remove(position);
            bucket.nodes.push(NodeEntry {
                info: candidate,
                last_responded_ms: now_ms,
                failed_queries: 0,
            });
            bucket.last_refresh_ms = Some(now_ms);
            return FailureOutcome::BecameBad {
                replaced_by: Some(candidate),
            };
        }
        FailureOutcome::BecameBad { replaced_by: None }
    }

    pub fn closest_nodes(&self, target: &NodeId, count: usize) -> Vec<NodeInfo> {
        let mut nodes: Vec<NodeInfo> = self
            .buckets
            .iter()
            .flat_map(|bucket| bucket.nodes.iter().map(|entry| entry.info))
            .collect();
        nodes.sort_by(|a, b| cmp_distance_to(&a.id, &b.id, target));
        nodes.truncate(count);
        nodes
    }

    pub fn address_of(&self, id: &NodeId) -> Option<SocketAddrV4> {
        self.buckets
            .iter()
            .flat_map(|bucket| bucket.nodes.iter())
            .find(|entry| entry.info.id == *id)
            .map(|entry| entry.info.addr)
    }

    pub fn entries(&self) -> Vec<NodeEntry> {
        self.buckets
            .iter()
            .flat_map(|bucket| bucket.nodes.iter())
            .cloned()
            .collect()
    }

    pub fn bucket_is_stale(&self, bucket_index: u8, now_ms: u64) -> bool {
        let Some(bucket) = self.buckets.get(usize::from(bucket_index)) else {
            return false;
        };
        match bucket.last_refresh_ms {
            Some(last) => now_ms.saturating_sub(last) >= BUCKET_REFRESH_MS,
            None => true,
        }
    }

    pub fn stale_buckets(&self, now_ms: u64) -> Vec<u8> {
        (0..u8::try_from(BUCKET_COUNT).unwrap_or(u8::MAX))
            .filter(|index| self.bucket_is_stale(*index, now_ms))
            .collect()
    }

    pub fn refresh_target(&self, bucket_index: u8, source: &dyn RandomBytes) -> NodeId {
        let position = usize::from(bucket_index);
        let byte_from_end = position / 8;
        let bit = position % 8;
        let mut bytes = [0u8; NODE_ID_LENGTH];
        source.fill(&mut bytes);
        let index = NODE_ID_LENGTH - 1 - byte_from_end;
        for prefix in bytes.iter_mut().take(index) {
            *prefix = 0;
        }
        let lower_mask: u8 = if bit == 0 {
            0
        } else {
            ((1u16 << bit) - 1) as u8
        };
        bytes[index] = (bytes[index] & lower_mask) | (1u8 << bit);
        let candidate = NodeId::from_bytes(bytes);
        if candidate == self.self_id {
            let mut flipped = bytes;
            flipped[NODE_ID_LENGTH - 1] ^= 1;
            return NodeId::from_bytes(flipped);
        }
        candidate
    }

    fn bucket_of(&self, id: &NodeId) -> Option<u8> {
        let index = bucket_index(&distance(id, &self.self_id))?;
        if usize::from(index) >= BUCKET_COUNT {
            return None;
        }
        Some(index)
    }

    fn nodes_per_ip(&self, ip: Ipv4Addr) -> usize {
        self.buckets
            .iter()
            .flat_map(|bucket| bucket.nodes.iter())
            .filter(|entry| *entry.info.addr.ip() == ip)
            .count()
    }
}

fn is_valid_address(addr: &SocketAddrV4) -> bool {
    if addr.port() == 0 {
        return false;
    }
    let ip = addr.ip();
    !(ip.is_unspecified() || ip.is_broadcast() || ip.is_multicast())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn node_at(bytes: [u8; NODE_ID_LENGTH], ip: [u8; 4], port: u16) -> NodeInfo {
        NodeInfo {
            id: NodeId::from_bytes(bytes),
            addr: SocketAddrV4::new(Ipv4Addr::from(ip), port),
        }
    }

    fn bucket7_node(lower: u8, ip_last: u8, port: u16) -> NodeInfo {
        let mut id = [0u8; NODE_ID_LENGTH];
        id[NODE_ID_LENGTH - 1] = 0x80 | lower;
        node_at(id, [10, 0, 0, ip_last], port)
    }

    struct FixedRandom(Mutex<Vec<u8>>);

    impl RandomBytes for FixedRandom {
        fn fill(&self, dest: &mut [u8]) {
            let mut source = self.0.lock().unwrap();
            for byte in dest.iter_mut() {
                *byte = if source.is_empty() {
                    0
                } else {
                    source.remove(0)
                };
            }
        }
    }

    fn fill_bucket_with_good_nodes(table: &mut RoutingTable) {
        for index in 0..K {
            let candidate = bucket7_node((index + 1) as u8, (index + 1) as u8, 2000 + index as u16);
            assert_eq!(table.offer(candidate, 0), OfferOutcome::Accepted);
        }
    }

    #[test]
    fn nodes_are_split_by_distance_around_our_id() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let mut table = RoutingTable::new(self_id, 0);
        let mut far_id = [0u8; NODE_ID_LENGTH];
        far_id[0] = 0x80;
        let far = node_at(far_id, [10, 0, 0, 1], 1000);
        let near = node_at(
            {
                let mut id = [0u8; NODE_ID_LENGTH];
                id[NODE_ID_LENGTH - 1] = 0x01;
                id
            },
            [10, 0, 0, 2],
            1001,
        );
        assert_eq!(table.offer(far, 0), OfferOutcome::Accepted);
        assert_eq!(table.offer(near, 0), OfferOutcome::Accepted);
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn own_id_is_rejected() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let mut table = RoutingTable::new(self_id, 0);
        let outcome = table.offer(node_at([0u8; NODE_ID_LENGTH], [10, 0, 0, 1], 1000), 0);
        assert_eq!(
            outcome,
            OfferOutcome::Rejected {
                reason: RejectReason::OwnId
            }
        );
    }

    #[test]
    fn invalid_addresses_are_ignored() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let mut table = RoutingTable::new(self_id, 0);
        for ip in [
            Ipv4Addr::UNSPECIFIED,
            Ipv4Addr::BROADCAST,
            Ipv4Addr::new(224, 0, 0, 1),
        ] {
            for port in [0u16, 6881] {
                let candidate = node_at([1u8; NODE_ID_LENGTH], ip.octets(), port);
                assert_eq!(
                    table.offer(candidate, 0),
                    OfferOutcome::Rejected {
                        reason: RejectReason::InvalidAddress
                    }
                );
            }
        }
        assert!(table.is_empty());
    }

    #[test]
    fn full_bucket_of_good_nodes_rejects_newcomers() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let mut table = RoutingTable::new(self_id, 0);
        fill_bucket_with_good_nodes(&mut table);
        let newcomer = bucket7_node(0x09, 9, 3000);
        assert_eq!(
            table.offer(newcomer, 0),
            OfferOutcome::Rejected {
                reason: RejectReason::BucketFull
            }
        );
        assert_eq!(table.len(), K);
    }

    #[test]
    fn bad_node_is_replaced_first() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let mut table = RoutingTable::new(self_id, 0);
        fill_bucket_with_good_nodes(&mut table);
        let mut first_id = [0u8; NODE_ID_LENGTH];
        first_id[NODE_ID_LENGTH - 1] = 0x81;
        assert_eq!(
            table.note_failure(&NodeId::from_bytes(first_id), 1000),
            FailureOutcome::Noted { failures: 1 }
        );
        assert_eq!(
            table.note_failure(&NodeId::from_bytes(first_id), 2000),
            FailureOutcome::BecameBad { replaced_by: None }
        );
        let newcomer = bucket7_node(0x09, 9, 3000);
        assert_eq!(
            table.offer(newcomer, 3000),
            OfferOutcome::AcceptedReplacingBad {
                evicted: NodeId::from_bytes(first_id)
            }
        );
        assert_eq!(table.len(), K);
    }

    #[test]
    fn questionable_node_is_pinged_before_eviction() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let mut table = RoutingTable::new(self_id, 0);
        let mut oldest_id = [0u8; NODE_ID_LENGTH];
        oldest_id[NODE_ID_LENGTH - 1] = 0x81;
        assert_eq!(
            table.offer(node_at(oldest_id, [10, 0, 0, 1], 2000), 0),
            OfferOutcome::Accepted
        );
        for index in 1..K {
            let candidate = bucket7_node((index + 1) as u8, (index + 1) as u8, 2000 + index as u16);
            assert_eq!(table.offer(candidate, 1000), OfferOutcome::Accepted);
        }
        let newcomer = bucket7_node(0x09, 9, 3000);
        assert_eq!(
            table.offer(newcomer, GOOD_WINDOW_MS + 1),
            OfferOutcome::PingRequired {
                questioned: NodeId::from_bytes(oldest_id)
            }
        );
        assert_eq!(
            table.note_response(&NodeId::from_bytes(oldest_id), GOOD_WINDOW_MS + 2),
            ResponseOutcome::PendingDiscarded {
                discarded: newcomer
            }
        );
        assert_eq!(table.len(), K);
    }

    #[test]
    fn failed_ping_evicts_the_questionable_node() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let mut table = RoutingTable::new(self_id, 0);
        let mut oldest_id = [0u8; NODE_ID_LENGTH];
        oldest_id[NODE_ID_LENGTH - 1] = 0x81;
        assert_eq!(
            table.offer(node_at(oldest_id, [10, 0, 0, 1], 2000), 0),
            OfferOutcome::Accepted
        );
        for index in 1..K {
            let candidate = bucket7_node((index + 1) as u8, (index + 1) as u8, 2000 + index as u16);
            assert_eq!(table.offer(candidate, 1000), OfferOutcome::Accepted);
        }
        let newcomer = bucket7_node(0x09, 9, 3000);
        assert_eq!(
            table.offer(newcomer, GOOD_WINDOW_MS + 1),
            OfferOutcome::PingRequired {
                questioned: NodeId::from_bytes(oldest_id)
            }
        );
        assert_eq!(
            table.note_failure(&NodeId::from_bytes(oldest_id), GOOD_WINDOW_MS + 2),
            FailureOutcome::Noted { failures: 1 }
        );
        assert_eq!(
            table.note_failure(&NodeId::from_bytes(oldest_id), GOOD_WINDOW_MS + 3),
            FailureOutcome::BecameBad {
                replaced_by: Some(newcomer)
            }
        );
    }

    #[test]
    fn repeated_offers_update_instead_of_duplicating() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let mut table = RoutingTable::new(self_id, 0);
        let candidate = bucket7_node(0x01, 1, 2000);
        assert_eq!(table.offer(candidate, 0), OfferOutcome::Accepted);
        assert_eq!(table.offer(candidate, 5000), OfferOutcome::Updated);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn at_most_two_nodes_per_ip_across_the_table() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let mut table = RoutingTable::new(self_id, 0);
        let first = node_at(
            {
                let mut id = [0u8; NODE_ID_LENGTH];
                id[NODE_ID_LENGTH - 1] = 0x02;
                id
            },
            [10, 0, 0, 7],
            2000,
        );
        let second = node_at(
            {
                let mut id = [0u8; NODE_ID_LENGTH];
                id[NODE_ID_LENGTH - 2] = 0x02;
                id
            },
            [10, 0, 0, 7],
            2001,
        );
        assert_eq!(table.offer(first, 0), OfferOutcome::Accepted);
        assert_eq!(table.offer(second, 0), OfferOutcome::Accepted);
        let third = node_at(
            {
                let mut id = [0u8; NODE_ID_LENGTH];
                id[NODE_ID_LENGTH - 3] = 0x02;
                id
            },
            [10, 0, 0, 7],
            2002,
        );
        assert_eq!(
            table.offer(third, 0),
            OfferOutcome::Rejected {
                reason: RejectReason::IpLimitPerTable
            }
        );
    }

    #[test]
    fn at_most_one_node_per_ip_per_bucket() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let mut table = RoutingTable::new(self_id, 0);
        let bucket7_first = bucket7_node(0x01, 1, 2000);
        let bucket7_second_same_ip = bucket7_node(0x02, 1, 2001);
        assert_eq!(table.offer(bucket7_first, 0), OfferOutcome::Accepted);
        assert_eq!(
            table.offer(bucket7_second_same_ip, 0),
            OfferOutcome::Rejected {
                reason: RejectReason::IpLimitPerBucket
            }
        );
        let bucket8_first = node_at(
            {
                let mut id = [0u8; NODE_ID_LENGTH];
                id[NODE_ID_LENGTH - 2] = 0x02;
                id
            },
            [10, 0, 0, 1],
            2001,
        );
        assert_eq!(table.offer(bucket8_first, 0), OfferOutcome::Accepted);
        let bucket16_same_ip = node_at(
            {
                let mut id = [0u8; NODE_ID_LENGTH];
                id[NODE_ID_LENGTH - 3] = 0x02;
                id
            },
            [10, 0, 0, 1],
            2003,
        );
        assert_eq!(
            table.offer(bucket16_same_ip, 0),
            OfferOutcome::Rejected {
                reason: RejectReason::IpLimitPerTable
            }
        );
    }

    #[test]
    fn closest_nodes_are_sorted_by_xor_distance() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let mut table = RoutingTable::new(self_id, 0);
        let target = NodeId::from_bytes({
            let mut id = [0u8; NODE_ID_LENGTH];
            id[NODE_ID_LENGTH - 1] = 0x42;
            id
        });
        for (index, suffix) in [0x10u8, 0x44, 0x41, 0x80].into_iter().enumerate() {
            let candidate = bucket7_node(suffix, (index + 1) as u8, 2000 + index as u16);
            assert_eq!(table.offer(candidate, 0), OfferOutcome::Accepted);
        }
        let closest = table.closest_nodes(&target, 3);
        assert_eq!(closest.len(), 3);
        let distances: Vec<u8> = closest
            .iter()
            .map(|info| distance(&info.id, &target)[NODE_ID_LENGTH - 1])
            .collect();
        assert!(distances.windows(2).all(|pair| pair[0] <= pair[1]));
        assert_eq!(
            table.closest_nodes(&target, 100).len(),
            4,
            "closest_nodes never returns more than the table holds"
        );
    }

    #[test]
    fn bucket_refresh_timestamps_drive_staleness() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let mut table = RoutingTable::new(self_id, 0);
        let candidate = bucket7_node(0x01, 1, 2000);
        assert_eq!(table.offer(candidate, 0), OfferOutcome::Accepted);
        let bucket = bucket_index(&distance(&candidate.id, &self_id)).unwrap();
        assert_eq!(bucket, 7);
        assert!(!table.bucket_is_stale(bucket, GOOD_WINDOW_MS - 1));
        assert!(table.bucket_is_stale(bucket, GOOD_WINDOW_MS));
        let id = *bucket7_node(0x01, 1, 0).id.as_bytes();
        assert_eq!(
            table.note_response(&NodeId::from_bytes(id), GOOD_WINDOW_MS + 10),
            ResponseOutcome::Refreshed
        );
        assert!(!table.bucket_is_stale(bucket, GOOD_WINDOW_MS + 10));
        assert!(table.bucket_is_stale(bucket, GOOD_WINDOW_MS * 2 + 20));
        assert!(table.stale_buckets(GOOD_WINDOW_MS * 3).contains(&bucket));
    }

    #[test]
    fn empty_buckets_become_stale_after_the_refresh_interval() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let table = RoutingTable::new(self_id, 0);
        assert!(!table.bucket_is_stale(100, BUCKET_REFRESH_MS - 1));
        assert!(table.bucket_is_stale(100, BUCKET_REFRESH_MS));
        assert!(table.stale_buckets(BUCKET_REFRESH_MS).contains(&100));
    }

    #[test]
    fn table_start_anchors_empty_bucket_staleness() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let started = 10_000;
        let table = RoutingTable::new(self_id, started);
        assert!(!table.bucket_is_stale(159, started + BUCKET_REFRESH_MS - 1));
        assert!(table.bucket_is_stale(159, started + BUCKET_REFRESH_MS));
    }

    #[test]
    fn activity_resets_only_the_touched_bucket() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let mut table = RoutingTable::new(self_id, 0);
        let candidate = bucket7_node(0x01, 1, 2000);
        assert_eq!(
            table.offer(candidate, BUCKET_REFRESH_MS - 1_000),
            OfferOutcome::Accepted
        );
        let bucket = bucket_index(&distance(&candidate.id, &self_id)).unwrap();
        assert_eq!(bucket, 7);
        assert!(!table.bucket_is_stale(bucket, BUCKET_REFRESH_MS));
        assert!(
            table.bucket_is_stale(150, BUCKET_REFRESH_MS),
            "an untouched empty bucket goes stale from table start"
        );
    }

    #[test]
    fn a_bucket_that_gains_nodes_freshens_only_itself() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let mut table = RoutingTable::new(self_id, 0);
        let far = node_at(
            {
                let mut id = [0u8; NODE_ID_LENGTH];
                id[0] = 0x80;
                id
            },
            [10, 0, 0, 1],
            2000,
        );
        let bucket = bucket_index(&distance(&far.id, &self_id)).unwrap();
        assert!(table.bucket_is_stale(bucket, BUCKET_REFRESH_MS));
        assert_eq!(table.offer(far, BUCKET_REFRESH_MS), OfferOutcome::Accepted);
        assert!(!table.bucket_is_stale(bucket, BUCKET_REFRESH_MS + 1));
        assert!(!table.bucket_is_stale(bucket, 2 * BUCKET_REFRESH_MS - 1));
        assert!(table.bucket_is_stale(bucket, 2 * BUCKET_REFRESH_MS));
        assert!(table.bucket_is_stale(150, 2 * BUCKET_REFRESH_MS));
    }

    #[test]
    fn refresh_target_lands_in_the_requested_bucket() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let table = RoutingTable::new(self_id, 0);
        let source = FixedRandom(Mutex::new(vec![0xFFu8; 40]));
        for bucket in [0u8, 1, 7, 8, 152, 159] {
            let target = table.refresh_target(bucket, &source);
            assert_eq!(
                bucket_index(&distance(&target, &self_id)),
                Some(bucket),
                "refresh target for bucket {bucket} landed elsewhere"
            );
        }
    }

    #[test]
    fn two_failures_make_a_node_bad() {
        let self_id = NodeId::from_bytes([0u8; NODE_ID_LENGTH]);
        let mut table = RoutingTable::new(self_id, 0);
        let id = NodeId::from_bytes(*bucket7_node(0x01, 1, 0).id.as_bytes());
        assert_eq!(
            table.note_failure(&id, 0),
            FailureOutcome::UnknownNode,
            "failures for unknown nodes are ignored"
        );
        assert_eq!(
            table.offer(bucket7_node(0x01, 1, 2000), 0),
            OfferOutcome::Accepted
        );
        assert_eq!(
            table.note_failure(&id, 0),
            FailureOutcome::Noted { failures: 1 }
        );
        assert_eq!(
            table.note_failure(&id, 0),
            FailureOutcome::BecameBad { replaced_by: None }
        );
    }
}
