use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use rand::Rng;
use tokio::time::Instant as TokioInstant;

use crate::peer::Bitfield;

use super::BLOCK_SIZE;

pub const MAX_REQUESTERS_PER_BLOCK: usize = 3;
pub const MIN_PIPELINE_DEPTH: usize = 4;
pub const MAX_PIPELINE_DEPTH: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockState {
    Missing,
    Requested { peer: SocketAddr, at: TokioInstant },
    Received { peer: SocketAddr },
}

impl BlockState {
    fn requested_peer(&self) -> Option<SocketAddr> {
        match self {
            BlockState::Requested { peer, .. } => Some(*peer),
            _ => None,
        }
    }
}

struct ActivePiece {
    blocks: Vec<BlockState>,
}

pub struct PiecePicker {
    piece_count: usize,
    piece_length: u32,
    total_length: u64,
    random_first: usize,
    max_active: usize,
    availability: Vec<u32>,
    have: Bitfield,
    active: HashMap<usize, ActivePiece>,
    inactive_unverified: usize,
    endgame_threshold: usize,
    endgame_extras: HashMap<(usize, usize), Vec<SocketAddr>>,
}

pub fn endgame_threshold(total_blocks: usize) -> usize {
    (total_blocks / 100).max(10)
}

pub fn endgame_enter(
    all_pieces_active: bool,
    none_missing: bool,
    remaining_blocks: usize,
    threshold: usize,
) -> bool {
    all_pieces_active && none_missing && remaining_blocks <= threshold
}

pub fn pipeline_depth(rate_bytes_per_s: f64, target_rtt_secs: f64, block_size: usize) -> usize {
    if !rate_bytes_per_s.is_finite() || block_size == 0 {
        return MIN_PIPELINE_DEPTH;
    }
    let blocks_in_flight = rate_bytes_per_s * target_rtt_secs / block_size as f64;
    (blocks_in_flight.round() as usize).clamp(MIN_PIPELINE_DEPTH, MAX_PIPELINE_DEPTH)
}

impl PiecePicker {
    pub fn new(
        piece_count: usize,
        piece_length: u32,
        total_length: u64,
        random_first: usize,
        max_active: usize,
    ) -> PiecePicker {
        let mut total_blocks = 0usize;
        for index in 0..piece_count {
            let start = index as u64 * piece_length as u64;
            if start >= total_length {
                break;
            }
            let size = (total_length - start).min(piece_length as u64) as usize;
            total_blocks += size.div_ceil(BLOCK_SIZE);
        }
        PiecePicker {
            piece_count,
            piece_length,
            total_length,
            random_first,
            max_active,
            availability: vec![0; piece_count],
            have: Bitfield::new(piece_count),
            active: HashMap::new(),
            inactive_unverified: piece_count,
            endgame_threshold: endgame_threshold(total_blocks),
            endgame_extras: HashMap::new(),
        }
    }

    pub fn have(&self) -> &Bitfield {
        &self.have
    }

    pub fn is_complete(&self) -> bool {
        self.have.count() == self.piece_count
    }

    pub fn set_have(&mut self, have: &Bitfield) {
        let before = self.have.count();
        for index in 0..self.piece_count {
            if have.get(index) {
                let _ = self.have.set(index);
            }
        }
        let added = self.have.count() - before;
        self.inactive_unverified = self.inactive_unverified.saturating_sub(added);
    }

    pub fn is_endgame(&self) -> bool {
        if self.inactive_unverified != 0 || self.active.is_empty() {
            return false;
        }
        let mut remaining = 0usize;
        for piece in self.active.values() {
            for state in &piece.blocks {
                match state {
                    BlockState::Missing => return false,
                    BlockState::Requested { .. } | BlockState::Received { .. } => {
                        remaining += 1;
                    }
                }
            }
        }
        endgame_enter(true, true, remaining, self.endgame_threshold)
    }

    pub fn next_endgame_block(
        &mut self,
        peer: SocketAddr,
        bitfield: &Bitfield,
    ) -> Option<(usize, usize, usize)> {
        if !self.is_endgame() {
            return None;
        }
        let mut indices: Vec<usize> = self.active.keys().copied().collect();
        indices.sort();
        for index in indices {
            if !bitfield.get(index) || self.have.get(index) {
                continue;
            }
            let Some(piece) = self.active.get(&index) else {
                continue;
            };
            for (slot, state) in piece.blocks.iter().enumerate() {
                let BlockState::Requested { peer: primary, .. } = state else {
                    continue;
                };
                if *primary == peer {
                    continue;
                }
                let key = (index, slot * BLOCK_SIZE);
                let extras = self.endgame_extras.entry(key).or_default();
                if extras.contains(&peer) || extras.len() + 1 >= MAX_REQUESTERS_PER_BLOCK {
                    continue;
                }
                extras.push(peer);
                let begin = key.1;
                let begin_usize = begin;
                let length = BLOCK_SIZE.min(self.piece_size(index) - begin_usize);
                return Some((index, begin_usize, length));
            }
        }
        None
    }

    pub fn add_peer(&mut self, bitfield: &Bitfield) {
        for index in 0..self.piece_count {
            if bitfield.get(index) {
                self.availability[index] += 1;
            }
        }
    }

    pub fn remove_peer(&mut self, bitfield: &Bitfield) {
        for index in 0..self.piece_count {
            if bitfield.get(index) {
                self.availability[index] = self.availability[index].saturating_sub(1);
            }
        }
    }

    pub fn add_have(&mut self, index: usize) {
        if index < self.piece_count {
            self.availability[index] += 1;
        }
    }

    pub fn mark_have(&mut self, index: usize) {
        if self.active.remove(&index).is_none() {
            self.inactive_unverified = self.inactive_unverified.saturating_sub(1);
        }
        self.endgame_extras.retain(|key, _| key.0 != index);
        let _ = self.have.set(index);
    }

    pub fn next_block(
        &mut self,
        peer: SocketAddr,
        bitfield: &Bitfield,
    ) -> Option<(usize, usize, usize)> {
        if let Some((index, begin)) = self.continue_active(bitfield) {
            self.mark_requested(index, begin, peer);
            return Some((index, begin, self.block_length(index, begin)));
        }
        if self.active.len() >= self.max_active {
            return None;
        }
        let index = self.choose_piece(bitfield)?;
        let blocks = self.piece_size(index).div_ceil(BLOCK_SIZE);
        self.active.insert(
            index,
            ActivePiece {
                blocks: vec![BlockState::Missing; blocks],
            },
        );
        self.inactive_unverified = self.inactive_unverified.saturating_sub(1);
        self.mark_requested(index, 0, peer);
        Some((index, 0, self.block_length(index, 0)))
    }

    fn mark_requested(&mut self, index: usize, begin: usize, peer: SocketAddr) {
        if let Some(piece) = self.active.get_mut(&index) {
            if let Some(state) = piece.blocks.get_mut(begin / BLOCK_SIZE) {
                *state = BlockState::Requested {
                    peer,
                    at: TokioInstant::now(),
                };
            }
        }
    }

    pub fn block_received(
        &mut self,
        index: usize,
        begin: usize,
        peer: SocketAddr,
    ) -> Vec<SocketAddr> {
        let mut cancel_targets = Vec::new();
        if let Some(piece) = self.active.get_mut(&index) {
            let block = begin / BLOCK_SIZE;
            if let Some(state) = piece.blocks.get_mut(block) {
                if matches!(state, BlockState::Requested { .. }) {
                    if let Some(previous) = state.requested_peer() {
                        if previous != peer {
                            cancel_targets.push(previous);
                        }
                    }
                    if let Some(extras) = self.endgame_extras.get_mut(&(index, begin)) {
                        for extra in extras.iter() {
                            if *extra != peer && !cancel_targets.contains(extra) {
                                cancel_targets.push(*extra);
                            }
                        }
                    }
                    self.endgame_extras.remove(&(index, begin));
                    *state = BlockState::Received { peer };
                }
            }
        }
        cancel_targets
    }

    pub fn return_blocks(&mut self, peer: SocketAddr) {
        for piece in self.active.values_mut() {
            for state in piece.blocks.iter_mut() {
                if matches!(state, BlockState::Requested { peer: held, .. } if *held == peer) {
                    *state = BlockState::Missing;
                }
            }
        }
        for extras in self.endgame_extras.values_mut() {
            extras.retain(|extra| *extra != peer);
        }
        self.endgame_extras.retain(|_, extras| !extras.is_empty());
    }

    pub fn reap_stale(&mut self, timeout: Duration, now: TokioInstant) -> Vec<(SocketAddr, usize)> {
        let mut reaped: HashMap<SocketAddr, usize> = HashMap::new();
        let mut reaped_keys = Vec::new();
        for (index, piece) in self.active.iter_mut() {
            for (slot, state) in piece.blocks.iter_mut().enumerate() {
                if let BlockState::Requested { peer, at } = state {
                    if now.duration_since(*at) >= timeout {
                        *reaped.entry(*peer).or_insert(0) += 1;
                        *state = BlockState::Missing;
                        reaped_keys.push((*index, slot * BLOCK_SIZE));
                    }
                }
            }
        }
        for key in reaped_keys {
            self.endgame_extras.remove(&key);
        }
        reaped.into_iter().collect()
    }

    pub fn requeue_piece(&mut self, index: usize) {
        if let Some(piece) = self.active.get_mut(&index) {
            for state in piece.blocks.iter_mut() {
                *state = BlockState::Missing;
            }
        }
        self.endgame_extras.retain(|key, _| key.0 != index);
    }

    pub fn requeue_block(&mut self, index: usize, begin: usize) {
        if let Some(piece) = self.active.get_mut(&index) {
            if let Some(state) = piece.blocks.get_mut(begin / BLOCK_SIZE) {
                if matches!(state, BlockState::Requested { .. }) {
                    *state = BlockState::Missing;
                }
            }
        }
    }

    pub fn contributors(&self, index: usize) -> Vec<SocketAddr> {
        let mut contributors = Vec::new();
        if let Some(piece) = self.active.get(&index) {
            for state in &piece.blocks {
                let addr = match state {
                    BlockState::Requested { peer, .. } | BlockState::Received { peer } => peer,
                    BlockState::Missing => continue,
                };
                if !contributors.contains(addr) {
                    contributors.push(*addr);
                }
            }
        }
        contributors
    }

    fn continue_active(&self, bitfield: &Bitfield) -> Option<(usize, usize)> {
        let mut best: Option<(usize, usize)> = None;
        for index in self.active.keys() {
            if !bitfield.get(*index) || self.have.get(*index) {
                continue;
            }
            let missing = self.active[index]
                .blocks
                .iter()
                .filter(|state| **state == BlockState::Missing)
                .count();
            if missing == 0 {
                continue;
            }
            match best {
                Some((best_missing, _)) if missing >= best_missing => {}
                _ => best = Some((missing, *index)),
            }
        }
        let index = best?.1;
        let block = self.active[&index]
            .blocks
            .iter()
            .position(|state| *state == BlockState::Missing)?;
        Some((index, block * BLOCK_SIZE))
    }

    fn choose_piece(&self, bitfield: &Bitfield) -> Option<usize> {
        if self.have.count() < self.random_first {
            let candidates: Vec<usize> = (0..self.piece_count)
                .filter(|&index| {
                    bitfield.get(index)
                        && !self.have.get(index)
                        && !self.active.contains_key(&index)
                })
                .collect();
            if candidates.is_empty() {
                return None;
            }
            let pick = rand::rng().random_range(0..candidates.len());
            return Some(candidates[pick]);
        }
        let mut best: Option<(u32, usize)> = None;
        for index in 0..self.piece_count {
            if !bitfield.get(index) || self.have.get(index) || self.active.contains_key(&index) {
                continue;
            }
            match best {
                Some((best_availability, _)) if self.availability[index] >= best_availability => {}
                _ => best = Some((self.availability[index], index)),
            }
        }
        best.map(|(_, index)| index)
    }

    fn piece_size(&self, index: usize) -> usize {
        let start = index as u64 * self.piece_length as u64;
        (self.total_length - start).min(self.piece_length as u64) as usize
    }

    fn block_length(&self, index: usize, begin: usize) -> usize {
        BLOCK_SIZE.min(self.piece_size(index) - begin)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PIECE_LENGTH: u32 = 32768;

    fn picker(random_first: usize, max_active: usize) -> PiecePicker {
        PiecePicker::new(
            4,
            PIECE_LENGTH,
            4 * PIECE_LENGTH as u64,
            random_first,
            max_active,
        )
    }

    fn bitfield_of(pieces: &[usize]) -> Bitfield {
        let mut bitfield = Bitfield::new(4);
        for &index in pieces {
            bitfield.set(index).unwrap();
        }
        bitfield
    }

    fn addr(host: &str) -> SocketAddr {
        format!("{}:1", host).parse().unwrap()
    }

    #[test]
    fn picks_rarest_first_and_prefers_partial_pieces() {
        let mut picker = picker(0, 8);
        let a = bitfield_of(&[0, 1]);
        let b = bitfield_of(&[1, 2]);
        let peer_a = addr("1.1.1.1");
        let peer_b = addr("2.2.2.2");
        picker.add_peer(&a);
        picker.add_peer(&b);
        assert_eq!(picker.next_block(peer_a, &a), Some((0, 0, BLOCK_SIZE)));
        assert_eq!(picker.next_block(peer_b, &b), Some((2, 0, BLOCK_SIZE)));
        assert_eq!(
            picker.next_block(peer_a, &a),
            Some((0, BLOCK_SIZE, BLOCK_SIZE))
        );
        assert_eq!(picker.next_block(peer_a, &a), Some((1, 0, BLOCK_SIZE)));
    }

    #[test]
    fn returns_blocks_when_peer_chokes() {
        let mut picker = picker(0, 8);
        let a = bitfield_of(&[0]);
        let b = bitfield_of(&[0, 1]);
        let peer_a = addr("1.1.1.1");
        let peer_b = addr("2.2.2.2");
        picker.add_peer(&a);
        picker.add_peer(&b);
        assert_eq!(picker.next_block(peer_a, &a), Some((0, 0, BLOCK_SIZE)));
        picker.return_blocks(peer_a);
        assert_eq!(picker.next_block(peer_b, &b), Some((0, 0, BLOCK_SIZE)));
    }

    #[test]
    fn skips_pieces_the_peer_lacks() {
        let mut picker = picker(0, 8);
        let a = bitfield_of(&[0]);
        let b = bitfield_of(&[1]);
        picker.add_peer(&a);
        picker.add_peer(&b);
        assert_eq!(
            picker.next_block(addr("2.2.2.2"), &b),
            Some((1, 0, BLOCK_SIZE))
        );
    }

    #[test]
    fn respects_max_active() {
        let mut picker = picker(0, 1);
        let a = bitfield_of(&[0]);
        let b = bitfield_of(&[1]);
        picker.add_peer(&a);
        picker.add_peer(&b);
        assert!(picker.next_block(addr("1.1.1.1"), &a).is_some());
        assert_eq!(picker.next_block(addr("2.2.2.2"), &b), None);
    }

    #[test]
    fn tracks_contributors_and_requeues() {
        let mut picker = picker(0, 8);
        let a = bitfield_of(&[0, 1, 2, 3]);
        let b = bitfield_of(&[0, 1, 2, 3]);
        let peer_a = addr("1.1.1.1");
        let peer_b = addr("2.2.2.2");
        picker.add_peer(&a);
        picker.add_peer(&b);
        assert_eq!(picker.next_block(peer_a, &a), Some((0, 0, BLOCK_SIZE)));
        assert_eq!(
            picker.next_block(peer_b, &b),
            Some((0, BLOCK_SIZE, BLOCK_SIZE))
        );
        picker.block_received(0, 0, peer_a);
        assert_eq!(picker.contributors(0), vec![peer_a, peer_b]);
        picker.requeue_piece(0);
        assert_eq!(picker.next_block(peer_a, &a), Some((0, 0, BLOCK_SIZE)));
    }

    #[test]
    fn requeues_single_block() {
        let mut picker = picker(0, 8);
        let a = bitfield_of(&[0, 1, 2, 3]);
        let peer_a = addr("1.1.1.1");
        let peer_b = addr("2.2.2.2");
        picker.add_peer(&a);
        assert_eq!(picker.next_block(peer_a, &a), Some((0, 0, BLOCK_SIZE)));
        picker.requeue_block(0, 0);
        assert_eq!(picker.next_block(peer_b, &a), Some((0, 0, BLOCK_SIZE)));
    }

    #[test]
    fn marks_have_and_completes() {
        let mut picker = picker(0, 8);
        let a = bitfield_of(&[0, 1, 2, 3]);
        picker.add_peer(&a);
        let peer = addr("1.1.1.1");
        for _ in 0..4 {
            assert!(picker.next_block(peer, &a).is_some());
        }
        for index in 0..4 {
            picker.mark_have(index);
        }
        assert!(picker.is_complete());
        assert_eq!(picker.next_block(peer, &a), None);
    }

    #[test]
    fn random_first_stays_within_peer_bitfield() {
        let mut picker = picker(4, 8);
        let a = bitfield_of(&[1, 3]);
        picker.add_peer(&a);
        let peer = addr("1.1.1.1");
        let first = picker.next_block(peer, &a).unwrap();
        assert!(first.0 == 1 || first.0 == 3);
    }

    #[test]
    fn endgame_thresholds_are_pure() {
        assert_eq!(endgame_threshold(0), 10);
        assert_eq!(endgame_threshold(100), 10);
        assert_eq!(endgame_threshold(1_000), 10);
        assert_eq!(endgame_threshold(2_000), 20);
        assert_eq!(endgame_threshold(10_000), 100);
    }

    #[test]
    fn endgame_enter_requires_all_conditions() {
        assert!(endgame_enter(true, true, 5, 10));
        assert!(!endgame_enter(false, true, 5, 10));
        assert!(!endgame_enter(true, false, 5, 10));
        assert!(!endgame_enter(true, true, 11, 10));
        assert!(endgame_enter(true, true, 10, 10));
    }

    #[test]
    fn pipeline_depth_scales_with_rate() {
        assert_eq!(pipeline_depth(2_000_000.0, 1.5, 16_384), MAX_PIPELINE_DEPTH);
        assert_eq!(pipeline_depth(1_048_576.0, 1.5, 16_384), 96);
        assert_eq!(pipeline_depth(0.0, 1.5, 16_384), MIN_PIPELINE_DEPTH);
        assert_eq!(pipeline_depth(100.0, 1.5, 16_384), MIN_PIPELINE_DEPTH);
        assert_eq!(pipeline_depth(f64::NAN, 1.5, 16_384), MIN_PIPELINE_DEPTH);
    }

    fn single_piece_picker() -> PiecePicker {
        PiecePicker::new(1, 32_768, 32_768, 0, 8)
    }

    #[test]
    fn enters_endgame_when_every_block_is_requested() {
        let mut picker = single_piece_picker();
        let a = bitfield_of(&[0]);
        picker.add_peer(&a);
        let peer_a = addr("1.1.1.1");
        assert_eq!(picker.next_block(peer_a, &a), Some((0, 0, BLOCK_SIZE)));
        assert!(!picker.is_endgame());
        assert_eq!(
            picker.next_block(peer_a, &a),
            Some((0, BLOCK_SIZE, BLOCK_SIZE))
        );
        assert!(picker.is_endgame());
        assert_eq!(picker.have().count(), 0);
    }

    #[test]
    fn endgame_reached_once_requests_cover_all_blocks() {
        let mut picker = single_piece_picker();
        let a = bitfield_of(&[0]);
        picker.add_peer(&a);
        let peer_a = addr("1.1.1.1");
        let _ = picker.next_block(peer_a, &a);
        let _ = picker.next_block(peer_a, &a);
        assert!(picker.is_endgame());
        let b = bitfield_of(&[0]);
        picker.add_peer(&b);
        let peer_b = addr("2.2.2.2");
        assert_eq!(picker.next_block(peer_b, &b), None);
        assert_eq!(
            picker.next_endgame_block(peer_b, &b),
            Some((0, 0, BLOCK_SIZE))
        );
    }

    #[test]
    fn endgame_duplicates_capped_at_three_requesters() {
        let mut picker = single_piece_picker();
        let full = bitfield_of(&[0]);
        picker.add_peer(&full);
        let peer_a = addr("1.1.1.1");
        let _ = picker.next_block(peer_a, &full);
        let _ = picker.next_block(peer_a, &full);
        let peer_b = addr("2.2.2.2");
        let peer_c = addr("3.3.3.3");
        let peer_d = addr("4.4.4.4");
        assert_eq!(
            picker.next_endgame_block(peer_b, &full),
            Some((0, 0, BLOCK_SIZE))
        );
        assert_eq!(
            picker.next_endgame_block(peer_c, &full),
            Some((0, 0, BLOCK_SIZE))
        );
        assert_eq!(
            picker.next_endgame_block(peer_d, &full),
            Some((0, BLOCK_SIZE, BLOCK_SIZE))
        );
        assert_eq!(picker.next_endgame_block(peer_a, &full), None);
    }

    #[test]
    fn block_received_returns_other_requesters_to_cancel() {
        let mut picker = single_piece_picker();
        let full = bitfield_of(&[0]);
        picker.add_peer(&full);
        let peer_a = addr("1.1.1.1");
        let _ = picker.next_block(peer_a, &full);
        let _ = picker.next_block(peer_a, &full);
        let peer_b = addr("2.2.2.2");
        let peer_c = addr("3.3.3.3");
        let _ = picker.next_endgame_block(peer_b, &full);
        let _ = picker.next_endgame_block(peer_c, &full);

        let targets = picker.block_received(0, 0, peer_b);
        assert_eq!(targets, vec![peer_a, peer_c]);

        let late = picker.block_received(0, 0, peer_c);
        assert!(late.is_empty());
    }

    #[test]
    fn leaves_endgame_when_piece_is_requeued() {
        let mut picker = single_piece_picker();
        let full = bitfield_of(&[0]);
        picker.add_peer(&full);
        let peer_a = addr("1.1.1.1");
        let _ = picker.next_block(peer_a, &full);
        let _ = picker.next_block(peer_a, &full);
        assert!(picker.is_endgame());
        picker.requeue_piece(0);
        assert!(!picker.is_endgame());
    }

    #[test]
    fn leaving_peer_is_dropped_from_endgame_extras() {
        let mut picker = single_piece_picker();
        let full = bitfield_of(&[0]);
        picker.add_peer(&full);
        let peer_a = addr("1.1.1.1");
        let _ = picker.next_block(peer_a, &full);
        let _ = picker.next_block(peer_a, &full);
        let peer_b = addr("2.2.2.2");
        let _ = picker.next_endgame_block(peer_b, &full);
        picker.return_blocks(peer_b);
        let peer_c = addr("3.3.3.3");
        assert_eq!(
            picker.next_endgame_block(peer_c, &full),
            Some((0, 0, BLOCK_SIZE))
        );
    }
}
