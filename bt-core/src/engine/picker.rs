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
    /// Pieces overlapping at least one non-skipped file.
    wanted: Bitfield,
    /// Wanted pieces that overlap at least one High priority file.
    high: Bitfield,
    /// Wanted pieces that are not verified yet, including active ones.
    wanted_missing: usize,
    active: HashMap<usize, ActivePiece>,
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
        let mut wanted = Bitfield::new(piece_count);
        for index in 0..piece_count {
            let _ = wanted.set(index);
        }
        PiecePicker {
            piece_count,
            piece_length,
            total_length,
            random_first,
            max_active,
            availability: vec![0; piece_count],
            have: Bitfield::new(piece_count),
            high: Bitfield::new(piece_count),
            wanted_missing: piece_count,
            wanted,
            active: HashMap::new(),
            endgame_threshold: endgame_threshold(total_blocks),
            endgame_extras: HashMap::new(),
        }
    }

    pub fn have(&self) -> &Bitfield {
        &self.have
    }

    pub fn is_complete(&self) -> bool {
        self.wanted_missing == 0
    }

    pub fn set_have(&mut self, have: &Bitfield) {
        for index in 0..self.piece_count {
            if have.get(index) {
                let _ = self.have.set(index);
            }
        }
        self.wanted_missing = (0..self.piece_count)
            .filter(|&index| self.wanted.get(index) && !self.have.get(index))
            .count();
    }

    /// Replaces the have state wholesale, for a recheck that runs on a picker
    /// that already has state: pieces the recheck no longer verifies are
    /// unmarked and any in-flight piece state is dropped.
    pub fn reset_have(&mut self, have: &Bitfield) {
        for index in 0..self.piece_count {
            if have.get(index) {
                let _ = self.have.set(index);
            } else {
                let _ = self.have.clear(index);
            }
        }
        self.active.clear();
        self.endgame_extras.clear();
        self.wanted_missing = (0..self.piece_count)
            .filter(|&index| self.wanted.get(index) && !self.have.get(index))
            .count();
    }

    /// Restricts the picker to the wanted piece classes. Returns the in-flight
    /// blocks of pieces that are no longer wanted, as `(peer, index, begin)`
    /// cancel targets, and the indices of the dropped pieces.
    pub fn set_wanted(
        &mut self,
        wanted: &Bitfield,
        high: &Bitfield,
    ) -> (Vec<(SocketAddr, usize, usize)>, Vec<usize>) {
        self.wanted = wanted.clone();
        self.high = high.clone();
        let dropped: Vec<usize> = self
            .active
            .keys()
            .copied()
            .filter(|index| !self.wanted.get(*index))
            .collect();
        let mut cancels = Vec::new();
        for index in &dropped {
            if let Some(piece) = self.active.remove(index) {
                for (slot, state) in piece.blocks.iter().enumerate() {
                    if let BlockState::Requested { peer, .. } = state {
                        cancels.push((*peer, *index, slot * BLOCK_SIZE));
                    }
                }
            }
            self.endgame_extras.retain(|key, _| key.0 != *index);
        }
        self.wanted_missing = (0..self.piece_count)
            .filter(|&index| self.wanted.get(index) && !self.have.get(index))
            .count();
        (cancels, dropped)
    }

    /// Wanted pieces that are neither active nor verified.
    fn inactive_unverified(&self) -> usize {
        self.wanted_missing.saturating_sub(self.active.len())
    }

    pub fn is_endgame(&self) -> bool {
        if self.inactive_unverified() != 0 || self.active.is_empty() {
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
        if !self.have.get(index) && self.wanted.get(index) {
            self.wanted_missing = self.wanted_missing.saturating_sub(1);
        }
        self.active.remove(&index);
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
                    self.wanted.get(index)
                        && bitfield.get(index)
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
        // High priority pieces first, rarest first inside each class.
        let mut best: Option<(u8, u32, usize)> = None;
        for index in 0..self.piece_count {
            if !self.wanted.get(index)
                || !bitfield.get(index)
                || self.have.get(index)
                || self.active.contains_key(&index)
            {
                continue;
            }
            let class = if self.high.get(index) { 0 } else { 1 };
            let rank = (class, self.availability[index]);
            match best {
                Some((best_class, best_availability, _))
                    if rank >= (best_class, best_availability) => {}
                _ => best = Some((class, self.availability[index], index)),
            }
        }
        best.map(|(_, _, index)| index)
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

    fn set_wanted(picker: &mut PiecePicker, wanted: &[usize], high: &[usize]) {
        let mut wanted_bits = Bitfield::new(4);
        for &index in wanted {
            wanted_bits.set(index).unwrap();
        }
        let mut high_bits = Bitfield::new(4);
        for &index in high {
            high_bits.set(index).unwrap();
        }
        picker.set_wanted(&wanted_bits, &high_bits);
    }

    #[test]
    fn requests_only_wanted_pieces() {
        let mut picker = picker(0, 8);
        let full = bitfield_of(&[0, 1, 2, 3]);
        picker.add_peer(&full);
        set_wanted(&mut picker, &[1, 2], &[]);
        let peer = addr("1.1.1.1");
        let mut picked = Vec::new();
        while let Some((index, _, _)) = picker.next_block(peer, &full) {
            picked.push(index);
        }
        assert!(
            picked.iter().all(|index| *index == 1 || *index == 2),
            "skipped pieces are never requested: {picked:?}"
        );
        assert_eq!(
            picked.len(),
            4,
            "both wanted pieces are fully requested: {picked:?}"
        );
        assert!(!picker.is_complete());
        picker.mark_have(1);
        picker.mark_have(2);
        assert!(
            picker.is_complete(),
            "completion only requires the wanted pieces"
        );
    }

    #[test]
    fn high_pieces_are_requested_before_normal_and_rarest_first_inside_a_class() {
        let mut picker = PiecePicker::new(4, PIECE_LENGTH, 4 * PIECE_LENGTH as u64, 0, 8);
        // Piece 0: High with two peers. Piece 1: High with one peer (rarest).
        // Piece 2: Normal with one peer. Piece 3: unwanted.
        let a = bitfield_of(&[0, 1, 3]);
        let b = bitfield_of(&[0, 2, 3]);
        picker.add_peer(&a);
        picker.add_peer(&b);
        let mut wanted = Bitfield::new(4);
        wanted.set(0).unwrap();
        wanted.set(1).unwrap();
        wanted.set(2).unwrap();
        let mut high = Bitfield::new(4);
        high.set(0).unwrap();
        high.set(1).unwrap();
        picker.set_wanted(&wanted, &high);
        let peer = addr("1.1.1.1");
        assert_eq!(
            picker.next_block(peer, &a),
            Some((1, 0, BLOCK_SIZE)),
            "rarest High piece first"
        );
        picker.mark_have(1);
        assert_eq!(
            picker.next_block(peer, &a),
            Some((0, 0, BLOCK_SIZE)),
            "then the remaining High piece"
        );
        picker.mark_have(0);
        assert_eq!(
            picker.next_block(addr("2.2.2.2"), &b),
            Some((2, 0, BLOCK_SIZE)),
            "Normal pieces only after the High class is exhausted"
        );
    }

    #[test]
    fn set_wanted_cancels_in_flight_blocks_of_unwanted_pieces() {
        let mut picker = picker(0, 8);
        let full = bitfield_of(&[0, 1, 2, 3]);
        picker.add_peer(&full);
        let peer = addr("1.1.1.1");
        assert_eq!(picker.next_block(peer, &full), Some((0, 0, BLOCK_SIZE)));
        assert_eq!(
            picker.next_block(peer, &full),
            Some((0, BLOCK_SIZE, BLOCK_SIZE))
        );
        let mut keep = Bitfield::new(4);
        for index in [1usize, 2, 3] {
            keep.set(index).unwrap();
        }
        let (cancels, dropped) = picker.set_wanted(&keep, &Bitfield::new(4));
        assert_eq!(dropped, vec![0]);
        assert_eq!(
            cancels,
            vec![(peer, 0, 0), (peer, 0, BLOCK_SIZE)],
            "every in-flight block of the dropped piece is cancelled"
        );
        assert!(picker.next_block(peer, &full).is_some());
    }

    #[test]
    fn random_first_only_picks_wanted_pieces() {
        let mut picker = picker(4, 8);
        let full = bitfield_of(&[0, 1, 2, 3]);
        picker.add_peer(&full);
        set_wanted(&mut picker, &[1, 3], &[]);
        let peer = addr("1.1.1.1");
        let first = picker.next_block(peer, &full).unwrap();
        assert!(first.0 == 1 || first.0 == 3);
    }

    #[test]
    fn newly_wanted_pieces_start_without_touching_verified_state() {
        let mut picker = picker(0, 8);
        let full = bitfield_of(&[0, 1, 2, 3]);
        picker.add_peer(&full);
        set_wanted(&mut picker, &[0, 1], &[]);
        picker.mark_have(0);
        assert!(!picker.is_complete());
        // The user re-enables pieces 2 and 3; verified piece 0 stays have.
        set_wanted(&mut picker, &[0, 1, 2, 3], &[]);
        assert!(picker.have().get(0));
        assert!(!picker.is_complete());
        picker.mark_have(1);
        picker.mark_have(2);
        picker.mark_have(3);
        assert!(picker.is_complete());
        assert_eq!(picker.have().count(), 4);
    }
}
