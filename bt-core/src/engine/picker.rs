use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use rand::Rng;
use tokio::time::Instant as TokioInstant;

use crate::peer::Bitfield;

use super::BLOCK_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockState {
    Missing,
    Requested { peer: SocketAddr, at: TokioInstant },
    Received { peer: SocketAddr },
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
}

impl PiecePicker {
    pub fn new(
        piece_count: usize,
        piece_length: u32,
        total_length: u64,
        random_first: usize,
        max_active: usize,
    ) -> PiecePicker {
        PiecePicker {
            piece_count,
            piece_length,
            total_length,
            random_first,
            max_active,
            availability: vec![0; piece_count],
            have: Bitfield::new(piece_count),
            active: HashMap::new(),
        }
    }

    pub fn have(&self) -> &Bitfield {
        &self.have
    }

    pub fn is_complete(&self) -> bool {
        self.have.count() == self.piece_count
    }

    pub fn set_have(&mut self, have: &Bitfield) {
        for index in 0..self.piece_count {
            if have.get(index) {
                let _ = self.have.set(index);
            }
        }
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
        let _ = self.have.set(index);
        self.active.remove(&index);
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

    pub fn block_received(&mut self, index: usize, begin: usize, peer: SocketAddr) {
        if let Some(piece) = self.active.get_mut(&index) {
            let block = begin / BLOCK_SIZE;
            if let Some(state) = piece.blocks.get_mut(block) {
                if matches!(state, BlockState::Requested { .. }) {
                    *state = BlockState::Received { peer };
                }
            }
        }
    }

    pub fn return_blocks(&mut self, peer: SocketAddr) {
        for piece in self.active.values_mut() {
            for state in piece.blocks.iter_mut() {
                if matches!(state, BlockState::Requested { peer: held, .. } if *held == peer) {
                    *state = BlockState::Missing;
                }
            }
        }
    }

    pub fn reap_stale(&mut self, timeout: Duration, now: TokioInstant) -> Vec<(SocketAddr, usize)> {
        let mut reaped: HashMap<SocketAddr, usize> = HashMap::new();
        for piece in self.active.values_mut() {
            for state in piece.blocks.iter_mut() {
                if let BlockState::Requested { peer, at } = state {
                    if now.duration_since(*at) >= timeout {
                        *reaped.entry(*peer).or_insert(0) += 1;
                        *state = BlockState::Missing;
                    }
                }
            }
        }
        reaped.into_iter().collect()
    }

    pub fn requeue_piece(&mut self, index: usize) {
        if let Some(piece) = self.active.get_mut(&index) {
            for state in piece.blocks.iter_mut() {
                *state = BlockState::Missing;
            }
        }
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
}
