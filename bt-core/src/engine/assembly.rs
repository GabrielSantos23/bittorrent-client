use std::collections::HashMap;

pub const BLOCK_SIZE: usize = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockOutcome {
    Accepted,
    Completed,
    Duplicate,
    Unexpected,
}

struct PieceBuffer {
    data: Vec<u8>,
    received: Vec<bool>,
    remaining: usize,
}

pub struct PieceAssembler {
    piece_length: u32,
    total_length: u64,
    pieces: HashMap<usize, PieceBuffer>,
}

impl PieceAssembler {
    pub fn new(piece_length: u32, total_length: u64) -> PieceAssembler {
        PieceAssembler {
            piece_length,
            total_length,
            pieces: HashMap::new(),
        }
    }

    pub fn piece_size(&self, index: usize) -> usize {
        let start = index as u64 * self.piece_length as u64;
        (self.total_length - start).min(self.piece_length as u64) as usize
    }

    pub fn open(&mut self, index: usize) {
        let size = self.piece_size(index);
        self.pieces.entry(index).or_insert_with(|| PieceBuffer {
            data: vec![0; size],
            received: vec![false; size.div_ceil(BLOCK_SIZE)],
            remaining: size.div_ceil(BLOCK_SIZE),
        });
    }

    pub fn is_open(&self, index: usize) -> bool {
        self.pieces.contains_key(&index)
    }

    pub fn write_block(&mut self, index: usize, begin: usize, data: &[u8]) -> BlockOutcome {
        let Some(buffer) = self.pieces.get_mut(&index) else {
            return BlockOutcome::Unexpected;
        };
        if !begin.is_multiple_of(BLOCK_SIZE) || begin >= buffer.data.len() {
            return BlockOutcome::Unexpected;
        }
        let expected = BLOCK_SIZE.min(buffer.data.len() - begin);
        if data.len() != expected {
            return BlockOutcome::Unexpected;
        }
        let block = begin / BLOCK_SIZE;
        if buffer.received[block] {
            return BlockOutcome::Duplicate;
        }
        buffer.data[begin..begin + data.len()].copy_from_slice(data);
        buffer.received[block] = true;
        buffer.remaining -= 1;
        if buffer.remaining == 0 {
            BlockOutcome::Completed
        } else {
            BlockOutcome::Accepted
        }
    }

    pub fn is_complete(&self, index: usize) -> bool {
        self.pieces
            .get(&index)
            .map(|buffer| buffer.remaining == 0)
            .unwrap_or(false)
    }

    /// Discards partial buffers, e.g. for pieces that are no longer wanted.
    pub fn drop_pieces(&mut self, indices: &[usize]) {
        for index in indices {
            self.pieces.remove(index);
        }
    }

    pub fn take(&mut self, index: usize) -> Option<Vec<u8>> {
        let buffer = self.pieces.get(&index)?;
        if buffer.remaining > 0 {
            return None;
        }
        self.pieces.remove(&index).map(|buffer| buffer.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PIECE_LENGTH: u32 = 32768;

    fn assembler(total: u64) -> PieceAssembler {
        PieceAssembler::new(PIECE_LENGTH, total)
    }

    #[test]
    fn assembles_blocks_in_order() {
        let mut asm = assembler(40000);
        asm.open(0);
        assert_eq!(
            asm.write_block(0, 0, &vec![1u8; BLOCK_SIZE]),
            BlockOutcome::Accepted
        );
        assert!(!asm.is_complete(0));
        assert_eq!(
            asm.write_block(0, BLOCK_SIZE, &vec![2u8; BLOCK_SIZE]),
            BlockOutcome::Completed
        );
        let data = asm.take(0).unwrap();
        assert_eq!(data.len(), 32768);
        assert_eq!(&data[..BLOCK_SIZE], &vec![1u8; BLOCK_SIZE][..]);
        assert_eq!(&data[BLOCK_SIZE..], &vec![2u8; BLOCK_SIZE][..]);
    }

    #[test]
    fn validates_last_block_length() {
        let mut asm = assembler(40000);
        asm.open(1);
        assert_eq!(
            asm.write_block(1, 0, &vec![7u8; 7232]),
            BlockOutcome::Completed
        );
        assert_eq!(asm.take(1).unwrap().len(), 7232);
    }

    #[test]
    fn ignores_duplicate_and_unexpected_blocks() {
        let mut asm = assembler(40000);
        assert_eq!(
            asm.write_block(0, 0, &vec![1u8; BLOCK_SIZE]),
            BlockOutcome::Unexpected
        );
        asm.open(0);
        assert_eq!(
            asm.write_block(0, 100, &vec![1u8; BLOCK_SIZE]),
            BlockOutcome::Unexpected
        );
        assert_eq!(asm.write_block(0, 0, &[1u8; 100]), BlockOutcome::Unexpected);
        assert_eq!(
            asm.write_block(0, 0, &vec![1u8; BLOCK_SIZE]),
            BlockOutcome::Accepted
        );
        assert_eq!(
            asm.write_block(0, 0, &vec![1u8; BLOCK_SIZE]),
            BlockOutcome::Duplicate
        );
        assert_eq!(
            asm.write_block(0, 40000, &vec![1u8; BLOCK_SIZE]),
            BlockOutcome::Unexpected
        );
    }

    #[test]
    fn take_requires_completion_and_clears_piece() {
        let mut asm = assembler(40000);
        asm.open(0);
        assert!(asm.take(0).is_none());
        assert_eq!(
            asm.write_block(0, 0, &vec![1u8; BLOCK_SIZE]),
            BlockOutcome::Accepted
        );
        assert_eq!(
            asm.write_block(0, BLOCK_SIZE, &vec![1u8; BLOCK_SIZE]),
            BlockOutcome::Completed
        );
        assert!(asm.take(0).is_some());
        assert!(asm.take(0).is_none());
        assert!(!asm.is_open(0));
        asm.open(0);
        assert!(asm.is_open(0));
    }

    #[test]
    fn reopens_after_taken_piece_fails_verification() {
        let mut asm = assembler(40000);
        asm.open(0);
        asm.write_block(0, 0, &vec![1u8; BLOCK_SIZE]);
        asm.write_block(0, BLOCK_SIZE, &vec![1u8; BLOCK_SIZE]);
        assert!(asm.take(0).is_some());
        assert!(asm.take(0).is_none());
        asm.open(0);
        assert_eq!(
            asm.write_block(0, 0, &vec![9u8; BLOCK_SIZE]),
            BlockOutcome::Accepted
        );
    }

    #[test]
    fn drop_pieces_discards_partial_buffers() {
        let mut asm = assembler(40000);
        asm.open(0);
        asm.write_block(0, 0, &vec![1u8; BLOCK_SIZE]);
        asm.drop_pieces(&[0]);
        assert!(!asm.is_open(0));
        assert_eq!(
            asm.write_block(0, BLOCK_SIZE, &vec![1u8; BLOCK_SIZE]),
            BlockOutcome::Unexpected,
            "blocks for dropped pieces are rejected until the piece is opened again"
        );
        asm.open(0);
        assert!(asm.is_open(0));
    }
}
