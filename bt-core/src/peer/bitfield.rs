use crate::error::BitfieldError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bitfield {
    bytes: Vec<u8>,
    piece_count: usize,
}

impl Bitfield {
    pub fn new(piece_count: usize) -> Bitfield {
        Bitfield {
            bytes: vec![0; piece_count.div_ceil(8)],
            piece_count,
        }
    }

    pub fn from_bytes(bytes: &[u8], piece_count: usize) -> Result<Bitfield, BitfieldError> {
        let expected = piece_count.div_ceil(8);
        if bytes.len() != expected {
            return Err(BitfieldError::InvalidLength(bytes.len(), expected));
        }
        if let Some(&last) = bytes.last() {
            if last & !valid_mask(piece_count) != 0 {
                return Err(BitfieldError::SpareBitsSet);
            }
        }
        Ok(Bitfield {
            bytes: bytes.to_vec(),
            piece_count,
        })
    }

    pub fn get(&self, index: usize) -> bool {
        match self.bytes.get(index / 8) {
            Some(&byte) => byte & mask(index % 8) != 0,
            None => false,
        }
    }

    pub fn set(&mut self, index: usize) -> Result<(), BitfieldError> {
        if index >= self.piece_count {
            return Err(BitfieldError::IndexOutOfRange(index));
        }
        self.bytes[index / 8] |= mask(index % 8);
        Ok(())
    }

    pub fn count(&self) -> usize {
        self.bytes
            .iter()
            .map(|byte| byte.count_ones() as usize)
            .sum()
    }

    pub fn piece_count(&self) -> usize {
        self.piece_count
    }

    pub fn is_complete(&self) -> bool {
        self.count() == self.piece_count
    }

    pub fn as_raw(&self) -> &[u8] {
        &self.bytes
    }
}

fn mask(offset: usize) -> u8 {
    0x80 >> offset
}

fn valid_mask(piece_count: usize) -> u8 {
    let remainder = piece_count % 8;
    if remainder == 0 {
        0xFF
    } else {
        u8::MAX << (8 - remainder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_pieces() {
        let mut bitfield = Bitfield::new(10);
        assert_eq!(bitfield.count(), 0);
        assert!(!bitfield.get(0));
        bitfield.set(0).unwrap();
        bitfield.set(9).unwrap();
        assert!(bitfield.get(0));
        assert!(bitfield.get(9));
        assert!(!bitfield.get(8));
        assert_eq!(bitfield.count(), 2);
        assert_eq!(bitfield.as_raw(), &[0x80, 0x40]);
    }

    #[test]
    fn rejects_out_of_range_set() {
        let mut bitfield = Bitfield::new(10);
        assert!(matches!(
            bitfield.set(10),
            Err(BitfieldError::IndexOutOfRange(10))
        ));
        assert!(!bitfield.get(999));
    }

    #[test]
    fn rejects_wrong_length_bytes() {
        assert!(matches!(
            Bitfield::from_bytes(&[0x80], 10),
            Err(BitfieldError::InvalidLength(1, 2))
        ));
    }

    #[test]
    fn rejects_spare_bits() {
        assert!(matches!(
            Bitfield::from_bytes(&[0x80, 0x01], 10),
            Err(BitfieldError::SpareBitsSet)
        ));
        assert!(Bitfield::from_bytes(&[0x80, 0x40], 10).is_ok());
        assert!(Bitfield::from_bytes(&[], 0).is_ok());
    }

    #[test]
    fn round_trips_through_bytes() {
        let mut original = Bitfield::new(19);
        for index in [0, 5, 18] {
            original.set(index).unwrap();
        }
        let decoded = Bitfield::from_bytes(original.as_raw(), 19).unwrap();
        assert_eq!(decoded, original);
        assert_eq!(decoded.count(), 3);
    }

    #[test]
    fn detects_complete_bitfield() {
        let mut bitfield = Bitfield::new(8);
        for index in 0..8 {
            bitfield.set(index).unwrap();
        }
        assert!(bitfield.is_complete());
        bitfield.set(7).unwrap();
        assert_eq!(bitfield.count(), 8);
    }
}
