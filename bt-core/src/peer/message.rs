use crate::error::MessageError;

use super::bitfield::Bitfield;

pub const MAX_MESSAGE_LENGTH: usize = 1024 * 1024;

const CHOKE: u8 = 0;
const UNCHOKE: u8 = 1;
const INTERESTED: u8 = 2;
const NOT_INTERESTED: u8 = 3;
const HAVE: u8 = 4;
const BITFIELD: u8 = 5;
const REQUEST: u8 = 6;
const PIECE: u8 = 7;
const CANCEL: u8 = 8;
const PORT: u8 = 9;
pub const EXTENDED: u8 = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have(u32),
    Bitfield(Bitfield),
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
    Port(u16),
    Extended {
        extension_id: u8,
        payload: Vec<u8>,
    },
    Unknown {
        id: u8,
        payload: Vec<u8>,
    },
}

impl Message {
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        match self {
            Message::KeepAlive => {}
            Message::Choke => body.push(CHOKE),
            Message::Unchoke => body.push(UNCHOKE),
            Message::Interested => body.push(INTERESTED),
            Message::NotInterested => body.push(NOT_INTERESTED),
            Message::Have(index) => {
                body.push(HAVE);
                body.extend_from_slice(&index.to_be_bytes());
            }
            Message::Bitfield(bitfield) => {
                body.push(BITFIELD);
                body.extend_from_slice(bitfield.as_raw());
            }
            Message::Request {
                index,
                begin,
                length,
            } => {
                body.push(REQUEST);
                push_index_block(&mut body, *index, *begin, *length);
            }
            Message::Piece {
                index,
                begin,
                block,
            } => {
                body.push(PIECE);
                body.extend_from_slice(&index.to_be_bytes());
                body.extend_from_slice(&begin.to_be_bytes());
                body.extend_from_slice(block);
            }
            Message::Cancel {
                index,
                begin,
                length,
            } => {
                body.push(CANCEL);
                push_index_block(&mut body, *index, *begin, *length);
            }
            Message::Port(port) => {
                body.push(PORT);
                body.extend_from_slice(&port.to_be_bytes());
            }
            Message::Extended {
                extension_id,
                payload,
            } => {
                body.push(EXTENDED);
                body.push(*extension_id);
                body.extend_from_slice(payload);
            }
            Message::Unknown { id, payload } => {
                body.push(*id);
                body.extend_from_slice(payload);
            }
        }
        let mut frame = Vec::with_capacity(4 + body.len());
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(&body);
        frame
    }

    pub fn decode(payload: &[u8], piece_count: Option<usize>) -> Result<Message, MessageError> {
        let (&id, rest) = payload.split_first().ok_or(MessageError::EmptyPayload)?;
        let message = match id {
            CHOKE | UNCHOKE | INTERESTED | NOT_INTERESTED => {
                require_empty(id, rest)?;
                match id {
                    CHOKE => Message::Choke,
                    UNCHOKE => Message::Unchoke,
                    INTERESTED => Message::Interested,
                    _ => Message::NotInterested,
                }
            }
            HAVE => Message::Have(fixed_u32(id, rest)?),
            BITFIELD => match piece_count {
                Some(count) => Message::Bitfield(Bitfield::from_bytes(rest, count)?),
                None => Message::Bitfield(Bitfield::from_bytes_unchecked(rest)),
            },
            REQUEST => {
                let (index, begin, length) = index_block(id, rest)?;
                Message::Request {
                    index,
                    begin,
                    length,
                }
            }
            PIECE => {
                if rest.len() < 8 {
                    return Err(MessageError::InvalidPayloadLength(id, rest.len()));
                }
                Message::Piece {
                    index: be_u32(&rest[0..4]),
                    begin: be_u32(&rest[4..8]),
                    block: rest[8..].to_vec(),
                }
            }
            CANCEL => {
                let (index, begin, length) = index_block(id, rest)?;
                Message::Cancel {
                    index,
                    begin,
                    length,
                }
            }
            PORT => {
                if rest.len() != 2 {
                    return Err(MessageError::InvalidPayloadLength(PORT, rest.len()));
                }
                Message::Port(u16::from_be_bytes([rest[0], rest[1]]))
            }
            EXTENDED => {
                if rest.is_empty() {
                    return Err(MessageError::InvalidPayloadLength(EXTENDED, 0));
                }
                Message::Extended {
                    extension_id: rest[0],
                    payload: rest[1..].to_vec(),
                }
            }
            other => Message::Unknown {
                id: other,
                payload: rest.to_vec(),
            },
        };
        Ok(message)
    }
}

fn push_index_block(body: &mut Vec<u8>, index: u32, begin: u32, length: u32) {
    body.extend_from_slice(&index.to_be_bytes());
    body.extend_from_slice(&begin.to_be_bytes());
    body.extend_from_slice(&length.to_be_bytes());
}

fn require_empty(id: u8, rest: &[u8]) -> Result<(), MessageError> {
    if rest.is_empty() {
        Ok(())
    } else {
        Err(MessageError::InvalidPayloadLength(id, rest.len()))
    }
}

fn fixed_u32(id: u8, rest: &[u8]) -> Result<u32, MessageError> {
    if rest.len() != 4 {
        return Err(MessageError::InvalidPayloadLength(id, rest.len()));
    }
    Ok(be_u32(rest))
}

fn index_block(id: u8, rest: &[u8]) -> Result<(u32, u32, u32), MessageError> {
    if rest.len() != 12 {
        return Err(MessageError::InvalidPayloadLength(id, rest.len()));
    }
    Ok((
        be_u32(&rest[0..4]),
        be_u32(&rest[4..8]),
        be_u32(&rest[8..12]),
    ))
}

fn be_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::BitfieldError;

    #[test]
    fn round_trips_every_message() {
        let mut bitfield = Bitfield::new(24);
        bitfield.set(0).unwrap();
        bitfield.set(23).unwrap();
        let messages = [
            Message::Choke,
            Message::Unchoke,
            Message::Interested,
            Message::NotInterested,
            Message::Have(7),
            Message::Bitfield(bitfield),
            Message::Request {
                index: 3,
                begin: 16384,
                length: 16384,
            },
            Message::Piece {
                index: 3,
                begin: 0,
                block: vec![1, 2, 3],
            },
            Message::Piece {
                index: 9,
                begin: 4096,
                block: Vec::new(),
            },
            Message::Cancel {
                index: 3,
                begin: 16384,
                length: 16384,
            },
            Message::Port(6881),
            Message::Extended {
                extension_id: 2,
                payload: b"ext".to_vec(),
            },
            Message::Unknown {
                id: 21,
                payload: b"ext".to_vec(),
            },
        ];
        for message in messages {
            let frame = message.encode();
            let (prefix, body) = frame.split_at(4);
            assert_eq!(
                u32::from_be_bytes(prefix.try_into().unwrap()) as usize,
                body.len()
            );
            assert_eq!(Message::decode(body, Some(24)).unwrap(), message);
        }
    }

    #[test]
    fn encodes_keep_alive_frame() {
        assert_eq!(Message::KeepAlive.encode(), vec![0, 0, 0, 0]);
    }

    #[test]
    fn decodes_extended_id() {
        assert_eq!(
            Message::decode(&[20, 2, 9, 9], Some(16)).unwrap(),
            Message::Extended {
                extension_id: 2,
                payload: vec![9, 9]
            }
        );
        assert!(matches!(
            Message::decode(&[20], Some(16)),
            Err(MessageError::InvalidPayloadLength(20, 0))
        ));
    }

    #[test]
    fn decodes_unknown_ids_as_unknown() {
        assert_eq!(
            Message::decode(&[21, 1, 2, 3], Some(16)).unwrap(),
            Message::Unknown {
                id: 21,
                payload: vec![1, 2, 3]
            }
        );
        assert_eq!(
            Message::decode(&[27], Some(16)).unwrap(),
            Message::Unknown {
                id: 27,
                payload: Vec::new()
            }
        );
    }

    #[test]
    fn rejects_malformed_payloads() {
        assert!(matches!(
            Message::decode(&[], Some(16)),
            Err(MessageError::EmptyPayload)
        ));
        assert!(matches!(
            Message::decode(&[CHOKE, 1], Some(16)),
            Err(MessageError::InvalidPayloadLength(CHOKE, 1))
        ));
        assert!(matches!(
            Message::decode(&[HAVE, 1, 2, 3], Some(16)),
            Err(MessageError::InvalidPayloadLength(HAVE, 3))
        ));
        assert!(matches!(
            Message::decode(&[REQUEST, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], Some(16)),
            Err(MessageError::InvalidPayloadLength(REQUEST, 11))
        ));
        assert!(matches!(
            Message::decode(&[PIECE, 1, 2, 3], Some(16)),
            Err(MessageError::InvalidPayloadLength(PIECE, 3))
        ));
        assert!(matches!(
            Message::decode(&[PORT, 1], Some(16)),
            Err(MessageError::InvalidPayloadLength(PORT, 1))
        ));
    }

    #[test]
    fn rejects_bitfield_with_spare_bits() {
        assert!(matches!(
            Message::decode(&[BITFIELD, 0x80, 0x01], Some(10)),
            Err(MessageError::Bitfield(BitfieldError::SpareBitsSet))
        ));
        assert!(matches!(
            Message::decode(&[BITFIELD, 0x80], Some(10)),
            Err(MessageError::Bitfield(BitfieldError::InvalidLength(1, 2)))
        ));
    }

    #[test]
    fn body_length_matches_frame_prefix() {
        let frame = Message::Piece {
            index: 0,
            begin: 0,
            block: vec![0xAB; 64],
        }
        .encode();
        assert_eq!(frame.len(), 4 + 9 + 64);
        assert_eq!(&frame[..4], &[0, 0, 0, 73]);
    }
}
