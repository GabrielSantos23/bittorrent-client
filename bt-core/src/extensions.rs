use crate::bencode::{self, Value};
use crate::error::BencodeError;
use thiserror::Error;

pub const EXTENDED_MESSAGE_ID: u8 = 20;
pub const EXTENSION_HANDSHAKE_ID: u8 = 0;
pub const METADATA_PIECE_SIZE: usize = 16 * 1024;
pub const MAX_METADATA_SIZE: u64 = 10 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum ExtensionError {
    #[error("extension payload is empty")]
    Empty,
    #[error("extension payload is not a bencoded dictionary: {0}")]
    Bencode(#[from] BencodeError),
    #[error("extension dict key '{0}' has an unexpected type")]
    WrongType(&'static str),
    #[error("extension handshake sub-id {0} is not the handshake id")]
    WrongSubId(u8),
}

pub fn reserved_with_extensions() -> [u8; 8] {
    let mut reserved = [0u8; 8];
    reserved[5] |= 0x10;
    reserved
}

pub fn supports_extensions(reserved: &[u8; 8]) -> bool {
    reserved[5] & 0x10 != 0
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExtensionHandshake {
    pub ut_metadata: Option<u8>,
    pub metadata_size: Option<u64>,
    pub client: Option<String>,
}

impl ExtensionHandshake {
    pub fn with_metadata_size(size: u64) -> ExtensionHandshake {
        ExtensionHandshake {
            ut_metadata: Some(1),
            metadata_size: Some(size),
            client: Some(concat!("bt-core/", env!("CARGO_PKG_VERSION")).to_string()),
        }
    }
}

pub fn encode_extension_handshake(handshake: &ExtensionHandshake) -> Vec<u8> {
    let mut root: std::collections::BTreeMap<Vec<u8>, Value> = std::collections::BTreeMap::new();
    if let Some(ut_metadata) = handshake.ut_metadata {
        let mut m: std::collections::BTreeMap<Vec<u8>, Value> = std::collections::BTreeMap::new();
        m.insert(b"ut_metadata".to_vec(), Value::Int(ut_metadata as i64));
        root.insert(b"m".to_vec(), Value::Dict(m));
    }
    if let Some(size) = handshake.metadata_size {
        root.insert(b"metadata_size".to_vec(), Value::Int(size as i64));
    }
    if let Some(client) = &handshake.client {
        root.insert(b"v".to_vec(), Value::Bytes(client.clone().into_bytes()));
    }
    let mut payload = vec![EXTENSION_HANDSHAKE_ID];
    payload.extend_from_slice(&bencode::encode(&Value::Dict(root)));
    payload
}

pub fn decode_extension_handshake(payload: &[u8]) -> Result<ExtensionHandshake, ExtensionError> {
    let (&sub_id, rest) = payload.split_first().ok_or(ExtensionError::Empty)?;
    if sub_id != EXTENSION_HANDSHAKE_ID {
        return Err(ExtensionError::WrongSubId(sub_id));
    }
    let value = bencode::decode(rest)?;
    let dict = value.as_dict().ok_or(ExtensionError::WrongType("root"))?;
    let ut_metadata = match dict.get(&b"m".to_vec()) {
        Some(m) => m
            .as_dict()
            .and_then(|m| m.get(b"ut_metadata".as_vec()))
            .and_then(Value::as_int)
            .map(|id| id as u8),
        None => None,
    };
    let metadata_size = match dict.get(&b"metadata_size".to_vec()) {
        Some(Value::Int(size)) if *size >= 0 => Some(*size as u64),
        _ => None,
    };
    let client = match dict.get(&b"v".to_vec()) {
        Some(Value::Bytes(bytes)) => Some(String::from_utf8_lossy(bytes).into_owned()),
        _ => None,
    };
    Ok(ExtensionHandshake {
        ut_metadata,
        metadata_size,
        client,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UtMetadata {
    Request {
        piece: u32,
    },
    Data {
        piece: u32,
        total_size: u64,
        data: Vec<u8>,
    },
    Reject {
        piece: u32,
    },
}

pub fn encode_ut_metadata(extension_id: u8, message: &UtMetadata) -> Vec<u8> {
    let (msg_type, piece, total_size, data) = match message {
        UtMetadata::Request { piece } => (0, *piece, None, None),
        UtMetadata::Data {
            piece,
            total_size,
            data,
        } => (1, *piece, Some(*total_size), Some(data)),
        UtMetadata::Reject { piece } => (2, *piece, None, None),
    };
    let mut dict: std::collections::BTreeMap<Vec<u8>, Value> = std::collections::BTreeMap::new();
    dict.insert(b"msg_type".to_vec(), Value::Int(msg_type));
    dict.insert(b"piece".to_vec(), Value::Int(piece as i64));
    if let Some(total_size) = total_size {
        dict.insert(b"total_size".to_vec(), Value::Int(total_size as i64));
    }
    let mut payload = vec![extension_id];
    payload.extend_from_slice(&bencode::encode(&Value::Dict(dict)));
    if let Some(data) = data {
        payload.extend_from_slice(data);
    }
    payload
}

pub fn decode_ut_metadata(payload: &[u8]) -> Result<UtMetadata, ExtensionError> {
    let (&extension_id, rest) = payload.split_first().ok_or(ExtensionError::Empty)?;
    if extension_id == EXTENSION_HANDSHAKE_ID {
        return Err(ExtensionError::WrongSubId(extension_id));
    }
    let (value, dict_end) = bencode::decode_prefix(rest)?;
    let dict = value.as_dict().ok_or(ExtensionError::WrongType("root"))?;
    let msg_type = dict
        .get(b"msg_type".as_vec())
        .and_then(Value::as_int)
        .ok_or(ExtensionError::WrongType("msg_type"))?;
    let piece = dict
        .get(b"piece".as_vec())
        .and_then(Value::as_int)
        .ok_or(ExtensionError::WrongType("piece"))?;
    let piece = u32::try_from(piece).map_err(|_| ExtensionError::WrongType("piece"))?;
    let data = &rest[dict_end..];
    match msg_type {
        0 => Ok(UtMetadata::Request { piece }),
        1 => {
            let total_size = dict
                .get(b"total_size".as_vec())
                .and_then(Value::as_int)
                .ok_or(ExtensionError::WrongType("total_size"))?;
            Ok(UtMetadata::Data {
                piece,
                total_size: total_size as u64,
                data: data.to_vec(),
            })
        }
        2 => Ok(UtMetadata::Reject { piece }),
        _other => Err(ExtensionError::WrongType("msg_type")),
    }
}

pub fn metadata_piece_len(total_size: u64, piece: u32) -> usize {
    let start = piece as u64 * METADATA_PIECE_SIZE as u64;
    if start >= total_size {
        return 0;
    }
    ((total_size - start).min(METADATA_PIECE_SIZE as u64)) as usize
}

pub fn metadata_piece_count(total_size: u64) -> u32 {
    total_size.div_ceil(METADATA_PIECE_SIZE as u64) as u32
}

pub fn sha1(data: &[u8]) -> [u8; 20] {
    use sha1::{Digest, Sha1};
    Sha1::digest(data).into()
}

trait VecAsBytes {
    fn as_vec(&self) -> &[u8];
}

impl VecAsBytes for [u8] {
    fn as_vec(&self) -> &[u8] {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_bit_round_trips() {
        let reserved = reserved_with_extensions();
        assert!(supports_extensions(&reserved));
        assert!(!supports_extensions(&[0u8; 8]));
    }

    #[test]
    fn handshake_round_trip() {
        let handshake = ExtensionHandshake {
            ut_metadata: Some(3),
            metadata_size: Some(48_000),
            client: Some("bt-core/test".to_string()),
        };
        let payload = encode_extension_handshake(&handshake);
        assert_eq!(payload[0], EXTENSION_HANDSHAKE_ID);
        assert_eq!(decode_extension_handshake(&payload).unwrap(), handshake);
    }

    #[test]
    fn handshake_with_defaults_only_ut_metadata() {
        let handshake = ExtensionHandshake {
            ut_metadata: Some(2),
            ..Default::default()
        };
        let payload = encode_extension_handshake(&handshake);
        assert_eq!(decode_extension_handshake(&payload).unwrap(), handshake);
    }

    #[test]
    fn handshake_rejects_wrong_subid_and_garbage() {
        assert!(matches!(
            decode_extension_handshake(&[]).unwrap_err(),
            ExtensionError::Empty
        ));
        assert!(matches!(
            decode_extension_handshake(&[5, b'd', b'e']).unwrap_err(),
            ExtensionError::WrongSubId(5)
        ));
        assert!(decode_extension_handshake(&[0, b'x']).is_err());
    }

    #[test]
    fn metadata_request_round_trip() {
        let payload = encode_ut_metadata(2, &UtMetadata::Request { piece: 7 });
        assert_eq!(
            decode_ut_metadata(&payload).unwrap(),
            UtMetadata::Request { piece: 7 }
        );
    }

    #[test]
    fn metadata_reject_round_trip() {
        let payload = encode_ut_metadata(2, &UtMetadata::Reject { piece: 0 });
        assert_eq!(
            decode_ut_metadata(&payload).unwrap(),
            UtMetadata::Reject { piece: 0 }
        );
    }

    #[test]
    fn metadata_data_round_trip_with_binary_payload() {
        let data = vec![0u8, 1, 0xFF, b'e', b'd', 2];
        let message = UtMetadata::Data {
            piece: 3,
            total_size: METADATA_PIECE_SIZE as u64 + 5,
            data: data.clone(),
        };
        let payload = encode_ut_metadata(4, &message);
        assert_eq!(payload[0], 4);
        assert_eq!(decode_ut_metadata(&payload).unwrap(), message);
    }

    #[test]
    fn metadata_data_size_independent_of_dict() {
        let data = vec![b'x'; METADATA_PIECE_SIZE];
        let message = UtMetadata::Data {
            piece: 1,
            total_size: 200_000,
            data,
        };
        let payload = encode_ut_metadata(1, &message);
        let decoded = decode_ut_metadata(&payload).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn decode_rejects_empty_and_wrong_types() {
        assert!(matches!(
            decode_ut_metadata(&[]).unwrap_err(),
            ExtensionError::Empty
        ));
        assert!(matches!(
            decode_ut_metadata(&[0, b'd', b'e']).unwrap_err(),
            ExtensionError::WrongSubId(0)
        ));
        assert!(decode_ut_metadata(&[1, b'd']).is_err());
    }
}
