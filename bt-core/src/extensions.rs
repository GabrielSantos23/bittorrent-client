use crate::bencode::{self, Value};
use crate::error::BencodeError;
use thiserror::Error;

pub const EXTENDED_MESSAGE_ID: u8 = 20;
pub const EXTENSION_HANDSHAKE_ID: u8 = 0;
pub const LOCAL_UT_METADATA_ID: u8 = 1;
pub const METADATA_PIECE_SIZE: usize = 16 * 1024;
pub const MAX_METADATA_SIZE: u64 = 10 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum ExtensionError {
    #[error("extension payload is not a bencoded dictionary: {0}")]
    Bencode(#[from] BencodeError),
    #[error("extension dict key '{0}' has an unexpected type")]
    WrongType(&'static str),
}

pub fn reserved_with_extensions() -> [u8; 8] {
    let mut reserved = [0u8; 8];
    reserved[5] |= 0x10;
    reserved
}

pub fn supports_extensions(reserved: &[u8; 8]) -> bool {
    reserved[5] & 0x10 != 0
}

pub fn with_dht_bit(mut reserved: [u8; 8]) -> [u8; 8] {
    reserved[7] |= 0x01;
    reserved
}

pub fn supports_dht(reserved: &[u8; 8]) -> bool {
    reserved[7] & 0x01 != 0
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
            ut_metadata: Some(LOCAL_UT_METADATA_ID),
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
    bencode::encode(&Value::Dict(root))
}

pub fn decode_extension_handshake(payload: &[u8]) -> Result<ExtensionHandshake, ExtensionError> {
    let value = bencode::decode(payload)?;
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

pub fn encode_ut_metadata(message: &UtMetadata) -> Vec<u8> {
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
    let mut payload = bencode::encode(&Value::Dict(dict));
    if let Some(data) = data {
        payload.extend_from_slice(data);
    }
    payload
}

pub fn decode_ut_metadata(payload: &[u8]) -> Result<UtMetadata, ExtensionError> {
    let (value, dict_end) = bencode::decode_prefix(payload)?;
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
    let data = &payload[dict_end..];
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
    use crate::peer::Message;

    #[test]
    fn reserved_bit_round_trips() {
        let reserved = reserved_with_extensions();
        assert!(supports_extensions(&reserved));
        assert!(!supports_extensions(&[0u8; 8]));
    }

    #[test]
    fn dht_reserved_bit_round_trips() {
        let reserved = with_dht_bit(reserved_with_extensions());
        assert!(supports_extensions(&reserved));
        assert!(supports_dht(&reserved));
        assert!(!supports_dht(&reserved_with_extensions()));
        assert!(!supports_dht(&[0u8; 8]));
        assert!(supports_dht(&with_dht_bit([0u8; 8])));
        let mut other_bits = [0u8; 8];
        other_bits[7] = 0x02;
        assert!(supports_dht(&with_dht_bit(other_bits)));
        assert_eq!(with_dht_bit(other_bits)[7], 0x03);
    }

    #[test]
    fn handshake_encodes_the_spec_example_bytes() {
        let handshake = ExtensionHandshake {
            ut_metadata: Some(2),
            metadata_size: Some(25_364),
            client: None,
        };
        let payload = encode_extension_handshake(&handshake);
        assert_eq!(
            payload,
            b"d1:md11:ut_metadatai2ee13:metadata_sizei25364ee".to_vec()
        );
    }

    #[test]
    fn handshake_with_client_matches_spec_shape() {
        let handshake = ExtensionHandshake {
            ut_metadata: Some(1),
            metadata_size: None,
            client: Some("bt-core/0.1.0".to_string()),
        };
        let payload = encode_extension_handshake(&handshake);
        assert_eq!(
            payload,
            b"d1:md11:ut_metadatai1ee1:v13:bt-core/0.1.0e".to_vec()
        );
    }

    #[test]
    fn handshake_decodes_a_spec_shaped_dictionary() {
        let payload = b"d1:md11:ut_metadatai1e7:\xc2\xb5T_PEXi2ee13:metadata_sizei25364e1:pi6881e1:v13:\xc2\xb5Torrent 1.2e";
        let handshake = decode_extension_handshake(payload).unwrap();
        assert_eq!(handshake.ut_metadata, Some(1));
        assert_eq!(handshake.metadata_size, Some(25_364));
        assert_eq!(handshake.client.as_deref(), Some("\u{b5}Torrent 1.2"));
    }

    #[test]
    fn handshake_round_trip() {
        let handshake = ExtensionHandshake {
            ut_metadata: Some(3),
            metadata_size: Some(48_000),
            client: Some("bt-core/test".to_string()),
        };
        let payload = encode_extension_handshake(&handshake);
        assert_eq!(payload[0], b'd');
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
    fn handshake_rejects_empty_and_garbage() {
        assert!(decode_extension_handshake(&[]).is_err());
        assert!(decode_extension_handshake(&[0, b'd', b'e']).is_err());
        assert!(decode_extension_handshake(b"dx").is_err());
    }

    #[test]
    fn metadata_request_matches_the_bep9_example_bytes() {
        let payload = encode_ut_metadata(&UtMetadata::Request { piece: 0 });
        assert_eq!(payload, b"d8:msg_typei0e5:piecei0ee".to_vec());
    }

    #[test]
    fn metadata_reject_matches_the_bep9_msg_type_value() {
        let payload = encode_ut_metadata(&UtMetadata::Reject { piece: 0 });
        assert_eq!(payload, b"d8:msg_typei2e5:piecei0ee".to_vec());
    }

    #[test]
    fn metadata_data_matches_the_bep9_example_bytes() {
        let data = b"xxxxxxxx".to_vec();
        let payload = encode_ut_metadata(&UtMetadata::Data {
            piece: 0,
            total_size: 3425,
            data: data.clone(),
        });
        let mut expected = b"d8:msg_typei1e5:piecei0e10:total_sizei3425ee".to_vec();
        expected.extend_from_slice(&data);
        assert_eq!(payload, expected);
    }

    #[test]
    fn metadata_data_round_trip_with_binary_payload() {
        let data = vec![0u8, 1, 0xFF, b'e', b'd', 2];
        let message = UtMetadata::Data {
            piece: 3,
            total_size: METADATA_PIECE_SIZE as u64 + 5,
            data: data.clone(),
        };
        let payload = encode_ut_metadata(&message);
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
        let payload = encode_ut_metadata(&message);
        let decoded = decode_ut_metadata(&payload).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn decode_rejects_empty_and_wrong_types() {
        assert!(decode_ut_metadata(&[]).is_err());
        assert!(decode_ut_metadata(&[0, b'd', b'e']).is_err());
        assert!(decode_ut_metadata(b"1:x").is_err());
    }

    #[test]
    fn extended_frame_carries_the_message_id_exactly_once() {
        let payload = encode_ut_metadata(&UtMetadata::Request { piece: 0 });
        let frame = Message::Extended {
            extension_id: 7,
            payload,
        }
        .encode();
        assert_eq!(&frame[..6], &[0, 0, 0, 0x1B, EXTENDED_MESSAGE_ID, 7]);
        assert_eq!(frame[6], b'd');

        let message = Message::decode(&frame[4..], None).unwrap();
        let Message::Extended {
            extension_id,
            payload,
        } = message
        else {
            panic!("expected extended message");
        };
        assert_eq!(extension_id, 7);
        assert_eq!(
            decode_ut_metadata(&payload).unwrap(),
            UtMetadata::Request { piece: 0 }
        );
    }

    #[test]
    fn extended_handshake_frame_carries_id_zero_before_the_dict() {
        let payload = encode_extension_handshake(&ExtensionHandshake {
            ut_metadata: Some(LOCAL_UT_METADATA_ID),
            ..Default::default()
        });
        let frame = Message::Extended {
            extension_id: EXTENSION_HANDSHAKE_ID,
            payload,
        }
        .encode();
        assert_eq!(&frame[..6], &[0, 0, 0, 0x1A, EXTENDED_MESSAGE_ID, 0]);
        assert_eq!(frame[6], b'd');

        let message = Message::decode(&frame[4..], None).unwrap();
        let Message::Extended {
            extension_id,
            payload,
        } = message
        else {
            panic!("expected extended message");
        };
        assert_eq!(extension_id, EXTENSION_HANDSHAKE_ID);
        assert_eq!(
            decode_extension_handshake(&payload).unwrap().ut_metadata,
            Some(LOCAL_UT_METADATA_ID)
        );
    }
}
