use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use crate::bencode::{self, Value};
use crate::dht::node_id::{NodeId, RandomBytes, SystemRandom, NODE_ID_LENGTH};
use thiserror::Error;

pub const MAX_DATAGRAM_SIZE: usize = 2048;
pub const TRANSACTION_ID_LENGTH: usize = 2;
pub const NODE_INFO_LENGTH: usize = NODE_ID_LENGTH + 6;
pub const PEER_INFO_LENGTH: usize = 6;

#[derive(Debug, Error)]
pub enum KrpcError {
    #[error("datagram of {0} bytes exceeds the 2048 byte limit")]
    DatagramTooLarge(usize),
    #[error("bencode error: {0}")]
    Bencode(#[from] crate::error::BencodeError),
    #[error("datagram is not a bencoded dictionary")]
    NotADictionary,
    #[error("message key '{0}' has an unexpected type")]
    WrongType(&'static str),
    #[error("message is missing key '{0}'")]
    MissingKey(&'static str),
    #[error("unknown query method '{0}'")]
    UnknownMethod(String),
    #[error("nodes string of {0} bytes is not a multiple of {NODE_INFO_LENGTH}")]
    InvalidNodes(usize),
    #[error("peer entry of {0} bytes is not {PEER_INFO_LENGTH} bytes")]
    InvalidPeerEntry(usize),
    #[error("node info entry must be {NODE_INFO_LENGTH} bytes, got {0}")]
    InvalidNodeInfo(usize),
    #[error("port {0} is outside the valid range")]
    InvalidPort(i64),
    #[error("message direction must be one byte, got {0}")]
    InvalidDirection(usize),
    #[error("encoded message of {0} bytes exceeds the 2048 byte limit")]
    EncodedTooLarge(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionId([u8; TRANSACTION_ID_LENGTH]);

impl TransactionId {
    pub const fn from_bytes(bytes: [u8; TRANSACTION_ID_LENGTH]) -> TransactionId {
        TransactionId(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; TRANSACTION_ID_LENGTH] {
        &self.0
    }

    pub fn random(source: &dyn RandomBytes) -> TransactionId {
        let mut bytes = [0u8; TRANSACTION_ID_LENGTH];
        source.fill(&mut bytes);
        TransactionId(bytes)
    }
}

pub fn random_transaction_id() -> TransactionId {
    TransactionId::random(&SystemRandom)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeInfo {
    pub id: NodeId,
    pub addr: SocketAddrV4,
}

impl NodeInfo {
    pub fn new(id: NodeId, addr: SocketAddrV4) -> NodeInfo {
        NodeInfo { id, addr }
    }

    pub fn encode(&self) -> [u8; NODE_INFO_LENGTH] {
        let mut out = [0u8; NODE_INFO_LENGTH];
        out[..NODE_ID_LENGTH].copy_from_slice(self.id.as_bytes());
        out[NODE_ID_LENGTH..NODE_ID_LENGTH + 4].copy_from_slice(&self.addr.ip().octets());
        out[NODE_ID_LENGTH + 4..].copy_from_slice(&self.addr.port().to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<NodeInfo, KrpcError> {
        if bytes.len() != NODE_INFO_LENGTH {
            return Err(KrpcError::InvalidNodeInfo(bytes.len()));
        }
        let mut id = [0u8; NODE_ID_LENGTH];
        id.copy_from_slice(&bytes[..NODE_ID_LENGTH]);
        let mut ip = [0u8; 4];
        ip.copy_from_slice(&bytes[NODE_ID_LENGTH..NODE_ID_LENGTH + 4]);
        let mut port = [0u8; 2];
        port.copy_from_slice(&bytes[NODE_ID_LENGTH + 4..]);
        Ok(NodeInfo {
            id: NodeId::from_bytes(id),
            addr: SocketAddrV4::new(Ipv4Addr::from(ip), u16::from_be_bytes(port)),
        })
    }
}

pub fn decode_node_infos(bytes: &[u8]) -> Result<Vec<NodeInfo>, KrpcError> {
    if !bytes.len().is_multiple_of(NODE_INFO_LENGTH) {
        return Err(KrpcError::InvalidNodes(bytes.len()));
    }
    bytes
        .chunks(NODE_INFO_LENGTH)
        .map(NodeInfo::decode)
        .collect()
}

pub fn encode_node_infos(nodes: &[NodeInfo]) -> Vec<u8> {
    let mut out = Vec::with_capacity(nodes.len() * NODE_INFO_LENGTH);
    for node in nodes {
        out.extend_from_slice(&node.encode());
    }
    out
}

pub fn decode_peers(values: &[Value]) -> Result<Vec<SocketAddr>, KrpcError> {
    let mut peers = Vec::with_capacity(values.len());
    for value in values {
        let bytes = value.as_bytes().ok_or(KrpcError::WrongType("values"))?;
        if bytes.len() != PEER_INFO_LENGTH {
            continue;
        }
        peers.push(peer_from_bytes(bytes));
    }
    Ok(peers)
}

fn peer_from_bytes(bytes: &[u8]) -> SocketAddr {
    let ip = Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]);
    let port = u16::from_be_bytes([bytes[4], bytes[5]]);
    SocketAddr::new(ip.into(), port)
}

pub fn encode_peers(peers: &[SocketAddr]) -> Option<Value> {
    if peers.is_empty() {
        return None;
    }
    let mut list = Vec::with_capacity(peers.len());
    for peer in peers {
        let SocketAddr::V4(addr) = peer else {
            return None;
        };
        let mut bytes = Vec::with_capacity(PEER_INFO_LENGTH);
        bytes.extend_from_slice(&addr.ip().octets());
        bytes.extend_from_slice(&addr.port().to_be_bytes());
        list.push(Value::Bytes(bytes));
    }
    Some(Value::List(list))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    Ping,
    FindNode {
        target: NodeId,
    },
    GetPeers {
        info_hash: [u8; 20],
    },
    AnnouncePeer {
        info_hash: [u8; 20],
        port: u16,
        token: Vec<u8>,
        implied_port: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub nodes: Vec<NodeInfo>,
    pub peers: Vec<SocketAddr>,
    pub token: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KrpcMessage {
    Query {
        transaction_id: TransactionId,
        requester: NodeId,
        query: Query,
    },
    Response {
        transaction_id: TransactionId,
        responder: NodeId,
        response: Response,
    },
    Error {
        transaction_id: TransactionId,
        code: i64,
        message: String,
    },
}

pub fn encode(message: &KrpcMessage) -> Result<Vec<u8>, KrpcError> {
    let transaction_id = match message {
        KrpcMessage::Query { transaction_id, .. }
        | KrpcMessage::Response { transaction_id, .. }
        | KrpcMessage::Error { transaction_id, .. } => *transaction_id,
    };
    let mut root: std::collections::BTreeMap<Vec<u8>, Value> = std::collections::BTreeMap::new();
    root.insert(
        b"t".to_vec(),
        Value::Bytes(transaction_id.as_bytes().to_vec()),
    );
    match message {
        KrpcMessage::Query {
            requester, query, ..
        } => {
            let mut arguments: std::collections::BTreeMap<Vec<u8>, Value> =
                std::collections::BTreeMap::new();
            arguments.insert(b"id".to_vec(), id_value(requester));
            let method = match query {
                Query::Ping => "ping",
                Query::FindNode { target } => {
                    arguments.insert(b"target".to_vec(), id_value(target));
                    "find_node"
                }
                Query::GetPeers { info_hash } => {
                    arguments.insert(b"info_hash".to_vec(), hash_value(info_hash));
                    "get_peers"
                }
                Query::AnnouncePeer {
                    info_hash,
                    port,
                    token,
                    implied_port,
                } => {
                    arguments.insert(b"info_hash".to_vec(), hash_value(info_hash));
                    arguments.insert(b"port".to_vec(), Value::Int(*port as i64));
                    arguments.insert(b"token".to_vec(), Value::Bytes(token.clone()));
                    if *implied_port {
                        arguments.insert(b"implied_port".to_vec(), Value::Int(1));
                    }
                    "announce_peer"
                }
            };
            root.insert(b"y".to_vec(), Value::Bytes(b"q".to_vec()));
            root.insert(b"q".to_vec(), Value::Bytes(method.as_bytes().to_vec()));
            root.insert(b"a".to_vec(), Value::Dict(arguments));
        }
        KrpcMessage::Response {
            responder,
            response,
            ..
        } => {
            let mut return_values: std::collections::BTreeMap<Vec<u8>, Value> =
                std::collections::BTreeMap::new();
            return_values.insert(b"id".to_vec(), id_value(responder));
            if !response.nodes.is_empty() {
                return_values.insert(
                    b"nodes".to_vec(),
                    Value::Bytes(encode_node_infos(&response.nodes)),
                );
            }
            if !response.peers.is_empty() {
                if let Some(values) = encode_peers(&response.peers) {
                    return_values.insert(b"values".to_vec(), values);
                }
            }
            if let Some(token) = &response.token {
                return_values.insert(b"token".to_vec(), Value::Bytes(token.clone()));
            }
            root.insert(b"y".to_vec(), Value::Bytes(b"r".to_vec()));
            root.insert(b"r".to_vec(), Value::Dict(return_values));
        }
        KrpcMessage::Error { code, message, .. } => {
            root.insert(
                b"e".to_vec(),
                Value::List(vec![
                    Value::Int(*code),
                    Value::Bytes(message.clone().into_bytes()),
                ]),
            );
            root.insert(b"y".to_vec(), Value::Bytes(b"e".to_vec()));
        }
    }
    let encoded = bencode::encode(&Value::Dict(root));
    if encoded.len() > MAX_DATAGRAM_SIZE {
        return Err(KrpcError::EncodedTooLarge(encoded.len()));
    }
    Ok(encoded)
}

pub fn decode(datagram: &[u8]) -> Result<KrpcMessage, KrpcError> {
    if datagram.len() > MAX_DATAGRAM_SIZE {
        return Err(KrpcError::DatagramTooLarge(datagram.len()));
    }
    let root = bencode::decode(datagram)?;
    let dict = root.as_dict().ok_or(KrpcError::NotADictionary)?;
    let transaction_id = read_transaction_id(dict)?;
    let direction = dict
        .get(b"y".as_vec())
        .and_then(Value::as_bytes)
        .ok_or(KrpcError::WrongType("y"))?;
    if direction.len() != 1 {
        return Err(KrpcError::InvalidDirection(direction.len()));
    }
    match direction[0] {
        b'q' => decode_query(dict, transaction_id),
        b'r' => decode_response(dict, transaction_id),
        b'e' => decode_error(dict, transaction_id),
        other => Err(KrpcError::UnknownMethod(
            String::from_utf8_lossy(&[other]).into_owned(),
        )),
    }
}

fn decode_query(
    dict: &std::collections::BTreeMap<Vec<u8>, Value>,
    transaction_id: TransactionId,
) -> Result<KrpcMessage, KrpcError> {
    let method = dict
        .get(b"q".as_vec())
        .and_then(Value::as_bytes)
        .ok_or(KrpcError::MissingKey("q"))?;
    let arguments = dict
        .get(b"a".as_vec())
        .and_then(Value::as_dict)
        .ok_or(KrpcError::WrongType("a"))?;
    let requester = read_node_id(arguments, "id")?;
    let query = match method {
        b"ping" => Query::Ping,
        b"find_node" => {
            let target = read_node_id(arguments, "target")?;
            Query::FindNode { target }
        }
        b"get_peers" => {
            let info_hash = read_hash(arguments, "info_hash")?;
            Query::GetPeers { info_hash }
        }
        b"announce_peer" => {
            let info_hash = read_hash(arguments, "info_hash")?;
            let port_value = arguments
                .get(b"port".as_vec())
                .and_then(Value::as_int)
                .ok_or(KrpcError::WrongType("port"))?;
            if !(0..=u16::MAX as i64).contains(&port_value) {
                return Err(KrpcError::InvalidPort(port_value));
            }
            let token = arguments
                .get(b"token".as_vec())
                .and_then(Value::as_bytes)
                .ok_or(KrpcError::WrongType("token"))?
                .to_vec();
            let implied_port = arguments
                .get(b"implied_port".as_vec())
                .and_then(Value::as_int)
                .map(|value| value != 0)
                .unwrap_or(false);
            Query::AnnouncePeer {
                info_hash,
                port: port_value as u16,
                token,
                implied_port,
            }
        }
        other => {
            return Err(KrpcError::UnknownMethod(
                String::from_utf8_lossy(other).into_owned(),
            ))
        }
    };
    Ok(KrpcMessage::Query {
        transaction_id,
        requester,
        query,
    })
}

fn decode_response(
    dict: &std::collections::BTreeMap<Vec<u8>, Value>,
    transaction_id: TransactionId,
) -> Result<KrpcMessage, KrpcError> {
    let return_values = dict
        .get(b"r".as_vec())
        .and_then(Value::as_dict)
        .ok_or(KrpcError::WrongType("r"))?;
    let responder = read_node_id(return_values, "id")?;
    let mut response = Response::default();
    if let Some(nodes) = return_values.get(b"nodes".as_vec()) {
        let bytes = nodes.as_bytes().ok_or(KrpcError::WrongType("nodes"))?;
        response.nodes = decode_node_infos(bytes)?;
    }
    if let Some(values) = return_values.get(b"values".as_vec()) {
        let list = values.as_list().ok_or(KrpcError::WrongType("values"))?;
        response.peers = decode_peers(list)?;
    }
    if let Some(token) = return_values.get(b"token".as_vec()) {
        let bytes = token.as_bytes().ok_or(KrpcError::WrongType("token"))?;
        response.token = Some(bytes.to_vec());
    }
    Ok(KrpcMessage::Response {
        transaction_id,
        responder,
        response,
    })
}

fn decode_error(
    dict: &std::collections::BTreeMap<Vec<u8>, Value>,
    transaction_id: TransactionId,
) -> Result<KrpcMessage, KrpcError> {
    let error = dict
        .get(b"e".as_vec())
        .and_then(Value::as_list)
        .ok_or(KrpcError::WrongType("e"))?;
    if error.len() != 2 {
        return Err(KrpcError::WrongType("e"));
    }
    let code = error[0].as_int().ok_or(KrpcError::WrongType("e"))?;
    let message = error[1].as_bytes().ok_or(KrpcError::WrongType("e"))?;
    Ok(KrpcMessage::Error {
        transaction_id,
        code,
        message: String::from_utf8_lossy(message).into_owned(),
    })
}

fn read_transaction_id(
    dict: &std::collections::BTreeMap<Vec<u8>, Value>,
) -> Result<TransactionId, KrpcError> {
    let bytes = dict
        .get(b"t".as_vec())
        .and_then(Value::as_bytes)
        .ok_or(KrpcError::MissingKey("t"))?;
    if bytes.len() != TRANSACTION_ID_LENGTH {
        return Err(KrpcError::WrongType("t"));
    }
    Ok(TransactionId::from_bytes([bytes[0], bytes[1]]))
}

fn read_node_id(
    dict: &std::collections::BTreeMap<Vec<u8>, Value>,
    key: &'static str,
) -> Result<NodeId, KrpcError> {
    let bytes = dict
        .get(key.as_bytes())
        .and_then(Value::as_bytes)
        .ok_or(KrpcError::WrongType(key))?;
    if bytes.len() != NODE_ID_LENGTH {
        return Err(KrpcError::WrongType(key));
    }
    let mut id = [0u8; NODE_ID_LENGTH];
    id.copy_from_slice(bytes);
    Ok(NodeId::from_bytes(id))
}

fn read_hash(
    dict: &std::collections::BTreeMap<Vec<u8>, Value>,
    key: &'static str,
) -> Result<[u8; 20], KrpcError> {
    let bytes = dict
        .get(key.as_bytes())
        .and_then(Value::as_bytes)
        .ok_or(KrpcError::WrongType(key))?;
    if bytes.len() != NODE_ID_LENGTH {
        return Err(KrpcError::WrongType(key));
    }
    let mut hash = [0u8; 20];
    hash.copy_from_slice(bytes);
    Ok(hash)
}

fn id_value(id: &NodeId) -> Value {
    Value::Bytes(id.as_bytes().to_vec())
}

fn hash_value(hash: &[u8; 20]) -> Value {
    Value::Bytes(hash.to_vec())
}

trait VecExt {
    fn as_vec(&self) -> &[u8];
}

impl VecExt for [u8] {
    fn as_vec(&self) -> &[u8] {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn id(text: &str) -> NodeId {
        let mut bytes = [0u8; NODE_ID_LENGTH];
        bytes.copy_from_slice(text.as_bytes());
        NodeId::from_bytes(bytes)
    }

    fn target() -> NodeId {
        id("mnopqrstuvwxyz123456")
    }

    fn requester() -> NodeId {
        id("abcdefghij0123456789")
    }

    fn fixed_random(bytes: Vec<u8>) -> FixedRandom {
        FixedRandom(Mutex::new(bytes))
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

    #[test]
    fn ping_query_matches_the_bep5_example_bytes() {
        let message = KrpcMessage::Query {
            transaction_id: TransactionId::from_bytes(*b"aa"),
            requester: requester(),
            query: Query::Ping,
        };
        let encoded = encode(&message).unwrap();
        assert_eq!(
            encoded,
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe".to_vec()
        );
        assert_eq!(decode(&encoded).unwrap(), message);
    }

    #[test]
    fn ping_response_matches_the_bep5_example_bytes() {
        let message = KrpcMessage::Response {
            transaction_id: TransactionId::from_bytes(*b"aa"),
            responder: id("mnopqrstuvwxyz123456"),
            response: Response::default(),
        };
        let encoded = encode(&message).unwrap();
        assert_eq!(
            encoded,
            b"d1:rd2:id20:mnopqrstuvwxyz123456e1:t2:aa1:y1:re".to_vec()
        );
        assert_eq!(decode(&encoded).unwrap(), message);
    }

    #[test]
    fn error_matches_the_bep5_example_bytes() {
        let encoded = b"d1:eli201e23:A Generic Error Ocurrede1:t2:aa1:y1:ee".to_vec();
        let message = decode(&encoded).unwrap();
        assert_eq!(
            message,
            KrpcMessage::Error {
                transaction_id: TransactionId::from_bytes(*b"aa"),
                code: 201,
                message: "A Generic Error Ocurred".to_string(),
            }
        );
        assert_eq!(encode(&message).unwrap(), encoded);
    }

    #[test]
    fn find_node_query_matches_the_bep5_example_bytes() {
        let message = KrpcMessage::Query {
            transaction_id: TransactionId::from_bytes(*b"aa"),
            requester: requester(),
            query: Query::FindNode { target: target() },
        };
        let encoded = encode(&message).unwrap();
        assert_eq!(
            encoded,
            b"d1:ad2:id20:abcdefghij01234567896:target20:mnopqrstuvwxyz123456e1:q9:find_node1:t2:aa1:y1:qe".to_vec()
        );
        assert_eq!(decode(&encoded).unwrap(), message);
    }

    #[test]
    fn get_peers_query_matches_the_bep5_example_bytes() {
        let message = KrpcMessage::Query {
            transaction_id: TransactionId::from_bytes(*b"aa"),
            requester: requester(),
            query: Query::GetPeers {
                info_hash: *target().as_bytes(),
            },
        };
        let encoded = encode(&message).unwrap();
        assert_eq!(
            encoded,
            b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz123456e1:q9:get_peers1:t2:aa1:y1:qe".to_vec()
        );
        assert_eq!(decode(&encoded).unwrap(), message);
    }

    #[test]
    fn announce_peer_query_matches_the_bep5_example_bytes() {
        let message = KrpcMessage::Query {
            transaction_id: TransactionId::from_bytes(*b"aa"),
            requester: requester(),
            query: Query::AnnouncePeer {
                info_hash: *target().as_bytes(),
                port: 6881,
                token: b"aoeusnth".to_vec(),
                implied_port: true,
            },
        };
        let encoded = encode(&message).unwrap();
        assert_eq!(
            encoded,
            b"d1:ad2:id20:abcdefghij012345678912:implied_porti1e9:info_hash20:mnopqrstuvwxyz1234564:porti6881e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe".to_vec()
        );
        assert_eq!(decode(&encoded).unwrap(), message);
    }

    #[test]
    fn get_peers_response_with_values_matches_the_bep5_example_bytes() {
        let message = KrpcMessage::Response {
            transaction_id: TransactionId::from_bytes(*b"aa"),
            responder: requester(),
            response: Response {
                nodes: Vec::new(),
                peers: vec![
                    SocketAddr::new(Ipv4Addr::new(0x61, 0x78, 0x6a, 0x65).into(), 0x2e75),
                    SocketAddr::new(Ipv4Addr::new(0x69, 0x64, 0x68, 0x74).into(), 0x6e6d),
                ],
                token: Some(b"aoeusnth".to_vec()),
            },
        };
        let encoded = encode(&message).unwrap();
        assert_eq!(
            encoded,
            b"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnth6:valuesl6:axje.u6:idhtnmee1:t2:aa1:y1:re".to_vec()
        );
        assert_eq!(decode(&encoded).unwrap(), message);
    }

    #[test]
    fn get_peers_response_with_token_only_round_trips() {
        let message = KrpcMessage::Response {
            transaction_id: TransactionId::from_bytes(*b"aa"),
            responder: requester(),
            response: Response {
                token: Some(b"aoeusnth".to_vec()),
                ..Default::default()
            },
        };
        let encoded = encode(&message).unwrap();
        assert_eq!(
            encoded,
            b"d1:rd2:id20:abcdefghij01234567895:token8:aoeusnthe1:t2:aa1:y1:re".to_vec()
        );
        assert_eq!(decode(&encoded).unwrap(), message);
    }

    #[test]
    fn response_with_nodes_round_trips_compact_entries() {
        let node = NodeInfo {
            id: id("0123456789abcdefghij"),
            addr: SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 6881),
        };
        let message = KrpcMessage::Response {
            transaction_id: TransactionId::from_bytes(*b"aa"),
            responder: requester(),
            response: Response {
                nodes: vec![node],
                ..Default::default()
            },
        };
        let encoded = encode(&message).unwrap();
        assert_eq!(decode(&encoded).unwrap(), message);
        let entry = node.encode();
        assert_eq!(entry.len(), 26);
        assert_eq!(entry[..20], *node.id.as_bytes());
        assert_eq!(&entry[20..24], &[1, 2, 3, 4]);
        assert_eq!(&entry[24..26], &6881u16.to_be_bytes());
    }

    #[test]
    fn node_info_rejects_wrong_lengths() {
        let entry = [0u8; 26];
        assert!(NodeInfo::decode(&entry).is_ok());
        let short = [0u8; 25];
        assert!(matches!(
            NodeInfo::decode(&short),
            Err(KrpcError::InvalidNodeInfo(25))
        ));
        let long = [0u8; 27];
        assert!(matches!(
            NodeInfo::decode(&long),
            Err(KrpcError::InvalidNodeInfo(27))
        ));
    }

    #[test]
    fn nodes_string_must_be_an_exact_multiple_of_26() {
        assert!(decode_node_infos(&[]).unwrap().is_empty());
        assert!(matches!(
            decode_node_infos(&[0u8; 27]),
            Err(KrpcError::InvalidNodes(27))
        ));
    }

    #[test]
    fn values_entries_that_are_not_ipv4_are_skipped() {
        let values = vec![
            Value::Bytes(vec![1, 2, 3, 4, 5, 6]),
            Value::Bytes(vec![1, 2, 3, 4, 5, 6, 7]),
            Value::Bytes([9u8; 18].to_vec()),
            Value::Bytes(vec![7, 7, 7, 7, 7, 8]),
        ];
        let peers = decode_peers(&values).unwrap();
        assert_eq!(peers.len(), 2, "only six byte ipv4 entries survive");
        assert_eq!(
            peers[1],
            SocketAddr::new(Ipv4Addr::new(7, 7, 7, 7).into(), 0x0708)
        );
    }

    #[test]
    fn oversized_datagrams_are_rejected_before_parsing() {
        let oversized = vec![b'd'; MAX_DATAGRAM_SIZE + 1];
        assert!(matches!(
            decode(&oversized),
            Err(KrpcError::DatagramTooLarge(2049))
        ));
    }

    #[test]
    fn truncated_datagrams_are_rejected() {
        let full = encode(&KrpcMessage::Query {
            transaction_id: TransactionId::from_bytes(*b"aa"),
            requester: requester(),
            query: Query::Ping,
        })
        .unwrap();
        assert!(decode(&full[..full.len() - 1]).is_err());
        assert!(decode(&full[..4]).is_err());
    }

    #[test]
    fn non_bencode_garbage_is_rejected() {
        assert!(decode(b"not bencode at all").is_err());
        assert!(decode(b"").is_err());
        assert!(decode(b"i42e").is_err());
    }

    #[test]
    fn wrong_key_types_are_rejected() {
        let base = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe".to_vec();
        let wrong_t = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:ti99e1:y1:qe".to_vec();
        assert!(decode(&wrong_t).is_err());
        let wrong_y = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:yi9ee".to_vec();
        assert!(decode(&wrong_y).is_err());
        let unknown_direction =
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:xe".to_vec();
        assert!(decode(&unknown_direction).is_err());
        let short_id = b"d1:ad2:id19:abcdefghij012345678e1:q4:ping1:t2:aa1:y1:qe".to_vec();
        assert!(decode(&short_id).is_err());
        let wrong_a = b"d1:ali1ee1:q4:ping1:t2:aa1:y1:qe".to_vec();
        assert!(decode(&wrong_a).is_err());
        let missing_y = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aae".to_vec();
        assert!(decode(&missing_y).is_err());
        let _ = base;
    }

    #[test]
    fn unknown_query_method_is_rejected() {
        let message = b"d1:ad2:id20:abcdefghij0123456789e1:q5:joker1:t2:aa1:y1:qe".to_vec();
        assert!(matches!(decode(&message), Err(KrpcError::UnknownMethod(_))));
    }

    #[test]
    fn announce_peer_port_must_be_in_range() {
        let negative = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti-1e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe".to_vec();
        assert!(matches!(decode(&negative), Err(KrpcError::InvalidPort(-1))));
        let huge = b"d1:ad2:id20:abcdefghij01234567899:info_hash20:mnopqrstuvwxyz1234564:porti70000e5:token8:aoeusnthe1:q13:announce_peer1:t2:aa1:y1:qe".to_vec();
        assert!(matches!(decode(&huge), Err(KrpcError::InvalidPort(70000))));
    }

    #[test]
    fn announce_peer_without_implied_port_defaults_to_false() {
        let message = KrpcMessage::Query {
            transaction_id: TransactionId::from_bytes(*b"aa"),
            requester: requester(),
            query: Query::AnnouncePeer {
                info_hash: *target().as_bytes(),
                port: 6881,
                token: b"aoeusnth".to_vec(),
                implied_port: false,
            },
        };
        let encoded = encode(&message).unwrap();
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn transaction_ids_are_two_random_bytes() {
        let source = fixed_random(vec![0x11, 0x22]);
        let first = TransactionId::random(&source);
        assert_eq!(first.as_bytes(), &[0x11, 0x22]);
        let second = TransactionId::random(&source);
        assert_eq!(second.as_bytes(), &[0, 0]);
        let third = random_transaction_id();
        let fourth = random_transaction_id();
        assert_ne!(third.as_bytes(), fourth.as_bytes());
    }

    #[test]
    fn peer_compact_encoding_round_trips() {
        let peer = SocketAddr::new(Ipv4Addr::new(127, 0, 0, 1).into(), 6881);
        let values = encode_peers(&[peer]).unwrap();
        let Value::List(entries) = &values else {
            panic!("expected a list");
        };
        assert_eq!(entries.len(), 1);
        let decoded = decode_peers(entries).unwrap();
        assert_eq!(decoded, vec![peer]);
        assert!(encode_peers(&[]).is_none());
    }
}
