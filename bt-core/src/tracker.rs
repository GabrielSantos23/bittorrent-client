use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use reqwest::Client;

use crate::bencode::{self, Value};
use crate::error::TrackerError;
use crate::metainfo::MetaInfo;
use crate::percent::percent_encode;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    Started,
    Completed,
    Stopped,
}

impl Event {
    fn as_str(self) -> &'static str {
        match self {
            Event::Started => "started",
            Event::Completed => "completed",
            Event::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnounceRequest {
    pub info_hash: [u8; 20],
    pub peer_id: [u8; 20],
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub numwant: u32,
    pub event: Option<Event>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnounceResponse {
    pub interval: u64,
    pub min_interval: Option<u64>,
    pub complete: u64,
    pub incomplete: u64,
    pub peers: Vec<SocketAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnnounceOutcome {
    pub url: String,
    pub response: AnnounceResponse,
}

pub const MAX_REDIRECTS: usize = 3;
pub const MAX_TRACKER_RESPONSE_BYTES: usize = 1024 * 1024;

pub fn http_client() -> Result<Client, TrackerError> {
    Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .redirect(reqwest::redirect::Policy::limited(MAX_REDIRECTS))
        .user_agent(concat!("bt-core/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(TrackerError::Request)
}

pub fn build_announce_url(base: &str, request: &AnnounceRequest) -> String {
    let mut url = String::with_capacity(base.len() + 200);
    url.push_str(base);
    url.push(if base.contains('?') { '&' } else { '?' });
    url.push_str("info_hash=");
    url.push_str(&percent_encode(&request.info_hash));
    url.push_str("&peer_id=");
    url.push_str(&percent_encode(&request.peer_id));
    url.push_str("&port=");
    url.push_str(&request.port.to_string());
    url.push_str("&uploaded=");
    url.push_str(&request.uploaded.to_string());
    url.push_str("&downloaded=");
    url.push_str(&request.downloaded.to_string());
    url.push_str("&left=");
    url.push_str(&request.left.to_string());
    url.push_str("&compact=1&numwant=");
    url.push_str(&request.numwant.to_string());
    if let Some(event) = request.event {
        url.push_str("&event=");
        url.push_str(event.as_str());
    }
    url
}

pub async fn announce(
    client: &Client,
    meta: &MetaInfo,
    request: &AnnounceRequest,
) -> Result<AnnounceOutcome, TrackerError> {
    let mut last_error: Option<TrackerError> = None;
    for url in candidate_urls(meta) {
        match http_announce(client, url, request).await {
            Ok(response) => {
                return Ok(AnnounceOutcome {
                    url: url.to_string(),
                    response,
                });
            }
            Err(err) => last_error = Some(err),
        }
    }
    match last_error {
        Some(err) => Err(err),
        None => Err(TrackerError::NoTrackers),
    }
}

pub async fn http_announce(
    client: &Client,
    url: &str,
    request: &AnnounceRequest,
) -> Result<AnnounceResponse, TrackerError> {
    let response = client.get(build_announce_url(url, request)).send().await?;
    let status = response.status();
    if !status.is_success() {
        return Err(TrackerError::HttpStatus(status.as_u16()));
    }
    if let Some(length) = response.content_length() {
        if length as usize > MAX_TRACKER_RESPONSE_BYTES {
            return Err(TrackerError::ResponseTooLarge(
                length,
                MAX_TRACKER_RESPONSE_BYTES,
            ));
        }
    }
    let mut body = Vec::new();
    let mut response = response;
    while let Some(chunk) = response.chunk().await? {
        if body.len() + chunk.len() > MAX_TRACKER_RESPONSE_BYTES {
            return Err(TrackerError::ResponseTooLarge(
                (body.len() + chunk.len()) as u64,
                MAX_TRACKER_RESPONSE_BYTES,
            ));
        }
        body.extend_from_slice(&chunk);
    }
    parse_response(&body)
}

fn candidate_urls(meta: &MetaInfo) -> Vec<&str> {
    let mut urls = Vec::new();
    for tier in &meta.announce_list {
        urls.extend(tier.iter().map(String::as_str));
    }
    if let Some(announce) = &meta.announce {
        urls.push(announce);
    }
    urls.into_iter()
        .filter(|url| url.starts_with("http://") || url.starts_with("https://"))
        .collect()
}

pub fn is_valid_peer_address(addr: SocketAddr) -> bool {
    if addr.port() == 0 {
        return false;
    }
    match addr {
        SocketAddr::V4(v4) => {
            !(v4.ip().is_unspecified()
                || v4.ip().is_loopback()
                || v4.ip().is_broadcast()
                || v4.ip().is_multicast())
        }
        SocketAddr::V6(v6) => {
            !(v6.ip().is_unspecified() || v6.ip().is_loopback() || v6.ip().is_multicast())
        }
    }
}

pub fn parse_response(raw: &[u8]) -> Result<AnnounceResponse, TrackerError> {
    let value = bencode::decode(raw)?;
    let dict = value.as_dict().ok_or(TrackerError::NotADictionary)?;
    if let Some(reason) = dict
        .get("failure reason".as_bytes())
        .and_then(Value::as_bytes)
    {
        return Err(TrackerError::Failure(
            String::from_utf8_lossy(reason).into_owned(),
        ));
    }
    let interval = get_u64(dict, "interval")?;
    let min_interval = get_opt_u64(dict, "min interval")?;
    let complete = get_opt_u64(dict, "complete")?.unwrap_or(0);
    let incomplete = get_opt_u64(dict, "incomplete")?.unwrap_or(0);
    let peers = parse_peer_sections(dict)?;
    Ok(AnnounceResponse {
        interval,
        min_interval,
        complete,
        incomplete,
        peers,
    })
}

fn parse_peer_sections(dict: &BTreeMap<Vec<u8>, Value>) -> Result<Vec<SocketAddr>, TrackerError> {
    let v4 = dict.get("peers".as_bytes());
    let v6 = dict.get("peers6".as_bytes());
    if v4.is_none() && v6.is_none() {
        return Err(TrackerError::MissingKey("peers"));
    }
    let mut peers = Vec::new();
    if let Some(value) = v4 {
        match value {
            Value::Bytes(raw) => peers.extend(parse_compact_peers(raw)?),
            Value::List(entries) => peers.extend(parse_dictionary_peers(entries)?),
            _ => return Err(TrackerError::WrongType("peers")),
        }
    }
    if let Some(value) = v6 {
        match value {
            Value::Bytes(raw) => peers.extend(parse_compact_peers6(raw)?),
            _ => return Err(TrackerError::WrongType("peers6")),
        }
    }
    Ok(peers)
}

fn parse_compact_peers(raw: &[u8]) -> Result<Vec<SocketAddr>, TrackerError> {
    if !raw.len().is_multiple_of(6) {
        return Err(TrackerError::InvalidCompactPeers);
    }
    raw.chunks_exact(6)
        .map(|chunk| {
            Ok(SocketAddr::from((
                [chunk[0], chunk[1], chunk[2], chunk[3]],
                u16::from_be_bytes([chunk[4], chunk[5]]),
            )))
        })
        .collect()
}

fn parse_compact_peers6(raw: &[u8]) -> Result<Vec<SocketAddr>, TrackerError> {
    if !raw.len().is_multiple_of(18) {
        return Err(TrackerError::InvalidCompactPeers6);
    }
    raw.chunks_exact(18)
        .map(|chunk| {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&chunk[..16]);
            Ok(SocketAddr::from((
                octets,
                u16::from_be_bytes([chunk[16], chunk[17]]),
            )))
        })
        .collect()
}

fn parse_dictionary_peers(entries: &[Value]) -> Result<Vec<SocketAddr>, TrackerError> {
    entries
        .iter()
        .map(|entry| {
            let dict = entry.as_dict().ok_or(TrackerError::InvalidPeerEntry)?;
            let ip = dict
                .get("ip".as_bytes())
                .and_then(Value::as_str)
                .ok_or(TrackerError::InvalidPeerEntry)?;
            let port = dict
                .get("port".as_bytes())
                .and_then(Value::as_int)
                .ok_or(TrackerError::InvalidPeerEntry)?;
            let ip: Ipv4Addr = ip.parse().map_err(|_| TrackerError::InvalidPeerEntry)?;
            let port = u16::try_from(port).map_err(|_| TrackerError::InvalidPeerEntry)?;
            Ok(SocketAddr::from((ip, port)))
        })
        .collect()
}

fn get_u64(dict: &BTreeMap<Vec<u8>, Value>, key: &'static str) -> Result<u64, TrackerError> {
    match dict.get(key.as_bytes()) {
        Some(Value::Int(n)) if *n >= 0 => Ok(*n as u64),
        Some(_) => Err(TrackerError::WrongType(key)),
        None => Err(TrackerError::MissingKey(key)),
    }
}

fn get_opt_u64(
    dict: &BTreeMap<Vec<u8>, Value>,
    key: &'static str,
) -> Result<Option<u64>, TrackerError> {
    match dict.get(key.as_bytes()) {
        None => Ok(None),
        Some(Value::Int(n)) if *n >= 0 => Ok(Some(*n as u64)),
        Some(_) => Err(TrackerError::WrongType(key)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metainfo::{Content, Info};

    fn push_string(raw: &mut Vec<u8>, text: &str) {
        raw.extend_from_slice(text.len().to_string().as_bytes());
        raw.push(b':');
        raw.extend_from_slice(text.as_bytes());
    }

    fn push_bytes(raw: &mut Vec<u8>, bytes: &[u8]) {
        raw.extend_from_slice(bytes.len().to_string().as_bytes());
        raw.push(b':');
        raw.extend_from_slice(bytes);
    }

    fn push_peer_entry(raw: &mut Vec<u8>, ip: &str, port: i64) {
        raw.extend_from_slice(b"d2:ip");
        push_string(raw, ip);
        raw.extend_from_slice(b"4:porti");
        raw.extend_from_slice(port.to_string().as_bytes());
        raw.extend_from_slice(b"ee");
    }

    fn sample_request() -> AnnounceRequest {
        AnnounceRequest {
            info_hash: [0xaa; 20],
            peer_id: *b"-BT0001-abcdefghijkl",
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 1000,
            numwant: 50,
            event: Some(Event::Started),
        }
    }

    #[test]
    fn builds_announce_url_with_encoded_ids() {
        let url = build_announce_url("http://tracker.example/announce", &sample_request());
        let expected = format!(
            "http://tracker.example/announce?info_hash={}&peer_id=%2D%42%54%30%30%30%31%2D%61%62%63%64%65%66%67%68%69%6A%6B%6C&port=6881&uploaded=0&downloaded=0&left=1000&compact=1&numwant=50&event=started",
            "%AA".repeat(20)
        );
        assert_eq!(url, expected);
    }

    #[test]
    fn appends_with_ampersand_when_query_exists() {
        let url = build_announce_url("http://tracker.example/announce?k=v", &sample_request());
        assert!(url.starts_with("http://tracker.example/announce?k=v&info_hash="));
    }

    #[test]
    fn omits_event_for_periodic_announce() {
        let request = AnnounceRequest {
            event: None,
            ..sample_request()
        };
        let url = build_announce_url("http://tracker.example/announce", &request);
        assert!(url.ends_with("&numwant=50"));
        assert!(!url.contains("event="));
    }

    #[test]
    fn parses_compact_response() {
        let mut peers = Vec::new();
        peers.extend_from_slice(&[192, 168, 1, 1]);
        peers.extend_from_slice(&6881u16.to_be_bytes());
        peers.extend_from_slice(&[10, 0, 0, 2]);
        peers.extend_from_slice(&51413u16.to_be_bytes());
        let mut raw = Vec::new();
        raw.extend_from_slice(
            b"d8:completei3e8:intervali1800e10:incompletei7e12:min intervali60e5:peers",
        );
        push_bytes(&mut raw, &peers);
        raw.push(b'e');
        let response = parse_response(&raw).unwrap();
        assert_eq!(response.interval, 1800);
        assert_eq!(response.min_interval, Some(60));
        assert_eq!(response.complete, 3);
        assert_eq!(response.incomplete, 7);
        assert_eq!(
            response.peers,
            vec![
                SocketAddr::from(([192, 168, 1, 1], 6881)),
                SocketAddr::from(([10, 0, 0, 2], 51413)),
            ]
        );
    }

    #[test]
    fn parses_compact_ipv6_peers() {
        let mut peers = Vec::new();
        peers.extend_from_slice(&[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
        peers.extend_from_slice(&6881u16.to_be_bytes());
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d8:intervali1e6:peers6");
        push_bytes(&mut raw, &peers);
        raw.push(b'e');
        let response = parse_response(&raw).unwrap();
        assert_eq!(
            response.peers,
            vec![SocketAddr::from(([0x2001, 0x0db8, 0, 0, 0, 0, 0, 1], 6881))]
        );
    }

    #[test]
    fn parses_both_compact_sections() {
        let mut v4 = Vec::new();
        v4.extend_from_slice(&[192, 168, 1, 1]);
        v4.extend_from_slice(&6881u16.to_be_bytes());
        let mut v6 = Vec::new();
        v6.extend_from_slice(&[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
        v6.extend_from_slice(&51413u16.to_be_bytes());
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d8:intervali1e5:peers");
        push_bytes(&mut raw, &v4);
        raw.extend_from_slice(b"6:peers6");
        push_bytes(&mut raw, &v6);
        raw.push(b'e');
        let response = parse_response(&raw).unwrap();
        assert_eq!(
            response.peers,
            vec![
                SocketAddr::from(([192, 168, 1, 1], 6881)),
                SocketAddr::from(([0x2001, 0x0db8, 0, 0, 0, 0, 0, 1], 51413)),
            ]
        );
    }

    #[test]
    fn parses_dictionary_peer_model() {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d8:completei1e8:intervali600e5:peersl");
        push_peer_entry(&mut raw, "127.0.0.1", 6881);
        push_peer_entry(&mut raw, "10.0.0.1", 51413);
        raw.extend_from_slice(b"ee");
        let response = parse_response(&raw).unwrap();
        assert_eq!(response.interval, 600);
        assert_eq!(response.min_interval, None);
        assert_eq!(response.complete, 1);
        assert_eq!(response.incomplete, 0);
        assert_eq!(
            response.peers,
            vec![
                SocketAddr::from(([127, 0, 0, 1], 6881)),
                SocketAddr::from(([10, 0, 0, 1], 51413)),
            ]
        );
    }

    #[test]
    fn reports_failure_reason() {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d14:failure reason");
        push_string(&mut raw, "no such info");
        raw.push(b'e');
        let err = parse_response(&raw).unwrap_err();
        assert!(matches!(err, TrackerError::Failure(reason) if reason == "no such info"));
    }

    #[test]
    fn rejects_non_multiple_compact_peers() {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d8:intervali1e5:peers5:abcdee");
        assert!(matches!(
            parse_response(&raw),
            Err(TrackerError::InvalidCompactPeers)
        ));
    }

    #[test]
    fn rejects_non_multiple_compact_peers6() {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d8:intervali1e6:peers617:abcdefghijklmnopqe");
        assert!(matches!(
            parse_response(&raw),
            Err(TrackerError::InvalidCompactPeers6)
        ));
    }

    #[test]
    fn rejects_wrong_peers_type() {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d8:intervali1e5:peersi7ee");
        assert!(matches!(
            parse_response(&raw),
            Err(TrackerError::WrongType("peers"))
        ));
    }

    #[test]
    fn rejects_wrong_peers6_type() {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d8:intervali1e6:peers6li1eee");
        assert!(matches!(
            parse_response(&raw),
            Err(TrackerError::WrongType("peers6"))
        ));
    }

    #[test]
    fn rejects_hostname_peer_entry() {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d8:intervali1e5:peersl");
        push_peer_entry(&mut raw, "localhost", 1);
        raw.extend_from_slice(b"ee");
        assert!(matches!(
            parse_response(&raw),
            Err(TrackerError::InvalidPeerEntry)
        ));
    }

    #[test]
    fn rejects_out_of_range_port() {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d8:intervali1e5:peersl");
        push_peer_entry(&mut raw, "10.0.0.1", 70000);
        raw.extend_from_slice(b"ee");
        assert!(matches!(
            parse_response(&raw),
            Err(TrackerError::InvalidPeerEntry)
        ));
    }

    #[test]
    fn requires_interval() {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d5:peers0:e");
        assert!(matches!(
            parse_response(&raw),
            Err(TrackerError::MissingKey("interval"))
        ));
    }

    #[test]
    fn rejects_negative_interval() {
        let mut raw = Vec::new();
        raw.extend_from_slice(b"d8:intervali-5e5:peers0:e");
        assert!(matches!(
            parse_response(&raw),
            Err(TrackerError::WrongType("interval"))
        ));
    }

    #[test]
    fn rejects_non_dictionary_response() {
        assert!(matches!(
            parse_response(b"i1e"),
            Err(TrackerError::NotADictionary)
        ));
    }

    #[test]
    fn filters_invalid_peer_addresses() {
        assert!(is_valid_peer_address(SocketAddr::from((
            [192, 168, 1, 10],
            6881
        ))));
        assert!(is_valid_peer_address(SocketAddr::from(([10, 0, 0, 1], 1))));
        assert!(is_valid_peer_address(SocketAddr::from((
            [172, 16, 5, 5],
            5
        ))));
        assert!(!is_valid_peer_address(SocketAddr::from(([1, 2, 3, 4], 0))));
        assert!(!is_valid_peer_address(SocketAddr::from((
            [0, 0, 0, 0],
            6881
        ))));
        assert!(!is_valid_peer_address(SocketAddr::from((
            [127, 0, 0, 1],
            6881
        ))));
        assert!(!is_valid_peer_address(SocketAddr::from((
            [255, 255, 255, 255],
            6881
        ))));
        assert!(!is_valid_peer_address(SocketAddr::from((
            [224, 0, 0, 1],
            6881
        ))));
        let v6_unspecified: SocketAddr = "[::]:6881".parse().unwrap();
        let v6_loopback: SocketAddr = "[::1]:6881".parse().unwrap();
        let v6_multicast: SocketAddr = "[ff02::1]:6881".parse().unwrap();
        let v6_valid: SocketAddr = "[2001:db8::1]:6881".parse().unwrap();
        assert!(!is_valid_peer_address(v6_unspecified));
        assert!(!is_valid_peer_address(v6_loopback));
        assert!(!is_valid_peer_address(v6_multicast));
        assert!(is_valid_peer_address(v6_valid));
    }

    #[test]
    fn orders_tiers_before_announce() {
        let meta = MetaInfo {
            info_hash: [0; 20],
            info: Info {
                name: "n".to_string(),
                piece_length: 16384,
                pieces: Vec::new(),
                private: false,
                content: Content::Single { length: 0 },
            },
            announce: Some("http://fallback/announce".to_string()),
            announce_list: vec![
                vec![
                    "http://a/announce".to_string(),
                    "udp://b/announce".to_string(),
                ],
                vec!["https://c/announce".to_string()],
            ],
            comment: None,
            created_by: None,
            creation_date: None,
        };
        assert_eq!(
            candidate_urls(&meta),
            vec![
                "http://a/announce",
                "https://c/announce",
                "http://fallback/announce"
            ]
        );
    }
}
