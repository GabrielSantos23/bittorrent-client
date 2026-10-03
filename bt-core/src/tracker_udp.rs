use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{lookup_host, UdpSocket};
use tokio::sync::Mutex;
use tokio::time::timeout;

use crate::error::TrackerError;
use crate::tracker::{AnnounceRequest, AnnounceResponse};

/// Initial retransmit wait per the spec; doubled on every retry.
pub const INITIAL_TIMEOUT: Duration = Duration::from_secs(15);
/// Hard retry cap so an unreachable tracker fails in bounded time:
/// worst case total wait is INITIAL_TIMEOUT * (2^(MAX_RETRANSMITS+1) - 1) = 225 s.
pub const MAX_RETRANSMITS: u32 = 3;
/// A connection id may be reused for 60 seconds after it was received.
pub const CONNECTION_LIFETIME: Duration = Duration::from_secs(60);
const MAX_PACKET: usize = 16 * 1024;
const UDP_PROTOCOL_MAGIC: u64 = 0x0000_0417_2710_1980;
const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;
const ACTION_ERROR: u32 = 3;

#[derive(Debug, Clone, Copy)]
pub struct UdpConfig {
    pub initial_timeout: Duration,
    pub max_retransmits: u32,
    pub connection_lifetime: Duration,
}

impl Default for UdpConfig {
    fn default() -> Self {
        UdpConfig {
            initial_timeout: INITIAL_TIMEOUT,
            max_retransmits: MAX_RETRANSMITS,
            connection_lifetime: CONNECTION_LIFETIME,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpAnnounceReply {
    pub interval: u32,
    pub leechers: u32,
    pub seeders: u32,
    pub peers: Vec<SocketAddr>,
}

pub fn parse_udp_tracker_url(url: &str) -> Result<(String, u16), TrackerError> {
    let Some(rest) = url.strip_prefix("udp://") else {
        return Err(TrackerError::Udp(format!("not a udp tracker url: {url}")));
    };
    let host_port = rest.split('/').next().unwrap_or(rest);
    if let Some(bracket_end) = host_port.rfind(']') {
        let host = host_port[1..bracket_end].to_string();
        let port_part = host_port
            .get(bracket_end + 1..)
            .and_then(|tail| tail.strip_prefix(':'))
            .ok_or_else(|| TrackerError::Udp(format!("udp url missing port: {url}")))?;
        let port: u16 = port_part
            .parse()
            .map_err(|_| TrackerError::Udp(format!("invalid udp port in {url}")))?;
        return Ok((host, port));
    }
    let (host, port) = host_port
        .rsplit_once(':')
        .ok_or_else(|| TrackerError::Udp(format!("udp url missing port: {url}")))?;
    let port: u16 = port
        .parse()
        .map_err(|_| TrackerError::Udp(format!("invalid udp port in {url}")))?;
    Ok((host.to_string(), port))
}

pub fn event_code(event: Option<crate::tracker::Event>) -> i32 {
    match event {
        None => 0,
        Some(crate::tracker::Event::Completed) => 1,
        Some(crate::tracker::Event::Started) => 2,
        Some(crate::tracker::Event::Stopped) => 3,
    }
}

pub fn encode_connect_request(transaction_id: [u8; 4]) -> [u8; 16] {
    let mut packet = [0u8; 16];
    packet[..8].copy_from_slice(&UDP_PROTOCOL_MAGIC.to_be_bytes());
    packet[8..12].copy_from_slice(&ACTION_CONNECT.to_be_bytes());
    packet[12..].copy_from_slice(&transaction_id);
    packet
}

fn packet_transaction_id(packet: &[u8]) -> Option<[u8; 4]> {
    if packet.len() < 8 {
        return None;
    }
    Some([packet[4], packet[5], packet[6], packet[7]])
}

fn packet_action(packet: &[u8]) -> Option<u32> {
    if packet.len() < 8 {
        return None;
    }
    Some(u32::from_be_bytes([
        packet[0], packet[1], packet[2], packet[3],
    ]))
}

pub fn decode_connect_reply(packet: &[u8], transaction_id: [u8; 4]) -> Result<u64, TrackerError> {
    if packet.len() < 16 {
        return Err(TrackerError::Udp("connect reply too short".to_string()));
    }
    if packet_action(packet) != Some(ACTION_CONNECT) {
        return Err(TrackerError::Udp(
            "connect reply has wrong action".to_string(),
        ));
    }
    if packet_transaction_id(packet) != Some(transaction_id) {
        return Err(TrackerError::Udp(
            "connect reply transaction id mismatch".to_string(),
        ));
    }
    let mut id = [0u8; 8];
    id.copy_from_slice(&packet[8..16]);
    Ok(u64::from_be_bytes(id))
}

pub fn encode_announce_request(
    connection_id: u64,
    transaction_id: [u8; 4],
    request: &AnnounceRequest,
    numwant: i32,
) -> [u8; 98] {
    let mut packet = [0u8; 98];
    packet[..8].copy_from_slice(&connection_id.to_be_bytes());
    packet[8..12].copy_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
    packet[12..16].copy_from_slice(&transaction_id);
    packet[16..36].copy_from_slice(&request.info_hash);
    packet[36..56].copy_from_slice(&request.peer_id);
    packet[56..64].copy_from_slice(&request.downloaded.to_be_bytes());
    packet[64..72].copy_from_slice(&request.left.to_be_bytes());
    packet[72..80].copy_from_slice(&request.uploaded.to_be_bytes());
    packet[80..84].copy_from_slice(&event_code(request.event).to_be_bytes());
    packet[84..88].copy_from_slice(&0u32.to_be_bytes());
    packet[88..92].copy_from_slice(&0u32.to_be_bytes());
    packet[92..96].copy_from_slice(&numwant.to_be_bytes());
    packet[96..98].copy_from_slice(&request.port.to_be_bytes());
    packet
}

pub fn decode_announce_reply(
    packet: &[u8],
    transaction_id: [u8; 4],
    ipv6: bool,
) -> Result<UdpAnnounceReply, TrackerError> {
    if packet.len() < 20 {
        return Err(TrackerError::Udp("announce reply too short".to_string()));
    }
    if packet_action(packet) != Some(ACTION_ANNOUNCE) {
        return Err(TrackerError::Udp(
            "announce reply has wrong action".to_string(),
        ));
    }
    if packet_transaction_id(packet) != Some(transaction_id) {
        return Err(TrackerError::Udp(
            "announce reply transaction id mismatch".to_string(),
        ));
    }
    let interval = u32::from_be_bytes([packet[8], packet[9], packet[10], packet[11]]);
    let leechers = u32::from_be_bytes([packet[12], packet[13], packet[14], packet[15]]);
    let seeders = u32::from_be_bytes([packet[16], packet[17], packet[18], packet[19]]);
    let entry = if ipv6 { 18 } else { 6 };
    let peers_bytes = &packet[20..];
    if !peers_bytes.len().is_multiple_of(entry) {
        return Err(TrackerError::Udp(
            "announce reply peers section is malformed".to_string(),
        ));
    }
    let mut peers = Vec::new();
    for chunk in peers_bytes.chunks_exact(entry) {
        let peer = if ipv6 {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&chunk[..16]);
            SocketAddr::from((octets, u16::from_be_bytes([chunk[16], chunk[17]])))
        } else {
            SocketAddr::from((
                [chunk[0], chunk[1], chunk[2], chunk[3]],
                u16::from_be_bytes([chunk[4], chunk[5]]),
            ))
        };
        peers.push(peer);
    }
    Ok(UdpAnnounceReply {
        interval,
        leechers,
        seeders,
        peers,
    })
}

pub fn decode_error_reply(packet: &[u8], transaction_id: [u8; 4]) -> Option<String> {
    if packet.len() < 8 {
        return None;
    }
    if packet_action(packet) != Some(ACTION_ERROR) {
        return None;
    }
    if packet_transaction_id(packet) != Some(transaction_id) {
        return None;
    }
    Some(String::from_utf8_lossy(&packet[8..]).into_owned())
}

pub struct UdpTrackerClient {
    socket: UdpSocket,
    tracker: SocketAddr,
    connection_id: Option<(u64, tokio::time::Instant)>,
    config: UdpConfig,
    transaction_id: [u8; 4],
}

impl UdpTrackerClient {
    pub async fn connect_tracker(
        url: &str,
        config: UdpConfig,
    ) -> Result<UdpTrackerClient, TrackerError> {
        let (host, port) = parse_udp_tracker_url(url)?;
        let resolved = lookup_host((host.as_str(), port))
            .await
            .map_err(|err| TrackerError::Udp(format!("cannot resolve {host}: {err}")))?
            .next()
            .ok_or_else(|| TrackerError::Udp(format!("no addresses for {host}")))?;
        let socket = match resolved.is_ipv4() {
            true => UdpSocket::bind((std::net::Ipv4Addr::UNSPECIFIED, 0)).await,
            false => UdpSocket::bind((std::net::Ipv6Addr::UNSPECIFIED, 0)).await,
        }
        .map_err(|err| TrackerError::Udp(format!("cannot bind udp socket: {err}")))?;
        Ok(UdpTrackerClient {
            socket,
            tracker: resolved,
            connection_id: None,
            config,
            transaction_id: rand_transaction_id(),
        })
    }

    pub fn tracker_address(&self) -> SocketAddr {
        self.tracker
    }

    fn transaction_id(&mut self) -> [u8; 4] {
        self.transaction_id
    }

    async fn exchange(
        &mut self,
        packet: &[u8],
        transaction_id: [u8; 4],
    ) -> Result<Vec<u8>, TrackerError> {
        let mut wait = self.config.initial_timeout;
        let mut buffer = vec![0u8; MAX_PACKET];
        for _ in 0..=self.config.max_retransmits {
            self.socket
                .send_to(packet, self.tracker)
                .await
                .map_err(|err| TrackerError::Udp(format!("udp send failed: {err}")))?;
            let deadline = wait;
            loop {
                match timeout(deadline, self.socket.recv_from(&mut buffer)).await {
                    Err(_) => break,
                    Ok(Err(err)) => {
                        return Err(TrackerError::Udp(format!("udp recv failed: {err}")))
                    }
                    Ok(Ok((read, from))) => {
                        if from != self.tracker {
                            continue;
                        }
                        if read > MAX_PACKET {
                            return Err(TrackerError::Udp("oversized datagram".to_string()));
                        }
                        if packet_transaction_id(&buffer[..read]) != Some(transaction_id) {
                            continue;
                        }
                        return Ok(buffer[..read].to_vec());
                    }
                }
            }
            wait = wait.saturating_mul(2);
        }
        Err(TrackerError::Udp(
            "tracker did not answer within the retransmission budget".to_string(),
        ))
    }

    async fn ensure_connection(&mut self) -> Result<u64, TrackerError> {
        let now = tokio::time::Instant::now();
        if let Some((id, at)) = self.connection_id {
            if now.duration_since(at) <= self.config.connection_lifetime {
                return Ok(id);
            }
        }
        let transaction_id = self.transaction_id();
        let request = encode_connect_request(transaction_id);
        let reply = self.exchange(&request, transaction_id).await?;
        let connection_id = decode_connect_reply(&reply, transaction_id)?;
        self.connection_id = Some((connection_id, now));
        Ok(connection_id)
    }

    pub async fn announce(
        &mut self,
        request: &AnnounceRequest,
        numwant: i32,
    ) -> Result<AnnounceResponse, TrackerError> {
        let connection_id = self.ensure_connection().await?;
        let transaction_id = self.transaction_id();
        let packet = encode_announce_request(connection_id, transaction_id, request, numwant);
        let reply = self.exchange(&packet, transaction_id).await?;
        if let Some(message) = decode_error_reply(&reply, transaction_id) {
            return Err(TrackerError::Udp(format!("tracker error: {message}")));
        }
        let ipv6 = self
            .socket
            .local_addr()
            .map(|addr| addr.is_ipv6())
            .unwrap_or(false);
        let reply = decode_announce_reply(&reply, transaction_id, ipv6)?;
        Ok(AnnounceResponse {
            interval: reply.interval as u64,
            min_interval: None,
            complete: reply.seeders as u64,
            incomplete: reply.leechers as u64,
            peers: reply.peers,
        })
    }
}

fn rand_transaction_id() -> [u8; 4] {
    use rand::Rng;
    let mut rng = rand::rng();
    [rng.random(), rng.random(), rng.random(), rng.random()]
}

pub type SharedUdpTrackerClient = Arc<Mutex<UdpTrackerClient>>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    fn announce_request() -> AnnounceRequest {
        AnnounceRequest {
            info_hash: [0x11; 20],
            peer_id: *crate::peer_id::session(),
            port: 6881,
            uploaded: 10,
            downloaded: 20,
            left: 30,
            numwant: 50,
            event: Some(crate::tracker::Event::Started),
        }
    }

    #[test]
    fn parses_udp_urls_including_ipv6_literals() {
        assert_eq!(
            parse_udp_tracker_url("udp://tracker.example.com:1337/announce").unwrap(),
            ("tracker.example.com".to_string(), 1337)
        );
        assert_eq!(
            parse_udp_tracker_url("udp://tracker.example.com:1337").unwrap(),
            ("tracker.example.com".to_string(), 1337)
        );
        assert_eq!(
            parse_udp_tracker_url("udp://[2001:db8::1]:6969/announce").unwrap(),
            ("2001:db8::1".to_string(), 6969)
        );
        assert!(parse_udp_tracker_url("http://x/announce").is_err());
        assert!(parse_udp_tracker_url("udp://tracker.example.com").is_err());
    }

    #[test]
    fn event_codes_map_per_spec() {
        assert_eq!(event_code(None), 0);
        assert_eq!(event_code(Some(crate::tracker::Event::Completed)), 1);
        assert_eq!(event_code(Some(crate::tracker::Event::Started)), 2);
        assert_eq!(event_code(Some(crate::tracker::Event::Stopped)), 3);
    }

    #[test]
    fn connect_round_trip() {
        let transaction_id = [7u8; 4];
        let packet = encode_connect_request(transaction_id);
        assert_eq!(packet.len(), 16);
        assert_eq!(u32::from_be_bytes(packet[8..12].try_into().unwrap()), 0);
        assert_eq!(&packet[12..16], &transaction_id);
        let id = decode_connect_reply(&packet, transaction_id).is_err();
        assert!(id);
        let mut reply = Vec::new();
        reply.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
        reply.extend_from_slice(&transaction_id);
        reply.extend_from_slice(&0xdead_beef_1234_5678u64.to_be_bytes());
        assert_eq!(
            decode_connect_reply(&reply, transaction_id).unwrap(),
            0xdead_beef_1234_5678
        );
        assert!(decode_connect_reply(&reply, [1u8; 4]).is_err());
        assert!(decode_connect_reply(&reply[..10], transaction_id).is_err());
    }

    #[test]
    fn announce_round_trip_v4_and_v6() {
        let request = announce_request();
        let packet = encode_announce_request(0x1234, [9u8; 4], &request, -1);
        assert_eq!(packet.len(), 98);
        assert_eq!(u32::from_be_bytes(packet[8..12].try_into().unwrap()), 1);
        assert_eq!(&packet[12..16], &[9u8; 4]);
        assert_eq!(&packet[16..36], &request.info_hash);
        assert_eq!(&packet[36..56], &request.peer_id);
        assert_eq!(u64::from_be_bytes(packet[56..64].try_into().unwrap()), 20);
        assert_eq!(u64::from_be_bytes(packet[64..72].try_into().unwrap()), 30);
        assert_eq!(u64::from_be_bytes(packet[72..80].try_into().unwrap()), 10);
        assert_eq!(i32::from_be_bytes(packet[80..84].try_into().unwrap()), 2);
        assert_eq!(u16::from_be_bytes(packet[96..98].try_into().unwrap()), 6881);

        let mut reply = Vec::new();
        reply.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
        reply.extend_from_slice(&[9u8; 4]);
        reply.extend_from_slice(&1800u32.to_be_bytes());
        reply.extend_from_slice(&7u32.to_be_bytes());
        reply.extend_from_slice(&3u32.to_be_bytes());
        reply.extend_from_slice(&[192, 168, 1, 1, 27, 10]);
        let parsed = decode_announce_reply(&reply, [9u8; 4], false).unwrap();
        assert_eq!(parsed.interval, 1800);
        assert_eq!(parsed.seeders, 3);
        assert_eq!(parsed.leechers, 7);
        assert_eq!(
            parsed.peers,
            vec![SocketAddr::from(([192, 168, 1, 1], 6922))]
        );

        let mut reply6 = reply[..20].to_vec();
        reply6.extend_from_slice(&[0x20; 16]);
        reply6.extend_from_slice(&80u16.to_be_bytes());
        let parsed6 = decode_announce_reply(&reply6, [9u8; 4], true).unwrap();
        assert_eq!(parsed6.peers.len(), 1);
        assert!(parsed6.peers[0].is_ipv6());
    }

    #[test]
    fn rejects_short_and_malformed_replies() {
        let mut reply = Vec::new();
        reply.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
        reply.extend_from_slice(&[9u8; 4]);
        reply.extend_from_slice(&1800u32.to_be_bytes());
        assert!(decode_announce_reply(&reply, [9u8; 4], false).is_err());
        let truncated = reply[..8].to_vec();
        assert!(decode_announce_reply(&truncated, [9u8; 4], false).is_err());
        assert!(decode_announce_reply(&[0xff; 30], [9u8; 4], false).is_err());
        let bad_peers = {
            let mut packet = Vec::new();
            packet.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
            packet.extend_from_slice(&[9u8; 4]);
            packet.extend_from_slice(&[0u8; 12]);
            packet.extend_from_slice(&[1u8; 7]);
            packet
        };
        assert!(decode_announce_reply(&bad_peers, [9u8; 4], false).is_err());
    }

    #[test]
    fn error_reply_surfaces_message() {
        let mut packet = Vec::new();
        packet.extend_from_slice(&ACTION_ERROR.to_be_bytes());
        packet.extend_from_slice(&[3u8; 4]);
        packet.extend_from_slice(b"no such torrent");
        assert_eq!(
            decode_error_reply(&packet, [3u8; 4]).as_deref(),
            Some("no such torrent")
        );
        assert!(decode_error_reply(&packet, [9u8; 4]).is_none());
        assert!(decode_error_reply(&packet[..4], [3u8; 4]).is_none());
    }

    #[tokio::test]
    async fn full_connect_and_announce_round_trip_with_fake_tracker() {
        let fake = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let tracker_addr = fake.local_addr().unwrap();
        let url = format!("udp://{tracker_addr}/announce");
        let mut client = UdpTrackerClient::connect_tracker(&url, UdpConfig::default())
            .await
            .unwrap();

        let fake_task = tokio::spawn(async move {
            let mut buffer = vec![0u8; 512];
            let (read, from) = fake.recv_from(&mut buffer).await.unwrap();
            assert_eq!(read, 16);
            assert_eq!(
                u64::from_be_bytes(buffer[..8].try_into().unwrap()),
                UDP_PROTOCOL_MAGIC
            );
            let _ = from;
            let transaction_id = [buffer[12], buffer[13], buffer[14], buffer[15]];
            let mut reply = Vec::new();
            reply.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
            reply.extend_from_slice(&transaction_id);
            reply.extend_from_slice(&0xfeedu64.to_be_bytes());
            fake.send_to(&reply, from).await.unwrap();

            let (read, from) = fake.recv_from(&mut buffer).await.unwrap();
            assert_eq!(read, 98);
            assert_eq!(u64::from_be_bytes(buffer[..8].try_into().unwrap()), 0xfeed);
            let transaction_id = [buffer[12], buffer[13], buffer[14], buffer[15]];
            assert_eq!(u16::from_be_bytes(buffer[96..98].try_into().unwrap()), 6881);
            let mut reply = Vec::new();
            reply.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
            reply.extend_from_slice(&transaction_id);
            reply.extend_from_slice(&120u32.to_be_bytes());
            reply.extend_from_slice(&2u32.to_be_bytes());
            reply.extend_from_slice(&5u32.to_be_bytes());
            reply.extend_from_slice(&[10, 0, 0, 1, 31, 144]);
            fake.send_to(&reply, from).await.unwrap();
        });

        let response = client.announce(&announce_request(), -1).await.unwrap();
        fake_task.await.unwrap();
        assert_eq!(response.interval, 120);
        assert_eq!(response.complete, 5);
        assert_eq!(response.incomplete, 2);
        assert_eq!(
            response.peers,
            vec![SocketAddr::from(([10, 0, 0, 1], 8080))]
        );
    }

    #[tokio::test]
    async fn wrong_transaction_id_is_ignored_then_correct_answer_accepted() {
        let fake = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let tracker_addr = fake.local_addr().unwrap();
        let url = format!("udp://{tracker_addr}/announce");
        let mut client = UdpTrackerClient::connect_tracker(&url, UdpConfig::default())
            .await
            .unwrap();

        let fake_task = tokio::spawn(async move {
            let mut buffer = vec![0u8; 512];
            let (_, from) = fake.recv_from(&mut buffer).await.unwrap();
            let transaction_id = [buffer[12], buffer[13], buffer[14], buffer[15]];
            let mut bad = Vec::new();
            bad.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
            bad.extend_from_slice(&[9u8; 4]);
            bad.extend_from_slice(&1u64.to_be_bytes());
            fake.send_to(&bad, from).await.unwrap();
            let mut good = Vec::new();
            good.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
            good.extend_from_slice(&transaction_id);
            good.extend_from_slice(&7u64.to_be_bytes());
            fake.send_to(&good, from).await.unwrap();

            let (_, from) = fake.recv_from(&mut buffer).await.unwrap();
            let transaction_id = [buffer[12], buffer[13], buffer[14], buffer[15]];
            let mut reply = Vec::new();
            reply.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
            reply.extend_from_slice(&transaction_id);
            reply.extend_from_slice(&[0u8; 12]);
            fake.send_to(&reply, from).await.unwrap();
        });

        let response = client.announce(&announce_request(), -1).await.unwrap();
        fake_task.await.unwrap();
        assert_eq!(response.interval, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_first_datagram_triggers_retransmit() {
        let fake = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let tracker_addr = fake.local_addr().unwrap();
        let url = format!("udp://{tracker_addr}/announce");
        let config = UdpConfig {
            initial_timeout: Duration::from_millis(100),
            max_retransmits: 2,
            connection_lifetime: CONNECTION_LIFETIME,
        };
        let mut client = UdpTrackerClient::connect_tracker(&url, config)
            .await
            .unwrap();

        let fake_task = tokio::spawn(async move {
            let mut buffer = vec![0u8; 512];
            let (_, from) = fake.recv_from(&mut buffer).await.unwrap();
            let (_, from2) = fake.recv_from(&mut buffer).await.unwrap();
            let _ = from2;
            let transaction_id = [buffer[12], buffer[13], buffer[14], buffer[15]];
            let mut reply = Vec::new();
            reply.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
            reply.extend_from_slice(&transaction_id);
            reply.extend_from_slice(&9u64.to_be_bytes());
            fake.send_to(&reply, from).await.unwrap();
        });

        let connection = client.ensure_connection().await.unwrap();
        fake_task.await.unwrap();
        assert_eq!(connection, 9);
    }

    #[tokio::test(start_paused = true)]
    async fn unreachable_tracker_fails_within_the_retransmit_budget() {
        let url = "udp://127.0.0.1:9/announce";
        let config = UdpConfig {
            initial_timeout: Duration::from_millis(100),
            max_retransmits: 2,
            connection_lifetime: CONNECTION_LIFETIME,
        };
        let mut client = UdpTrackerClient::connect_tracker(url, config)
            .await
            .unwrap();
        let result = client.ensure_connection().await;
        assert!(matches!(result, Err(TrackerError::Udp(_))));
    }

    #[tokio::test]
    async fn packet_from_another_address_is_ignored() {
        let fake = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let tracker_addr = fake.local_addr().unwrap();
        let impostor = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let url = format!("udp://{tracker_addr}/announce");
        let mut client = UdpTrackerClient::connect_tracker(&url, UdpConfig::default())
            .await
            .unwrap();

        let impostor_task = tokio::spawn(async move {
            impostor.recv_from(&mut [0u8; 1]).await.unwrap();
            let mut bogus = Vec::new();
            bogus.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
            bogus.extend_from_slice(&[1u8; 4]);
            bogus.extend_from_slice(&42u64.to_be_bytes());
            bogus
        });
        let fake_task = tokio::spawn(async move {
            let mut buffer = vec![0u8; 512];
            let (_, from) = fake.recv_from(&mut buffer).await.unwrap();
            let transaction_id = [buffer[12], buffer[13], buffer[14], buffer[15]];
            let mut reply = Vec::new();
            reply.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
            reply.extend_from_slice(&transaction_id);
            reply.extend_from_slice(&0x77u64.to_be_bytes());
            fake.send_to(&reply, from).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        impostor_task.abort();
        let connection = client.ensure_connection().await.unwrap();
        fake_task.await.unwrap();
        assert_eq!(connection, 0x77);
    }

    #[tokio::test(start_paused = true)]
    async fn expired_connection_id_causes_a_reconnect() {
        let fake = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let tracker_addr = fake.local_addr().unwrap();
        let url = format!("udp://{tracker_addr}/announce");
        let config = UdpConfig {
            initial_timeout: Duration::from_millis(100),
            max_retransmits: 2,
            connection_lifetime: Duration::from_millis(200),
        };
        let mut client = UdpTrackerClient::connect_tracker(&url, config)
            .await
            .unwrap();

        let fake_task = tokio::spawn(async move {
            let mut buffer = vec![0u8; 512];
            let mut connects = 0;
            for expected_connects in 0..2 {
                loop {
                    let (_, from) = fake.recv_from(&mut buffer).await.unwrap();
                    let action = u32::from_be_bytes(buffer[8..12].try_into().unwrap());
                    if action == ACTION_CONNECT {
                        connects += 1;
                        let transaction_id = [buffer[12], buffer[13], buffer[14], buffer[15]];
                        let mut reply = Vec::new();
                        reply.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
                        reply.extend_from_slice(&transaction_id);
                        reply.extend_from_slice(&(1000 + expected_connects as u64).to_be_bytes());
                        fake.send_to(&reply, from).await.unwrap();
                        break;
                    }
                }
            }
            connects
        });

        let _ = client.ensure_connection().await.unwrap();
        tokio::time::advance(Duration::from_millis(300)).await;
        let second = client.ensure_connection().await.unwrap();
        let connects = fake_task.await.unwrap();
        assert_eq!(connects, 2);
        assert_eq!(second, 1001);
    }
}
