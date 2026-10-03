use std::time::Duration;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::timeout;

use crate::error::{HandshakeError, PeerError};

pub const HANDSHAKE_LENGTH: usize = 68;
const PROTOCOL: &[u8; 19] = b"BitTorrent protocol";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Handshake {
    pub info_hash: [u8; 20],
    pub reserved: [u8; 8],
    pub peer_id: [u8; 20],
}

pub fn encode(handshake: &Handshake) -> [u8; HANDSHAKE_LENGTH] {
    let mut bytes = [0u8; HANDSHAKE_LENGTH];
    bytes[0] = 19;
    bytes[1..20].copy_from_slice(PROTOCOL);
    bytes[20..28].copy_from_slice(&handshake.reserved);
    bytes[28..48].copy_from_slice(&handshake.info_hash);
    bytes[48..68].copy_from_slice(&handshake.peer_id);
    bytes
}

pub fn decode(bytes: &[u8]) -> Result<Handshake, HandshakeError> {
    if bytes.len() != HANDSHAKE_LENGTH {
        return Err(HandshakeError::InvalidLength(bytes.len()));
    }
    if bytes[0] != 19 || &bytes[1..20] != PROTOCOL {
        return Err(HandshakeError::InvalidProtocol);
    }
    let mut reserved = [0u8; 8];
    reserved.copy_from_slice(&bytes[20..28]);
    let mut info_hash = [0u8; 20];
    info_hash.copy_from_slice(&bytes[28..48]);
    let mut peer_id = [0u8; 20];
    peer_id.copy_from_slice(&bytes[48..68]);
    Ok(Handshake {
        info_hash,
        reserved,
        peer_id,
    })
}

pub async fn exchange<S>(
    stream: &mut S,
    info_hash: [u8; 20],
    our_peer_id: [u8; 20],
    handshake_timeout: Duration,
) -> Result<Handshake, PeerError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let request = encode(&Handshake {
        info_hash,
        reserved: [0; 8],
        peer_id: our_peer_id,
    });
    let exchange = async {
        stream.write_all(&request).await?;
        let mut response = [0u8; HANDSHAKE_LENGTH];
        stream.read_exact(&mut response).await?;
        Ok::<_, std::io::Error>(response)
    };
    let response = timeout(handshake_timeout, exchange)
        .await
        .map_err(|_| PeerError::Timeout)?
        .map_err(PeerError::Io)?;
    let remote = decode(&response).map_err(PeerError::Handshake)?;
    if remote.info_hash != info_hash {
        return Err(PeerError::Handshake(HandshakeError::InfoHashMismatch));
    }
    if remote.peer_id == our_peer_id {
        return Err(PeerError::Handshake(HandshakeError::SelfConnection));
    }
    Ok(remote)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[test]
    fn round_trips_handshake() {
        let handshake = Handshake {
            info_hash: [1; 20],
            reserved: [3; 8],
            peer_id: [2; 20],
        };
        let bytes = encode(&handshake);
        assert_eq!(bytes.len(), 68);
        assert_eq!(&bytes[1..20], b"BitTorrent protocol");
        assert_eq!(decode(&bytes).unwrap(), handshake);
    }

    #[test]
    fn rejects_bad_protocol_string() {
        let mut bytes = [0u8; 68];
        bytes[0] = 19;
        bytes[1..20].copy_from_slice(b"BitTorrent-protoco1");
        assert!(matches!(
            decode(&bytes),
            Err(HandshakeError::InvalidProtocol)
        ));
        bytes[1..20].copy_from_slice(PROTOCOL);
        bytes[0] = 18;
        assert!(matches!(
            decode(&bytes),
            Err(HandshakeError::InvalidProtocol)
        ));
    }

    #[test]
    fn rejects_wrong_handshake_length() {
        assert!(matches!(
            decode(&[0u8; 67]),
            Err(HandshakeError::InvalidLength(67))
        ));
        assert!(matches!(
            decode(&[0u8; 69]),
            Err(HandshakeError::InvalidLength(69))
        ));
    }

    #[tokio::test]
    async fn rejects_reply_with_our_peer_id() {
        let (mut our_side, mut their_side) = duplex(128);
        let info_hash = [0x42; 20];
        let our_peer_id = *b"-BT0001-abcdefghijkl";
        let fake = tokio::spawn(async move {
            let mut request = [0u8; 68];
            their_side.read_exact(&mut request).await.unwrap();
            let reply = encode(&Handshake {
                info_hash,
                reserved: [0; 8],
                peer_id: our_peer_id,
            });
            their_side.write_all(&reply).await.unwrap();
        });
        let err = exchange(
            &mut our_side,
            info_hash,
            our_peer_id,
            Duration::from_millis(200),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            PeerError::Handshake(HandshakeError::SelfConnection)
        ));
        fake.await.unwrap();
    }
}
