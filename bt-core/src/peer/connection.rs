use std::net::SocketAddr;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::error::PeerError;

use super::handshake::{self, Handshake};
use super::message::{Message, MAX_MESSAGE_LENGTH};

const LENGTH_PREFIX: usize = 4;
const KEEP_ALIVE_FRAME: [u8; 4] = [0, 0, 0, 0];
const READ_CHUNK: usize = 8192;

#[derive(Debug, Clone, Copy)]
pub struct PeerConfig {
    pub connect_timeout: Duration,
    pub handshake_timeout: Duration,
    pub read_timeout: Duration,
    pub keep_alive_interval: Duration,
}

impl Default for PeerConfig {
    fn default() -> Self {
        PeerConfig {
            connect_timeout: Duration::from_secs(5),
            handshake_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(120),
            keep_alive_interval: Duration::from_secs(90),
        }
    }
}

pub struct PeerConnection<S> {
    reader: PeerReader<S>,
    writer: PeerWriter<S>,
    remote: Handshake,
    piece_count: Option<usize>,
    config: PeerConfig,
}

impl<S> std::fmt::Debug for PeerConnection<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerConnection")
            .field("remote", &self.remote)
            .field("piece_count", &self.piece_count)
            .field("config", &self.config)
            .finish()
    }
}

impl<S: AsyncRead + AsyncWrite> PeerConnection<S> {
    pub fn new(
        stream: S,
        remote: Handshake,
        piece_count: Option<usize>,
        config: PeerConfig,
    ) -> Self {
        let (read, write) = tokio::io::split(stream);
        PeerConnection {
            reader: PeerReader {
                stream: read,
                buffer: BytesMut::new(),
            },
            writer: PeerWriter { stream: write },
            remote,
            piece_count,
            config,
        }
    }

    pub fn remote_peer_id(&self) -> [u8; 20] {
        self.remote.peer_id
    }

    pub fn remote(&self) -> Handshake {
        self.remote
    }

    pub fn reserved(&self) -> [u8; 8] {
        self.remote.reserved
    }
}

impl<S: AsyncRead + Unpin> PeerConnection<S> {
    pub async fn read_message(&mut self) -> Result<Message, PeerError> {
        let frame = self.reader.read_frame(self.config.read_timeout).await?;
        if frame.is_empty() {
            return Ok(Message::KeepAlive);
        }
        Ok(Message::decode(&frame, self.piece_count)?)
    }
}

impl<S: AsyncWrite + Unpin> PeerConnection<S> {
    pub async fn write_message(&mut self, message: &Message) -> Result<(), PeerError> {
        self.writer.write_frame(&message.encode()).await
    }

    pub async fn send_keep_alive(&mut self) -> Result<(), PeerError> {
        self.writer.write_frame(&KEEP_ALIVE_FRAME).await
    }
}

struct PeerReader<R> {
    stream: ReadHalf<R>,
    buffer: BytesMut,
}

impl<R: AsyncRead + Unpin> PeerReader<R> {
    async fn read_frame(&mut self, idle_timeout: Duration) -> Result<Vec<u8>, PeerError> {
        loop {
            if let Some(frame) = self.take_frame()? {
                return Ok(frame);
            }
            match timeout(idle_timeout, self.fill()).await {
                Err(_) => return Err(PeerError::Timeout),
                Ok(Ok(0)) => return Err(PeerError::ConnectionClosed),
                Ok(Ok(_)) => {}
                Ok(Err(err)) => return Err(err.into()),
            }
        }
    }

    async fn fill(&mut self) -> std::io::Result<usize> {
        let PeerReader { stream, buffer } = self;
        if buffer.capacity() - buffer.len() < READ_CHUNK {
            buffer.reserve(READ_CHUNK);
        }
        stream.read_buf(buffer).await
    }

    fn take_frame(&mut self) -> Result<Option<Vec<u8>>, PeerError> {
        if self.buffer.len() < LENGTH_PREFIX {
            return Ok(None);
        }
        let length = u32::from_be_bytes([
            self.buffer[0],
            self.buffer[1],
            self.buffer[2],
            self.buffer[3],
        ]) as usize;
        if length > MAX_MESSAGE_LENGTH {
            return Err(PeerError::OversizedMessage(MAX_MESSAGE_LENGTH));
        }
        let total = LENGTH_PREFIX + length;
        if self.buffer.len() < total {
            self.buffer.reserve(total - self.buffer.len());
            return Ok(None);
        }
        let mut frame = self.buffer.split_to(total);
        let _ = frame.split_to(LENGTH_PREFIX);
        Ok(Some(frame.to_vec()))
    }
}

struct PeerWriter<W> {
    stream: WriteHalf<W>,
}

impl<W: AsyncWrite + Unpin> PeerWriter<W> {
    async fn write_frame(&mut self, frame: &[u8]) -> Result<(), PeerError> {
        self.stream.write_all(frame).await?;
        self.stream.flush().await?;
        Ok(())
    }
}

pub struct PeerReadHalf<S> {
    inner: PeerReader<S>,
    piece_count: Option<usize>,
    read_timeout: Duration,
}

impl<S: AsyncRead + Unpin> PeerReadHalf<S> {
    pub async fn read_message(&mut self) -> Result<Message, PeerError> {
        let frame = self.inner.read_frame(self.read_timeout).await?;
        if frame.is_empty() {
            return Ok(Message::KeepAlive);
        }
        Ok(Message::decode(&frame, self.piece_count)?)
    }
}

pub struct PeerWriteHalf<S> {
    inner: PeerWriter<S>,
}

impl<S: AsyncWrite + Unpin> PeerWriteHalf<S> {
    pub async fn write_message(&mut self, message: &Message) -> Result<(), PeerError> {
        self.inner.write_frame(&message.encode()).await
    }

    pub async fn send_keep_alive(&mut self) -> Result<(), PeerError> {
        self.inner.write_frame(&KEEP_ALIVE_FRAME).await
    }
}

impl<S: AsyncRead + AsyncWrite> PeerConnection<S> {
    pub fn into_halves(self) -> (PeerReadHalf<S>, PeerWriteHalf<S>) {
        let PeerConnection {
            reader,
            writer,
            piece_count,
            config,
            ..
        } = self;
        (
            PeerReadHalf {
                inner: reader,
                piece_count,
                read_timeout: config.read_timeout,
            },
            PeerWriteHalf { inner: writer },
        )
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> PeerConnection<S> {
    pub async fn connect_stream(
        mut stream: S,
        info_hash: [u8; 20],
        our_peer_id: [u8; 20],
        piece_count: Option<usize>,
        config: PeerConfig,
    ) -> Result<PeerConnection<S>, PeerError> {
        let remote = handshake::exchange(
            &mut stream,
            info_hash,
            our_peer_id,
            crate::extensions::reserved_with_extensions(),
            config.handshake_timeout,
        )
        .await?;
        Ok(PeerConnection::new(stream, remote, piece_count, config))
    }
}

pub async fn connect(
    addr: SocketAddr,
    info_hash: [u8; 20],
    our_peer_id: [u8; 20],
    piece_count: Option<usize>,
    config: PeerConfig,
) -> Result<PeerConnection<TcpStream>, PeerError> {
    let mut stream = timeout(config.connect_timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| PeerError::Timeout)??;
    let remote = handshake::exchange(
        &mut stream,
        info_hash,
        our_peer_id,
        crate::extensions::reserved_with_extensions(),
        config.handshake_timeout,
    )
    .await?;
    Ok(PeerConnection::new(stream, remote, piece_count, config))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::HandshakeError;
    use crate::peer::bitfield::Bitfield;
    use tokio::io::duplex;

    const OUR_PEER_ID: [u8; 20] = *b"-BT0001-abcdefghijkl";

    fn test_config() -> PeerConfig {
        PeerConfig {
            connect_timeout: Duration::from_secs(1),
            handshake_timeout: Duration::from_millis(200),
            read_timeout: Duration::from_millis(500),
            keep_alive_interval: Duration::from_millis(100),
        }
    }

    fn fake_handshake(info_hash: [u8; 20], peer_id: [u8; 20]) -> [u8; 68] {
        handshake::encode(&Handshake {
            info_hash,
            reserved: [0; 8],
            peer_id,
        })
    }

    async fn write_handshake<S: AsyncWrite + Unpin>(stream: &mut S, bytes: &[u8; 68]) {
        stream.write_all(bytes).await.unwrap();
    }

    #[tokio::test]
    async fn exchanges_handshake_and_messages_with_fake_peer() {
        let (our_side, mut their_side) = duplex(4096);
        let info_hash = [0x42; 20];
        let fake = tokio::spawn(async move {
            let mut request = [0u8; 68];
            their_side.read_exact(&mut request).await.unwrap();
            write_handshake(
                &mut their_side,
                &fake_handshake(info_hash, *b"fake-peer-id........"),
            )
            .await;
            their_side
                .write_all(&[0, 0, 0, 3, 5, 0x80, 0x00])
                .await
                .unwrap();
            their_side.write_all(&[0, 0, 0, 1, 1]).await.unwrap();
        });

        let mut conn = PeerConnection::connect_stream(
            our_side,
            info_hash,
            *b"-BT0001-abcdefghijkl",
            Some(16),
            test_config(),
        )
        .await
        .unwrap();
        assert_eq!(conn.remote_peer_id(), *b"fake-peer-id........");
        assert_eq!(conn.reserved(), [0; 8]);

        let mut bitfield = Bitfield::new(16);
        bitfield.set(0).unwrap();
        assert_eq!(
            conn.read_message().await.unwrap(),
            Message::Bitfield(bitfield)
        );
        assert_eq!(conn.read_message().await.unwrap(), Message::Unchoke);
        fake.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_mismatched_info_hash() {
        let (our_side, mut their_side) = duplex(4096);
        let fake = tokio::spawn(async move {
            let mut request = [0u8; 68];
            their_side.read_exact(&mut request).await.unwrap();
            write_handshake(&mut their_side, &fake_handshake([0x99; 20], [7; 20])).await;
        });
        let err = PeerConnection::connect_stream(
            our_side,
            [0x42; 20],
            OUR_PEER_ID,
            Some(16),
            test_config(),
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err,
            PeerError::Handshake(HandshakeError::InfoHashMismatch)
        ));
        fake.await.unwrap();
    }

    #[tokio::test]
    async fn reports_truncated_frames_as_closed() {
        let (our_side, mut their_side) = duplex(4096);
        let info_hash = [0x42; 20];
        let fake = tokio::spawn(async move {
            let mut request = [0u8; 68];
            their_side.read_exact(&mut request).await.unwrap();
            write_handshake(&mut their_side, &fake_handshake(info_hash, [7; 20])).await;
            their_side.write_all(&[0, 0, 0, 10, 1, 2]).await.unwrap();
        });
        let mut conn = PeerConnection::connect_stream(
            our_side,
            info_hash,
            OUR_PEER_ID,
            Some(16),
            test_config(),
        )
        .await
        .unwrap();
        let err = conn.read_message().await.unwrap_err();
        assert!(matches!(err, PeerError::ConnectionClosed));
        fake.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_oversized_frames() {
        let (our_side, mut their_side) = duplex(4096);
        let info_hash = [0x42; 20];
        let fake = tokio::spawn(async move {
            let mut request = [0u8; 68];
            their_side.read_exact(&mut request).await.unwrap();
            write_handshake(&mut their_side, &fake_handshake(info_hash, [7; 20])).await;
            their_side
                .write_all(&[0x00, 0x10, 0x00, 0x01])
                .await
                .unwrap();
        });
        let mut conn = PeerConnection::connect_stream(
            our_side,
            info_hash,
            OUR_PEER_ID,
            Some(16),
            test_config(),
        )
        .await
        .unwrap();
        let err = conn.read_message().await.unwrap_err();
        assert!(matches!(err, PeerError::OversizedMessage(1_048_576)));
        fake.await.unwrap();
    }

    #[tokio::test]
    async fn writes_keep_alive_frame_on_demand() {
        let (our_side, mut their_side) = duplex(64);
        let info_hash = [0x42; 20];
        let fake = tokio::spawn(async move {
            let mut request = [0u8; 68];
            their_side.read_exact(&mut request).await.unwrap();
            write_handshake(&mut their_side, &fake_handshake(info_hash, [7; 20])).await;
            let mut keep_alive = [0u8; 4];
            their_side.read_exact(&mut keep_alive).await.unwrap();
            assert_eq!(keep_alive, [0, 0, 0, 0]);
            their_side.write_all(&[0, 0, 0, 0]).await.unwrap();
        });
        let mut conn = PeerConnection::connect_stream(
            our_side,
            info_hash,
            OUR_PEER_ID,
            Some(16),
            test_config(),
        )
        .await
        .unwrap();
        conn.send_keep_alive().await.unwrap();
        assert_eq!(conn.read_message().await.unwrap(), Message::KeepAlive);
        fake.await.unwrap();
    }

    #[tokio::test]
    async fn decodes_frame_split_across_idle_gap() {
        let (our_side, mut their_side) = duplex(64);
        let info_hash = [0x42; 20];
        let fake = tokio::spawn(async move {
            let mut request = [0u8; 68];
            their_side.read_exact(&mut request).await.unwrap();
            write_handshake(&mut their_side, &fake_handshake(info_hash, [7; 20])).await;
            their_side.write_all(&[0, 0, 0, 1]).await.unwrap();
            let mut probe = [0u8; 4];
            let received = timeout(
                Duration::from_millis(120),
                their_side.read_exact(&mut probe),
            )
            .await;
            assert!(
                received.is_err(),
                "keep-alive must not be sent from the read path"
            );
            their_side.write_all(&[1]).await.unwrap();
        });
        let config = PeerConfig {
            read_timeout: Duration::from_secs(1),
            keep_alive_interval: Duration::from_millis(50),
            ..test_config()
        };
        let mut conn =
            PeerConnection::connect_stream(our_side, info_hash, OUR_PEER_ID, Some(16), config)
                .await
                .unwrap();
        assert_eq!(conn.read_message().await.unwrap(), Message::Unchoke);
        fake.await.unwrap();
    }

    #[tokio::test]
    async fn recovers_from_idle_timeout_mid_frame() {
        let (our_side, mut their_side) = duplex(64);
        let info_hash = [0x42; 20];
        let fake = tokio::spawn(async move {
            let mut request = [0u8; 68];
            their_side.read_exact(&mut request).await.unwrap();
            write_handshake(&mut their_side, &fake_handshake(info_hash, [7; 20])).await;
            their_side.write_all(&[0, 0, 0, 1]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
            their_side.write_all(&[1]).await.unwrap();
            their_side
                .write_all(&[0, 0, 0, 5, 4, 0, 0, 0, 5])
                .await
                .unwrap();
        });
        let config = PeerConfig {
            read_timeout: Duration::from_millis(80),
            ..test_config()
        };
        let mut conn =
            PeerConnection::connect_stream(our_side, info_hash, OUR_PEER_ID, Some(16), config)
                .await
                .unwrap();
        assert!(matches!(
            conn.read_message().await.unwrap_err(),
            PeerError::Timeout
        ));
        assert_eq!(conn.read_message().await.unwrap(), Message::Unchoke);
        assert_eq!(conn.read_message().await.unwrap(), Message::Have(5));
        fake.await.unwrap();
    }

    #[tokio::test]
    async fn times_out_when_peer_is_silent() {
        let (our_side, mut their_side) = duplex(4096);
        let info_hash = [0x42; 20];
        let fake = tokio::spawn(async move {
            let mut request = [0u8; 68];
            their_side.read_exact(&mut request).await.unwrap();
            write_handshake(&mut their_side, &fake_handshake(info_hash, [7; 20])).await;
            let mut never = [0u8; 4];
            their_side.read_exact(&mut never).await.unwrap();
        });
        let mut conn = PeerConnection::connect_stream(
            our_side,
            info_hash,
            OUR_PEER_ID,
            Some(16),
            test_config(),
        )
        .await
        .unwrap();
        let err = conn.read_message().await.unwrap_err();
        assert!(matches!(err, PeerError::Timeout));
        fake.abort();
    }

    #[tokio::test]
    async fn times_out_handshake_with_silent_peer() {
        let (our_side, mut their_side) = duplex(4096);
        let fake = tokio::spawn(async move {
            let mut request = [0u8; 68];
            their_side.read_exact(&mut request).await.unwrap();
            let mut never = [0u8; 4];
            their_side.read_exact(&mut never).await.unwrap();
        });
        let err = PeerConnection::connect_stream(
            our_side,
            [0x42; 20],
            OUR_PEER_ID,
            Some(16),
            test_config(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, PeerError::Timeout));
        fake.abort();
    }

    #[tokio::test]
    async fn round_trips_messages_through_the_wire() {
        let (our_side, mut their_side) = duplex(4096);
        let info_hash = [0x42; 20];
        let fake = tokio::spawn(async move {
            let mut request = [0u8; 68];
            their_side.read_exact(&mut request).await.unwrap();
            write_handshake(&mut their_side, &fake_handshake(info_hash, [7; 20])).await;
            let mut interested = [0u8; 5];
            their_side.read_exact(&mut interested).await.unwrap();
            assert_eq!(interested, [0, 0, 0, 1, 2]);
            their_side
                .write_all(&[0, 0, 0, 9, 7, 0, 0, 0, 1, 0, 0, 16, 0])
                .await
                .unwrap();
        });
        let mut conn = PeerConnection::connect_stream(
            our_side,
            info_hash,
            OUR_PEER_ID,
            Some(16),
            test_config(),
        )
        .await
        .unwrap();
        conn.write_message(&Message::Interested).await.unwrap();
        assert_eq!(
            conn.read_message().await.unwrap(),
            Message::Piece {
                index: 1,
                begin: 4096,
                block: Vec::new()
            }
        );
        fake.await.unwrap();
    }
}
