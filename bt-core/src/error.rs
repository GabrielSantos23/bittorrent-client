use thiserror::Error;

#[derive(Debug, Error)]
pub enum BencodeError {
    #[error("unexpected end of input at byte {0}")]
    UnexpectedEof(usize),
    #[error("invalid type marker at byte {0}")]
    InvalidMarker(usize),
    #[error("invalid integer at byte {0}")]
    InvalidInteger(usize),
    #[error("invalid string length at byte {0}")]
    InvalidStringLength(usize),
    #[error("string of {0} bytes exceeds the remaining input")]
    StringTooLong(usize),
    #[error("duplicate dictionary key at byte {0}")]
    DuplicateKey(usize),
    #[error("dictionary key is not a byte string at byte {0}")]
    InvalidKey(usize),
    #[error("trailing bytes after the top-level value at byte {0}")]
    TrailingBytes(usize),
    #[error("nesting depth limit exceeded at byte {0}")]
    MaxDepthExceeded(usize),
}

#[derive(Debug, Error)]
pub enum MetaInfoError {
    #[error("bencode error: {0}")]
    Bencode(#[from] BencodeError),
    #[error("top-level value is not a dictionary")]
    NotADictionary,
    #[error("missing 'info' dictionary")]
    MissingInfo,
    #[error("missing required key '{0}'")]
    MissingKey(&'static str),
    #[error("key '{0}' has an unexpected type")]
    WrongType(&'static str),
    #[error("'pieces' is not a multiple of 20 bytes")]
    InvalidPieces,
    #[error("'piece length' must be between 1 and {0}")]
    InvalidPieceLength(u64),
    #[error("'pieces' contains {actual} hashes but {expected} are required")]
    PieceCountMismatch { expected: u64, actual: u64 },
    #[error("file lengths overflow a u64 total")]
    LengthOverflow,
    #[error("key '{0}' contains an unsafe path")]
    InvalidPath(&'static str),
}

#[derive(Debug, Error)]
pub enum HexError {
    #[error("hex string has odd length {0}")]
    OddLength(usize),
    #[error("invalid hex digit '{0}'")]
    InvalidDigit(char),
}

#[derive(Debug, Error)]
pub enum TrackerError {
    #[error("bencode error: {0}")]
    Bencode(#[from] BencodeError),
    #[error("tracker response is not a bencoded dictionary")]
    NotADictionary,
    #[error("tracker failure: {0}")]
    Failure(String),
    #[error("tracker response is missing key '{0}'")]
    MissingKey(&'static str),
    #[error("tracker response key '{0}' has an unexpected type")]
    WrongType(&'static str),
    #[error("compact peers section is not a multiple of 6 bytes")]
    InvalidCompactPeers,
    #[error("compact peers6 section is not a multiple of 18 bytes")]
    InvalidCompactPeers6,
    #[error("peer entry in dictionary model is invalid")]
    InvalidPeerEntry,
    #[error("tracker returned HTTP status {0}")]
    HttpStatus(u16),
    #[error("tracker response of {0} bytes exceeds the {1} byte limit")]
    ResponseTooLarge(u64, usize),
    #[error("announce request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("no tracker urls available")]
    NoTrackers,
    #[error("udp tracker: {0}")]
    Udp(String),
}

#[derive(Debug, Error)]
pub enum PeerError {
    #[error("peer io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("operation timed out")]
    Timeout,
    #[error("peer closed the connection unexpectedly")]
    ConnectionClosed,
    #[error("peer message exceeds the {0} byte limit")]
    OversizedMessage(usize),
    #[error("handshake failed: {0}")]
    Handshake(#[from] HandshakeError),
    #[error("message decode failed: {0}")]
    Message(#[from] MessageError),
}

#[derive(Debug, Error)]
pub enum HandshakeError {
    #[error("handshake must be exactly 68 bytes, got {0}")]
    InvalidLength(usize),
    #[error("handshake has an invalid protocol string")]
    InvalidProtocol,
    #[error("handshake info_hash does not match the torrent")]
    InfoHashMismatch,
    #[error("handshake reply carries our own peer id")]
    SelfConnection,
}

#[derive(Debug, Error)]
pub enum MessageError {
    #[error("message payload is empty")]
    EmptyPayload,
    #[error("message id {0} has an invalid payload length of {1} bytes")]
    InvalidPayloadLength(u8, usize),
    #[error("invalid bitfield: {0}")]
    Bitfield(#[from] BitfieldError),
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("piece index {0} is out of range")]
    PieceOutOfRange(usize),
    #[error("torrent contains an unsafe path")]
    UnsafePath,
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("tracker error: {0}")]
    Tracker(#[from] TrackerError),
    #[error("engine channel is closed")]
    Closed,
    #[error("internal task failed")]
    Task,
}

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("torrent already exists: {0}")]
    Duplicate(String),
    #[error("unknown torrent: {0}")]
    Unknown(String),
    #[error("invalid metainfo: {0}")]
    Metainfo(#[from] MetaInfoError),
    #[error("engine error: {0}")]
    Engine(#[from] EngineError),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("session channel is closed")]
    Closed,
    #[error("{0}")]
    Listen(String),
    #[error("{0}")]
    Dht(String),
    #[error("{0}")]
    Magnet(#[from] crate::magnet::MagnetError),
}

#[derive(Debug, Error)]
pub enum BitfieldError {
    #[error("bitfield length is {0} but {1} bytes are required")]
    InvalidLength(usize, usize),
    #[error("bitfield has spare bits set beyond the piece count")]
    SpareBitsSet,
    #[error("piece index {0} is out of range")]
    IndexOutOfRange(usize),
}

#[derive(Debug, Error)]
pub enum ResumeError {
    #[error("no resume file")]
    Missing,
    #[error("resume file of {0} bytes exceeds the {1} byte limit")]
    TooLarge(u64, u64),
    #[error("resume file is corrupt or truncated")]
    Corrupt,
    #[error("resume file uses unsupported format version {0}")]
    UnknownVersion(u32),
    #[error("resume file info hash does not match the torrent")]
    InfoHashMismatch,
    #[error("resume file claims {actual} pieces but the torrent has {expected}")]
    PieceCountMismatch { expected: usize, actual: usize },
    #[error("resume file lists {actual} files but the torrent has {expected}")]
    FileCountMismatch { expected: usize, actual: usize },
    #[error("resume file records length {actual} for file {index} but the torrent has {expected}")]
    FileLengthMismatch {
        index: usize,
        expected: u64,
        actual: u64,
    },
    #[error("resume file bitfield is invalid: {0}")]
    Bitfield(#[from] BitfieldError),
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
