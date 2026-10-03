use bt_core::hex;
use bt_core::metainfo::{Content, MetaInfo};

const FIXTURE: &[u8] = include_bytes!("fixtures/debian-13.7.0-amd64-netinst.iso.torrent");

const LENGTH_BYTES: u64 = 792_723_456;
const PIECE_LENGTH_BYTES: u32 = 262_144;
const PIECE_COUNT: usize = 3_024;
const INFO_HASH_HEX: &str = "7acf8fb590b2060dd9c3146ef770169d593433b0";

#[test]
fn parses_real_debian_netinst_torrent() {
    let meta = MetaInfo::from_bytes(FIXTURE).unwrap();

    assert_eq!(meta.info.name, "debian-13.7.0-amd64-netinst.iso");
    let Content::Single { length } = meta.info.content else {
        panic!("expected a single-file torrent");
    };
    assert_eq!(length, LENGTH_BYTES);
    assert_eq!(meta.info.total_length().unwrap(), LENGTH_BYTES);
    assert_eq!(meta.info.piece_length, PIECE_LENGTH_BYTES);
    assert_eq!(meta.info.pieces.len(), PIECE_COUNT);
    assert_eq!(
        length.div_ceil(u64::from(meta.info.piece_length)),
        meta.info.pieces.len() as u64
    );
    assert_eq!(hex::encode(&meta.info_hash), INFO_HASH_HEX);
    assert_eq!(
        meta.announce.as_deref(),
        Some("http://bttracker.debian.org:6969/announce")
    );
    assert!(!meta.info.private);
}
