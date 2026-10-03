use std::net::SocketAddr;
use std::str::FromStr;

use thiserror::Error;

pub const MAX_MAGNET_LENGTH: usize = 8 * 1024;
pub const MAX_MAGNET_TRACKERS: usize = 50;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MagnetError {
    #[error("magnet uri is {0} bytes, above the {1} byte limit")]
    TooLong(usize, usize),
    #[error("magnet uri is missing the xt field")]
    MissingXt,
    #[error("magnet uri contains more than one xt field")]
    DuplicateXt,
    #[error("unsupported hash urn '{0}' (only urn:btih v1 hashes are supported)")]
    UnsupportedUrn(String),
    #[error("btih hash has {0} characters, expected 40 hex or 32 base32")]
    BadHashLength(usize),
    #[error("btih hash contains invalid characters")]
    BadHashCharacters,
    #[error("magnet uri lists {0} trackers, above the limit of {1}")]
    TooManyTrackers(usize, usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MagnetPeer {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MagnetLink {
    pub info_hash: [u8; 20],
    pub display_name: Option<String>,
    pub trackers: Vec<String>,
    pub peers: Vec<MagnetPeer>,
}

impl MagnetPeer {
    pub fn to_socket_addr(&self) -> Option<SocketAddr> {
        let text = if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        };
        SocketAddr::from_str(&text).ok()
    }
}

pub fn parse(uri: &str) -> Result<MagnetLink, MagnetError> {
    if uri.len() > MAX_MAGNET_LENGTH {
        return Err(MagnetError::TooLong(uri.len(), MAX_MAGNET_LENGTH));
    }
    let query = uri
        .strip_prefix("magnet:?")
        .ok_or(MagnetError::MissingXt)?;

    let mut info_hash: Option<[u8; 20]> = None;
    let mut display_name: Option<String> = None;
    let mut trackers: Vec<String> = Vec::new();
    let mut peers: Vec<MagnetPeer> = Vec::new();

    for pair in query.split('&') {
        let (key, value) = match pair.split_once('=') {
            Some(split) => split,
            None => continue,
        };
        match key {
            "xt" => {
                if info_hash.is_some() {
                    return Err(MagnetError::DuplicateXt);
                }
                info_hash = Some(parse_xt(value)?);
            }
            "dn" => {
                if display_name.is_none() {
                    display_name = Some(
                        percent_decode(value)
                            .ok_or(MagnetError::BadHashCharacters)?
                            .replace('+', " "),
                    );
                }
            }
            "tr" => {
                let decoded = percent_decode(value).ok_or(MagnetError::BadHashCharacters)?;
                let scheme_ok = ["http://", "https://", "udp://"]
                    .iter()
                    .any(|scheme| decoded.to_ascii_lowercase().starts_with(scheme));
                if !scheme_ok || trackers.contains(&decoded) {
                    continue;
                }
                trackers.push(decoded);
                if trackers.len() > MAX_MAGNET_TRACKERS {
                    return Err(MagnetError::TooManyTrackers(
                        trackers.len(),
                        MAX_MAGNET_TRACKERS,
                    ));
                }
            }
            "x.pe" => {
                if let Some(peer) = parse_peer(value) {
                    peers.push(peer);
                }
            }
            _ => {}
        }
    }

    let info_hash = info_hash.ok_or(MagnetError::MissingXt)?;
    Ok(MagnetLink {
        info_hash,
        display_name,
        trackers,
        peers,
    })
}

fn parse_xt(value: &str) -> Result<[u8; 20], MagnetError> {
    let lower = value.to_ascii_lowercase();
    let hash = lower
        .strip_prefix("urn:btih:")
        .ok_or_else(|| MagnetError::UnsupportedUrn(value.to_string()))?;
    match hash.len() {
        40 => {
            let mut bytes = [0u8; 20];
            for (index, byte) in bytes.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&hash[index * 2..index * 2 + 2], 16)
                    .map_err(|_| MagnetError::BadHashCharacters)?;
            }
            Ok(bytes)
        }
        32 => {
            let mut bytes = [0u8; 20];
            let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
            let mut accumulator: u64 = 0;
            let mut bits = 0;
            let mut cursor = 0;
            for symbol in hash.bytes() {
                let upper = (symbol as char).to_ascii_uppercase() as u8;
                let value = alphabet
                    .iter()
                    .position(|candidate| *candidate == upper)
                    .ok_or(MagnetError::BadHashCharacters)? as u64;
                accumulator = (accumulator << 5) | value;
                bits += 5;
                if bits >= 8 {
                    bits -= 8;
                    bytes[cursor] = (accumulator >> bits) as u8;
                    cursor += 1;
                }
            }
            Ok(bytes)
        }
        other => Err(MagnetError::BadHashLength(other)),
    }
}

fn parse_peer(value: &str) -> Option<MagnetPeer> {
    let decoded = percent_decode(value)?;
    if let Some(bracket_end) = decoded.rfind(']') {
        let host = decoded[1..bracket_end].to_string();
        let port = decoded
            .get(bracket_end + 1..)?
            .strip_prefix(':')?
            .parse()
            .ok()?;
        return Some(MagnetPeer { host, port });
    }
    let (host, port) = decoded.rsplit_once(':')?;
    Some(MagnetPeer {
        host: host.to_string(),
        port: port.parse().ok()?,
    })
}

fn percent_decode(value: &str) -> Option<String> {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() + 1 && index + 2 > bytes.len() - 1 {
                return None;
            }
            if index + 2 >= bytes.len() {
                return None;
            }
            let high = (bytes[index + 1] as char).to_digit(16)?;
            let low = (bytes[index + 2] as char).to_digit(16)?;
            out.push((high * 16 + low) as u8);
            index += 3;
        } else {
            out.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEX_HASH: &str = "7acf8fb590b2060dd9c3146ef770169d593433b0";
    const HASH_BYTES: [u8; 20] = [
        0x7a, 0xcf, 0x8f, 0xb5, 0x90, 0xb2, 0x06, 0x0d, 0xd9, 0xc3, 0x14, 0x6e, 0xf7, 0x70,
        0x16, 0x9d, 0x59, 0x34, 0x33, 0xb0,
    ];

    #[test]
    fn parses_hex_hash_with_trackers_and_name() {
        let uri = format!(
            "magnet:?xt=urn:btih:{HEX_HASH}&dn=Debian%2013.7.0%20netinst&tr=http%3A%2F%2Fbttracker.debian.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce"
        );
        let link = parse(&uri).unwrap();
        assert_eq!(link.info_hash, HASH_BYTES);
        assert_eq!(link.display_name.as_deref(), Some("Debian 13.7.0 netinst"));
        assert_eq!(link.trackers.len(), 2);
        assert!(link.trackers[0].starts_with("http://bttracker.debian.org"));
        assert!(link.trackers[1].starts_with("udp://tracker.opentrackr.org"));
    }

    #[test]
    fn parses_uppercase_hex_and_uppercase_scheme() {
        let uri = format!("magnet:?xt=URN:BTIH:{}", HEX_HASH.to_ascii_uppercase());
        let link = parse(&uri).unwrap();
        assert_eq!(link.info_hash, HASH_BYTES);
    }

    #[test]
    fn parses_base32_hash() {
        let uri = "magnet:?xt=urn:btih:AEBAGBAF".to_string() + &"A".repeat(24);
        let link = parse(&uri).unwrap();
        assert_eq!(&link.info_hash[..5], &[0x01, 0x02, 0x03, 0x04, 0x05]);
    }

    #[test]
    fn parses_lowercase_base32() {
        let uri = "magnet:?xt=urn:btih:aebagbaf".to_string() + &"a".repeat(24);
        let link = parse(&uri).unwrap();
        assert_eq!(&link.info_hash[..5], &[0x01, 0x02, 0x03, 0x04, 0x05]);
    }

    #[test]
    fn dedupes_trackers_and_drops_unknown_schemes() {
        let uri = format!(
            "magnet:?xt=urn:btih:{HEX_HASH}&tr=udp%3A%2F%2Ft1%3A80&tr=udp%3A%2F%2Ft1%3A80&tr=ftp%3A%2F%2Fnope&tr=https%3A%2F%2Ft2"
        );
        let link = parse(&uri).unwrap();
        assert_eq!(link.trackers, vec!["udp://t1:80".to_string(), "https://t2".to_string()]);
    }

    #[test]
    fn parses_ip_literal_peers_and_skips_invalid_ones() {
        let uri = format!(
            "magnet:?xt=urn:btih:{HEX_HASH}&x.pe=192.168.1.5:6881&x.pe=%5B2001%3Adb8%3A%3A1%5D%3A1337&x.pe=notanaddress"
        );
        let link = parse(&uri).unwrap();
        assert_eq!(link.peers.len(), 2);
        assert_eq!(
            link.peers[0].to_socket_addr(),
            Some(SocketAddr::from(([192, 168, 1, 5], 6881)))
        );
        assert_eq!(
            link.peers[1].to_socket_addr(),
            Some("[2001:db8::1]:1337".parse().unwrap())
        );
    }

    #[test]
    fn accepts_magnet_without_trackers() {
        let uri = format!("magnet:?xt=urn:btih:{HEX_HASH}&dn=solo");
        let link = parse(&uri).unwrap();
        assert!(link.trackers.is_empty());
        assert!(link.peers.is_empty());
    }

    #[test]
    fn rejects_btmh_only() {
        let uri = "magnet:?xt=urn:btmh:1220caf1e1c30e81cb361b9ee167c4a76c1"
            .to_string()
            + &"0".repeat(32);
        match parse(&uri).unwrap_err() {
            MagnetError::UnsupportedUrn(urn) => assert!(urn.starts_with("urn:btmh:")),
            other => panic!("expected unsupported urn, got {other:?}"),
        }
    }

    #[test]
    fn rejects_missing_and_garbage_input() {
        assert_eq!(parse("magnet:?dn=only").unwrap_err(), MagnetError::MissingXt);
        assert_eq!(parse("not a magnet").unwrap_err(), MagnetError::MissingXt);
        assert_eq!(
            parse("magnet:?").unwrap_err(),
            MagnetError::MissingXt
        );
    }

    #[test]
    fn rejects_bad_hash_lengths_and_characters() {
        assert_eq!(
            parse("magnet:?xt=urn:btih:abcd").unwrap_err(),
            MagnetError::BadHashLength(4)
        );
        assert_eq!(
            parse(&format!("magnet:?xt=urn:btih:{}", "g".repeat(40))).unwrap_err(),
            MagnetError::BadHashCharacters
        );
        assert_eq!(
            parse(&format!("magnet:?xt=urn:btih:{}", "a".repeat(39))).unwrap_err(),
            MagnetError::BadHashLength(39)
        );
    }

    #[test]
    fn rejects_duplicate_xt() {
        let uri = format!(
            "magnet:?xt=urn:btih:{HEX_HASH}&xt=urn:btih:{HEX_HASH}"
        );
        assert_eq!(parse(&uri).unwrap_err(), MagnetError::DuplicateXt);
    }

    #[test]
    fn rejects_too_many_trackers() {
        let mut uri = format!("magnet:?xt=urn:btih:{HEX_HASH}");
        for index in 0..=MAX_MAGNET_TRACKERS {
            uri.push_str(&format!("&tr=udp%3A%2F%2Ft{index}%3A80"));
        }
        assert_eq!(
            parse(&uri).unwrap_err(),
            MagnetError::TooManyTrackers(MAX_MAGNET_TRACKERS + 1, MAX_MAGNET_TRACKERS)
        );
    }

    #[test]
    fn rejects_absurdly_long_uris() {
        let filler = "x".repeat(MAX_MAGNET_LENGTH);
        let uri = format!("magnet:?xt=urn:btih:{HEX_HASH}&dn={filler}");
        assert_eq!(
            parse(&uri).unwrap_err(),
            MagnetError::TooLong(uri.len(), MAX_MAGNET_LENGTH)
        );
    }

    #[test]
    fn rejects_invalid_percent_escapes_in_name() {
        let uri = format!("magnet:?xt=urn:btih:{HEX_HASH}&dn=%zz");
        assert_eq!(parse(&uri).unwrap_err(), MagnetError::BadHashCharacters);
    }
}
