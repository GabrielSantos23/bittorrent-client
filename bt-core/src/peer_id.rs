use std::sync::OnceLock;

use rand::Rng;

const PREFIX: &[u8; 8] = b"-BT0001-";
const ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

pub fn generate() -> [u8; 20] {
    let mut id = [0u8; 20];
    id[..PREFIX.len()].copy_from_slice(PREFIX);
    let mut rng = rand::rng();
    for byte in &mut id[PREFIX.len()..] {
        *byte = ALPHABET[rng.random_range(0..ALPHABET.len())];
    }
    id
}

pub fn session() -> &'static [u8; 20] {
    static PEER_ID: OnceLock<[u8; 20]> = OnceLock::new();
    PEER_ID.get_or_init(generate)
}

const CLIENT_NAMES: &[(&str, &str)] = &[
    ("AZ", "Azureus"),
    ("BT", "bt-core"),
    ("DE", "Deluge"),
    ("lt", "libtorrent"),
    ("LT", "libtorrent"),
    ("qB", "qBittorrent"),
    ("TR", "Transmission"),
    ("UM", "uTorrent Mac"),
    ("UT", "uTorrent"),
];

pub fn client_name(peer_id: &[u8; 20]) -> String {
    if peer_id[0] == b'-' && peer_id[7] == b'-' {
        let code = String::from_utf8_lossy(&peer_id[1..7]).into_owned();
        let prefix = String::from_utf8_lossy(&peer_id[1..3]).into_owned();
        match CLIENT_NAMES.iter().find(|(key, _)| *key == prefix) {
            Some((_, name)) => format!("{name} ({code})"),
            None => code,
        }
    } else {
        let text: Vec<u8> = peer_id
            .iter()
            .copied()
            .take_while(|&byte| byte != 0)
            .collect();
        let text = String::from_utf8_lossy(&text).into_owned();
        if !text.is_empty() && text.chars().all(|c| c.is_ascii_graphic()) {
            text
        } else {
            crate::hex::encode(peer_id)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_have_azureus_prefix() {
        let id = generate();
        assert_eq!(&id[..8], b"-BT0001-");
        assert!(id[8..].iter().all(|byte| ALPHABET.contains(byte)));
    }

    #[test]
    fn generated_ids_differ() {
        assert_ne!(generate(), generate());
    }

    #[test]
    fn session_ids_are_stable() {
        assert_eq!(session(), session());
    }

    #[test]
    fn maps_known_client_codes() {
        assert_eq!(client_name(b"-qB4530-abcdefghijkl"), "qBittorrent (qB4530)");
        assert_eq!(client_name(b"-BT0001-abcdefghijkl"), "bt-core (BT0001)");
        assert_eq!(client_name(b"-lt0D80-abcdefghijkl"), "libtorrent (lt0D80)");
        assert_eq!(client_name(b"-XY9999-abcdefghijkl"), "XY9999");
    }

    #[test]
    fn falls_back_for_other_conventions() {
        let mut id = [0u8; 20];
        id[..6].copy_from_slice(b"M7-6-0");
        assert_eq!(client_name(&id), "M7-6-0");
        let binary = [0xff; 20];
        assert_eq!(client_name(&binary), crate::hex::encode(&binary));
    }
}
