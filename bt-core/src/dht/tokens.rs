use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
use std::net::Ipv4Addr;

use crate::dht::node_id::RandomBytes;

pub const TOKEN_ROTATION_MS: u64 = 5 * 60 * 1000;
pub const TOKEN_LENGTH: usize = 8;
const SECRET_BYTES: usize = 16;

pub struct TokenVault {
    current: [u8; SECRET_BYTES],
    previous: Option<[u8; SECRET_BYTES]>,
    rotated_ms: u64,
}

fn derive(secret: &[u8], ip: Ipv4Addr) -> [u8; TOKEN_LENGTH] {
    let mut hasher = DefaultHasher::new();
    hasher.write(secret);
    hasher.write(&ip.octets());
    hasher.finish().to_be_bytes()
}

impl TokenVault {
    pub fn new(now_ms: u64, random: &dyn RandomBytes) -> TokenVault {
        let mut current = [0u8; SECRET_BYTES];
        random.fill(&mut current);
        TokenVault {
            current,
            previous: None,
            rotated_ms: now_ms,
        }
    }

    pub fn rotate_if_due(&mut self, now_ms: u64, random: &dyn RandomBytes) -> bool {
        if now_ms.saturating_sub(self.rotated_ms) < TOKEN_ROTATION_MS {
            return false;
        }
        let mut fresh = [0u8; SECRET_BYTES];
        random.fill(&mut fresh);
        self.previous = Some(self.current);
        self.current = fresh;
        self.rotated_ms = now_ms;
        true
    }

    pub fn token_for(&self, ip: Ipv4Addr) -> Vec<u8> {
        derive(&self.current, ip).to_vec()
    }

    pub fn validate(&self, ip: Ipv4Addr, token: &[u8]) -> bool {
        if token.len() != TOKEN_LENGTH {
            return false;
        }
        if derive(&self.current, ip) == token {
            return true;
        }
        match &self.previous {
            Some(previous) => derive(previous, ip) == token,
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

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

    fn seeded_random(start: u8) -> FixedRandom {
        let bytes: Vec<u8> = (start..start + SECRET_BYTES as u8).collect();
        FixedRandom(Mutex::new(bytes))
    }

    fn independent_token(secret: &[u8], ip: Ipv4Addr) -> Vec<u8> {
        let mut hasher = DefaultHasher::new();
        hasher.write(secret);
        hasher.write(&ip.octets());
        hasher.finish().to_be_bytes().to_vec()
    }

    #[test]
    fn tokens_are_derived_from_the_secret_and_the_requester_ip() {
        let vault = TokenVault::new(0, &seeded_random(1));
        let expected = independent_token(
            &(1u8..=16).collect::<Vec<u8>>(),
            Ipv4Addr::new(93, 184, 216, 34),
        );
        assert_eq!(vault.token_for(Ipv4Addr::new(93, 184, 216, 34)), expected);
        assert_ne!(
            vault.token_for(Ipv4Addr::new(93, 184, 216, 35)),
            expected,
            "a different requester ip must derive a different token"
        );
    }

    #[test]
    fn validation_accepts_only_exact_tokens() {
        let vault = TokenVault::new(0, &seeded_random(1));
        let ip = Ipv4Addr::new(10, 1, 2, 3);
        let token = vault.token_for(ip);
        assert!(vault.validate(ip, &token));
        assert!(vault.validate(ip, &independent_token(&(1u8..=16).collect::<Vec<u8>>(), ip)));
        assert!(!vault.validate(ip, &token[..token.len() - 1]));
        assert!(!vault.validate(ip, b"aoeusnth"));
        assert!(!vault.validate(Ipv4Addr::new(10, 1, 2, 4), &token));
    }

    #[test]
    fn rotation_keeps_the_previous_secret_valid_for_one_interval() {
        let ip = Ipv4Addr::new(10, 0, 0, 9);
        let mut vault = TokenVault::new(0, &seeded_random(1));
        let old_token = vault.token_for(ip);
        assert!(!vault.rotate_if_due(TOKEN_ROTATION_MS - 1, &seeded_random(1)));
        assert!(vault.validate(ip, &old_token));
        assert!(vault.rotate_if_due(TOKEN_ROTATION_MS, &seeded_random(17)));
        let new_token = vault.token_for(ip);
        assert_ne!(old_token, new_token);
        assert!(
            vault.validate(ip, &old_token),
            "the previous secret must still validate"
        );
        assert!(vault.validate(ip, &new_token));
        assert!(
            vault.rotate_if_due(2 * TOKEN_ROTATION_MS, &seeded_random(33)),
            "a second rotation must be due"
        );
        assert!(
            !vault.validate(ip, &old_token),
            "a token older than one rotation must be rejected"
        );
        assert!(vault.validate(ip, &new_token));
    }
}
