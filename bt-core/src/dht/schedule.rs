pub const LOOKUP_INTERVAL_MS: u64 = 15 * 60 * 1000;
pub const RETRY_INTERVAL_MS: u64 = 30 * 1000;
pub const ANNOUNCE_INTERVAL_MS: u64 = 15 * 60 * 1000;
pub const EAGER_PEER_THRESHOLD: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LookupSchedule {
    pub enabled: bool,
    pub private: bool,
    pub fetching_metadata: bool,
    pub connected_peers: usize,
    pub lookup_in_flight: bool,
    pub last_lookup_ms: Option<u64>,
}

impl LookupSchedule {
    fn eager(&self) -> bool {
        self.fetching_metadata || self.connected_peers < EAGER_PEER_THRESHOLD
    }
}

pub fn should_lookup(schedule: &LookupSchedule, now_ms: u64) -> bool {
    if !schedule.enabled || schedule.private || schedule.lookup_in_flight {
        return false;
    }
    match schedule.last_lookup_ms {
        None => true,
        Some(last) => {
            let since = now_ms.saturating_sub(last);
            if since >= LOOKUP_INTERVAL_MS {
                return true;
            }
            schedule.eager() && since >= RETRY_INTERVAL_MS
        }
    }
}

pub fn should_announce_after_lookup(last_announce_ms: Option<u64>, now_ms: u64) -> bool {
    match last_announce_ms {
        None => true,
        Some(last) => now_ms.saturating_sub(last) >= ANNOUNCE_INTERVAL_MS,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schedule(patches: impl FnOnce(&mut LookupSchedule)) -> LookupSchedule {
        let mut base = LookupSchedule {
            enabled: true,
            private: false,
            fetching_metadata: false,
            connected_peers: EAGER_PEER_THRESHOLD,
            lookup_in_flight: false,
            last_lookup_ms: None,
        };
        patches(&mut base);
        base
    }

    #[test]
    fn table_driven_lookup_decisions() {
        let cases: Vec<(LookupSchedule, u64, bool, &str)> = vec![
            (
                schedule(|_| {}),
                0,
                true,
                "first lookup happens immediately",
            ),
            (
                schedule(|s| s.enabled = false),
                0,
                false,
                "a disabled dht never looks up",
            ),
            (
                schedule(|s| s.private = true),
                0,
                false,
                "private torrents never look up",
            ),
            (
                schedule(|s| s.lookup_in_flight = true),
                0,
                false,
                "never more than one lookup in flight",
            ),
            (
                schedule(|s| s.last_lookup_ms = Some(60_000)),
                60_000 + RETRY_INTERVAL_MS - 1,
                false,
                "a saturated torrent waits the full interval",
            ),
            (
                schedule(|s| s.last_lookup_ms = Some(60_000)),
                60_000 + LOOKUP_INTERVAL_MS,
                true,
                "the periodic interval fires regardless of eagerness",
            ),
            (
                schedule(|s| {
                    s.last_lookup_ms = Some(60_000);
                    s.connected_peers = EAGER_PEER_THRESHOLD - 1;
                }),
                60_000 + RETRY_INTERVAL_MS - 1,
                false,
                "the retry waits at least thirty seconds",
            ),
            (
                schedule(|s| {
                    s.last_lookup_ms = Some(60_000);
                    s.connected_peers = EAGER_PEER_THRESHOLD - 1;
                }),
                60_000 + RETRY_INTERVAL_MS,
                true,
                "fewer than five peers retries after thirty seconds",
            ),
            (
                schedule(|s| {
                    s.last_lookup_ms = Some(60_000);
                    s.fetching_metadata = true;
                }),
                60_000 + RETRY_INTERVAL_MS,
                true,
                "a magnet fetching metadata retries quickly",
            ),
            (
                schedule(|s| {
                    s.last_lookup_ms = Some(60_000);
                    s.fetching_metadata = true;
                    s.private = true;
                }),
                60_000 + RETRY_INTERVAL_MS,
                false,
                "a private magnet fetching metadata still never looks up",
            ),
            (
                schedule(|s| {
                    s.last_lookup_ms = Some(60_000);
                    s.connected_peers = 0;
                    s.lookup_in_flight = true;
                }),
                60_000 + RETRY_INTERVAL_MS,
                false,
                "eagerness does not bypass the in-flight guard",
            ),
            (
                schedule(|s| {
                    s.last_lookup_ms = Some(60_000);
                    s.enabled = false;
                    s.connected_peers = 0;
                }),
                60_000 + RETRY_INTERVAL_MS,
                false,
                "disabling blocks the eager retry too",
            ),
        ];
        for (index, (state, now, expected, why)) in cases.into_iter().enumerate() {
            assert_eq!(
                should_lookup(&state, now),
                expected,
                "case {index} failed: {why}"
            );
        }
    }

    #[test]
    fn table_driven_announce_decisions() {
        let cases: Vec<(Option<u64>, u64, bool, &str)> = vec![
            (None, 0, true, "the first announce follows the first lookup"),
            (
                Some(1_000),
                1_000 + ANNOUNCE_INTERVAL_MS - 1,
                false,
                "no re-announce inside the interval",
            ),
            (
                Some(1_000),
                1_000 + ANNOUNCE_INTERVAL_MS,
                true,
                "re-announce after fifteen minutes",
            ),
            (
                Some(1_000),
                1_000 + 5 * RETRY_INTERVAL_MS,
                false,
                "eager retries do not re-announce",
            ),
        ];
        for (index, (last, now, expected, why)) in cases.into_iter().enumerate() {
            assert_eq!(
                should_announce_after_lookup(last, now),
                expected,
                "case {index} failed: {why}"
            );
        }
    }
}
