use std::collections::HashMap;
use std::net::Ipv4Addr;

pub const GLOBAL_BURST: f64 = 64.0;
pub const GLOBAL_REFILL_PER_SEC: f64 = 32.0;
pub const PER_IP_BURST: f64 = 8.0;
pub const PER_IP_REFILL_PER_SEC: f64 = 2.0;
const MAX_TRACKED_IPS: usize = 4096;
const IP_IDLE_MS: u64 = 60_000;
const MS_PER_SEC: f64 = 1000.0;

#[derive(Clone, Copy)]
struct Bucket {
    capacity: f64,
    refill_per_sec: f64,
    tokens: f64,
    last_ms: u64,
}

impl Bucket {
    fn new(capacity: f64, refill_per_sec: f64, now_ms: u64) -> Bucket {
        Bucket {
            capacity,
            refill_per_sec,
            tokens: capacity,
            last_ms: now_ms,
        }
    }

    fn refill(&mut self, now_ms: u64) {
        let elapsed_ms = now_ms.saturating_sub(self.last_ms) as f64;
        self.last_ms = now_ms;
        if elapsed_ms > 0.0 {
            self.tokens =
                (self.tokens + elapsed_ms / MS_PER_SEC * self.refill_per_sec).min(self.capacity);
        }
    }

    fn try_take(&mut self, now_ms: u64) -> bool {
        self.refill(now_ms);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

pub struct ResponseGate {
    global: Bucket,
    per_ip: HashMap<Ipv4Addr, Bucket>,
}

impl ResponseGate {
    pub fn new(now_ms: u64) -> ResponseGate {
        ResponseGate {
            global: Bucket::new(GLOBAL_BURST, GLOBAL_REFILL_PER_SEC, now_ms),
            per_ip: HashMap::new(),
        }
    }

    pub fn try_acquire(&mut self, ip: Ipv4Addr, now_ms: u64) -> bool {
        if self.per_ip.len() >= MAX_TRACKED_IPS {
            self.drop_idle_ips(now_ms);
        }
        if !self.global.try_take(now_ms) {
            return false;
        }
        let bucket = self
            .per_ip
            .entry(ip)
            .or_insert_with(|| Bucket::new(PER_IP_BURST, PER_IP_REFILL_PER_SEC, now_ms));
        if !bucket.try_take(now_ms) {
            return false;
        }
        true
    }

    #[cfg(test)]
    pub fn tracked_ips(&self) -> usize {
        self.per_ip.len()
    }

    fn drop_idle_ips(&mut self, now_ms: u64) {
        self.per_ip
            .retain(|_, bucket| now_ms.saturating_sub(bucket.last_ms) < IP_IDLE_MS);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_per_ip_burst_is_bounded_and_refills_over_time() {
        let mut gate = ResponseGate::new(0);
        let ip = Ipv4Addr::new(10, 0, 0, 1);
        for _ in 0..PER_IP_BURST as u64 {
            assert!(gate.try_acquire(ip, 0));
        }
        assert!(
            !gate.try_acquire(ip, 0),
            "the burst must not exceed the capacity"
        );
        assert!(!gate.try_acquire(ip, 250));
        assert!(gate.try_acquire(ip, 500), "one token refills after 500 ms");
        assert!(!gate.try_acquire(ip, 500));
    }

    #[test]
    fn the_global_gate_blocks_every_ip_once_exhausted() {
        let mut gate = ResponseGate::new(0);
        for ip_index in 0..(GLOBAL_BURST / PER_IP_BURST) as u8 {
            let ip = Ipv4Addr::new(10, 0, 0, ip_index);
            for _ in 0..PER_IP_BURST as u64 {
                assert!(gate.try_acquire(ip, 0));
            }
        }
        let fresh = Ipv4Addr::new(10, 1, 0, 1);
        assert!(
            !gate.try_acquire(fresh, 0),
            "the global gate must bound all sources"
        );
        assert!(
            gate.try_acquire(
                fresh,
                (GLOBAL_BURST / GLOBAL_REFILL_PER_SEC * 1000.0) as u64
            ),
            "the global gate must refill over time"
        );
    }

    #[test]
    fn tracked_ips_stay_bounded_and_idle_entries_are_dropped() {
        let mut gate = ResponseGate::new(0);
        for index in 0..(MAX_TRACKED_IPS + 16) as u32 {
            let ip = Ipv4Addr::from(index);
            assert!(
                gate.try_acquire(ip, u64::from(index * 32)),
                "global refill keeps up one token per 32 ms"
            );
        }
        assert!(
            gate.tracked_ips() < MAX_TRACKED_IPS,
            "idle ip buckets must be dropped instead of growing without bound"
        );
        let after = (MAX_TRACKED_IPS + 16) as u64 * 32 + 1_000;
        assert!(gate.try_acquire(Ipv4Addr::from(0xFFFF_0000u32), after));
    }

    #[test]
    fn a_spent_ip_bucket_does_not_block_a_fresh_ip() {
        let mut gate = ResponseGate::new(0);
        let spent = Ipv4Addr::new(10, 0, 0, 1);
        for _ in 0..PER_IP_BURST as u64 {
            assert!(gate.try_acquire(spent, 0));
        }
        assert!(!gate.try_acquire(spent, 0));
        let fresh = Ipv4Addr::new(10, 0, 0, 2);
        assert!(
            gate.try_acquire(fresh, 0),
            "a fresh ip must still be served"
        );
    }
}
