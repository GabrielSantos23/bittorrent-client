use std::sync::Mutex;
use std::time::{Duration, Instant};

const MIN_CAPACITY: u64 = 16 * 1024;
const MAX_WAIT: Duration = Duration::from_secs(60);

struct BucketState {
    limit: u64,
    tokens: f64,
    last: Instant,
}

pub struct UploadBucket {
    state: Mutex<BucketState>,
}

fn capacity(limit: u64) -> u64 {
    limit.max(MIN_CAPACITY)
}

impl UploadBucket {
    pub fn new(limit_bytes_per_second: u64) -> UploadBucket {
        UploadBucket {
            state: Mutex::new(BucketState {
                limit: limit_bytes_per_second,
                tokens: capacity(limit_bytes_per_second) as f64,
                last: Instant::now(),
            }),
        }
    }

    pub fn limit(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .limit
    }

    pub fn set_limit(&self, limit_bytes_per_second: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.limit = limit_bytes_per_second;
        state.tokens = state.tokens.min(capacity(limit_bytes_per_second) as f64);
    }

    pub async fn acquire(&self, tokens: u64) {
        loop {
            let wait = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let now = Instant::now();
                let elapsed = now.duration_since(state.last).as_secs_f64();
                state.last = now;
                if state.limit == 0 {
                    return;
                }
                let cap = capacity(state.limit).max(tokens) as f64;
                state.tokens = (state.tokens + elapsed * state.limit as f64).min(cap);
                if state.tokens >= tokens as f64 {
                    state.tokens -= tokens as f64;
                    return;
                }
                let missing = tokens as f64 - state.tokens;
                Duration::from_secs_f64((missing / state.limit as f64).clamp(0.001, 60.0))
            };
            let wait = wait.min(MAX_WAIT);
            tokio::time::sleep(wait).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unlimited_bucket_never_waits() {
        let bucket = UploadBucket::new(0);
        let start = Instant::now();
        bucket.acquire(1024 * 1024 * 1024).await;
        assert!(start.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn limit_change_applies_immediately() {
        let bucket = UploadBucket::new(100_000);
        bucket.set_limit(0);
        let start = Instant::now();
        bucket.acquire(1024 * 1024 * 1024).await;
        assert!(start.elapsed() < Duration::from_millis(100));
        bucket.set_limit(16 * 1024);
        assert_eq!(bucket.limit(), 16 * 1024);
    }

    #[tokio::test]
    async fn limited_bucket_throttles_bursts() {
        let bucket = UploadBucket::new(100_000);
        let start = Instant::now();
        bucket.acquire(350_000).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(2_300),
            "350 KiB at 100 KiB/s with a 100 KiB burst must wait, took {elapsed:?}"
        );
        assert!(elapsed < Duration::from_secs(10), "took {elapsed:?}");
    }

    #[tokio::test]
    async fn within_burst_capacity_is_immediate() {
        let bucket = UploadBucket::new(100_000);
        let start = Instant::now();
        bucket.acquire(100_000).await;
        assert!(start.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn tokens_replenish_over_time() {
        let bucket = UploadBucket::new(100_000);
        bucket.acquire(100_000).await;
        let start = Instant::now();
        bucket.acquire(50_000).await;
        let first = start.elapsed();
        assert!(first >= Duration::from_millis(400), "took {first:?}");
        tokio::time::sleep(Duration::from_millis(550)).await;
        let start = Instant::now();
        bucket.acquire(50_000).await;
        assert!(start.elapsed() < Duration::from_millis(200));
    }
}
