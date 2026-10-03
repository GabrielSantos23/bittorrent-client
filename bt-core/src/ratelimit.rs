use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::time::Instant as TokioInstant;

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

pub struct RateWindow {
    window: Duration,
    samples: VecDeque<(TokioInstant, u64)>,
}

impl RateWindow {
    pub fn new(window: Duration) -> RateWindow {
        RateWindow {
            window,
            samples: VecDeque::new(),
        }
    }

    pub fn push(&mut self, now: TokioInstant, cumulative_bytes: u64) {
        self.samples.push_back((now, cumulative_bytes));
        while let Some((at, _)) = self.samples.front() {
            if now.duration_since(*at) <= self.window {
                break;
            }
            self.samples.pop_front();
        }
    }

    pub fn rate(&self) -> f64 {
        match (self.samples.front(), self.samples.back()) {
            (Some((start, start_bytes)), Some((end, end_bytes))) if end > start => {
                (*end_bytes - *start_bytes) as f64 / (*end - *start).as_secs_f64()
            }
            _ => 0.0,
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

    #[tokio::test(start_paused = true)]
    async fn rate_window_is_zero_without_two_spaced_samples() {
        let mut window = RateWindow::new(Duration::from_secs(6));
        assert_eq!(window.rate(), 0.0);
        window.push(TokioInstant::now(), 100);
        assert_eq!(window.rate(), 0.0);
        tokio::time::advance(Duration::from_millis(500)).await;
        window.push(TokioInstant::now(), 100);
        assert_eq!(window.rate(), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn rate_window_computes_bytes_per_second() {
        let mut window = RateWindow::new(Duration::from_secs(6));
        tokio::time::advance(Duration::from_secs(1)).await;
        window.push(TokioInstant::now(), 16_384);
        tokio::time::advance(Duration::from_secs(2)).await;
        window.push(TokioInstant::now(), 49_152);
        assert_eq!(window.rate(), 16_384.0);
    }

    #[tokio::test(start_paused = true)]
    async fn rate_window_expires_old_samples() {
        let mut window = RateWindow::new(Duration::from_secs(6));
        tokio::time::advance(Duration::from_secs(1)).await;
        window.push(TokioInstant::now(), 0);
        tokio::time::advance(Duration::from_secs(10)).await;
        window.push(TokioInstant::now(), 32_768);
        assert_eq!(window.rate(), 0.0);
        tokio::time::advance(Duration::from_secs(1)).await;
        window.push(TokioInstant::now(), 65_536);
        assert_eq!(window.rate(), 32_768.0);
    }

    #[tokio::test(start_paused = true)]
    async fn rate_window_counts_burst_within_span() {
        let mut window = RateWindow::new(Duration::from_secs(6));
        tokio::time::advance(Duration::from_secs(2)).await;
        window.push(TokioInstant::now(), 1_000);
        tokio::time::advance(Duration::from_secs(1)).await;
        window.push(TokioInstant::now(), 1_000);
        window.push(TokioInstant::now(), 2_000);
        window.push(TokioInstant::now(), 3_000);
        assert_eq!(window.rate(), 2_000.0);
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
