use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{connection::ConnectionError, transaction::TransactionResponse};

pub struct Statistics {
    inner: Arc<Mutex<StatisticsInner>>,
}

struct StatisticsInner {
    total: usize,
    success: Vec<TransactionResponse>,
    failure: Vec<ConnectionError>,
    response_time: Vec<u128>,
}

impl Clone for Statistics {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Statistics {
    pub fn new(total_transactions: usize) -> Self {
        let inner = StatisticsInner {
            total: 0,
            success: Vec::with_capacity(total_transactions),
            failure: Vec::with_capacity(total_transactions),
            response_time: Vec::with_capacity(total_transactions),
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn sent(&self) {
        let mut statistics = self.inner.lock().await;
        statistics.total += 1;
    }

    pub async fn succeed(&self, response: TransactionResponse, response_time: u128) {
        let mut statistics = self.inner.lock().await;
        statistics.success.push(response);
        statistics.response_time.push(response_time);
    }

    pub async fn failed(&self, error: ConnectionError, response_time: u128) {
        let mut statistics = self.inner.lock().await;
        statistics.failure.push(error);
        statistics.response_time.push(response_time);
    }

    pub fn print_stats(self) {
        let mut inner = self.inner.blocking_lock();
        inner.response_time.sort();
        let k = inner.response_time.len();
        let p50 = get_nth_response_time(&inner.response_time, k * 50 / 100);
        let p90 = get_nth_response_time(&inner.response_time, k * 90 / 100);
        let p95 = get_nth_response_time(&inner.response_time, k * 95 / 100);
        let p99 = get_nth_response_time(&inner.response_time, k * 99 / 100);

        let total = inner.total;
        let success = inner.success.len();
        let failure = inner.failure.len();

        let tps: f64 = {
            if k == 0 {
                0.0
            } else {
                let total_response_time: u128 = inner.response_time.iter().sum();
                let average_response_time: f64 = total_response_time as f64 / k as f64 / 1000.0;
                let sent = (success + failure) as f64;

                sent / average_response_time
            }
        };

        tracing::info!(
            "\nTotal: {}\nSuccess: {}\nFailure: {}\nTPS: {}\nLatency(ms):\n\tP50: {:?}\n\tP90: {:?}\n\tP95: {:?}\n\tP99: {:?}",
            total,
            success,
            failure,
            tps,
            p50,
            p90,
            p95,
            p99,
        );
    }
}

/// # Panics
///
/// The function panics if it fails to get a response time for a given index.
fn get_nth_response_time(response_time: &Vec<u128>, index: usize) -> Option<u128> {
    if response_time.is_empty() {
        None
    } else {
        let p = response_time
            .get(index)
            .ok_or_else(|| panic!("Failed to get {}th index.", index))
            .unwrap();

        Some(*p)
    }
}
