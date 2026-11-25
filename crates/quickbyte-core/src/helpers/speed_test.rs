//! The arithmetic of a speed test, kept apart from [`crate::net::speed_test`]
//! so it can be tested without a network.
//!
//! **Latency** is the time from asking for nothing to the answer's headers,
//! less the time the server says it spent (`Server-Timing: cfRequestDuration`),
//! over a connection that is already open. The caller leaves out the first
//! probe, which paid for DNS, TCP and TLS. The figure shown is the median,
//! which one slow answer does not move. Jitter is the mean difference between
//! consecutive samples, as Cloudflare's own test and RFC 3550 compute it.
//!
//! **Throughput** is bytes over time, read every hundred milliseconds or so from
//! a counter the connections add to. The start of each phase is left out: TCP
//! starts slowly and opens its window as it goes, and the first bytes of an
//! upload only fill the socket's send buffer. What is left is the bytes between
//! the end of that warm-up and the end of the phase, over the time between
//! them: the steady rate the line carries. The send buffer is full at both ends
//! of that stretch, so for an upload it cancels out.

use std::collections::VecDeque;
use std::time::Duration;

use crate::helpers::speed_calculator::SpeedCalculator;
use crate::models::LatencyStats;
use crate::serde_compat::TimeSpan;

/// How far back the live rate looks: long enough to be steady, short enough
/// that the gauge follows the line.
pub const LIVE_WINDOW: Duration = Duration::from_secs(1);

/// Median, jitter and minimum of `samples`, in milliseconds. `None` for none.
pub fn latency_stats(samples: &[Duration]) -> Option<LatencyStats> {
    if samples.is_empty() {
        return None;
    }
    let ms: Vec<f64> = samples.iter().map(|d| millis(*d)).collect();
    let mut sorted = ms.clone();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    let median_ms = if sorted.len() % 2 == 0 { (sorted[middle - 1] + sorted[middle]) / 2.0 } else { sorted[middle] };
    let jitter_ms = if ms.len() < 2 {
        0.0
    } else {
        ms.windows(2).map(|pair| (pair[1] - pair[0]).abs()).sum::<f64>() / (ms.len() - 1) as f64
    };
    Some(LatencyStats { median_ms, jitter_ms, min_ms: sorted[0], samples: ms.len() })
}

/// The server's own time out of a `Server-Timing` header, in milliseconds:
/// the `dur` of its `cfRequestDuration` metric. Any other metric is not the
/// time this request spent being answered, so it is not used.
pub fn server_processing_ms(server_timing: &str) -> Option<f64> {
    server_timing.split(',').find_map(|metric| {
        let mut parts = metric.split(';').map(str::trim);
        if !parts.next()?.eq_ignore_ascii_case("cfRequestDuration") {
            return None;
        }
        parts.find_map(|param| {
            let (name, value) = param.split_once('=')?;
            if !name.trim().eq_ignore_ascii_case("dur") {
                return None;
            }
            value.trim().trim_matches('"').parse::<f64>().ok().filter(|ms| ms.is_finite() && *ms >= 0.0)
        })
    })
}

/// A probe's round trip with the server's own time taken off. Kept as
/// measured when that time is missing, or would leave nothing: subtracting a
/// number that cannot be right is worse than not subtracting.
pub fn network_time(measured: Duration, server_ms: Option<f64>) -> Duration {
    match server_ms {
        Some(ms) if ms > 0.0 && ms < millis(measured) => measured - Duration::from_nanos((ms * 1_000_000.0).round() as u64),
        _ => measured,
    }
}

fn millis(d: Duration) -> f64 {
    d.as_nanos() as f64 / 1_000_000.0
}

/// The answering data centre as a place: "Frankfurt (FRA)", or "FRA" alone
/// when the server does not name its city. The code comes from `cf-meta-colo`,
/// or from the end of `cf-ray` (`8f1c2b3a4d5e6f70-FRA`), which every Cloudflare
/// answer carries.
pub fn server_location(colo: Option<&str>, city: Option<&str>, cf_ray: Option<&str>) -> Option<String> {
    let is_code = |code: &&str| (3..=4).contains(&code.len()) && code.chars().all(|c| c.is_ascii_alphabetic());
    let code = colo
        .map(str::trim)
        .filter(is_code)
        .or_else(|| cf_ray.and_then(|ray| ray.trim().rsplit_once('-')).map(|(_, code)| code).filter(is_code))?
        .to_ascii_uppercase();
    let city = city.map(str::trim).filter(|c| !c.is_empty() && c.len() <= 64 && !c.chars().any(char::is_control));
    Some(match city {
        Some(city) => format!("{city} ({code})"),
        None => code,
    })
}

/// A phase's byte counter, read over time.
#[derive(Clone, Debug)]
pub struct ThroughputMeter {
    warmup: Duration,
    /// Readings inside the live window, oldest first, plus the one just before it.
    recent: VecDeque<(Duration, u64)>,
    /// The first reading at or after the end of the warm-up.
    baseline: Option<(Duration, u64)>,
    last: (Duration, u64),
}

impl ThroughputMeter {
    pub fn new(warmup: Duration) -> Self {
        let start = (Duration::ZERO, 0);
        Self { warmup, recent: VecDeque::from([start]), baseline: None, last: start }
    }

    /// The counter read `elapsed` after the phase began. A reading earlier than
    /// the last one, or a counter that went down, is ignored.
    pub fn record(&mut self, elapsed: Duration, total_bytes: u64) {
        if elapsed <= self.last.0 || total_bytes < self.last.1 {
            return;
        }
        self.last = (elapsed, total_bytes);
        if self.baseline.is_none() && elapsed >= self.warmup {
            self.baseline = Some(self.last);
        }
        self.recent.push_back(self.last);
        while self.recent.len() > 2 && self.recent[1].0 + LIVE_WINDOW <= elapsed {
            self.recent.pop_front();
        }
    }

    pub fn total_bytes(&self) -> u64 {
        self.last.1
    }

    /// Bytes per second over the last [`LIVE_WINDOW`]: what the gauge shows
    /// while the phase runs.
    pub fn current(&self) -> f64 {
        let first = self.recent.front().copied().unwrap_or(self.last);
        rate(first, self.last).unwrap_or(0.0)
    }

    /// The phase's result in bytes per second: the stretch after the warm-up.
    /// A phase that ended inside its warm-up has no such stretch and is
    /// measured from its start instead; one with no time at all has no result.
    pub fn result(&self) -> Option<f64> {
        self.baseline.and_then(|baseline| rate(baseline, self.last)).or_else(|| rate((Duration::ZERO, 0), self.last))
    }
}

fn rate(from: (Duration, u64), to: (Duration, u64)) -> Option<f64> {
    let seconds = to.0.checked_sub(from.0)?.as_secs_f64();
    (seconds > 0.0).then(|| to.1.saturating_sub(from.1) as f64 / seconds)
}

/// Megabits per second, the unit speed tests and internet plans are quoted in.
pub fn megabits(bytes_per_second: f64) -> f64 {
    bytes_per_second.max(0.0) * 8.0 / 1_000_000.0
}

/// A rate as a number of Mbps with the precision its size deserves: "8.43",
/// "87.4", "940", "2450".
pub fn format_megabits(bytes_per_second: f64) -> String {
    let mbps = megabits(bytes_per_second);
    if mbps < 10.0 {
        format!("{mbps:.2}")
    } else if mbps < 100.0 {
        format!("{mbps:.1}")
    } else {
        format!("{mbps:.0}")
    }
}

/// How long `bytes` would take at `bytes_per_second` — the speed put in a
/// download manager's terms.
pub fn time_to_download(bytes: i64, bytes_per_second: f64) -> Option<TimeSpan> {
    SpeedCalculator::estimate_time_remaining(0, bytes, bytes_per_second)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(values: &[u64]) -> Vec<Duration> {
        values.iter().map(|v| Duration::from_millis(*v)).collect()
    }

    #[test]
    fn latency_is_the_median_and_jitter_the_mean_step() {
        let stats = latency_stats(&ms(&[20, 24, 18, 90, 22])).unwrap();
        // One slow answer does not move the median...
        assert_eq!(stats.median_ms, 22.0);
        assert_eq!(stats.min_ms, 18.0);
        assert_eq!(stats.samples, 5);
        // ...but it is jitter: |4| + |6| + |72| + |68| over four steps.
        assert_eq!(stats.jitter_ms, 37.5);

        let even = latency_stats(&ms(&[10, 30, 20, 40])).unwrap();
        assert_eq!(even.median_ms, 25.0);
    }

    #[test]
    fn one_sample_has_no_jitter_and_none_has_no_stats() {
        let one = latency_stats(&ms(&[15])).unwrap();
        assert_eq!((one.median_ms, one.jitter_ms, one.min_ms), (15.0, 0.0, 15.0));
        assert_eq!(latency_stats(&[]), None);
    }

    #[test]
    fn the_servers_own_time_is_read_from_its_request_duration_only() {
        assert_eq!(server_processing_ms("cfRequestDuration;dur=12.5"), Some(12.5));
        assert_eq!(
            server_processing_ms(r#"cfL4;desc="?proto=TCP&rtt=9000", cfRequestDuration;dur=3.25"#),
            Some(3.25)
        );
        assert_eq!(server_processing_ms("cfRequestDuration; dur=\"7\""), Some(7.0));
        // Another metric's duration is not this request's.
        assert_eq!(server_processing_ms("cache;dur=40"), None);
        assert_eq!(server_processing_ms("cfRequestDuration;dur=-1"), None);
        assert_eq!(server_processing_ms("cfRequestDuration;dur=soon"), None);
        assert_eq!(server_processing_ms(""), None);
    }

    #[test]
    fn the_servers_time_comes_off_the_round_trip_only_when_it_can() {
        let measured = Duration::from_millis(30);
        assert_eq!(network_time(measured, Some(10.0)), Duration::from_millis(20));
        assert_eq!(network_time(measured, None), measured);
        // More than the whole round trip cannot be right.
        assert_eq!(network_time(measured, Some(45.0)), measured);
        assert_eq!(network_time(measured, Some(30.0)), measured);
    }

    #[test]
    fn the_server_is_named_by_its_city_and_code() {
        assert_eq!(server_location(Some("FRA"), Some("Frankfurt"), None).as_deref(), Some("Frankfurt (FRA)"));
        assert_eq!(server_location(Some("fra"), None, None).as_deref(), Some("FRA"));
        // Every Cloudflare answer ends its ray id with the data centre.
        assert_eq!(server_location(None, None, Some("8f1c2b3a4d5e6f70-AMS")).as_deref(), Some("AMS"));
        assert_eq!(server_location(Some(""), Some("Paris"), Some("8f1c2b3a4d5e6f70-CDG")).as_deref(), Some("Paris (CDG)"));
        assert_eq!(server_location(None, Some("Nowhere"), Some("not-a-ray-12")), None);
        assert_eq!(server_location(None, None, None), None);
    }

    #[test]
    fn the_result_leaves_the_warm_up_out() {
        let mut meter = ThroughputMeter::new(Duration::from_secs(2));
        // A slow first second (slow start), then a steady 10 MB/s.
        meter.record(Duration::from_secs(1), 1_000_000);
        meter.record(Duration::from_secs(2), 6_000_000);
        for second in 3..=10 {
            meter.record(Duration::from_secs(second), 6_000_000 + (second - 2) * 10_000_000);
        }
        assert_eq!(meter.result(), Some(10_000_000.0));
        assert_eq!(meter.total_bytes(), 86_000_000);
    }

    #[test]
    fn the_live_rate_follows_the_last_second() {
        let mut meter = ThroughputMeter::new(Duration::from_secs(2));
        let step = Duration::from_millis(100);
        let mut bytes = 0;
        for tick in 1..=30u32 {
            // 1 MB/s for two seconds, then 5 MB/s.
            bytes += if tick <= 20 { 100_000 } else { 500_000 };
            meter.record(step * tick, bytes);
        }
        assert!((meter.current() - 5_000_000.0).abs() < 1.0, "{}", meter.current());
        assert!((meter.result().unwrap() - 5_000_000.0).abs() < 1.0);
    }

    #[test]
    fn a_phase_that_ends_inside_its_warm_up_is_measured_from_the_start() {
        let mut meter = ThroughputMeter::new(Duration::from_secs(2));
        assert_eq!(meter.result(), None);
        assert_eq!(meter.current(), 0.0);
        meter.record(Duration::from_millis(500), 1_000_000);
        assert_eq!(meter.result(), Some(2_000_000.0));
        assert_eq!(meter.current(), 2_000_000.0);
    }

    #[test]
    fn readings_out_of_order_are_ignored() {
        let mut meter = ThroughputMeter::new(Duration::ZERO);
        meter.record(Duration::from_secs(1), 1_000);
        meter.record(Duration::from_secs(1), 9_000);
        meter.record(Duration::from_millis(500), 9_000);
        meter.record(Duration::from_secs(2), 500);
        assert_eq!(meter.total_bytes(), 1_000);
        meter.record(Duration::from_secs(2), 3_000);
        assert_eq!(meter.result(), Some(2_000.0));
    }

    #[test]
    fn rates_read_in_megabits() {
        assert_eq!(megabits(12_500_000.0), 100.0);
        assert_eq!(megabits(-5.0), 0.0);
        assert_eq!(format_megabits(1_053_750.0), "8.43");
        assert_eq!(format_megabits(10_925_000.0), "87.4");
        assert_eq!(format_megabits(117_500_000.0), "940");
        assert_eq!(format_megabits(306_250_000.0), "2450");
        assert_eq!(format_megabits(0.0), "0.00");
    }

    #[test]
    fn a_rate_becomes_the_time_a_file_would_take() {
        let gigabyte = 1024 * 1024 * 1024;
        let at = time_to_download(gigabyte, 8.0 * 1024.0 * 1024.0).unwrap();
        assert_eq!((at.minutes(), at.seconds()), (2, 8));
        assert_eq!(time_to_download(gigabyte, 0.0), None);
    }
}
