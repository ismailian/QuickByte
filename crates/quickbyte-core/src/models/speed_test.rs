//! What a speed test (Tasks > Speed Test) reports while it runs and once it
//! is done. Nothing here is persisted: a result is shown, not kept.

/// The three measurements, in the order a test makes them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SpeedTestPhase {
    Latency,
    Download,
    Upload,
}

impl SpeedTestPhase {
    pub const ALL: [SpeedTestPhase; 3] = [SpeedTestPhase::Latency, SpeedTestPhase::Download, SpeedTestPhase::Upload];

    /// How the phase reads in a sentence: "The download test failed".
    pub fn name(self) -> &'static str {
        match self {
            SpeedTestPhase::Latency => "latency",
            SpeedTestPhase::Download => "download",
            SpeedTestPhase::Upload => "upload",
        }
    }
}

/// Ping and jitter, in milliseconds.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LatencyStats {
    /// The median round trip: one slow answer does not move it.
    pub median_ms: f64,
    /// The mean difference between consecutive round trips.
    pub jitter_ms: f64,
    pub min_ms: f64,
    pub samples: usize,
}

/// Where a running test has got to. Sent several times a second; every figure
/// measured so far is carried in each one, so the last one received is all a
/// window needs to draw.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SpeedTestProgress {
    /// The phase running now; `None` once the test is over.
    pub phase: Option<SpeedTestPhase>,
    /// How far through the current phase, 0–1.
    pub phase_fraction: f32,
    /// What the running download or upload phase is reaching, in bytes per
    /// second, over the last second. 0 while latency is measured.
    pub current_rate: f64,
    pub latency: Option<LatencyStats>,
    /// Bytes per second, once the download phase is over.
    pub download: Option<f64>,
    /// Bytes per second, once the upload phase is over.
    pub upload: Option<f64>,
    /// The server that answered, as a place: "Frankfurt (FRA)".
    pub server: Option<String>,
    /// Bytes moved so far, both ways — a speed test on a metered line costs data.
    pub data_used: u64,
}

/// A finished test.
#[derive(Clone, Debug, PartialEq)]
pub struct SpeedTestResult {
    pub latency: LatencyStats,
    /// Bytes per second.
    pub download: f64,
    /// Bytes per second.
    pub upload: f64,
    pub server: Option<String>,
    pub data_used: u64,
}
