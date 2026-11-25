//! Tasks > Speed Test, against Cloudflare's speed test service: the endpoints
//! speed.cloudflare.com's own page measures with. `GET /__down?bytes=N` answers
//! N bytes, and `POST /__up` takes a body and answers once it has all of it.
//! Cloudflare answers from the data centre nearest the user, which is what a
//! speed test is after: the line, not the road to one far-away server.
//!
//! **Over the shared client**, so it goes through the proxy in Options as a
//! download does, and measures what a download would get. It does not go
//! through the bandwidth limiter: that caps downloads, not the line.
//!
//! **Three phases**, each bounded in time whatever the server does:
//!
//! 1. *Latency*: requests for nothing, one after another on one open
//!    connection. A first one opens it, finds out which server answers, and is
//!    not counted.
//! 2. *Download*: several connections at once, each fetching
//!    [`DOWNLOAD_CHUNK`] bytes after another until the phase's time is up. One
//!    TCP connection rarely fills a fast line, and a download manager's
//!    downloads do not use one either.
//! 3. *Upload*: the same, posting random bytes, so nothing on the way can
//!    compress them into a better figure.
//!
//! `helpers::speed_test` turns the timings and byte counts into figures.
//!
//! **What goes over the wire** is requests for test data, and random bytes.
//! Nothing of the user's: no download address, cookie, login or header.
//! Cloudflare sees the IP address and QuickByte's User-Agent, as any server does.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use reqwest::StatusCode;
use reqwest::header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE, HeaderMap};
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::error::{CoreError, Result};
use crate::helpers::speed_test::{self, ThroughputMeter};
use crate::models::{SpeedTestPhase, SpeedTestProgress, SpeedTestResult};
use crate::net::app_proxy::AppProxy;
use crate::net::http_client::{HttpClient, map_reqwest_error, with_deadline};
use crate::traits::{ProgressFn, SpeedTestService};

pub const CLOUDFLARE: &str = "https://speed.cloudflare.com";

/// What one download request asks for: enough that a fast connection spends
/// its time receiving rather than asking. Cloudflare's own test fetches 25 MB
/// and more.
pub const DOWNLOAD_CHUNK: u64 = 25_000_000;
/// What one upload request sends. Cloudflare's own test sends up to 50 MB.
pub const UPLOAD_CHUNK: u64 = 10_000_000;
/// The piece an upload body is handed over in, and so how often its count moves.
const UPLOAD_PIECE: usize = 64 * 1024;

pub const NO_DATA: &str = "The speed test server sent no data.";
pub const RATE_LIMITED: &str = "The speed test server is limiting tests from this address. Try again in a few minutes.";
pub const NO_ANSWER: &str = "The speed test server did not answer in time.";

#[derive(Clone, Debug)]
pub struct SpeedTestOptions {
    /// Latency probes counted (one more opens the connection).
    pub latency_samples: usize,
    pub download_time: Duration,
    pub upload_time: Duration,
    /// The start of each download and upload phase that is not counted.
    pub warmup: Duration,
    /// Connections at once while downloading and uploading.
    pub connections: usize,
    pub download_chunk: u64,
    pub upload_chunk: u64,
    /// How long one latency probe may take.
    pub probe_timeout: Duration,
    /// How often the byte counters are read, and progress reported.
    pub sample_interval: Duration,
}

impl Default for SpeedTestOptions {
    /// About 25 seconds in all: Ookla's and Cloudflare's tests take as long.
    fn default() -> Self {
        Self {
            latency_samples: 20,
            download_time: Duration::from_secs(10),
            upload_time: Duration::from_secs(10),
            warmup: Duration::from_secs(2),
            connections: 6,
            download_chunk: DOWNLOAD_CHUNK,
            upload_chunk: UPLOAD_CHUNK,
            probe_timeout: Duration::from_secs(10),
            sample_interval: Duration::from_millis(100),
        }
    }
}

pub struct SpeedTestClient {
    http: Arc<HttpClient>,
    base: String,
    options: SpeedTestOptions,
}

impl Default for SpeedTestClient {
    fn default() -> Self {
        Self::new()
    }
}

impl SpeedTestClient {
    /// Cloudflare, over the shared client, with the standard timings.
    pub fn new() -> Self {
        Self::with(CLOUDFLARE, HttpClient::shared(), SpeedTestOptions::default())
    }

    /// Another endpoint, client and timings: for tests against a local server.
    pub fn with(base: &str, http: Arc<HttpClient>, options: SpeedTestOptions) -> Self {
        Self { http, base: base.trim_end_matches('/').to_string(), options }
    }

    /// One request for nothing: the time its headers took, and the headers.
    async fn probe(&self, client: &reqwest::Client, cancel: &CancellationToken) -> Result<(Duration, HeaderMap)> {
        let request = client.get(format!("{}/__down?bytes=0", self.base)).header(CACHE_CONTROL, "no-store");
        let proxy = self.http.proxy();
        let exchange = async {
            let started = Instant::now();
            let response = request.send().await.map_err(|e| map_reqwest_error(&e, proxy))?;
            let elapsed = started.elapsed();
            check_status(response.status())?;
            let headers = response.headers().clone();
            // Read to the end, so the connection goes back to the pool for the next probe.
            response.bytes().await.map_err(|e| map_reqwest_error(&e, proxy))?;
            Ok((elapsed, headers))
        };
        with_deadline(exchange, Some(self.options.probe_timeout), cancel).await
    }

    async fn latency(
        &self,
        client: &reqwest::Client,
        state: &mut SpeedTestProgress,
        progress: &ProgressFn<SpeedTestProgress>,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let (_, headers) = self.probe(client, cancel).await?;
        state.server = speed_test::server_location(
            header(&headers, "cf-meta-colo"),
            header(&headers, "cf-meta-city"),
            header(&headers, "cf-ray"),
        );
        progress(state.clone());
        let count = self.options.latency_samples.max(1);
        let mut samples = Vec::with_capacity(count);
        for done in 1..=count {
            let (measured, headers) = self.probe(client, cancel).await?;
            let server_ms = header(&headers, "server-timing").and_then(speed_test::server_processing_ms);
            samples.push(speed_test::network_time(measured, server_ms));
            state.latency = speed_test::latency_stats(&samples);
            state.phase_fraction = done as f32 / count as f32;
            progress(state.clone());
        }
        Ok(())
    }

    /// Runs a worker per connection until `duration` is up, reading their
    /// shared byte count every sample interval. Returns the phase's rate in
    /// bytes per second, and its bytes.
    async fn throughput<W, F>(
        &self,
        duration: Duration,
        worker: W,
        state: &mut SpeedTestProgress,
        progress: &ProgressFn<SpeedTestProgress>,
        cancel: &CancellationToken,
    ) -> Result<(f64, u64)>
    where
        W: Fn(Arc<AtomicU64>, CancellationToken) -> F,
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let duration = duration.max(Duration::from_millis(1));
        let counter = Arc::new(AtomicU64::new(0));
        let stop = cancel.child_token();
        let mut workers = JoinSet::new();
        for _ in 0..self.options.connections.max(1) {
            workers.spawn(worker(Arc::clone(&counter), stop.clone()));
        }
        let data_before = state.data_used;
        let started = Instant::now();
        let mut meter = ThroughputMeter::new(self.options.warmup);
        let mut ticker = tokio::time::interval(self.options.sample_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let outcome = loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break Err(CoreError::Cancelled),
                Some(joined) = workers.join_next() => match joined {
                    // A worker only ends by itself on an error: it is told when to stop.
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => break Err(error),
                    Err(failed) => break Err(CoreError::other(failed.to_string())),
                },
                _ = ticker.tick() => {
                    let elapsed = started.elapsed();
                    meter.record(elapsed, counter.load(Ordering::Relaxed));
                    state.phase_fraction = (elapsed.as_secs_f64() / duration.as_secs_f64()).min(1.0) as f32;
                    state.current_rate = meter.current();
                    state.data_used = data_before + meter.total_bytes();
                    progress(state.clone());
                    if elapsed >= duration {
                        break Ok(());
                    }
                }
            }
        };
        stop.cancel();
        workers.shutdown().await;
        outcome?;
        match meter.result() {
            Some(rate) if meter.total_bytes() > 0 => Ok((rate, meter.total_bytes())),
            _ => Err(CoreError::other(NO_DATA)),
        }
    }
}

#[async_trait]
impl SpeedTestService for SpeedTestClient {
    async fn run(&self, progress: ProgressFn<SpeedTestProgress>, cancel: &CancellationToken) -> Result<SpeedTestResult> {
        let client = self.http.reqwest();
        let proxy = Arc::clone(self.http.proxy());
        let mut state = SpeedTestProgress { phase: Some(SpeedTestPhase::Latency), ..Default::default() };
        progress(state.clone());

        self.latency(&client, &mut state, &progress, cancel).await.map_err(|e| in_phase(SpeedTestPhase::Latency, e))?;
        let latency = state.latency.ok_or_else(|| CoreError::other(NO_ANSWER))?;

        state.phase = Some(SpeedTestPhase::Download);
        state.phase_fraction = 0.0;
        progress(state.clone());
        let url = format!("{}/__down?bytes={}", self.base, self.options.download_chunk.max(1));
        let receive = |counter, stop| download_worker(client.clone(), url.clone(), Arc::clone(&proxy), counter, stop);
        let (download, _) = self
            .throughput(self.options.download_time, receive, &mut state, &progress, cancel)
            .await
            .map_err(|e| in_phase(SpeedTestPhase::Download, e))?;
        state.download = Some(download);

        state.phase = Some(SpeedTestPhase::Upload);
        state.phase_fraction = 0.0;
        state.current_rate = 0.0;
        progress(state.clone());
        let url = format!("{}/__up", self.base);
        let piece: Bytes = (0..UPLOAD_PIECE).map(|_| rand::random::<u8>()).collect::<Vec<u8>>().into();
        let size = self.options.upload_chunk.max(1);
        let send = |counter, stop| {
            upload_worker(client.clone(), url.clone(), Arc::clone(&proxy), counter, stop, piece.clone(), size)
        };
        let (upload, _) = self
            .throughput(self.options.upload_time, send, &mut state, &progress, cancel)
            .await
            .map_err(|e| in_phase(SpeedTestPhase::Upload, e))?;
        state.upload = Some(upload);

        state.phase = None;
        state.phase_fraction = 1.0;
        state.current_rate = 0.0;
        progress(state.clone());
        Ok(SpeedTestResult { latency, download, upload, server: state.server, data_used: state.data_used })
    }
}

/// One connection of the download phase: a chunk, then another, counting
/// every byte as it arrives.
async fn download_worker(
    client: reqwest::Client,
    url: String,
    proxy: Arc<AppProxy>,
    counter: Arc<AtomicU64>,
    stop: CancellationToken,
) -> Result<()> {
    loop {
        let request = client.get(&url).header(CACHE_CONTROL, "no-store");
        let mut response = tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(()),
            sent = request.send() => sent.map_err(|e| map_reqwest_error(&e, &proxy))?,
        };
        check_status(response.status())?;
        let mut received = 0u64;
        loop {
            let chunk = tokio::select! {
                biased;
                _ = stop.cancelled() => return Ok(()),
                chunk = response.chunk() => chunk.map_err(|e| map_reqwest_error(&e, &proxy))?,
            };
            match chunk {
                Some(bytes) => {
                    received += bytes.len() as u64;
                    counter.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                }
                None => break,
            }
        }
        // An empty answer, asked again at once, would be a busy loop against the server.
        if received == 0 {
            return Err(CoreError::other(NO_DATA));
        }
    }
}

/// One connection of the upload phase: a body of `size` random bytes, then
/// another, counting each piece as the connection takes it.
async fn upload_worker(
    client: reqwest::Client,
    url: String,
    proxy: Arc<AppProxy>,
    counter: Arc<AtomicU64>,
    stop: CancellationToken,
    piece: Bytes,
    size: u64,
) -> Result<()> {
    loop {
        let request = client
            .post(&url)
            .header(CACHE_CONTROL, "no-store")
            .header(CONTENT_TYPE, "application/octet-stream")
            // A stated length rather than a chunked body: a plain upload is what
            // every server and proxy takes.
            .header(CONTENT_LENGTH, size)
            .body(counting_body(piece.clone(), size, Arc::clone(&counter)));
        let response = tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(()),
            sent = request.send() => sent.map_err(|e| map_reqwest_error(&e, &proxy))?,
        };
        check_status(response.status())?;
        tokio::select! {
            biased;
            _ = stop.cancelled() => return Ok(()),
            body = response.bytes() => { body.map_err(|e| map_reqwest_error(&e, &proxy))?; }
        }
    }
}

/// `size` bytes of `piece` repeated, adding each piece to `counter` as the
/// connection asks for it.
fn counting_body(piece: Bytes, size: u64, counter: Arc<AtomicU64>) -> reqwest::Body {
    let pieces = futures::stream::unfold(size, move |left| {
        let piece = piece.clone();
        let counter = Arc::clone(&counter);
        async move {
            if left == 0 {
                return None;
            }
            let take = left.min(piece.len() as u64) as usize;
            counter.fetch_add(take as u64, Ordering::Relaxed);
            Some((Ok::<Bytes, std::io::Error>(piece.slice(..take)), left - take as u64))
        }
    });
    reqwest::Body::wrap_stream(pieces)
}

fn check_status(status: StatusCode) -> Result<()> {
    if status.is_success() {
        return Ok(());
    }
    let message = if status == StatusCode::TOO_MANY_REQUESTS {
        RATE_LIMITED.to_string()
    } else {
        format!("The speed test server answered {status}.")
    };
    Err(CoreError::http(Some(status.as_u16()), message))
}

/// An error as the window shows it: what stopped which part of the test.
fn in_phase(phase: SpeedTestPhase, error: CoreError) -> CoreError {
    match error {
        // These already say what happened, and what to do about it.
        CoreError::Cancelled | CoreError::ProxyAuthentication(_) | CoreError::Http { status: Some(_), .. } => error,
        CoreError::Timeout => CoreError::other(NO_ANSWER),
        error if error.to_string() == NO_DATA => error,
        error => match phase {
            SpeedTestPhase::Latency => CoreError::other(format!("Could not reach the speed test server: {error}")),
            _ => CoreError::other(format!("The {} test stopped: {error}", phase.name())),
        },
    }
}

fn header<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}
