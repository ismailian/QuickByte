//! The speed test against a local stand-in for Cloudflare's speed test
//! endpoints: what each phase asks for, what it reports while it runs, and how
//! it stops — on a refusal, an empty answer, an unreachable server, or Cancel.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use quickbyte_core::error::CoreError;
use quickbyte_core::models::{SpeedTestPhase, SpeedTestProgress};
use quickbyte_core::net::speed_test::{
    CONNECTION_LOST, NO_DATA, RATE_LIMITED, SpeedTestClient, SpeedTestOptions, UNREACHABLE,
};
use quickbyte_core::traits::{ProgressFn, SpeedTestService};
use tokio_util::sync::CancellationToken;

use crate::support::{Asked, Reply, TestServer, direct_client};

const CHUNK: u64 = 256 * 1024;

/// A whole test in well under two seconds.
fn quick() -> SpeedTestOptions {
    SpeedTestOptions {
        latency_samples: 3,
        download_time: Duration::from_millis(400),
        upload_time: Duration::from_millis(400),
        warmup: Duration::from_millis(100),
        connections: 2,
        download_chunk: CHUNK,
        upload_chunk: CHUNK,
        probe_timeout: Duration::from_secs(5),
        sample_interval: Duration::from_millis(20),
        data_budget: u64::MAX,
    }
}

/// Cloudflare's endpoints as they behave: nothing or N bytes down, anything up.
fn speed_server(asked: &Asked) -> Reply {
    match asked.path.as_str() {
        "/__down?bytes=0" => Reply::ok(b"")
            .header("cf-meta-colo", "FRA")
            .header("cf-meta-city", "Frankfurt")
            .header("Server-Timing", "cfRequestDuration;dur=0.05"),
        path if path.starts_with("/__down?bytes=") => {
            let bytes: usize = path["/__down?bytes=".len()..].parse().unwrap();
            Reply::ok(&vec![0u8; bytes])
        }
        "/__up" if asked.method == "POST" => Reply::ok(b""),
        _ => Reply::status(404),
    }
}

/// Every progress report, in order.
fn recorder() -> (ProgressFn<SpeedTestProgress>, Arc<Mutex<Vec<SpeedTestProgress>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    (Arc::new(move |p: SpeedTestProgress| sink.lock().push(p)), seen)
}

#[tokio::test]
async fn a_test_measures_latency_then_download_then_upload() {
    let server = TestServer::start(|asked, _| speed_server(asked)).await;
    let client = SpeedTestClient::with(&server.url(""), direct_client(), quick());
    let (progress, seen) = recorder();

    let result = client.run(progress, &CancellationToken::new()).await.unwrap();

    assert_eq!(result.latency.samples, 3);
    assert!(result.latency.median_ms > 0.0);
    assert!(result.download > 0.0, "{result:?}");
    assert!(result.upload > 0.0, "{result:?}");
    assert_eq!(result.server.as_deref(), Some("Frankfurt (FRA)"));

    // The phases in order, each reported as it starts, and a last report that
    // carries every figure the result has.
    let seen = seen.lock();
    let mut phases: Vec<Option<SpeedTestPhase>> = seen.iter().map(|p| p.phase).collect();
    phases.dedup();
    assert_eq!(
        phases,
        [Some(SpeedTestPhase::Latency), Some(SpeedTestPhase::Download), Some(SpeedTestPhase::Upload), None]
    );
    let last = seen.last().unwrap();
    assert_eq!(last.latency, Some(result.latency));
    assert_eq!((last.download, last.upload), (Some(result.download), Some(result.upload)));
    assert_eq!(last.data_used, result.data_used);
    assert!(seen.iter().any(|p| p.phase == Some(SpeedTestPhase::Download) && p.current_rate > 0.0));

    // One probe to open the connection, then the counted ones.
    let requests = server.requests();
    assert!(requests[..4].iter().all(|r| r.method == "GET" && r.path == "/__down?bytes=0"));
    assert!(requests.iter().any(|r| r.path == format!("/__down?bytes={CHUNK}")));
    // Uploads say how long they are, and send exactly that.
    let uploads: Vec<&Asked> = requests.iter().filter(|r| r.path == "/__up").collect();
    assert!(!uploads.is_empty());
    for upload in uploads {
        assert_eq!(upload.method, "POST");
        assert_eq!(upload.header("Content-Length"), Some(CHUNK.to_string()));
        assert_eq!(upload.header("Transfer-Encoding"), None);
        assert_eq!(upload.body.len() as u64, CHUNK);
    }
    // Every byte moved, both ways, is accounted for: at least the requests that finished.
    let finished_up = server.requests().iter().filter(|r| r.path == "/__up").count() as u64;
    assert!(result.data_used >= finished_up * CHUNK);
}

#[tokio::test]
async fn a_refusal_stops_the_test_with_what_the_server_said() {
    let server = TestServer::start(|asked, _| {
        if asked.path.starts_with("/__down?bytes=") && asked.path != "/__down?bytes=0" {
            Reply::status(503)
        } else {
            speed_server(asked)
        }
    })
    .await;
    let client = SpeedTestClient::with(&server.url(""), direct_client(), quick());
    let (progress, seen) = recorder();

    let error = client.run(progress, &CancellationToken::new()).await.unwrap_err();

    assert_eq!(error.to_string(), "The speed test server answered 503 Service Unavailable.");
    // The latency it did measure is still with the caller.
    assert!(seen.lock().last().unwrap().latency.is_some());
}

#[tokio::test]
async fn a_rate_limit_is_explained() {
    let server = TestServer::start(|_, _| Reply::status(429)).await;
    let client = SpeedTestClient::with(&server.url(""), direct_client(), quick());

    let error = client.run(recorder().0, &CancellationToken::new()).await.unwrap_err();

    assert_eq!(error.to_string(), RATE_LIMITED);
}

#[tokio::test]
async fn an_empty_download_is_no_data_rather_than_zero_speed() {
    let server = TestServer::start(|asked, _| {
        if asked.path.starts_with("/__down?bytes=") { Reply::ok(b"") } else { speed_server(asked) }
    })
    .await;
    let client = SpeedTestClient::with(&server.url(""), direct_client(), quick());

    let error = client.run(recorder().0, &CancellationToken::new()).await.unwrap_err();

    assert_eq!(error.to_string(), NO_DATA);
    // It gave up on the first empty answer rather than asking again and again.
    assert!(server.requests().len() < 20, "{} requests", server.requests().len());
}

#[tokio::test]
async fn an_unreachable_server_says_so() {
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let client = SpeedTestClient::with(&format!("http://127.0.0.1:{port}"), direct_client(), quick());

    let error = client.run(recorder().0, &CancellationToken::new()).await.unwrap_err();

    assert_eq!(error.to_string(), UNREACHABLE);
}

#[tokio::test]
async fn cancel_stops_a_running_phase_at_once() {
    let server = TestServer::start(|asked, _| speed_server(asked)).await;
    let options = SpeedTestOptions { download_time: Duration::from_secs(30), ..quick() };
    let client = SpeedTestClient::with(&server.url(""), direct_client(), options);
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let progress: ProgressFn<SpeedTestProgress> = Arc::new(move |p: SpeedTestProgress| {
        if p.phase == Some(SpeedTestPhase::Download) && p.current_rate > 0.0 {
            stop.cancel();
        }
    });

    let started = Instant::now();
    let error = client.run(progress, &cancel).await.unwrap_err();

    assert!(matches!(error, CoreError::Cancelled), "{error}");
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn a_dropped_connection_is_ridden_out() {
    // The first download and the first upload each lose their connection part
    // way through, as a reset on a busy line does; the test carries on.
    let (downs, ups) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let server = TestServer::start(move |asked, _| {
        let path = asked.path.as_str();
        if path.starts_with("/__down?bytes=") && path != "/__down?bytes=0" && downs.fetch_add(1, Ordering::SeqCst) == 0 {
            return Reply::ok(&vec![0u8; 1024]).declared(CHUNK);
        }
        if path == "/__up" && ups.fetch_add(1, Ordering::SeqCst) == 0 {
            return Reply::hang_up();
        }
        speed_server(asked)
    })
    .await;
    let client = SpeedTestClient::with(&server.url(""), direct_client(), quick());

    let result = client.run(recorder().0, &CancellationToken::new()).await.unwrap();

    assert!(result.download > 0.0 && result.upload > 0.0);
}

#[tokio::test]
async fn a_connection_that_keeps_dropping_stops_the_phase_in_plain_words() {
    let server = TestServer::start(|asked, _| {
        if asked.path.starts_with("/__down?bytes=") && asked.path != "/__down?bytes=0" {
            Reply::hang_up()
        } else {
            speed_server(asked)
        }
    })
    .await;
    // Long enough that the connections give up before the phase's time is up.
    let options = SpeedTestOptions { download_time: Duration::from_secs(10), ..quick() };
    let client = SpeedTestClient::with(&server.url(""), direct_client(), options);

    let started = Instant::now();
    let error = client.run(recorder().0, &CancellationToken::new()).await.unwrap_err();

    assert_eq!(error.to_string(), format!("The download test stopped: {CONNECTION_LOST}"));
    // Every connection gave up after a few tries, well before the phase's time.
    assert!(started.elapsed() < Duration::from_secs(5));
}

#[tokio::test]
async fn a_test_stops_each_phase_at_its_share_of_the_data_budget() {
    let server = TestServer::start(|asked, _| speed_server(asked)).await;
    let budget = 8 * CHUNK;
    // Plenty of time: the bytes, not the clock, end each phase.
    let options = SpeedTestOptions {
        download_time: Duration::from_secs(30),
        upload_time: Duration::from_secs(30),
        data_budget: budget,
        ..quick()
    };
    let client = SpeedTestClient::with(&server.url(""), direct_client(), options);

    let started = Instant::now();
    let result = client.run(recorder().0, &CancellationToken::new()).await.unwrap();

    assert!(started.elapsed() < Duration::from_secs(10));
    assert!(result.download > 0.0 && result.upload > 0.0);
    // At most one read or one upload piece per connection past each half.
    let slack = 2 * 2 * 64 * 1024;
    assert!(result.data_used >= budget && result.data_used <= budget + slack, "{}", result.data_used);
}
