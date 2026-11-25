//! Tasks > Speed Test: the connection measured against Cloudflare's nearest
//! server (`quickbyte_core::net::speed_test`). Ping and jitter come first,
//! then download, then upload, on a dial that follows the test as it runs.
//!
//! The test starts as the window opens: choosing Speed Test is the asking. It
//! runs on the engine's runtime and reports several times a second into a slot
//! this window reads each frame. Closing the window stops it. A failure part
//! way through keeps the figures measured before it.
//!
//! Downloads in progress share the line with the test, so while any are
//! running the window says the reading will be low because of them.

use std::any::Any;
use std::sync::Arc;

use egui::{
    Align, Align2, Color32, FontId, Layout, Margin, Painter, Pos2, Rect, RichText, Sense, Shape,
    Stroke, Ui, Vec2, ViewportBuilder, pos2, vec2,
};
use parking_lot::Mutex;
use quickbyte_core::enums::DownloadStatus;
use quickbyte_core::error::CoreError;
use quickbyte_core::helpers::byte_formatter::{format_bytes, format_eta, format_speed};
use quickbyte_core::helpers::speed_test::{format_megabits, megabits, time_to_download};
use quickbyte_core::models::{ProxyMode, SpeedTestPhase, SpeedTestProgress, SpeedTestResult};
use quickbyte_core::traits::ProgressFn;
use tokio_util::sync::CancellationToken;

use super::{AppWindow, WindowCx, WindowKind};
use crate::services::Pending;
use crate::shell::Waker;
use crate::theme;
use crate::view_model::ViewModel;
use crate::widgets::controls::{self, ButtonKind};
use crate::widgets::{form, progress_bar};

/// The dial's scale in Mbps, its marks evenly spaced: on a linear gigabit
/// scale most lines would barely lift the needle off zero.
const SCALE: [f64; 8] = [0.0, 5.0, 10.0, 50.0, 100.0, 250.0, 500.0, 1000.0];

/// The dial sweeps 270°, clockwise from the bottom left (y points down).
const DIAL_START: f32 = 135.0;
const DIAL_SWEEP: f32 = 270.0;
const DIAL_WIDTH: f32 = 8.0;
const DIAL_SIZE: Vec2 = vec2(150.0, 124.0);

/// Upload's colour: the app's purple (the Compressed badge), set apart from
/// download's accent.
const UPLOAD: Color32 = Color32::from_rgb(146, 110, 206);

/// "A 1 GB file would take…": the speed in a download manager's terms.
const EXAMPLE_FILE: i64 = 1024 * 1024 * 1024;

/// How much of the whole test each phase is, for the progress bar.
fn phase_share(phase: SpeedTestPhase) -> (f32, f32) {
    match phase {
        SpeedTestPhase::Latency => (0.0, 0.1),
        SpeedTestPhase::Download => (0.1, 0.45),
        SpeedTestPhase::Upload => (0.55, 0.45),
    }
}

/// Where `mbps` sits on the dial, 0–1.
pub fn dial_fraction(mbps: f64) -> f32 {
    if !mbps.is_finite() || mbps <= 0.0 {
        return 0.0;
    }
    let segments = (SCALE.len() - 1) as f64;
    for (i, pair) in SCALE.windows(2).enumerate() {
        if mbps <= pair[1] {
            let within = (mbps - pair[0]) / (pair[1] - pair[0]);
            return ((i as f64 + within) / segments) as f32;
        }
    }
    1.0
}

/// "18 ms", or "3.4 ms" on a line fast enough that whole milliseconds hide it.
fn format_ms(ms: f64) -> String {
    if ms < 10.0 {
        format!("{ms:.1}")
    } else {
        format!("{ms:.0}")
    }
}

/// How the test stands.
enum Run {
    Running {
        cancel: CancellationToken,
        pending: Pending<quickbyte_core::Result<SpeedTestResult>>,
    },
    Done,
    Failed(String),
    Stopped,
}

pub struct SpeedTestWindow {
    /// `None` until the first frame, which starts the test.
    run: Option<Run>,
    /// Where the worker puts each report.
    latest: Arc<Mutex<SpeedTestProgress>>,
    /// The report being drawn.
    shown: SpeedTestProgress,
    /// The test goes through the proxy in Options.
    through_proxy: bool,
    /// The dial as drawn, eased toward where the figures put it.
    dial: f32,
    dial_target: f32,
}

impl Default for SpeedTestWindow {
    fn default() -> Self {
        Self::new()
    }
}

impl SpeedTestWindow {
    pub fn new() -> Self {
        Self {
            run: None,
            latest: Arc::new(Mutex::new(SpeedTestProgress::default())),
            shown: SpeedTestProgress::default(),
            through_proxy: false,
            dial: 0.0,
            dial_target: 0.0,
        }
    }

    fn is_running(&self) -> bool {
        matches!(self.run, Some(Run::Running { .. }))
    }

    fn start(&mut self, ui: &Ui, cx: &mut WindowCx<'_>) {
        *self.latest.lock() = SpeedTestProgress::default();
        self.shown = SpeedTestProgress::default();
        self.through_proxy = cx.services.settings.current().proxy.mode == ProxyMode::Manual;
        let cancel = CancellationToken::new();
        let wake = Waker::new(ui.ctx(), cx.viewport);
        let (slot, progress_wake) = (Arc::clone(&self.latest), wake.clone());
        let progress: ProgressFn<SpeedTestProgress> = Arc::new(move |report| {
            *slot.lock() = report;
            progress_wake.wake();
        });
        let (service, token) = (cx.services.speed_test.clone(), cancel.clone());
        let pending = cx
            .services
            .run_waking(wake, async move { service.run(progress, &token).await });
        self.run = Some(Run::Running { cancel, pending });
    }

    fn stop(&mut self) {
        if let Some(Run::Running { cancel, .. }) = &self.run {
            cancel.cancel();
        }
    }

    /// Takes in what the worker reported since the last frame.
    fn collect(&mut self) {
        if let Some(Run::Running { pending, .. }) = &mut self.run
            && let Some(result) = pending.poll()
        {
            self.run = Some(match result {
                Ok(_) => Run::Done,
                Err(CoreError::Cancelled) => Run::Stopped,
                Err(error) => Run::Failed(error.to_string()),
            });
        }
        // After the result: the last report is in by the time the result is.
        self.shown = self.latest.lock().clone();
        self.dial_target = match (&self.run, self.shown.phase) {
            (
                Some(Run::Running { .. }),
                Some(SpeedTestPhase::Download | SpeedTestPhase::Upload),
            ) => dial_fraction(megabits(self.shown.current_rate)),
            (Some(Run::Done), _) => dial_fraction(megabits(self.shown.download.unwrap_or(0.0))),
            _ => 0.0,
        };
    }

    fn ease_dial(&mut self, dt: f32) {
        let step = 1.0 - (-dt.clamp(0.0, 0.1) * 9.0).exp();
        self.dial += (self.dial_target - self.dial) * step;
        if (self.dial_target - self.dial).abs() < 0.0005 {
            self.dial = self.dial_target;
        }
    }

    /// The dial's colour, and what its centre reads: value and unit. Which
    /// phase it shows is said beside it, where that phase's figure says "measuring…".
    fn readout(&self) -> (Color32, String, &'static str) {
        let p = theme::palette();
        let running = self.is_running();
        match self.shown.phase {
            Some(SpeedTestPhase::Latency) if running => (
                p.accent,
                self.shown
                    .latency
                    .map_or_else(|| "—".into(), |l| format_ms(l.median_ms)),
                "ms",
            ),
            Some(SpeedTestPhase::Download) if running => (
                p.accent,
                format_megabits(self.shown.current_rate),
                "Mbps",
            ),
            Some(SpeedTestPhase::Upload) if running => (
                UPLOAD,
                format_megabits(self.shown.current_rate),
                "Mbps",
            ),
            _ => match (&self.run, self.shown.download) {
                (Some(Run::Done), Some(download)) => {
                    (p.accent, format_megabits(download), "Mbps")
                }
                _ => (p.accent, "—".into(), "Mbps"),
            },
        }
    }

    /// The window title: which server answered, once one has, and whether the
    /// test goes through the proxy in Options.
    fn window_title(&self) -> String {
        let mut text = match &self.shown.server {
            Some(server) => format!("Speed Test — Cloudflare · {server}"),
            None => "Speed Test — Cloudflare".into(),
        };
        if self.through_proxy {
            text.push_str(" · via proxy");
        }
        text
    }

    /// The line under the figures, and its colour.
    fn status(&self, vm: &ViewModel) -> (String, Color32) {
        let p = theme::palette();
        match &self.run {
            None | Some(Run::Running { .. }) => {
                let busy = vm
                    .items()
                    .filter(|i| {
                        matches!(
                            i.status,
                            DownloadStatus::Connecting | DownloadStatus::Downloading
                        )
                    })
                    .count();
                if busy > 0 {
                    let (count, verb) = if busy == 1 {
                        ("1 download is".to_string(), "shares")
                    } else {
                        (format!("{busy} downloads are"), "share")
                    };
                    return (
                        format!(
                            "{count} running and {verb} the connection, so this result will read low."
                        ),
                        p.warning,
                    );
                }
                let text = match self.shown.phase {
                    Some(SpeedTestPhase::Download) => "Downloading test data…",
                    Some(SpeedTestPhase::Upload) => "Uploading test data…",
                    _ => "Measuring the round trip to the server…",
                };
                (text.into(), p.text_muted)
            }
            Some(Run::Done) => {
                let download = self.shown.download.unwrap_or(0.0);
                let eta = format_eta(time_to_download(EXAMPLE_FILE, download));
                (
                    format!(
                        "At this speed a 1 GB file takes about {eta}. The test used {} of data.",
                        format_bytes(self.shown.data_used as i64)
                    ),
                    p.text_muted,
                )
            }
            Some(Run::Failed(message)) => (message.clone(), p.danger),
            Some(Run::Stopped) => (
                "Stopped. Test Again runs the whole test from the start.".into(),
                p.text_muted,
            ),
        }
    }

    fn overall_percentage(&self) -> f64 {
        match self.shown.phase {
            Some(phase) => {
                let (before, share) = phase_share(phase);
                f64::from((before + share * self.shown.phase_fraction.clamp(0.0, 1.0)) * 100.0)
            }
            None => 100.0,
        }
    }

    fn results(&self, ui: &mut Ui) {
        let p = theme::palette();
        let running = self.is_running();
        let phase = if running { self.shown.phase } else { None };
        let ping = self.shown.latency;
        result_block(
            ui,
            Legend::Dot(p.text_muted),
            "Ping",
            phase == Some(SpeedTestPhase::Latency),
            ping.map(|l| format_ms(l.median_ms)),
            "ms",
            ping.map(|l| format!("jitter {} ms", format_ms(l.jitter_ms))),
        );
        ui.add_space(4.0);
        let download = match phase {
            Some(SpeedTestPhase::Download) => Some(self.shown.current_rate),
            _ => self.shown.download,
        };
        result_block(
            ui,
            Legend::Arrow {
                up: false,
                color: p.accent,
            },
            "Download",
            phase == Some(SpeedTestPhase::Download),
            download.map(format_megabits),
            "Mbps",
            download.map(format_speed),
        );
        ui.add_space(4.0);
        let upload = match phase {
            Some(SpeedTestPhase::Upload) => Some(self.shown.current_rate),
            _ => self.shown.upload,
        };
        result_block(
            ui,
            Legend::Arrow {
                up: true,
                color: UPLOAD,
            },
            "Upload",
            phase == Some(SpeedTestPhase::Upload),
            upload.map(format_megabits),
            "Mbps",
            upload.map(format_speed),
        );
    }

    fn paint_dial(&self, ui: &mut Ui) {
        let p = theme::palette();
        let (rect, _) = ui.allocate_exact_size(DIAL_SIZE, Sense::hover());
        let painter = ui.painter_at(rect);
        let pad = DIAL_WIDTH / 2.0 + 2.0;
        // The open bottom of a 270° dial reaches sin 45° of the radius below its centre.
        let radius = ((rect.height() - 2.0 * pad) / (1.0 + std::f32::consts::FRAC_1_SQRT_2))
            .min(rect.width() / 2.0 - pad);
        let center = pos2(rect.center().x, rect.top() + pad + radius);
        let (color, value, unit) = self.readout();

        arc(&painter, center, radius, DIAL_START, DIAL_SWEEP, p.track);
        if self.dial > 0.0005 {
            arc(
                &painter,
                center,
                radius,
                DIAL_START,
                DIAL_SWEEP * self.dial,
                color,
            );
        }
        let last = (SCALE.len() - 1) as f32;
        for (i, mark) in SCALE.iter().enumerate() {
            let angle = (DIAL_START + DIAL_SWEEP * i as f32 / last).to_radians();
            let at = center + vec2(angle.cos(), angle.sin()) * (radius - DIAL_WIDTH - 10.0);
            painter.text(
                at,
                Align2::CENTER_CENTER,
                format!("{mark:.0}"),
                FontId::proportional(9.0),
                p.text_disabled,
            );
        }
        painter.text(
            center + vec2(0.0, -4.0),
            Align2::CENTER_CENTER,
            value,
            FontId::new(22.0, theme::semibold_family()),
            p.text,
        );
        painter.text(
            center + vec2(0.0, 14.0),
            Align2::CENTER_CENTER,
            unit,
            theme::small(),
            p.text_muted,
        );
    }
}

/// The mark beside a figure's caption, in the colour the dial uses for it.
enum Legend {
    Dot(Color32),
    Arrow { up: bool, color: Color32 },
}

fn paint_legend(painter: &Painter, rect: Rect, legend: &Legend) {
    let c = rect.center();
    match *legend {
        Legend::Dot(color) => {
            painter.circle_filled(c, 3.5, color);
        }
        Legend::Arrow { up, color } => {
            let stroke = Stroke::new(1.6, color);
            let (tip, tail) = if up {
                (c - vec2(0.0, 4.5), c + vec2(0.0, 4.5))
            } else {
                (c + vec2(0.0, 4.5), c - vec2(0.0, 4.5))
            };
            let back = if up { 3.5 } else { -3.5 };
            painter.line_segment([tail, tip], stroke);
            painter.line_segment([tip, tip + vec2(-3.5, back)], stroke);
            painter.line_segment([tip, tip + vec2(3.5, back)], stroke);
        }
    }
}

/// One figure: its caption, then the value large, with its unit and a detail.
fn result_block(
    ui: &mut Ui,
    legend: Legend,
    caption: &str,
    active: bool,
    value: Option<String>,
    unit: &str,
    detail: Option<String>,
) {
    let p = theme::palette();
    ui.horizontal(|ui| {
        let (rect, _) = ui.allocate_exact_size(vec2(10.0, 14.0), Sense::hover());
        paint_legend(ui.painter(), rect, &legend);
        let mut text = RichText::new(caption).font(theme::small_strong());
        text = text.color(if active { p.text } else { p.text_muted });
        ui.label(text);
        if active {
            ui.label(
                RichText::new("measuring…")
                    .font(theme::small())
                    .color(p.text_muted),
            );
        }
    });
    ui.horizontal(|ui| {
        ui.add_space(14.0);
        ui.spacing_mut().item_spacing.x = 4.0;
        match value {
            Some(value) => {
                ui.label(
                    RichText::new(value)
                        .font(FontId::new(20.0, theme::semibold_family()))
                        .color(p.text),
                );
                ui.label(RichText::new(unit).font(theme::small()).color(p.text_muted));
                if let Some(detail) = detail {
                    ui.add_space(8.0);
                    ui.label(
                        RichText::new(detail)
                            .font(theme::small())
                            .color(p.text_muted),
                    );
                }
            }
            None => {
                ui.label(
                    RichText::new("—")
                        .font(FontId::new(20.0, theme::semibold_family()))
                        .color(p.text_disabled),
                );
            }
        }
    });
}

/// A round-capped arc, angles in degrees clockwise from +x.
fn arc(painter: &Painter, center: Pos2, radius: f32, start: f32, sweep: f32, color: Color32) {
    let segments = ((sweep.abs() / 4.0).ceil() as usize).max(2);
    let points: Vec<Pos2> = (0..=segments)
        .map(|i| {
            let angle = (start + sweep * i as f32 / segments as f32).to_radians();
            center + vec2(angle.cos(), angle.sin()) * radius
        })
        .collect();
    for end in [points[0], points[segments]] {
        painter.circle_filled(end, DIAL_WIDTH / 2.0, color);
    }
    painter.add(Shape::line(points, Stroke::new(DIAL_WIDTH, color)));
}

impl AppWindow for SpeedTestWindow {
    fn kind(&self) -> WindowKind {
        WindowKind::SpeedTest
    }

    fn builder(&self, _vm: &ViewModel) -> ViewportBuilder {
        ViewportBuilder::default()
            .with_title("Speed Test")
            .with_inner_size(vec2(440.0, 262.0))
            .with_resizable(false)
            .with_maximize_button(false)
    }

    fn title(&self, _vm: &ViewModel) -> String {
        self.window_title()
    }

    fn is_animating(&self) -> bool {
        (self.dial_target - self.dial).abs() >= 0.0005
    }

    fn ui(&mut self, ui: &mut Ui, cx: &mut WindowCx<'_>) {
        if self.run.is_none() {
            self.start(ui, cx);
        }
        self.collect();
        self.ease_dial(cx.dt);
        let p = theme::palette();

        let running = self.is_running();
        let (mut again, mut stop, mut close) = (false, false, false);
        controls::footer(ui, "speed-footer", |ui| {
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                if running {
                    stop |= controls::button(ui, "Stop", ButtonKind::Primary, true).clicked();
                } else {
                    again |=
                        controls::button(ui, "Test Again", ButtonKind::Primary, true).clicked();
                }
                close |= controls::button(ui, "Close", ButtonKind::Secondary, true).clicked();
            });
        });
        let (status, status_color) = self.status(cx.vm);
        form::body(
            ui,
            Margin {
                left: 16,
                right: 16,
                top: 12,
                bottom: 8,
            },
            |ui| {
                ui.horizontal_top(|ui| {
                    self.paint_dial(ui);
                    ui.add_space(16.0);
                    ui.vertical(|ui| {
                        self.results(ui);
                    });
                });
                ui.add_space(6.0);
                if running {
                    // No label: at 4 px it would only be cut off.
                    progress_bar::show(ui, 4.0, self.overall_percentage(), p.accent, Some(""));
                } else {
                    ui.add_space(4.0);
                }
                ui.add_space(6.0);
                form::note_colored(ui, &status, status_color);
            },
        );

        if form::enter_pressed(ui) && !running {
            again = true;
        }
        if form::escape_pressed(ui) {
            close = true;
        }
        if stop {
            self.stop();
        }
        if again {
            self.start(ui, cx);
        }
        if close {
            cx.close();
        }
    }

    fn on_closed(&mut self, _cx: &mut WindowCx<'_>) {
        self.stop();
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(a: f32, b: f32) -> bool {
        (a - b).abs() < 1e-6
    }

    #[test]
    fn the_dial_spaces_its_marks_evenly() {
        assert_eq!(dial_fraction(0.0), 0.0);
        assert!(close(dial_fraction(5.0), 1.0 / 7.0));
        assert!(close(dial_fraction(100.0), 4.0 / 7.0));
        // Half way between the 50 and 100 marks is half way between their places.
        assert!(close(dial_fraction(75.0), 3.5 / 7.0));
        assert!(close(dial_fraction(1000.0), 1.0));
    }

    #[test]
    fn the_dial_stops_at_its_ends() {
        assert_eq!(dial_fraction(5_000.0), 1.0);
        assert_eq!(dial_fraction(-3.0), 0.0);
        assert_eq!(dial_fraction(f64::NAN), 0.0);
        assert_eq!(dial_fraction(f64::INFINITY), 0.0);
    }

    #[test]
    fn latency_keeps_a_decimal_only_when_it_is_small() {
        assert_eq!(format_ms(3.44), "3.4");
        assert_eq!(format_ms(18.4), "18");
    }

    #[test]
    fn the_progress_bar_shares_add_up_to_the_whole_test() {
        let mut end = 0.0;
        for phase in SpeedTestPhase::ALL {
            let (before, share) = phase_share(phase);
            assert!(close(before, end), "{phase:?} starts where the last ended");
            end = before + share;
        }
        assert!(close(end, 1.0));
    }
}
