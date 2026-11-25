//! A download's properties: the file, where it is, how big, when, and where it
//! came from. Read-only, with no progress bar or connections table: the
//! download window (`details.rs`) is for watching a transfer, this one for
//! looking a download up.
//!
//! Opened from Properties in the row menu, the Downloads menu, or Alt+Enter.
//! One per download.

use std::any::Any;

use egui::{Align, Layout, RichText, Stroke, Ui, ViewportBuilder, vec2};
use quickbyte_core::enums::DownloadStatus;
use quickbyte_core::helpers::byte_formatter::format_bytes;
use quickbyte_core::models::DownloadItem;
use uuid::Uuid;

use super::main::format_short_date_time;
use super::{AppWindow, WindowCx, WindowKind};
use crate::actions;
use crate::theme;
use crate::view_model::ViewModel;
use crate::widgets::controls::{self, ButtonKind};

/// The caption column, so the values line up.
const CAPTION_WIDTH: f32 = 78.0;
/// Room kept beside the URL for its Copy link.
const COPY_WIDTH: f32 = 46.0;

pub struct PropertiesWindow {
    id: Uuid,
}

impl PropertiesWindow {
    pub fn new(id: Uuid) -> Self {
        Self { id }
    }
}

/// "4.2 GB", or "1.1 GB of 4.2 GB" while it is still coming in.
fn size_text(item: &DownloadItem) -> String {
    match (item.status, item.total_bytes) {
        (DownloadStatus::Completed, total) if total > 0 => format_bytes(total),
        (_, total) if total > 0 => format!(
            "{} of {}",
            format_bytes(item.downloaded_bytes),
            format_bytes(total)
        ),
        _ if item.downloaded_bytes > 0 => format!("{} so far", format_bytes(item.downloaded_bytes)),
        _ => "Unknown".into(),
    }
}

/// When it finished downloading; for one that has not, when it was added.
fn date_row(item: &DownloadItem) -> (&'static str, String) {
    match item.completed_at {
        Some(done) => ("Downloaded", format_short_date_time(done)),
        None => ("Added", format_short_date_time(item.created_at)),
    }
}

fn caption(ui: &mut Ui, text: &str) {
    controls::field(
        ui,
        CAPTION_WIDTH,
        RichText::new(text)
            .font(theme::small())
            .color(theme::palette().text_muted),
    );
}

/// A value cut off with an ellipsis when too long, the whole of it on hover.
fn value(ui: &mut Ui, width: f32, text: impl Into<RichText>, full: &str) {
    controls::field(ui, width, text).on_hover_text(full);
}

impl AppWindow for PropertiesWindow {
    fn kind(&self) -> WindowKind {
        WindowKind::Properties(self.id)
    }

    fn builder(&self, vm: &ViewModel) -> ViewportBuilder {
        ViewportBuilder::default()
            .with_title(self.title(vm))
            .with_inner_size(vec2(500.0, 196.0))
            .with_resizable(false)
            .with_maximize_button(false)
    }

    fn title(&self, _vm: &ViewModel) -> String {
        "Properties".into()
    }

    fn download(&self) -> Option<Uuid> {
        Some(self.id)
    }

    fn ui(&mut self, ui: &mut Ui, cx: &mut WindowCx<'_>) {
        let Some(item) = cx.vm.item(self.id).cloned() else {
            cx.close();
            return;
        };
        let p = theme::palette();
        let can_open = item.status == DownloadStatus::Completed && actions::can_open_file(&item, &cx.shell);
        let can_folder = actions::can_open_folder(&item, &cx.shell);
        let full_path = item.full_path();

        controls::footer(ui, "properties-footer", |ui| {
            ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                if controls::button(ui, "Close", ButtonKind::Secondary, true).clicked() {
                    cx.close();
                }
                if controls::button(ui, "Open Folder", ButtonKind::Secondary, can_folder).clicked() {
                    cx.shell.reveal_in_explorer(&full_path, &item.save_folder);
                }
                if controls::button(ui, "Open File", ButtonKind::Primary, can_open).clicked() {
                    cx.shell.open_file(&full_path);
                }
            });
        });

        controls::body(ui, 0, |ui| {
            egui::Frame::NONE
                .inner_margin(egui::Margin { left: 20, right: 20, top: 14, bottom: 12 })
                .show(ui, |ui| {

                    egui::Frame::NONE
                        .fill(p.header_back)
                        .stroke(Stroke::new(1.0, p.border))
                        .corner_radius(6.0)
                        .inner_margin(egui::Margin::symmetric(14, 10))
                        .show(ui, |ui| {
                            ui.set_width(ui.available_width());
                            let width = (ui.available_width() - CAPTION_WIDTH - 12.0).max(160.0);
                            egui::Grid::new("properties-grid")
                                .num_columns(2)
                                .spacing(vec2(12.0, 4.0))
                                .show(ui, |ui| {
                                    caption(ui, "File name");
                                    value(ui, width, RichText::new(&item.file_name).color(p.text), &item.file_name);
                                    ui.end_row();

                                    caption(ui, "Location");
                                    value(ui, width, RichText::new(&item.save_folder).color(p.text), &full_path);
                                    ui.end_row();

                                    caption(ui, "Size");
                                    let size = size_text(&item);
                                    value(ui, width, RichText::new(&size).color(p.text), &size);
                                    ui.end_row();

                                    let (label, date) = date_row(&item);
                                    caption(ui, label);
                                    value(ui, width, RichText::new(&date).color(p.text), &date);
                                    ui.end_row();

                                    caption(ui, "URL");
                                    ui.allocate_ui_with_layout(
                                        vec2(width, 16.0),
                                        Layout::left_to_right(Align::Center),
                                        |ui| {
                                            value(
                                                ui,
                                                (width - COPY_WIDTH).max(80.0),
                                                RichText::new(&item.url).color(p.accent),
                                                &item.url,
                                            );
                                            if controls::link(ui, "Copy", theme::small()).clicked() {
                                                ui.ctx().copy_text(item.url.clone());
                                            }
                                        },
                                    );
                                    ui.end_row();
                                });
                        });
                });
        });

        if ui.input_mut(|i| i.consume_key(egui::Modifiers::NONE, egui::Key::Escape)) {
            cx.close();
        }
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
