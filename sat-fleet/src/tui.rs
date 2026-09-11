use crate::config::{FleetConfig, Preset};
use crate::engine::{CHANNELS, Engine, EngineStats, StatsListener};
use crate::nominal::{EnvConfig, LogWriter, NominalApi, ProvisionProgress, RunInfo};
use crate::satgen::{self, SatelliteConfig};
use anyhow::Result;
use crossterm::event::{Event, KeyCode, KeyEventKind};
use nominal_streaming::api::scout::rids::api::AssetRid;
use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Gauge, List, ListItem, ListState, Paragraph};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender, channel};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const LOG_STATS_INTERVAL: Duration = Duration::from_secs(10);

enum AppEvent {
    ProvisionProgress(ProvisionProgress),
    ProvisionDone {
        sats: Vec<SatelliteConfig>,
        asset_rids: Vec<AssetRid>,
        existing: usize,
        created: usize,
        failed: usize,
        first_error: Option<String>,
    },
    RunCreated(RunInfo),
    CutoverComplete(RunInfo),
    RunEnded,
    EngineStopped,
    BackgroundError(String),
}

enum LogMsg {
    Event(String),
    Shutdown,
}

#[derive(PartialEq)]
enum Screen {
    Select,
    Provisioning,
    Streaming,
    Stopping,
}

struct Session {
    preset: Preset,
    debug: bool,
    sats: Vec<SatelliteConfig>,
    asset_rids: Vec<AssetRid>,
    run: Option<RunInfo>,
    engine: Option<Engine>,
    stats: Arc<EngineStats>,
    stream_started: Instant,
    samples: VecDeque<(Instant, u64)>,
    log_tx: Option<Sender<LogMsg>>,
    log_thread: Option<std::thread::JoinHandle<()>>,
    cutover_pending: bool,
}

struct App {
    fleet: FleetConfig,
    env: Arc<EnvConfig>,
    api: Arc<NominalApi>,
    handle: tokio::runtime::Handle,
    events_tx: Sender<AppEvent>,
    screen: Screen,
    list_state: ListState,
    /// When set, presets stream at 1 Hz instead of their configured rate
    /// (same batch and assets, marked as a debug run).
    debug_rate: bool,
    provision_only: bool,
    provision: Option<ProvisionProgress>,
    provision_preset: Option<Preset>,
    provision_debug: bool,
    session: Option<Session>,
    status: Option<(String, bool)>,
    // stop coordination
    waiting_engine: bool,
    waiting_run_end: bool,
    quit_after_stop: bool,
    should_exit: bool,
}

pub fn run(
    fleet: FleetConfig,
    env: EnvConfig,
    api: NominalApi,
    handle: tokio::runtime::Handle,
) -> Result<()> {
    let (events_tx, events_rx) = channel();
    let mut app = App {
        fleet,
        env: Arc::new(env),
        api: Arc::new(api),
        handle,
        events_tx,
        screen: Screen::Select,
        list_state: ListState::default().with_selected(Some(0)),
        debug_rate: false,
        provision_only: false,
        provision: None,
        provision_preset: None,
        provision_debug: false,
        session: None,
        status: None,
        waiting_engine: false,
        waiting_run_end: false,
        quit_after_stop: false,
        should_exit: false,
    };

    let mut terminal = ratatui::init();
    let result = app.event_loop(&mut terminal, events_rx);
    ratatui::restore();
    result
}

impl App {
    fn event_loop(
        &mut self,
        terminal: &mut ratatui::DefaultTerminal,
        events_rx: Receiver<AppEvent>,
    ) -> Result<()> {
        loop {
            self.sample_stats();
            terminal.draw(|frame| self.render(frame))?;

            if crossterm::event::poll(Duration::from_millis(100))? {
                if let Event::Key(key) = crossterm::event::read()? {
                    if key.kind == KeyEventKind::Press {
                        self.on_key(key.code);
                    }
                }
            }
            while let Ok(event) = events_rx.try_recv() {
                self.on_event(event);
            }
            if self.should_exit {
                return Ok(());
            }
        }
    }

    // ---- input ----

    fn on_key(&mut self, code: KeyCode) {
        match self.screen {
            Screen::Select => match code {
                KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
                KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
                KeyCode::Enter => self.start_provision(false),
                KeyCode::Char('p') => self.start_provision(true),
                KeyCode::Char('d') => {
                    self.debug_rate = !self.debug_rate;
                    self.status = Some((
                        if self.debug_rate {
                            "debug mode on: presets will stream at 1 Hz".to_string()
                        } else {
                            "debug mode off: presets stream at their configured rate".to_string()
                        },
                        false,
                    ));
                }
                KeyCode::Char('q') => self.should_exit = true,
                _ => {}
            },
            Screen::Provisioning => {
                if code == KeyCode::Char('q') {
                    // Abandons the in-flight provisioning task; re-running is
                    // idempotent so this is safe.
                    self.should_exit = true;
                }
            }
            Screen::Streaming => match code {
                KeyCode::Char('n') => self.start_cutover(),
                KeyCode::Char('s') => self.start_stop(false),
                KeyCode::Char('q') => self.start_stop(true),
                _ => {}
            },
            Screen::Stopping => {}
        }
    }

    fn move_selection(&mut self, delta: i64) {
        let len = self.fleet.presets.len() as i64;
        let current = self.list_state.selected().unwrap_or(0) as i64;
        let next = (current + delta).rem_euclid(len);
        self.list_state.select(Some(next as usize));
    }

    fn selected_preset(&self) -> Preset {
        self.fleet.presets[self.list_state.selected().unwrap_or(0)].clone()
    }

    // ---- actions ----

    fn start_provision(&mut self, provision_only: bool) {
        let mut preset = self.selected_preset();
        // Debug mode: same batch, same assets, just a slower tick. Only the
        // streaming rate changes; the batch name stays intact so provisioning
        // still targets the batch's existing assets.
        let debug = self.debug_rate && preset.rate_hz > 1.0;
        if debug {
            preset.rate_hz = 1.0;
        }
        self.status = None;
        self.provision_only = provision_only;
        self.provision = None;
        self.provision_preset = Some(preset.clone());
        self.provision_debug = debug;
        self.screen = Screen::Provisioning;

        let api = self.api.clone();
        let common_label = self.fleet.common_label.clone();
        let orbits = self.fleet.orbits;
        let tx = self.events_tx.clone();
        self.handle.spawn(async move {
            let sats = satgen::generate(&preset, &orbits);
            let progress_tx = tx.clone();
            let result = api
                .provision_assets(&preset, &common_label, &sats, move |p| {
                    let _ = progress_tx.send(AppEvent::ProvisionProgress(p));
                })
                .await;
            let event = match result {
                Ok(outcome) => AppEvent::ProvisionDone {
                    sats,
                    asset_rids: outcome.asset_rids,
                    existing: outcome.existing,
                    created: outcome.created,
                    failed: outcome.failed,
                    first_error: outcome.first_error,
                },
                Err(e) => AppEvent::BackgroundError(format!("provisioning failed: {e:#}")),
            };
            let _ = tx.send(event);
        });
    }

    fn spawn_create_run(
        &mut self,
        preset: Preset,
        asset_rids: Vec<AssetRid>,
        cutover: bool,
        debug: bool,
    ) {
        let api = self.api.clone();
        let common_label = self.fleet.common_label.clone();
        let tx = self.events_tx.clone();
        let now_unix = unix_now();
        self.handle.spawn(async move {
            let result = api
                .create_run(&preset, &common_label, asset_rids, now_unix, debug)
                .await;
            let event = match result {
                Ok(info) if cutover => AppEvent::CutoverComplete(info),
                Ok(info) => AppEvent::RunCreated(info),
                Err(e) => AppEvent::BackgroundError(format!("run creation failed: {e:#}")),
            };
            let _ = tx.send(event);
        });
    }

    fn start_cutover(&mut self) {
        let (run, preset, asset_rids, debug) = {
            let Some(session) = &mut self.session else {
                return;
            };
            if session.cutover_pending {
                return;
            }
            let Some(run) = session.run.clone() else {
                return;
            };
            session.cutover_pending = true;
            (
                run,
                session.preset.clone(),
                session.asset_rids.clone(),
                session.debug,
            )
        };
        self.log_event(format!("run cutover: ending '{}'", run.title));

        let api = self.api.clone();
        let tx = self.events_tx.clone();
        let end_unix = unix_now();
        self.handle.spawn(async move {
            if let Err(e) = api.end_run(&run.rid, end_unix).await {
                let _ = tx.send(AppEvent::BackgroundError(format!(
                    "cutover: failed to end previous run: {e:#}"
                )));
            }
        });
        self.spawn_create_run(preset, asset_rids, true, debug);
    }

    fn start_stop(&mut self, quit_after: bool) {
        if self.session.is_none() {
            self.should_exit = quit_after;
            return;
        }
        // A cutover has ended the old run and is about to create a new one;
        // stopping now would leave that new run dangling open.
        if self.session.as_ref().is_some_and(|s| s.cutover_pending) {
            self.status = Some((
                "cutover in progress — wait for it to finish before stopping".to_string(),
                true,
            ));
            return;
        }
        self.screen = Screen::Stopping;
        self.quit_after_stop = quit_after;
        self.log_event("stream stopping".to_string());
        let (engine, run) = {
            let session = self.session.as_mut().unwrap();
            (session.engine.take(), session.run.take())
        };

        // Stop the engine off-thread: the final stream flush can block.
        if let Some(engine) = engine {
            self.waiting_engine = true;
            let tx = self.events_tx.clone();
            std::thread::spawn(move || {
                engine.stop();
                let _ = tx.send(AppEvent::EngineStopped);
            });
        }

        if let Some(run) = run {
            self.waiting_run_end = true;
            let api = self.api.clone();
            let tx = self.events_tx.clone();
            let end_unix = unix_now();
            self.handle.spawn(async move {
                if let Err(e) = api.end_run(&run.rid, end_unix).await {
                    let _ = tx.send(AppEvent::BackgroundError(format!(
                        "failed to end run: {e:#}"
                    )));
                }
                let _ = tx.send(AppEvent::RunEnded);
            });
        }

        self.finish_stop_if_done();
    }

    fn finish_stop_if_done(&mut self) {
        if self.screen != Screen::Stopping || self.waiting_engine || self.waiting_run_end {
            return;
        }
        if let Some(session) = &mut self.session {
            if let Some(log_tx) = session.log_tx.take() {
                let _ = log_tx.send(LogMsg::Shutdown);
            }
            if let Some(handle) = session.log_thread.take() {
                let _ = handle.join();
            }
        }
        self.session = None;
        if self.quit_after_stop {
            self.should_exit = true;
        } else {
            self.status = Some(("stream stopped, run ended".to_string(), false));
            self.screen = Screen::Select;
        }
    }

    // ---- events ----

    fn on_event(&mut self, event: AppEvent) {
        match event {
            AppEvent::ProvisionProgress(p) => self.provision = Some(p),
            AppEvent::ProvisionDone {
                sats,
                asset_rids,
                existing,
                created,
                failed,
                first_error,
            } => self.on_provision_done(sats, asset_rids, existing, created, failed, first_error),
            AppEvent::RunCreated(info) => self.on_run_created(info),
            AppEvent::CutoverComplete(info) => {
                if let Some(session) = &mut self.session {
                    session.cutover_pending = false;
                    session.run = Some(info.clone());
                }
                self.status = Some((format!("cut over to new run '{}'", info.title), false));
                self.log_event(format!("run cutover: started '{}'", info.title));
            }
            AppEvent::RunEnded => {
                self.waiting_run_end = false;
                self.finish_stop_if_done();
            }
            AppEvent::EngineStopped => {
                self.waiting_engine = false;
                self.finish_stop_if_done();
            }
            AppEvent::BackgroundError(message) => {
                let truncated: String = message.chars().take(300).collect();
                self.status = Some((truncated, true));
                if self.screen == Screen::Provisioning {
                    self.screen = Screen::Select;
                }
                if let Some(session) = &mut self.session {
                    session.cutover_pending = false;
                }
            }
        }
    }

    fn on_provision_done(
        &mut self,
        sats: Vec<SatelliteConfig>,
        asset_rids: Vec<AssetRid>,
        existing: usize,
        created: usize,
        failed: usize,
        first_error: Option<String>,
    ) {
        let Some(preset) = self.provision_preset.clone() else {
            return;
        };
        if failed > 0 {
            let detail = first_error.unwrap_or_default();
            self.status = Some((
                format!(
                    "provisioning: {failed} of {} asset creations failed (run again to retry). First error: {detail}",
                    failed + created
                ),
                true,
            ));
            self.screen = Screen::Select;
            return;
        }
        if self.provision_only {
            self.status = Some((
                format!(
                    "batch '{}' ready: {} assets ({created} created, {existing} already existed)",
                    preset.name,
                    asset_rids.len()
                ),
                false,
            ));
            self.screen = Screen::Select;
            return;
        }

        self.status = Some((
            format!("assets ready ({created} created, {existing} existing); creating run…"),
            false,
        ));
        self.session = Some(Session {
            preset: preset.clone(),
            debug: self.provision_debug,
            sats,
            asset_rids: asset_rids.clone(),
            run: None,
            engine: None,
            stats: Arc::new(EngineStats::default()),
            stream_started: Instant::now(),
            samples: VecDeque::new(),
            log_tx: None,
            log_thread: None,
            cutover_pending: false,
        });
        let debug = self.provision_debug;
        self.spawn_create_run(preset, asset_rids, false, debug);
    }

    fn on_run_created(&mut self, info: RunInfo) {
        let env = self.env.clone();
        let Some(session) = &mut self.session else {
            return;
        };
        session.run = Some(info.clone());

        // Start the streaming engine now that the run window is open.
        let stats = session.stats.clone();
        let stream = crate::nominal::build_stream(
            &env,
            self.handle.clone(),
            Arc::new(StatsListener(stats.clone())),
        );
        match stream {
            Ok(stream) => {
                session.engine = Some(Engine::start(
                    stream,
                    session.sats.clone(),
                    self.fleet.ground_station.clone(),
                    session.preset.rate_hz,
                    stats.clone(),
                ));
                session.stream_started = Instant::now();
                session.samples.clear();

                // log.system writer thread: periodic stats + lifecycle events.
                let (log_tx, log_rx) = channel();
                let writer = LogWriter::new(&env);
                let batch = session.preset.name.clone();
                let sat_count = session.preset.count;
                session.log_thread = Some(std::thread::spawn(move || {
                    log_loop(writer, log_rx, stats, batch, sat_count);
                }));
                session.log_tx = Some(log_tx);

                self.screen = Screen::Streaming;
                self.status = None;
                self.log_event(format!("run started '{}' ({})", info.title, rid_str(&info)));
            }
            Err(e) => {
                self.status = Some((format!("failed to start stream: {e:#}"), true));
                self.screen = Screen::Select;
                self.session = None;
            }
        }
    }

    fn log_event(&self, message: String) {
        if let Some(session) = &self.session {
            if let Some(log_tx) = &session.log_tx {
                let _ = log_tx.send(LogMsg::Event(message));
            }
        }
    }

    // ---- stats sampling ----

    fn sample_stats(&mut self) {
        if let Some(session) = &mut self.session {
            let now = Instant::now();
            let sent = session.stats.sent.load(Ordering::Relaxed);
            session.samples.push_back((now, sent));
            while let Some((t, _)) = session.samples.front() {
                if now.duration_since(*t) > Duration::from_secs(61) {
                    session.samples.pop_front();
                } else {
                    break;
                }
            }
        }
    }

    // ---- rendering ----

    fn render(&mut self, frame: &mut Frame) {
        let [body, status_area] =
            Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(frame.area());

        match self.screen {
            Screen::Select => self.render_select(frame, body),
            Screen::Provisioning => self.render_provisioning(frame, body),
            Screen::Streaming | Screen::Stopping => self.render_streaming(frame, body),
        }

        if let Some((message, is_error)) = &self.status {
            let style = if *is_error {
                Style::default().fg(Color::Red)
            } else {
                Style::default().fg(Color::Green)
            };
            frame.render_widget(
                Paragraph::new(message.as_str()).style(style),
                status_area,
            );
        }
    }

    fn render_select(&mut self, frame: &mut Frame, area: Rect) {
        let [list_area, help_area] =
            Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(area);

        let items: Vec<ListItem> = self
            .fleet
            .presets
            .iter()
            .map(|p| {
                ListItem::new(format!(
                    "{:<14} {:>6} sats @ {:>5.1} Hz  ≈ {:>9} pts/s   label: {}",
                    p.name,
                    p.count,
                    p.rate_hz,
                    group_thousands(p.points_per_sec() as u64),
                    p.label,
                ))
            })
            .collect();
        let title = if self.debug_rate {
            " sat-fleet — select batch preset [DEBUG: 1 Hz] "
        } else {
            " sat-fleet — select batch preset "
        };
        let list = List::new(items)
            .block(Block::default().borders(Borders::ALL).title(title))
            .highlight_style(
                Style::default()
                    .bg(Color::Blue)
                    .fg(Color::White)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("▶ ");
        frame.render_stateful_widget(list, list_area, &mut self.list_state);

        frame.render_widget(
            Paragraph::new(" ↑/↓ select · enter: provision + stream · p: provision assets only · d: toggle debug 1 Hz · q: quit")
                .style(Style::default().fg(Color::DarkGray)),
            help_area,
        );
    }

    fn render_provisioning(&self, frame: &mut Frame, area: Rect) {
        let preset_name = self
            .provision_preset
            .as_ref()
            .map(|p| p.name.clone())
            .unwrap_or_default();
        let block = Block::default()
            .borders(Borders::ALL)
            .title(format!(" provisioning assets — {preset_name} "));
        let inner = block.inner(area);
        frame.render_widget(block, area);

        let [text_area, gauge_area] =
            Layout::vertical([Constraint::Length(3), Constraint::Length(3)]).areas(inner);

        let (label, ratio) = match &self.provision {
            Some(p) => {
                let done = p.existing + p.created + p.failed;
                (
                    format!(
                        "total {} · existing {} · created {} · failed {}",
                        p.total, p.existing, p.created, p.failed
                    ),
                    if p.total == 0 {
                        1.0
                    } else {
                        done as f64 / p.total as f64
                    },
                )
            }
            None => ("searching for existing batch assets…".to_string(), 0.0),
        };

        frame.render_widget(Paragraph::new(label), text_area);
        frame.render_widget(
            Gauge::default()
                .gauge_style(Style::default().fg(Color::Blue))
                .ratio(ratio.clamp(0.0, 1.0)),
            gauge_area,
        );
    }

    fn render_streaming(&self, frame: &mut Frame, area: Rect) {
        let Some(session) = &self.session else {
            return;
        };
        let [config_area, stats_area, help_area] = Layout::vertical([
            Constraint::Length(10),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .areas(area);

        // --- config panel ---
        let run_title = session
            .run
            .as_ref()
            .map(|r| {
                let started = chrono::DateTime::from_timestamp(r.start_unix, 0)
                    .map(|t| {
                        t.with_timezone(&chrono::Local)
                            .format("%H:%M:%S")
                            .to_string()
                    })
                    .unwrap_or_default();
                format!("{} (started {})", r.title, started)
            })
            .unwrap_or_else(|| "—".to_string());
        let run_rid = session
            .run
            .as_ref()
            .map(rid_str)
            .unwrap_or_else(|| "—".to_string());
        let batch_label = if session.debug {
            format!("{} [debug: rate forced to 1 Hz]", session.preset.name)
        } else {
            session.preset.name.clone()
        };
        let config_lines = vec![
            kv_line("batch", &batch_label),
            kv_line(
                "satellites",
                &format!(
                    "{} @ {} Hz · {} channels · ≈{} pts/s target",
                    session.preset.count,
                    session.preset.rate_hz,
                    CHANNELS.len(),
                    group_thousands(session.preset.points_per_sec() as u64)
                ),
            ),
            kv_line(
                "labels",
                &format!("{} · {}", self.fleet.common_label, session.preset.label),
            ),
            kv_line("dataset", &self.env.dataset),
            kv_line(
                "ground station",
                &self
                    .fleet
                    .ground_station
                    .as_ref()
                    .map(|g| g.name.clone())
                    .unwrap_or_else(|| "none".to_string()),
            ),
            kv_line("run", &run_title),
            kv_line("run rid", &run_rid),
            kv_line(
                "run status",
                if session.cutover_pending {
                    "cutting over to a new run…"
                } else if self.screen == Screen::Stopping {
                    "stopping…"
                } else {
                    "live"
                },
            ),
        ];
        frame.render_widget(
            Paragraph::new(config_lines)
                .block(Block::default().borders(Borders::ALL).title(" session ")),
            config_area,
        );

        // --- stats panel ---
        let stats = &session.stats;
        let enqueued = stats.enqueued.load(Ordering::Relaxed);
        let sent = stats.sent.load(Ordering::Relaxed);
        let failed_points = stats.failed_points.load(Ordering::Relaxed);
        let failed_requests = stats.failed_requests.load(Ordering::Relaxed);
        let ticks = stats.ticks.load(Ordering::Relaxed);
        let skipped = stats.skipped_ticks.load(Ordering::Relaxed);
        let backlog = enqueued.saturating_sub(sent + failed_points);
        let uptime = session.stream_started.elapsed();

        let rate_5s = window_rate(&session.samples, Duration::from_secs(5));
        let rate_60s = window_rate(&session.samples, Duration::from_secs(60));

        let stats_lines = vec![
            kv_line("uptime", &format_duration(uptime)),
            kv_line("points sent", &group_thousands(sent)),
            kv_line("points enqueued", &group_thousands(enqueued)),
            kv_line(
                "send rate",
                &format!(
                    "{}/s (5s avg) · {}/min (60s avg)",
                    group_thousands(rate_5s as u64),
                    group_thousands((rate_60s * 60.0) as u64)
                ),
            ),
            kv_line("backlog (buffered)", &group_thousands(backlog)),
            kv_line(
                "failures",
                &format!(
                    "{} points across {} requests · {} log writes",
                    group_thousands(failed_points),
                    failed_requests,
                    stats.log_failures.load(Ordering::Relaxed),
                ),
            ),
            kv_line(
                "ticks",
                &format!("{} ({} skipped)", group_thousands(ticks), skipped),
            ),
        ];
        frame.render_widget(
            Paragraph::new(stats_lines)
                .block(Block::default().borders(Borders::ALL).title(" this session ")),
            stats_area,
        );

        frame.render_widget(
            Paragraph::new(" n: cut over to new run (stream continues) · s: stop stream + end run · q: stop + quit")
                .style(Style::default().fg(Color::DarkGray)),
            help_area,
        );
    }
}

// ---- log.system writer thread ----

fn log_loop(
    writer: LogWriter,
    rx: Receiver<LogMsg>,
    stats: Arc<EngineStats>,
    batch: String,
    sat_count: u32,
) {
    let mut last_sent: u64 = 0;
    let mut last_time = Instant::now();
    let track = |result: anyhow::Result<()>| {
        if result.is_err() {
            stats.log_failures.fetch_add(1, Ordering::Relaxed);
        }
    };
    loop {
        match rx.recv_timeout(LOG_STATS_INTERVAL) {
            Ok(LogMsg::Event(message)) => {
                track(writer.write(&message, &[("batch", batch.clone())]));
            }
            Ok(LogMsg::Shutdown) | Err(RecvTimeoutError::Disconnected) => {
                let sent = stats.sent.load(Ordering::Relaxed);
                track(writer.write(
                    "stream stopped",
                    &[
                        ("batch", batch.clone()),
                        ("points_sent", sent.to_string()),
                    ],
                ));
                return;
            }
            Err(RecvTimeoutError::Timeout) => {
                let sent = stats.sent.load(Ordering::Relaxed);
                let enqueued = stats.enqueued.load(Ordering::Relaxed);
                let failed = stats.failed_points.load(Ordering::Relaxed);
                let elapsed = last_time.elapsed().as_secs_f64();
                let rate = if elapsed > 0.0 {
                    (sent.saturating_sub(last_sent)) as f64 / elapsed
                } else {
                    0.0
                };
                last_sent = sent;
                last_time = Instant::now();
                let message = format!(
                    "stats batch={batch} sats={sat_count} sent={sent} enqueued={enqueued} failed={failed} rate={rate:.0}pts/s"
                );
                track(writer.write(
                    &message,
                    &[
                        ("batch", batch.clone()),
                        ("sats", sat_count.to_string()),
                        ("points_sent", sent.to_string()),
                        ("points_enqueued", enqueued.to_string()),
                        ("points_failed", failed.to_string()),
                        ("rate_pts_per_sec", format!("{rate:.0}")),
                    ],
                ));
            }
        }
    }
}

// ---- helpers ----

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

fn rid_str(info: &RunInfo) -> String {
    info.rid.0.to_string()
}

fn kv_line<'a>(key: &'a str, value: &str) -> Line<'a> {
    Line::from(vec![
        Span::styled(
            format!("{key:>18}  "),
            Style::default().fg(Color::DarkGray),
        ),
        Span::raw(value.to_string()),
    ])
}

fn window_rate(samples: &VecDeque<(Instant, u64)>, window: Duration) -> f64 {
    let Some((newest_t, newest_v)) = samples.back() else {
        return 0.0;
    };
    let target = *newest_t - window;
    let oldest_in_window = samples
        .iter()
        .find(|(t, _)| *t >= target)
        .or_else(|| samples.front());
    let Some((old_t, old_v)) = oldest_in_window else {
        return 0.0;
    };
    let dt = newest_t.duration_since(*old_t).as_secs_f64();
    if dt <= 0.0 {
        return 0.0;
    }
    newest_v.saturating_sub(*old_v) as f64 / dt
}

fn group_thousands(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    format!("{:02}:{:02}:{:02}", secs / 3600, (secs % 3600) / 60, secs % 60)
}
