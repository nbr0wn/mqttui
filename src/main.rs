use anyhow::{Context, Result};
use clap::Parser;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
    Frame, Terminal,
};
use regex::Regex;
use rumqttc::{AsyncClient, Event as MqttEvent, MqttOptions, Packet, QoS};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

/// A timestamped message
#[derive(Debug, Clone)]
struct TimestampedMessage {
    timestamp_ms: u128,
    payload: String,
}

impl TimestampedMessage {
    fn format_timestamp(&self) -> String {
        let total_ms = self.timestamp_ms;
        let hours = (total_ms / 3_600_000) % 24;
        let minutes = (total_ms / 60_000) % 60;
        let seconds = (total_ms / 1_000) % 60;
        let millis = total_ms % 1_000;
        format!("{:02}:{:02}:{:02}.{:03}", hours, minutes, seconds, millis)
    }
}

/// MQTT Topic Tree Viewer
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// MQTT broker hostname or hostname:port (default port: 1883)
    #[arg(value_name = "HOST[:PORT]")]
    broker: String,

    /// Client ID
    #[arg(short, long, default_value = "mqttui-client")]
    client_id: String,

    /// Username for authentication
    #[arg(short, long)]
    username: Option<String>,

    /// Password for authentication
    #[arg(short = 'P', long)]
    password: Option<String>,

    /// Topic to subscribe to (supports wildcards)
    #[arg(short, long, default_value = "#")]
    topic: String,
}

impl Args {
    fn parse_broker(&self) -> (String, u16) {
        if let Some((host, port_str)) = self.broker.rsplit_once(':') {
            if let Ok(port) = port_str.parse::<u16>() {
                return (host.to_string(), port);
            }
        }
        (self.broker.clone(), 1883)
    }
}

/// A node in the topic tree
#[derive(Debug, Clone)]
struct TopicNode {
    name: String,
    full_path: String,
    children: BTreeMap<String, TopicNode>,
    has_messages: bool,
    last_payload: Option<String>,
}

impl TopicNode {
    fn new(name: String, full_path: String) -> Self {
        Self {
            name,
            full_path,
            children: BTreeMap::new(),
            has_messages: false,
            last_payload: None,
        }
    }

    fn insert_topic(&mut self, topic: &str, payload: Option<String>) {
        let parts: Vec<&str> = topic.split('/').collect();
        self.insert_parts(&parts, topic, payload);
    }

    fn insert_parts(&mut self, parts: &[&str], full_topic: &str, payload: Option<String>) {
        if parts.is_empty() {
            return;
        }

        let part = parts[0];
        let remaining = &parts[1..];

        let child_path = if self.full_path.is_empty() {
            part.to_string()
        } else {
            format!("{}/{}", self.full_path, part)
        };

        let child = self
            .children
            .entry(part.to_string())
            .or_insert_with(|| TopicNode::new(part.to_string(), child_path));

        if remaining.is_empty() {
            child.has_messages = true;
            child.last_payload = payload;
        } else {
            child.insert_parts(remaining, full_topic, payload);
        }
    }

    fn flatten(&self, depth: usize, expanded_set: &HashSet<String>) -> Vec<FlatNode> {
        let mut result = Vec::new();

        for (_, child) in &self.children {
            let is_expanded = expanded_set.contains(&child.full_path);
            let has_children = !child.children.is_empty();

            result.push(FlatNode {
                name: child.name.clone(),
                full_path: child.full_path.clone(),
                depth,
                has_children,
                expanded: is_expanded,
                has_messages: child.has_messages,
            });

            if is_expanded && has_children {
                result.extend(child.flatten(depth + 1, expanded_set));
            }
        }

        result
    }
}

#[derive(Debug, Clone)]
struct FlatNode {
    name: String,
    full_path: String,
    depth: usize,
    has_children: bool,
    expanded: bool,
    has_messages: bool,
}

#[derive(Debug, Clone, PartialEq)]
enum InputMode {
    Normal,
    FilterInput,
}

#[derive(Debug, Clone, PartialEq)]
enum FocusedPane {
    Topics,
    Messages,
}

struct App {
    input_mode: InputMode,
    focused_pane: FocusedPane,
    filter_input: String,
    filter_regex: Option<Regex>,
    filter_active: bool,
    topic_tree: TopicNode,
    expanded_paths: HashSet<String>,
    flat_nodes: Vec<FlatNode>,
    list_state: ListState,
    message_scroll: u16,
    should_quit: bool,
    status_message: String,
    message_count: usize,
    message_history: HashMap<String, Vec<TimestampedMessage>>,
    start_time: Instant,
}

impl App {
    fn new() -> Self {
        let mut app = Self {
            input_mode: InputMode::Normal,
            focused_pane: FocusedPane::Topics,
            filter_input: String::new(),
            filter_regex: None,
            filter_active: false,
            topic_tree: TopicNode::new(String::new(), String::new()),
            expanded_paths: HashSet::new(),
            flat_nodes: Vec::new(),
            list_state: ListState::default(),
            message_scroll: 0,
            should_quit: false,
            status_message: String::from("Connecting..."),
            message_count: 0,
            message_history: HashMap::new(),
            start_time: Instant::now(),
        };
        app.list_state.select(Some(0));
        app
    }

    fn add_topic(&mut self, topic: &str, payload: Option<String>) {
        self.topic_tree.insert_topic(topic, payload.clone());
        self.message_count += 1;

        // Store timestamped message in history
        if let Some(p) = payload {
            let timestamp_ms = self.start_time.elapsed().as_millis();
            let msg = TimestampedMessage {
                timestamp_ms,
                payload: p,
            };
            self.message_history
                .entry(topic.to_string())
                .or_insert_with(Vec::new)
                .push(msg);
        }

        // Auto-expand new paths
        let parts: Vec<&str> = topic.split('/').collect();
        let mut path = String::new();
        for part in parts {
            if !path.is_empty() {
                path.push('/');
            }
            path.push_str(part);
            self.expanded_paths.insert(path.clone());
        }

        self.refresh_flat_nodes();
    }

    fn refresh_flat_nodes(&mut self) {
        self.flat_nodes = self.topic_tree.flatten(0, &self.expanded_paths);
    }

    fn toggle_expand(&mut self) {
        if let Some(selected) = self.list_state.selected() {
            if let Some(node) = self.flat_nodes.get(selected) {
                if node.has_children {
                    let path = node.full_path.clone();
                    if self.expanded_paths.contains(&path) {
                        self.expanded_paths.remove(&path);
                    } else {
                        self.expanded_paths.insert(path);
                    }
                    self.refresh_flat_nodes();
                }
            }
        }
    }

    fn move_up(&mut self) {
        if let Some(selected) = self.list_state.selected() {
            if selected > 0 {
                self.list_state.select(Some(selected - 1));
            }
        }
    }

    fn move_down(&mut self) {
        if let Some(selected) = self.list_state.selected() {
            if selected < self.flat_nodes.len().saturating_sub(1) {
                self.list_state.select(Some(selected + 1));
            }
        }
    }

    fn get_selected_topic(&self) -> Option<String> {
        if let Some(selected) = self.list_state.selected() {
            if let Some(node) = self.flat_nodes.get(selected) {
                return Some(node.full_path.clone());
            }
        }
        None
    }

    fn get_selected_message_history(&self) -> Option<&Vec<TimestampedMessage>> {
        if let Some(topic) = self.get_selected_topic() {
            return self.message_history.get(&topic);
        }
        None
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let (host, port) = args.parse_broker();

    let app = Arc::new(Mutex::new(App::new()));
    let (tx, mut rx) = mpsc::channel::<(String, String)>(100);

    // Set up MQTT connection
    let mut mqtt_options = MqttOptions::new(&args.client_id, &host, port);
    mqtt_options.set_keep_alive(Duration::from_secs(30));

    if let (Some(username), Some(password)) = (&args.username, &args.password) {
        mqtt_options.set_credentials(username, password);
    }

    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

    // Subscribe to the topic
    let topic = args.topic.clone();
    client
        .subscribe(&topic, QoS::AtMostOnce)
        .await
        .context("Failed to subscribe")?;

    // Spawn MQTT event loop
    let app_clone = Arc::clone(&app);
    let mqtt_handle = tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(notification) => {
                    if let MqttEvent::Incoming(Packet::Publish(publish)) = notification {
                        let topic = publish.topic.clone();
                        let payload =
                            String::from_utf8_lossy(&publish.payload).to_string();
                        let _ = tx.send((topic, payload)).await;
                    }
                }
                Err(e) => {
                    {
                        let mut app = app_clone.lock().unwrap();
                        app.status_message = format!("MQTT Error: {}", e);
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });

    // Set up terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    {
        let mut app_guard = app.lock().unwrap();
        app_guard.status_message = format!("Connected to {}:{} | Subscribed to '{}'", host, port, args.topic);
    }

    // Main loop
    let result = run_app(&mut terminal, app, &mut rx).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    mqtt_handle.abort();

    result
}

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: Arc<Mutex<App>>,
    rx: &mut mpsc::Receiver<(String, String)>,
) -> Result<()> {
    loop {
        // Check for new MQTT messages
        while let Ok((topic, payload)) = rx.try_recv() {
            let mut app = app.lock().unwrap();
            app.add_topic(&topic, Some(payload));
        }

        // Draw UI
        {
            let app = app.lock().unwrap();
            terminal.draw(|f| ui(f, &app))?;
        }

        // Handle input with timeout
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    let mut app = app.lock().unwrap();
                    match app.input_mode {
                        InputMode::Normal => match app.focused_pane {
                            FocusedPane::Topics => match key.code {
                                KeyCode::Char('q') | KeyCode::Esc => {
                                    app.should_quit = true;
                                }
                                KeyCode::Char('/') => {
                                    app.input_mode = InputMode::FilterInput;
                                    // Keep existing filter for editing, disable active filtering while editing
                                    app.filter_active = false;
                                }
                                KeyCode::Up | KeyCode::Char('k') => app.move_up(),
                                KeyCode::Down | KeyCode::Char('j') => app.move_down(),
                                KeyCode::Enter | KeyCode::Char(' ') => app.toggle_expand(),
                                KeyCode::Left | KeyCode::Char('h') => {
                                    // Collapse current node
                                    if let Some(selected) = app.list_state.selected() {
                                        if let Some(node) = app.flat_nodes.get(selected) {
                                            if node.expanded && node.has_children {
                                                app.toggle_expand();
                                            }
                                        }
                                    }
                                }
                                KeyCode::Right | KeyCode::Char('l') => {
                                    // Expand current node, or if fully expanded/leaf, move to messages pane
                                    if let Some(selected) = app.list_state.selected() {
                                        if let Some(node) = app.flat_nodes.get(selected) {
                                            if !node.expanded && node.has_children {
                                                app.toggle_expand();
                                            } else {
                                                // Already expanded or no children - move to messages pane
                                                app.focused_pane = FocusedPane::Messages;
                                                app.message_scroll = 0;
                                            }
                                        }
                                    }
                                }
                                _ => {}
                            },
                            FocusedPane::Messages => match key.code {
                                KeyCode::Char('q') => {
                                    app.should_quit = true;
                                }
                                KeyCode::Esc | KeyCode::Left | KeyCode::Char('h') => {
                                    // Return to topics pane
                                    app.focused_pane = FocusedPane::Topics;
                                }
                                KeyCode::Char('/') => {
                                    app.input_mode = InputMode::FilterInput;
                                    // Keep existing filter for editing, disable active filtering while editing
                                    app.filter_active = false;
                                }
                                KeyCode::Up | KeyCode::Char('k') => {
                                    app.message_scroll = app.message_scroll.saturating_sub(1);
                                }
                                KeyCode::Down | KeyCode::Char('j') => {
                                    app.message_scroll = app.message_scroll.saturating_add(1);
                                }
                                KeyCode::PageUp => {
                                    app.message_scroll = app.message_scroll.saturating_sub(10);
                                }
                                KeyCode::PageDown => {
                                    app.message_scroll = app.message_scroll.saturating_add(10);
                                }
                                KeyCode::Home => {
                                    app.message_scroll = 0;
                                }
                                KeyCode::End => {
                                    // Will be clamped in UI rendering
                                    app.message_scroll = u16::MAX;
                                }
                                _ => {}
                            },
                        },
                        InputMode::FilterInput => match key.code {
                            KeyCode::Enter => {
                                // Apply filter and return to normal mode
                                app.filter_active = app.filter_regex.is_some();
                                app.input_mode = InputMode::Normal;
                            }
                            KeyCode::Esc => {
                                // Cancel filter input
                                app.input_mode = InputMode::Normal;
                                app.filter_input.clear();
                                app.filter_regex = None;
                                app.filter_active = false;
                            }
                            KeyCode::Backspace => {
                                app.filter_input.pop();
                                // Update regex as user types
                                app.filter_regex = if app.filter_input.is_empty() {
                                    None
                                } else {
                                    Regex::new(&app.filter_input).ok()
                                };
                            }
                            KeyCode::Char(c) => {
                                app.filter_input.push(c);
                                // Update regex as user types (for live highlighting)
                                app.filter_regex = Regex::new(&app.filter_input).ok();
                            }
                            _ => {}
                        },
                    }
                }
            }
        }

        let should_quit = app.lock().unwrap().should_quit;
        if should_quit {
            break;
        }
    }

    Ok(())
}

fn ui(f: &mut Frame, app: &App) {
    // Main layout: content area on top, status bar at bottom
    let main_chunks = Layout::vertical([
        Constraint::Min(0),
        Constraint::Length(3),
    ])
    .split(f.area());

    // Split content area horizontally: topics (1/3) | payload (2/3)
    let content_chunks = Layout::horizontal([
        Constraint::Ratio(1, 3),
        Constraint::Ratio(2, 3),
    ])
    .split(main_chunks[0]);

    // Topic tree view
    let items: Vec<ListItem> = app
        .flat_nodes
        .iter()
        .map(|node| {
            let indent = "  ".repeat(node.depth);
            let prefix = if node.has_children {
                if node.expanded {
                    "▼ "
                } else {
                    "▶ "
                }
            } else {
                "  "
            };

            let style = if node.has_messages {
                Style::default().fg(Color::Green)
            } else {
                Style::default().fg(Color::White)
            };

            let line = Line::from(vec![
                Span::raw(indent),
                Span::styled(prefix, Style::default().fg(Color::Yellow)),
                Span::styled(&node.name, style),
            ]);

            ListItem::new(line)
        })
        .collect();

    let topics_border_color = if app.focused_pane == FocusedPane::Topics {
        Color::Cyan
    } else {
        Color::DarkGray
    };
    let tree_block = Block::default()
        .title(format!(" Topics ({} messages) ", app.message_count))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(topics_border_color));

    let tree = List::new(items)
        .block(tree_block)
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("→ ");

    f.render_stateful_widget(tree, content_chunks[0], &mut app.list_state.clone());

    // Payload view - show message history with timestamps, highlighting, and filtering
    // Build as a single string so wrapping works naturally with timestamp on same line as message
    let payload_text: String = if let Some(history) = app.get_selected_message_history() {
        history
            .iter()
            .filter_map(|msg| {
                // Replace newlines in payload to keep timestamp and message on same logical line
                // Replace spaces with non-breaking spaces to force character-level wrapping
                let clean_payload = msg.payload.replace('\n', "\\n").replace('\r', "\\r").replace(' ', "\u{00A0}");
                let line_text = format!("[{}]\u{00A0}{}", msg.format_timestamp(), clean_payload);

                // If filter is active (Enter was pressed), only show matching lines
                if app.filter_active {
                    if let Some(ref regex) = app.filter_regex {
                        if !regex.is_match(&line_text) {
                            return None;
                        }
                    }
                }

                Some(line_text)
            })
            .collect::<Vec<_>>()
            .join("\n")
    } else if let Some(topic) = app.get_selected_topic() {
        format!("Topic: {} (no messages)", topic)
    } else {
        String::from("Select a topic to view messages")
    };

    // Apply regex highlighting if active
    let payload_content: ratatui::text::Text = if let Some(ref regex) = app.filter_regex {
        let mut lines = Vec::new();
        for line_str in payload_text.lines() {
            let mut spans = Vec::new();
            let mut last_end = 0;

            for mat in regex.find_iter(line_str) {
                // Add text before the match
                if mat.start() > last_end {
                    spans.push(Span::styled(
                        line_str[last_end..mat.start()].to_string(),
                        Style::default().fg(Color::White),
                    ));
                }
                // Add the highlighted match
                spans.push(Span::styled(
                    mat.as_str().to_string(),
                    Style::default().fg(Color::Black).bg(Color::Yellow),
                ));
                last_end = mat.end();
            }
            // Add remaining text after last match
            if last_end < line_str.len() {
                spans.push(Span::styled(
                    line_str[last_end..].to_string(),
                    Style::default().fg(Color::White),
                ));
            }

            if spans.is_empty() {
                lines.push(Line::from(Span::styled(line_str.to_string(), Style::default().fg(Color::White))));
            } else {
                lines.push(Line::from(spans));
            }
        }
        ratatui::text::Text::from(lines)
    } else {
        ratatui::text::Text::raw(&payload_text)
    };

    let payload_title = if app.filter_active {
        format!(" Messages (filtered: {}) ", app.filter_input)
    } else {
        " Messages ".to_string()
    };

    let messages_border_color = if app.focused_pane == FocusedPane::Messages {
        Color::Magenta
    } else {
        Color::DarkGray
    };
    let payload_block = Block::default()
        .title(payload_title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(messages_border_color));

    // Clamp scroll to valid range - only allow scrolling if content exceeds visible height
    // Subtract 2 for top and bottom borders
    let visible_height = content_chunks[1].height.saturating_sub(2) as usize;
    let content_lines = payload_content.lines.len();
    let max_scroll = if content_lines > visible_height {
        (content_lines - visible_height) as u16
    } else {
        0
    };
    let scroll = app.message_scroll.min(max_scroll);

    let payload = Paragraph::new(payload_content)
        .block(payload_block)
        .wrap(Wrap { trim: false })
        .scroll((scroll, 0));

    f.render_widget(payload, content_chunks[1]);

    // Status bar
    let (status_title, status_content) = match app.input_mode {
        InputMode::FilterInput => {
            let valid_indicator = if app.filter_input.is_empty() {
                ""
            } else if app.filter_regex.is_some() {
                " ✓"
            } else {
                " ✗ (invalid regex)"
            };
            (
                " Filter ".to_string(),
                format!("/{}{} | Enter: Apply | Esc: Cancel", app.filter_input, valid_indicator),
            )
        }
        InputMode::Normal => (
            " Status ".to_string(),
            format!(
                "{} | ↑↓/jk: Navigate | ←→/hl: Collapse/Expand | /: Filter | q: Quit",
                app.status_message
            ),
        ),
    };

    let status_block = Block::default()
        .title(status_title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Blue));

    let status = Paragraph::new(status_content)
        .block(status_block)
        .style(Style::default().fg(Color::Gray));

    f.render_widget(status, main_chunks[1]);
}
