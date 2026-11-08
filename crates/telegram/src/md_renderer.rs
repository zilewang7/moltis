//! Optional external markdown renderer (home patch).
//!
//! When `MOLTIS_TELEGRAM_MD_RENDERER` holds a command (for example
//! `node /opt/tg-md/render.mjs`), markdown is rendered by that long-lived
//! child process instead of the built-in converter. Protocol: one JSON object
//! per line on stdin (`{"id","markdown","max_length","streaming"}`), one reply
//! per line on stdout (`{"id","chunks":[{"text","entities"}...]}` or
//! `{"id","error"}`), entities in Bot API shape. Every chunk must already fit
//! Telegram's limits; chunks are sent as text + entities, without parse mode.
//!
//! Any failure (spawn, timeout, bad reply) falls back to the built-in
//! converter, so message delivery never depends on the helper.

use std::{
    io::{BufRead, BufReader, Write},
    process::{Child, ChildStdin, Command, Stdio},
    sync::{Mutex, OnceLock, mpsc},
    thread,
    time::{Duration, Instant},
};

use {
    serde::{Deserialize, Serialize},
    teloxide::types::MessageEntity,
    tracing::{info, warn},
};

use crate::body::TgBody;

const ENV_COMMAND: &str = "MOLTIS_TELEGRAM_MD_RENDERER";
const REPLY_TIMEOUT: Duration = Duration::from_secs(3);
const RESPAWN_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Serialize)]
struct RenderRequest<'a> {
    id: u64,
    markdown: &'a str,
    max_length: usize,
    streaming: bool,
}

#[derive(Deserialize)]
struct RenderedChunk {
    text: String,
    #[serde(default)]
    entities: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct RenderReply {
    id: u64,
    #[serde(default)]
    chunks: Vec<RenderedChunk>,
    #[serde(default)]
    error: Option<String>,
}

impl RenderedChunk {
    /// Entities Telegram types cannot represent (e.g. an unparsable link URL)
    /// are dropped individually; their text stays.
    fn into_body(self) -> TgBody {
        let entities = self
            .entities
            .into_iter()
            .filter_map(|value| serde_json::from_value::<MessageEntity>(value).ok())
            .collect();
        TgBody::Entities {
            text: self.text,
            entities,
        }
    }
}

struct Helper {
    child: Child,
    stdin: ChildStdin,
    replies: mpsc::Receiver<String>,
}

impl Drop for Helper {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Default)]
struct HelperState {
    helper: Option<Helper>,
    next_id: u64,
    last_failure: Option<Instant>,
}

struct Renderer {
    argv: Vec<String>,
    state: Mutex<HelperState>,
}

fn renderer() -> Option<&'static Renderer> {
    static RENDERER: OnceLock<Option<Renderer>> = OnceLock::new();
    RENDERER
        .get_or_init(|| {
            let raw = std::env::var(ENV_COMMAND).ok()?;
            let argv: Vec<String> = raw.split_whitespace().map(str::to_owned).collect();
            if argv.is_empty() {
                return None;
            }
            info!(command = %raw, "telegram markdown: using external renderer");
            Some(Renderer {
                argv,
                state: Mutex::new(HelperState::default()),
            })
        })
        .as_ref()
}

/// Render markdown into message bodies of at most `max_len` visible UTF-16
/// units. Returns `None` when no helper is configured or it failed.
pub(crate) fn render_chunks(markdown: &str, max_len: usize, streaming: bool) -> Option<Vec<TgBody>> {
    let renderer = renderer()?;
    let mut state = renderer
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if state.helper.is_none()
        && state
            .last_failure
            .is_some_and(|at| at.elapsed() < RESPAWN_BACKOFF)
    {
        return None;
    }
    match renderer.request(&mut state, markdown, max_len, streaming) {
        Ok(chunks) => Some(chunks),
        Err(error) => {
            warn!(error = %error, "telegram markdown: external renderer failed, using built-in");
            state.helper = None;
            state.last_failure = Some(Instant::now());
            None
        },
    }
}

impl Renderer {
    fn spawn(&self) -> Result<Helper, String> {
        let (program, args) = self
            .argv
            .split_first()
            .ok_or_else(|| "empty command".to_string())?;
        let mut child = Command::new(program)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .map_err(|error| format!("spawn: {error}"))?;
        let stdin = child.stdin.take().ok_or_else(|| "no stdin".to_string())?;
        let stdout = child.stdout.take().ok_or_else(|| "no stdout".to_string())?;
        let (sender, replies) = mpsc::channel();
        thread::Builder::new()
            .name("tg-md-renderer".into())
            .spawn(move || {
                for line in BufReader::new(stdout).lines() {
                    let Ok(line) = line else { break };
                    if sender.send(line).is_err() {
                        break;
                    }
                }
            })
            .map_err(|error| format!("reader thread: {error}"))?;
        Ok(Helper {
            child,
            stdin,
            replies,
        })
    }

    fn request(
        &self,
        state: &mut HelperState,
        markdown: &str,
        max_len: usize,
        streaming: bool,
    ) -> Result<Vec<TgBody>, String> {
        if state.helper.is_none() {
            state.helper = Some(self.spawn()?);
        }
        state.next_id += 1;
        let id = state.next_id;
        let Some(helper) = state.helper.as_mut() else {
            return Err("helper missing".into());
        };

        let mut line = serde_json::to_string(&RenderRequest {
            id,
            markdown,
            max_length: max_len,
            streaming,
        })
        .map_err(|error| format!("encode: {error}"))?;
        line.push('\n');
        helper
            .stdin
            .write_all(line.as_bytes())
            .and_then(|()| helper.stdin.flush())
            .map_err(|error| format!("write: {error}"))?;

        let deadline = Instant::now() + REPLY_TIMEOUT;
        loop {
            let wait = deadline.saturating_duration_since(Instant::now());
            let raw = helper
                .replies
                .recv_timeout(wait)
                .map_err(|error| format!("reply: {error}"))?;
            let reply: RenderReply =
                serde_json::from_str(&raw).map_err(|error| format!("decode: {error}"))?;
            if reply.id != id {
                // Stale reply from an earlier request; keep waiting for ours.
                continue;
            }
            if let Some(error) = reply.error {
                return Err(format!("helper: {error}"));
            }
            return Ok(reply
                .chunks
                .into_iter()
                .filter(|chunk| !chunk.text.trim().is_empty())
                .map(RenderedChunk::into_body)
                .collect());
        }
    }
}
