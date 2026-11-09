//! Provider failover chain with per-provider circuit breakers.
//!
//! `ProviderChain` wraps a primary `LlmProvider` with a list of fallbacks.
//! When the primary fails with a retryable error (rate limit, auth, server error),
//! it automatically tries the next provider in the chain, skipping any that have
//! their circuit breaker tripped.

use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use {
    async_trait::async_trait,
    tokio_stream::{Stream, StreamExt},
    tracing::warn,
};

#[cfg(feature = "metrics")]
use moltis_metrics::{counter, histogram, labels, llm as llm_metrics};

use crate::model::{
    AgentToolControls, ChatMessage, CompletionResponse, LlmProvider, ProviderAttemptEvent,
    ProviderIdentity, StreamEvent, TrackedStreamEvent,
};

/// How a provider error should be handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderErrorKind {
    /// 429 — rotate to next provider.
    RateLimit,
    /// 401/403 — rotate (bad key or permissions).
    AuthError,
    /// 5xx — rotate to next provider.
    ServerError,
    /// Billing/usage limit exhausted — rotate.
    BillingExhausted,
    /// Context window exceeded — don't rotate, caller should compact.
    ContextWindow,
    /// 400, bad format — don't rotate, it'll fail everywhere.
    InvalidRequest,
    /// Unrecognised error — attempt failover.
    Unknown,
}

impl ProviderErrorKind {
    /// Whether this error kind should trigger failover to the next provider.
    #[must_use]
    pub fn should_failover(self) -> bool {
        matches!(
            self,
            Self::RateLimit
                | Self::AuthError
                | Self::ServerError
                | Self::BillingExhausted
                | Self::Unknown
        )
    }

    /// Whether this error is worth re-trying against the **same** provider
    /// before rotating down the chain.
    ///
    /// A Cloudflare edge in front of an OpenAI-compatible gateway answers 52x
    /// when the origin is slow, restarting or briefly unreachable. Those blips
    /// clear in seconds, so retrying the selected model keeps the user on the
    /// model they chose instead of silently switching to a fallback.
    #[must_use]
    pub fn is_transient(self) -> bool {
        matches!(self, Self::RateLimit | Self::ServerError | Self::Unknown)
    }
}

/// Error patterns for context window overflow (reused from runner.rs).
const CONTEXT_WINDOW_PATTERNS: &[&str] = &[
    "context_length_exceeded",
    "context_window_exceeded",
    "model_context_window_exceeded",
    "max_tokens",
    "too many tokens",
    "request too large",
    "maximum context length",
    "context window",
    "token limit",
    "input too long",
    "input_too_long",
    "content_too_large",
    "request_too_large",
];

/// Classify an error into a `ProviderErrorKind` based on the error message.
#[must_use]
pub fn classify_error(err: &anyhow::Error) -> ProviderErrorKind {
    let msg = err.to_string().to_lowercase();

    // Context window — must check first since "request too large" overlaps.
    if CONTEXT_WINDOW_PATTERNS.iter().any(|p| msg.contains(p)) {
        return ProviderErrorKind::ContextWindow;
    }

    // Rate limiting.
    if msg.contains("429")
        || msg.contains("rate limit")
        || msg.contains("rate_limit")
        || msg.contains("too many requests")
    {
        return ProviderErrorKind::RateLimit;
    }

    // Auth errors.
    if msg.contains("401")
        || msg.contains("403")
        || msg.contains("unauthorized")
        || msg.contains("forbidden")
        || msg.contains("invalid api key")
        || msg.contains("invalid_api_key")
        || msg.contains("authentication")
    {
        return ProviderErrorKind::AuthError;
    }

    // Billing / quota exhaustion.
    if msg.contains("billing")
        || msg.contains("quota")
        || msg.contains("insufficient_quota")
        || msg.contains("usage limit")
        || msg.contains("credit")
    {
        return ProviderErrorKind::BillingExhausted;
    }

    // Server errors. 52x is Cloudflare: 520–525 mean the edge could not get a
    // usable answer from the origin (524 in particular is the 100 s upstream
    // timeout). They are transient — the origin usually answers a retry.
    if msg.contains("500")
        || msg.contains("502")
        || msg.contains("503")
        || msg.contains("504")
        || msg.contains("520")
        || msg.contains("521")
        || msg.contains("522")
        || msg.contains("523")
        || msg.contains("524")
        || msg.contains("525")
        || msg.contains("internal server error")
        || msg.contains("bad gateway")
        || msg.contains("service unavailable")
        || msg.contains("overloaded")
        || msg.contains("timeout")
        || msg.contains("timed out")
        || msg.contains("connection reset")
        || msg.contains("connection closed")
        || msg.contains("connection aborted")
        || msg.contains("broken pipe")
        || msg.contains("unexpected eof")
        || msg.contains("error sending request")
        || msg.contains("no data for")
    {
        return ProviderErrorKind::ServerError;
    }

    // Invalid request (400-level, non-auth, non-rate-limit).
    if msg.contains("400") || msg.contains("bad request") || msg.contains("invalid_request") {
        return ProviderErrorKind::InvalidRequest;
    }

    ProviderErrorKind::Unknown
}

// ── Circuit breaker (same pattern as embeddings_fallback.rs) ─────────────

// ── Transient-failure retries ────────────────────────────────────────────────

/// How many attempts the same provider entry gets before the chain rotates.
///
/// Override with `MOLTIS_PROVIDER_RETRY_ATTEMPTS` (1 disables retries).
fn transient_retry_attempts() -> usize {
    env_usize("MOLTIS_PROVIDER_RETRY_ATTEMPTS")
        .unwrap_or(3)
        .clamp(1, 6)
}

/// Give up on a provider whose stream has produced nothing for this long.
///
/// A hung upstream otherwise sits silent until the whole agent run hits its
/// wall clock, which loses the turn. 180 s is comfortably longer than the
/// slowest first token on a reasoning model, and far shorter than the run
/// budget. `MOLTIS_PROVIDER_STREAM_IDLE_SECS=0` disables the watchdog.
fn stream_idle_timeout() -> Option<Duration> {
    match env_usize("MOLTIS_PROVIDER_STREAM_IDLE_SECS") {
        Some(0) => None,
        Some(secs) => Some(Duration::from_secs(secs as u64)),
        None => Some(Duration::from_secs(180)),
    }
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.trim().parse().ok()
}

/// Circuit breaker state for a single provider.
struct ProviderState {
    consecutive_failures: AtomicUsize,
    last_failure: Mutex<Option<Instant>>,
}

impl ProviderState {
    fn new() -> Self {
        Self {
            consecutive_failures: AtomicUsize::new(0),
            last_failure: Mutex::new(None),
        }
    }

    fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::SeqCst);
    }

    fn record_failure(&self) {
        self.consecutive_failures.fetch_add(1, Ordering::SeqCst);
        *self.last_failure.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
    }

    /// Returns `true` when the circuit is open (provider should be skipped).
    /// Trips after 3 consecutive failures; resets after 60s cooldown.
    fn is_tripped(&self) -> bool {
        let failures = self.consecutive_failures.load(Ordering::SeqCst);
        if failures < 3 {
            return false;
        }
        let last = self.last_failure.lock().unwrap_or_else(|e| e.into_inner());
        match *last {
            Some(t) if t.elapsed() < Duration::from_secs(60) => true,
            _ => {
                drop(last);
                self.consecutive_failures.store(0, Ordering::SeqCst);
                false
            },
        }
    }
}

/// A provider entry in the failover chain.
struct ChainEntry {
    provider: Arc<dyn LlmProvider>,
    state: ProviderState,
}

/// Failover chain that tries providers in order, with circuit breakers.
///
/// Implements `LlmProvider` itself so callers don't need to know about failover.
pub struct ProviderChain {
    chain: Vec<ChainEntry>,
    /// Same-provider attempts before rotating down the chain (1 = no retries).
    retry_attempts: usize,
    /// Base backoff in milliseconds; 0 makes retries immediate.
    retry_base_ms: u64,
    /// Drop a stream that sends nothing for this long, treat it as transient.
    stream_idle: Option<Duration>,
}

impl ProviderChain {
    /// Build a chain from a list of providers (primary first, then fallbacks).
    pub fn new(providers: Vec<Arc<dyn LlmProvider>>) -> Self {
        Self::with_retry_policy(
            providers,
            transient_retry_attempts(),
            env_usize("MOLTIS_PROVIDER_RETRY_BASE_MS").unwrap_or(1000) as u64,
            stream_idle_timeout(),
        )
    }

    /// Build a chain with an explicit retry policy.
    ///
    /// Used by tests to pin the policy: `retry_attempts == 1` disables
    /// same-provider retries, `retry_base_ms == 0` removes the backoff wait and
    /// `stream_idle == None` disables the idle watchdog.
    pub fn with_retry_policy(
        providers: Vec<Arc<dyn LlmProvider>>,
        retry_attempts: usize,
        retry_base_ms: u64,
        stream_idle: Option<Duration>,
    ) -> Self {
        let chain = providers
            .into_iter()
            .map(|provider| ChainEntry {
                provider,
                state: ProviderState::new(),
            })
            .collect();
        Self {
            chain,
            retry_attempts: retry_attempts.max(1),
            retry_base_ms,
            stream_idle,
        }
    }

    /// Build a chain with one provider (no failover). Useful as a passthrough.
    pub fn single(provider: Arc<dyn LlmProvider>) -> Self {
        Self::new(vec![provider])
    }

    /// Backoff before retrying the same entry: base, 3×base, 7×base, …
    fn retry_delay(&self, attempt: usize) -> Duration {
        if self.retry_base_ms == 0 {
            return Duration::ZERO;
        }
        let multiplier = 2u64.saturating_pow(u32::try_from(attempt).unwrap_or(1));
        Duration::from_millis(
            self.retry_base_ms
                .saturating_mul(multiplier.saturating_sub(1)),
        )
    }

    fn primary(&self) -> &ChainEntry {
        &self.chain[0]
    }

    fn failover_stream(
        &self,
        messages: Vec<ChatMessage>,
        tools: Vec<serde_json::Value>,
        options: AgentToolControls,
    ) -> Pin<Box<dyn Stream<Item = TrackedStreamEvent> + Send + '_>> {
        Box::pin(async_stream::stream! {
            let mut errors = Vec::new();
            let force_primary = self.chain.iter().all(|entry| entry.state.is_tripped());

            for (index, entry) in self.chain.iter().enumerate() {
                if entry.state.is_tripped() && !(force_primary && index == 0) {
                    continue;
                }

                let identity = ProviderIdentity::new(entry.provider.name(), entry.provider.id());
                let max_attempts = self.retry_attempts;
                let mut attempt = 1usize;

                'entry: loop {
                    yield TrackedStreamEvent::Attempt(ProviderAttemptEvent::Started(
                        identity.clone(),
                    ));

                    let mut stream = entry.provider.stream_with_tools_and_options(
                        messages.clone(),
                        tools.clone(),
                        options.clone(),
                    );
                    let mut emitted_output = false;
                    let mut failure: Option<(String, ProviderErrorKind)> = None;

                    loop {
                        let next = match self.stream_idle {
                            Some(idle) => match tokio::time::timeout(idle, stream.next()).await {
                                Ok(item) => item,
                                Err(_) => Some(StreamEvent::Error(format!(
                                    "provider stream sent no data for {}s",
                                    idle.as_secs()
                                ))),
                            },
                            None => stream.next().await,
                        };
                        let Some(event) = next else { break };
                        match event {
                            StreamEvent::Error(error) => {
                                let anyhow_error = anyhow::anyhow!(error.clone());
                                let kind = classify_error(&anyhow_error);
                                entry.state.record_failure();
                                yield TrackedStreamEvent::Attempt(ProviderAttemptEvent::Failed {
                                    identity: identity.clone(),
                                    error: error.clone(),
                                });

                                if kind.should_failover() && !emitted_output {
                                    failure = Some((error, kind));
                                    break;
                                }

                                yield TrackedStreamEvent::Event(StreamEvent::Error(error));
                                return;
                            },
                            StreamEvent::Done(usage) => {
                                entry.state.record_success();
                                yield TrackedStreamEvent::Event(StreamEvent::Done(usage));
                                return;
                            },
                            StreamEvent::ProviderRaw(raw) => {
                                yield TrackedStreamEvent::Event(StreamEvent::ProviderRaw(raw));
                            },
                            event => {
                                emitted_output = true;
                                yield TrackedStreamEvent::Event(event);
                            },
                        }
                    }

                    let (error, kind) = match failure {
                        Some(failure) => failure,
                        None => {
                            let error = "provider stream ended without a terminal event".to_string();
                            if emitted_output {
                                entry.state.record_failure();
                                yield TrackedStreamEvent::Attempt(ProviderAttemptEvent::Failed {
                                    identity: identity.clone(),
                                    error: error.clone(),
                                });
                                yield TrackedStreamEvent::Event(StreamEvent::Error(error));
                                return;
                            }
                            entry.state.record_failure();
                            yield TrackedStreamEvent::Attempt(ProviderAttemptEvent::Failed {
                                identity: identity.clone(),
                                error: error.clone(),
                            });
                            (error, ProviderErrorKind::Unknown)
                        },
                    };

                    if kind.is_transient() && attempt < max_attempts {
                        let delay = self.retry_delay(attempt);
                        warn!(
                            provider = entry.provider.id(),
                            error = %error,
                            kind = ?kind,
                            attempt,
                            max_attempts,
                            delay_ms = delay.as_millis(),
                            "provider stream failed, retrying the same provider"
                        );
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                        attempt += 1;
                        continue 'entry;
                    }

                    warn!(
                        provider = entry.provider.id(),
                        error = %error,
                        kind = ?kind,
                        attempts = attempt,
                        "provider stream failed, trying next in chain"
                    );
                    errors.push(format!("{}: {error}", entry.provider.id()));
                    break 'entry;
                }
            }

            yield TrackedStreamEvent::Event(StreamEvent::Error(format!(
                "all providers in failover chain failed: {}",
                errors.join("; ")
            )));
        })
    }
}

#[async_trait]
impl LlmProvider for ProviderChain {
    fn name(&self) -> &str {
        self.primary().provider.name()
    }

    fn id(&self) -> &str {
        self.primary().provider.id()
    }

    fn supports_tools(&self) -> bool {
        self.primary().provider.supports_tools()
    }

    fn context_window(&self) -> u32 {
        self.primary().provider.context_window()
    }

    async fn complete(
        &self,
        messages: &[ChatMessage],
        tools: &[serde_json::Value],
    ) -> anyhow::Result<CompletionResponse> {
        self.complete_with_options(messages, tools, &AgentToolControls::default())
            .await
    }

    async fn complete_with_options(
        &self,
        messages: &[ChatMessage],
        tools: &[serde_json::Value],
        options: &AgentToolControls,
    ) -> anyhow::Result<CompletionResponse> {
        let mut ignore_attempt = |_: ProviderAttemptEvent| {};
        self.complete_with_options_tracked(messages, tools, options, &mut ignore_attempt)
            .await
    }

    async fn complete_with_options_tracked(
        &self,
        messages: &[ChatMessage],
        tools: &[serde_json::Value],
        options: &AgentToolControls,
        on_attempt: &mut (dyn FnMut(ProviderAttemptEvent) + Send),
    ) -> anyhow::Result<CompletionResponse> {
        let mut errors = Vec::new();
        let force_primary = self.chain.iter().all(|entry| entry.state.is_tripped());
        #[cfg(feature = "metrics")]
        let start = Instant::now();

        for (index, entry) in self.chain.iter().enumerate() {
            if entry.state.is_tripped() && !(force_primary && index == 0) {
                continue;
            }

            let provider_name = entry.provider.name().to_string();
            let model_id = entry.provider.id().to_string();
            let max_attempts = self.retry_attempts;
            let mut attempt = 1usize;

            loop {
                match entry
                    .provider
                    .complete_with_options_tracked(messages, tools, options, &mut *on_attempt)
                    .await
                {
                    Ok(resp) => {
                        entry.state.record_success();

                        // Record metrics on successful completion
                        #[cfg(feature = "metrics")]
                        {
                            let duration = start.elapsed().as_secs_f64();

                            counter!(
                                llm_metrics::COMPLETIONS_TOTAL,
                                labels::PROVIDER => provider_name.clone(),
                                labels::MODEL => model_id.clone()
                            )
                            .increment(1);

                            counter!(
                                llm_metrics::INPUT_TOKENS_TOTAL,
                                labels::PROVIDER => provider_name.clone(),
                                labels::MODEL => model_id.clone()
                            )
                            .increment(u64::from(resp.usage.input_tokens));

                            counter!(
                                llm_metrics::OUTPUT_TOKENS_TOTAL,
                                labels::PROVIDER => provider_name.clone(),
                                labels::MODEL => model_id.clone()
                            )
                            .increment(u64::from(resp.usage.output_tokens));

                            counter!(
                                llm_metrics::CACHE_READ_TOKENS_TOTAL,
                                labels::PROVIDER => provider_name.clone(),
                                labels::MODEL => model_id.clone()
                            )
                            .increment(u64::from(resp.usage.cache_read_tokens));

                            counter!(
                                llm_metrics::CACHE_WRITE_TOKENS_TOTAL,
                                labels::PROVIDER => provider_name.clone(),
                                labels::MODEL => model_id.clone()
                            )
                            .increment(u64::from(resp.usage.cache_write_tokens));

                            histogram!(
                                llm_metrics::COMPLETION_DURATION_SECONDS,
                                labels::PROVIDER => provider_name,
                                labels::MODEL => model_id
                            )
                            .record(duration);
                        }

                        return Ok(resp);
                    },
                    Err(e) => {
                        let kind = classify_error(&e);
                        entry.state.record_failure();

                        // Record error metrics
                        #[cfg(feature = "metrics")]
                        {
                            counter!(
                                llm_metrics::COMPLETION_ERRORS_TOTAL,
                                labels::PROVIDER => provider_name.clone(),
                                labels::MODEL => model_id.clone(),
                                labels::ERROR_TYPE => format!("{kind:?}")
                            )
                            .increment(1);
                        }

                        if !kind.should_failover() {
                            // Non-retryable error — propagate immediately.
                            return Err(e);
                        }

                        if kind.is_transient() && attempt < max_attempts {
                            let delay = self.retry_delay(attempt);
                            warn!(
                                provider = entry.provider.id(),
                                error = %e,
                                kind = ?kind,
                                attempt,
                                max_attempts,
                                delay_ms = delay.as_millis(),
                                "provider failed, retrying the same provider"
                            );
                            if !delay.is_zero() {
                                tokio::time::sleep(delay).await;
                            }
                            attempt += 1;
                            continue;
                        }

                        warn!(
                            provider = entry.provider.id(),
                            error = %e,
                            kind = ?kind,
                            attempts = attempt,
                            "provider failed, trying next in chain"
                        );
                        errors.push(format!("{}: {e}", entry.provider.id()));
                        break;
                    },
                }
            }
        }

        anyhow::bail!(
            "all providers in failover chain failed: {}",
            errors.join("; ")
        )
    }

    fn stream(
        &self,
        messages: Vec<ChatMessage>,
    ) -> Pin<Box<dyn Stream<Item = StreamEvent> + Send + '_>> {
        self.stream_with_tools(messages, vec![])
    }

    fn stream_with_tools(
        &self,
        messages: Vec<ChatMessage>,
        tools: Vec<serde_json::Value>,
    ) -> Pin<Box<dyn Stream<Item = StreamEvent> + Send + '_>> {
        Box::pin(
            self.failover_stream(messages, tools, AgentToolControls::default())
                .filter_map(|event| match event {
                    TrackedStreamEvent::Attempt(_) => None,
                    TrackedStreamEvent::Event(event) => Some(event),
                }),
        )
    }

    fn stream_with_tools_and_options(
        &self,
        messages: Vec<ChatMessage>,
        tools: Vec<serde_json::Value>,
        options: AgentToolControls,
    ) -> Pin<Box<dyn Stream<Item = StreamEvent> + Send + '_>> {
        Box::pin(
            self.failover_stream(messages, tools, options)
                .filter_map(|event| match event {
                    TrackedStreamEvent::Attempt(_) => None,
                    TrackedStreamEvent::Event(event) => Some(event),
                }),
        )
    }

    fn stream_with_tools_and_options_tracked(
        &self,
        messages: Vec<ChatMessage>,
        tools: Vec<serde_json::Value>,
        options: AgentToolControls,
    ) -> Pin<Box<dyn Stream<Item = TrackedStreamEvent> + Send + '_>> {
        self.failover_stream(messages, tools, options)
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used)]
#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::model::{ChatMessage, StreamEvent, Usage},
        async_trait::async_trait,
        tokio_stream::StreamExt,
    };

    /// A mock provider that always succeeds.
    struct SuccessProvider {
        id: &'static str,
    }

    #[async_trait]
    impl LlmProvider for SuccessProvider {
        fn name(&self) -> &str {
            "success"
        }

        fn id(&self) -> &str {
            self.id
        }

        async fn complete(
            &self,
            _messages: &[ChatMessage],
            _tools: &[serde_json::Value],
        ) -> anyhow::Result<CompletionResponse> {
            Ok(CompletionResponse {
                reasoning: None,
                text: Some("ok".into()),
                tool_calls: vec![],
                usage: Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                    ..Default::default()
                },
            })
        }

        fn stream(
            &self,
            _messages: Vec<ChatMessage>,
        ) -> Pin<Box<dyn Stream<Item = StreamEvent> + Send + '_>> {
            Box::pin(tokio_stream::once(StreamEvent::Done(Usage {
                input_tokens: 1,
                output_tokens: 1,
                ..Default::default()
            })))
        }
    }

    /// A mock provider that always fails with a configurable error message.
    struct FailingProvider {
        id: &'static str,
        error_msg: &'static str,
    }

    #[async_trait]
    impl LlmProvider for FailingProvider {
        fn name(&self) -> &str {
            "failing"
        }

        fn id(&self) -> &str {
            self.id
        }

        async fn complete(
            &self,
            _messages: &[ChatMessage],
            _tools: &[serde_json::Value],
        ) -> anyhow::Result<CompletionResponse> {
            anyhow::bail!("{}", self.error_msg)
        }

        fn stream(
            &self,
            _messages: Vec<ChatMessage>,
        ) -> Pin<Box<dyn Stream<Item = StreamEvent> + Send + '_>> {
            Box::pin(tokio_stream::once(StreamEvent::Error(
                self.error_msg.into(),
            )))
        }
    }

    /// A mock provider that fails `failures` times, then succeeds. Each call
    /// (stream or complete) counts as one attempt.
    struct FlakyProvider {
        id: &'static str,
        failures: usize,
        calls: AtomicUsize,
    }

    impl FlakyProvider {
        fn new(id: &'static str, failures: usize) -> Self {
            Self {
                id,
                failures,
                calls: AtomicUsize::new(0),
            }
        }

        fn should_fail(&self) -> bool {
            self.calls.fetch_add(1, Ordering::SeqCst) < self.failures
        }
    }

    #[async_trait]
    impl LlmProvider for FlakyProvider {
        fn name(&self) -> &str {
            "flaky"
        }

        fn id(&self) -> &str {
            self.id
        }

        async fn complete(
            &self,
            _messages: &[ChatMessage],
            _tools: &[serde_json::Value],
        ) -> anyhow::Result<CompletionResponse> {
            if self.should_fail() {
                anyhow::bail!("HTTP 524: error code: 524");
            }
            Ok(CompletionResponse {
                reasoning: None,
                text: Some("primary ok".into()),
                tool_calls: vec![],
                usage: Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                    ..Default::default()
                },
            })
        }

        fn stream(
            &self,
            _messages: Vec<ChatMessage>,
        ) -> Pin<Box<dyn Stream<Item = StreamEvent> + Send + '_>> {
            if self.should_fail() {
                Box::pin(tokio_stream::once(StreamEvent::Error(
                    "HTTP 524: error code: 524".into(),
                )))
            } else {
                Box::pin(tokio_stream::once(StreamEvent::Done(Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                    ..Default::default()
                })))
            }
        }
    }

    /// Build a chain whose retries are instant so tests do not sleep.
    fn test_chain(providers: Vec<Arc<dyn LlmProvider>>, retry_attempts: usize) -> ProviderChain {
        ProviderChain::with_retry_policy(providers, retry_attempts, 0, None)
    }

    /// A transient 524 on the primary must be retried on the primary itself,
    /// not silently answered by the fallback model.
    #[tokio::test]
    async fn stream_retries_same_provider_before_rotating() {
        let chain = test_chain(
            vec![
                Arc::new(FlakyProvider::new("primary", 2)),
                Arc::new(SuccessProvider { id: "fallback" }),
            ],
            3,
        );

        let events = chain
            .stream_with_tools_and_options_tracked(vec![], vec![], AgentToolControls::default())
            .collect::<Vec<_>>()
            .await;

        let started: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                TrackedStreamEvent::Attempt(ProviderAttemptEvent::Started(identity)) => {
                    Some(identity.model.clone())
                },
                _ => None,
            })
            .collect();
        assert_eq!(started, vec!["primary", "primary", "primary"]);
        assert!(
            !events.iter().any(|event| matches!(
                event,
                TrackedStreamEvent::Attempt(ProviderAttemptEvent::Started(identity))
                    if identity.model == "fallback"
            )),
            "fallback must not be used when the retry succeeds"
        );
        assert!(matches!(
            events.last(),
            Some(TrackedStreamEvent::Event(StreamEvent::Done(_)))
        ));
    }

    /// Same guarantee for the non-streaming path.
    #[tokio::test]
    async fn complete_retries_same_provider_before_rotating() {
        let chain = test_chain(
            vec![
                Arc::new(FlakyProvider::new("primary", 1)),
                Arc::new(SuccessProvider { id: "fallback" }),
            ],
            3,
        );

        let response = chain.complete(&[], &[]).await.unwrap();
        assert_eq!(response.text.as_deref(), Some("primary ok"));
    }

    /// Once retries are exhausted the chain still rotates, and the error it
    /// reports names the attempts that were made.
    #[tokio::test]
    async fn exhausted_retries_still_fail_over() {
        let chain = test_chain(
            vec![
                Arc::new(FailingProvider {
                    id: "primary",
                    error_msg: "HTTP 524: error code: 524",
                }),
                Arc::new(SuccessProvider { id: "fallback" }),
            ],
            3,
        );

        let events = chain
            .stream_with_tools_and_options_tracked(vec![], vec![], AgentToolControls::default())
            .collect::<Vec<_>>()
            .await;

        let primary_attempts = events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    TrackedStreamEvent::Attempt(ProviderAttemptEvent::Started(identity))
                        if identity.model == "primary"
                )
            })
            .count();
        assert_eq!(primary_attempts, 3);
        assert!(
            events
                .iter()
                .any(|event| matches!(event, TrackedStreamEvent::Event(StreamEvent::Done(_))))
        );
    }

    struct EmptyStreamProvider;
    #[async_trait]
    impl LlmProvider for EmptyStreamProvider {
        fn name(&self) -> &str {
            "empty"
        }

        fn id(&self) -> &str {
            "empty"
        }

        async fn complete(
            &self,
            _messages: &[ChatMessage],
            _tools: &[serde_json::Value],
        ) -> anyhow::Result<CompletionResponse> {
            anyhow::bail!("not used")
        }

        fn stream(
            &self,
            _messages: Vec<ChatMessage>,
        ) -> Pin<Box<dyn Stream<Item = StreamEvent> + Send + '_>> {
            Box::pin(tokio_stream::empty())
        }
    }

    #[tokio::test]
    async fn primary_succeeds_no_failover() {
        let chain = ProviderChain::new(vec![
            Arc::new(SuccessProvider { id: "primary" }),
            Arc::new(SuccessProvider { id: "fallback" }),
        ]);

        let resp = chain.complete(&[], &[]).await.unwrap();
        assert_eq!(resp.text.as_deref(), Some("ok"));
        assert_eq!(chain.id(), "primary");
    }

    #[tokio::test]
    async fn failover_on_rate_limit() {
        let chain = ProviderChain::new(vec![
            Arc::new(FailingProvider {
                id: "primary",
                error_msg: "429 rate limit exceeded",
            }),
            Arc::new(SuccessProvider { id: "fallback" }),
        ]);

        let resp = chain.complete(&[], &[]).await.unwrap();
        assert_eq!(resp.text.as_deref(), Some("ok"));
    }

    #[tokio::test]
    async fn non_streaming_attempts_report_fallback_identity() {
        let chain = ProviderChain::with_retry_policy(
            vec![
                Arc::new(FailingProvider {
                    id: "primary",
                    error_msg: "429 rate limit exceeded",
                }),
                Arc::new(SuccessProvider { id: "fallback" }),
            ],
            1,
            0,
            None,
        );
        let mut attempts = Vec::new();

        let response = chain
            .complete_with_options_tracked(
                &[],
                &[],
                &AgentToolControls::default(),
                &mut |attempt| attempts.push(attempt),
            )
            .await
            .unwrap();

        assert_eq!(response.text.as_deref(), Some("ok"));
        assert_eq!(attempts, vec![
            ProviderAttemptEvent::Started(ProviderIdentity::new("failing", "primary")),
            ProviderAttemptEvent::Failed {
                identity: ProviderIdentity::new("failing", "primary"),
                error: "429 rate limit exceeded".into(),
            },
            ProviderAttemptEvent::Started(ProviderIdentity::new("success", "fallback")),
        ]);
    }

    #[tokio::test]
    async fn failover_on_server_error() {
        let chain = ProviderChain::new(vec![
            Arc::new(FailingProvider {
                id: "primary",
                error_msg: "500 internal server error",
            }),
            Arc::new(SuccessProvider { id: "fallback" }),
        ]);

        let resp = chain.complete(&[], &[]).await.unwrap();
        assert_eq!(resp.text.as_deref(), Some("ok"));
    }

    #[tokio::test]
    async fn failover_on_auth_error() {
        let chain = ProviderChain::new(vec![
            Arc::new(FailingProvider {
                id: "primary",
                error_msg: "401 unauthorized: invalid api key",
            }),
            Arc::new(SuccessProvider { id: "fallback" }),
        ]);

        let resp = chain.complete(&[], &[]).await.unwrap();
        assert_eq!(resp.text.as_deref(), Some("ok"));
    }

    #[tokio::test]
    async fn no_failover_on_context_window() {
        let chain = ProviderChain::new(vec![
            Arc::new(FailingProvider {
                id: "primary",
                error_msg: "context_length_exceeded: too many tokens",
            }),
            Arc::new(SuccessProvider { id: "fallback" }),
        ]);

        let err = chain.complete(&[], &[]).await.unwrap_err();
        assert!(err.to_string().contains("context_length_exceeded"));
    }

    #[tokio::test]
    async fn no_failover_on_invalid_request() {
        let chain = ProviderChain::new(vec![
            Arc::new(FailingProvider {
                id: "primary",
                error_msg: "400 bad request: invalid_request",
            }),
            Arc::new(SuccessProvider { id: "fallback" }),
        ]);

        let err = chain.complete(&[], &[]).await.unwrap_err();
        assert!(err.to_string().contains("bad request"));
    }

    #[tokio::test]
    async fn all_providers_fail() {
        let chain = ProviderChain::new(vec![
            Arc::new(FailingProvider {
                id: "a",
                error_msg: "429 rate limit",
            }),
            Arc::new(FailingProvider {
                id: "b",
                error_msg: "503 service unavailable",
            }),
        ]);

        let err = chain.complete(&[], &[]).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("all providers in failover chain failed")
        );
    }

    #[tokio::test]
    async fn circuit_breaker_trips_after_three_failures() {
        let chain = ProviderChain::new(vec![
            Arc::new(FailingProvider {
                id: "flaky",
                error_msg: "500 internal server error",
            }),
            Arc::new(SuccessProvider { id: "backup" }),
        ]);

        // Fail 3 times to trip the circuit breaker on the first provider.
        for _ in 0..3 {
            let _ = chain.complete(&[], &[]).await;
        }

        // After tripping, the flaky provider should be skipped.
        assert!(chain.chain[0].state.is_tripped());
    }

    #[tokio::test]
    async fn stream_uses_first_non_tripped() {
        let chain = ProviderChain::new(vec![
            Arc::new(FailingProvider {
                id: "tripped",
                error_msg: "500 error",
            }),
            Arc::new(SuccessProvider { id: "backup" }),
        ]);

        // Trip the first provider through stream failures.
        for _ in 0..3 {
            chain.stream(vec![]).collect::<Vec<_>>().await;
        }
        assert!(chain.chain[0].state.is_tripped());

        // Stream should use backup.
        let mut stream = chain.stream_with_tools_and_options_tracked(
            vec![],
            vec![],
            AgentToolControls::default(),
        );
        let event = stream.next().await.unwrap();
        assert!(matches!(
            event,
            TrackedStreamEvent::Attempt(ProviderAttemptEvent::Started(identity))
                if identity == ProviderIdentity::new("success", "backup")
        ));
        assert!(matches!(
            stream.next().await,
            Some(TrackedStreamEvent::Event(StreamEvent::Done(_)))
        ));
    }

    #[tokio::test]
    async fn stream_error_fails_over_and_records_attempts() {
        let chain = ProviderChain::with_retry_policy(
            vec![
                Arc::new(FailingProvider {
                    id: "primary",
                    error_msg: "503 service unavailable",
                }),
                Arc::new(SuccessProvider { id: "fallback" }),
            ],
            1,
            0,
            None,
        );

        let events = chain
            .stream_with_tools_and_options_tracked(vec![], vec![], AgentToolControls::default())
            .collect::<Vec<_>>()
            .await;

        assert!(matches!(
            &events[0],
            TrackedStreamEvent::Attempt(ProviderAttemptEvent::Started(identity))
                if identity == &ProviderIdentity::new("failing", "primary")
        ));
        assert!(matches!(
            &events[1],
            TrackedStreamEvent::Attempt(ProviderAttemptEvent::Failed { identity, error })
                if identity == &ProviderIdentity::new("failing", "primary")
                    && error == "503 service unavailable"
        ));
        assert!(matches!(
            &events[2],
            TrackedStreamEvent::Attempt(ProviderAttemptEvent::Started(identity))
                if identity == &ProviderIdentity::new("success", "fallback")
        ));
        assert!(matches!(
            events[3],
            TrackedStreamEvent::Event(StreamEvent::Done(_))
        ));
        assert_eq!(
            chain.chain[0]
                .state
                .consecutive_failures
                .load(Ordering::SeqCst),
            1
        );
        assert_eq!(
            chain.chain[1]
                .state
                .consecutive_failures
                .load(Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn empty_stream_fails_over_instead_of_reporting_success() {
        let chain = ProviderChain::new(vec![
            Arc::new(EmptyStreamProvider),
            Arc::new(SuccessProvider { id: "fallback" }),
        ]);

        let events = chain
            .stream_with_tools_and_options_tracked(vec![], vec![], AgentToolControls::default())
            .collect::<Vec<_>>()
            .await;

        assert!(events.iter().any(|event| matches!(
            event,
            TrackedStreamEvent::Attempt(ProviderAttemptEvent::Failed { identity, error })
                if identity == &ProviderIdentity::new("empty", "empty")
                    && error.contains("without a terminal event")
        )));
        assert!(matches!(
            events.last(),
            Some(TrackedStreamEvent::Event(StreamEvent::Done(_)))
        ));
    }

    #[tokio::test]
    async fn all_tripped_streaming_chain_still_probes_primary() {
        let chain = ProviderChain::single(Arc::new(SuccessProvider { id: "primary" }));
        for _ in 0..3 {
            chain.chain[0].state.record_failure();
        }
        assert!(chain.chain[0].state.is_tripped());

        let events = chain
            .stream_with_tools_and_options_tracked(vec![], vec![], AgentToolControls::default())
            .collect::<Vec<_>>()
            .await;

        assert!(matches!(
            events.first(),
            Some(TrackedStreamEvent::Attempt(ProviderAttemptEvent::Started(identity)))
                if identity == &ProviderIdentity::new("success", "primary")
        ));
        assert!(matches!(
            events.last(),
            Some(TrackedStreamEvent::Event(StreamEvent::Done(_)))
        ));
    }

    #[tokio::test]
    async fn all_tripped_non_streaming_chain_still_probes_primary() {
        let chain = ProviderChain::single(Arc::new(SuccessProvider { id: "primary" }));
        for _ in 0..3 {
            chain.chain[0].state.record_failure();
        }
        assert!(chain.chain[0].state.is_tripped());

        let response = chain.complete(&[], &[]).await.unwrap();

        assert_eq!(response.text.as_deref(), Some("ok"));
    }

    #[test]
    fn classify_rate_limit() {
        let err = anyhow::anyhow!("429 Too Many Requests: rate limit exceeded");
        assert_eq!(classify_error(&err), ProviderErrorKind::RateLimit);
    }

    #[test]
    fn classify_auth() {
        let err = anyhow::anyhow!("401 Unauthorized");
        assert_eq!(classify_error(&err), ProviderErrorKind::AuthError);
    }

    #[test]
    fn classify_server() {
        let err = anyhow::anyhow!("502 Bad Gateway");
        assert_eq!(classify_error(&err), ProviderErrorKind::ServerError);
    }

    #[test]
    fn classify_context_window() {
        let err = anyhow::anyhow!("context_length_exceeded: maximum context length is 200000");
        assert_eq!(classify_error(&err), ProviderErrorKind::ContextWindow);
        // Z.AI / OpenAI-compat finish_reason
        let err = anyhow::anyhow!("model_context_window_exceeded");
        assert_eq!(classify_error(&err), ProviderErrorKind::ContextWindow);
        // Generic underscore variant
        let err = anyhow::anyhow!("context_window_exceeded");
        assert_eq!(classify_error(&err), ProviderErrorKind::ContextWindow);
        // Provider input-too-long
        let err = anyhow::anyhow!("input_too_long: your prompt exceeds the limit");
        assert_eq!(classify_error(&err), ProviderErrorKind::ContextWindow);
        // Space variant
        let err = anyhow::anyhow!("input too long");
        assert_eq!(classify_error(&err), ProviderErrorKind::ContextWindow);
    }

    #[test]
    fn classify_billing() {
        let err = anyhow::anyhow!("insufficient_quota: billing limit reached");
        assert_eq!(classify_error(&err), ProviderErrorKind::BillingExhausted);
    }

    #[test]
    fn classify_invalid_request() {
        let err = anyhow::anyhow!("400 Bad Request: invalid JSON");
        assert_eq!(classify_error(&err), ProviderErrorKind::InvalidRequest);
    }

    #[test]
    fn classify_unknown() {
        let err = anyhow::anyhow!("totally unexpected provider hiccup");
        assert_eq!(classify_error(&err), ProviderErrorKind::Unknown);
    }

    /// A Cloudflare 524 used to fall through as `Unknown`; it must be retried
    /// like any other transient 5xx, and network-level failures too.
    #[test]
    fn classify_transient_network_errors() {
        for message in [
            "HTTP 524: error code: 524",
            "HTTP 522: Connection timed out",
            "HTTP 503: Service temporarily unavailable",
            "error sending request for url (https://example.test/v1/chat/completions)",
            "connection reset by peer",
            "provider stream sent no data for 180s",
        ] {
            let kind = classify_error(&anyhow::anyhow!(message));
            assert_eq!(kind, ProviderErrorKind::ServerError, "{message}");
            assert!(kind.is_transient(), "{message}");
            assert!(kind.should_failover(), "{message}");
        }
        assert!(!ProviderErrorKind::AuthError.is_transient());
        assert!(!ProviderErrorKind::InvalidRequest.is_transient());
        assert!(!ProviderErrorKind::ContextWindow.is_transient());
    }

    #[test]
    fn should_failover_mapping() {
        assert!(ProviderErrorKind::RateLimit.should_failover());
        assert!(ProviderErrorKind::AuthError.should_failover());
        assert!(ProviderErrorKind::ServerError.should_failover());
        assert!(ProviderErrorKind::BillingExhausted.should_failover());
        assert!(ProviderErrorKind::Unknown.should_failover());
        assert!(!ProviderErrorKind::ContextWindow.should_failover());
        assert!(!ProviderErrorKind::InvalidRequest.should_failover());
    }

    #[test]
    fn single_provider_chain() {
        let chain = ProviderChain::single(Arc::new(SuccessProvider { id: "only" }));
        assert_eq!(chain.id(), "only");
        assert_eq!(chain.chain.len(), 1);
    }

    // ── Regression: stream_with_tools must forward tools to the provider ──

    /// A mock provider that records whether stream_with_tools received tools.
    struct ToolTrackingProvider {
        received_tools: Mutex<Option<Vec<serde_json::Value>>>,
    }

    impl ToolTrackingProvider {
        fn new() -> Self {
            Self {
                received_tools: Mutex::new(None),
            }
        }

        fn received_tools_count(&self) -> usize {
            self.received_tools
                .lock()
                .unwrap()
                .as_ref()
                .map_or(0, |t| t.len())
        }
    }

    #[async_trait]
    impl LlmProvider for ToolTrackingProvider {
        fn name(&self) -> &str {
            "tool-tracker"
        }

        fn id(&self) -> &str {
            "tool-tracker"
        }

        fn supports_tools(&self) -> bool {
            true
        }

        async fn complete(
            &self,
            _messages: &[ChatMessage],
            _tools: &[serde_json::Value],
        ) -> anyhow::Result<CompletionResponse> {
            Ok(CompletionResponse {
                reasoning: None,
                text: Some("ok".into()),
                tool_calls: vec![],
                usage: Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                    ..Default::default()
                },
            })
        }

        fn stream(
            &self,
            _messages: Vec<ChatMessage>,
        ) -> Pin<Box<dyn Stream<Item = StreamEvent> + Send + '_>> {
            Box::pin(tokio_stream::once(StreamEvent::Done(Usage {
                input_tokens: 1,
                output_tokens: 1,
                ..Default::default()
            })))
        }

        fn stream_with_tools(
            &self,
            _messages: Vec<ChatMessage>,
            tools: Vec<serde_json::Value>,
        ) -> Pin<Box<dyn Stream<Item = StreamEvent> + Send + '_>> {
            *self.received_tools.lock().unwrap() = Some(tools);
            Box::pin(tokio_stream::once(StreamEvent::Done(Usage {
                input_tokens: 1,
                output_tokens: 1,
                ..Default::default()
            })))
        }
    }

    #[tokio::test]
    async fn chain_stream_with_tools_forwards_tools() {
        // Regression test: before the fix, ProviderChain::stream_with_tools()
        // used the default trait impl which dropped tools and called stream().
        let tracker = Arc::new(ToolTrackingProvider::new());
        let chain = ProviderChain::single(tracker.clone());

        let tools = vec![serde_json::json!({
            "name": "test_tool",
            "description": "A test",
            "parameters": {"type": "object"}
        })];

        let mut stream = chain.stream_with_tools(vec![], tools);
        while stream.next().await.is_some() {}

        assert_eq!(
            tracker.received_tools_count(),
            1,
            "ProviderChain must forward tools to the underlying provider's stream_with_tools()"
        );
    }

    #[tokio::test]
    async fn chain_stream_with_tools_forwards_empty_tools() {
        let tracker = Arc::new(ToolTrackingProvider::new());
        let chain = ProviderChain::single(tracker.clone());

        let mut stream = chain.stream_with_tools(vec![], vec![]);
        while stream.next().await.is_some() {}

        assert_eq!(tracker.received_tools_count(), 0);
    }
}
