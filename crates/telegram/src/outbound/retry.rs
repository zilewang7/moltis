//! Telegram API retry logic and request error handling.
//!
//! Provides retry-after handling for rate-limited requests, HTML-to-plain-text
//! fallback for send/edit operations, and the `RequestResultExt` trait for
//! ergonomic error conversion.

use {
    std::{future::Future, time::Duration},
    teloxide::{
        ApiError, RequestError,
        payloads::SendMessageSetters,
        prelude::*,
        types::{ChatId, MessageId, ThreadId},
    },
    tracing::warn,
};

use moltis_channels::{Error as ChannelError, Result};

use crate::body::TgBody;

use super::{TELEGRAM_RETRY_AFTER_MAX_RETRIES, TelegramOutbound};

pub(crate) trait RequestResultExt<T> {
    fn channel_context(self, context: &'static str) -> Result<T>;
}

impl<T> RequestResultExt<T> for std::result::Result<T, RequestError> {
    fn channel_context(self, context: &'static str) -> Result<T> {
        self.map_err(|e| ChannelError::external(context, e))
    }
}

pub(crate) fn retry_after_duration(error: &RequestError) -> Option<Duration> {
    match error {
        RequestError::RetryAfter(wait) => Some(wait.duration()),
        _ => None,
    }
}

pub(crate) fn is_message_not_modified_error(error: &RequestError) -> bool {
    matches!(error, RequestError::Api(ApiError::MessageNotModified))
}

impl TelegramOutbound {
    pub(crate) async fn send_chunk_with_fallback(
        &self,
        bot: &Bot,
        account_id: &str,
        to: &str,
        chat_id: ChatId,
        thread_id: Option<ThreadId>,
        chunk: &TgBody,
        reply_params: Option<&teloxide::types::ReplyParameters>,
        silent: bool,
    ) -> Result<MessageId> {
        match self
            .run_telegram_request_with_retry(account_id, to, "send message (formatted)", || {
                let mut formatted_req = chunk.send_request(bot, chat_id);
                if silent {
                    formatted_req = formatted_req.disable_notification(true);
                }
                if let Some(tid) = thread_id {
                    formatted_req = formatted_req.message_thread_id(tid);
                }
                if let Some(rp) = reply_params {
                    formatted_req = formatted_req.reply_parameters(rp.clone());
                }
                async move { formatted_req.await }
            })
            .await
        {
            Ok(message) => Ok(message.id),
            Err(e) => {
                let plain_chunk = chunk.plain_text();
                warn!(
                    account_id,
                    chat_id = to,
                    error = %e,
                    "telegram formatted send failed, retrying as plain text"
                );
                let message = self
                    .run_telegram_request_with_retry(account_id, to, "send message (plain)", || {
                        let mut plain_req = bot.send_message(chat_id, &plain_chunk);
                        if silent {
                            plain_req = plain_req.disable_notification(true);
                        }
                        if let Some(tid) = thread_id {
                            plain_req = plain_req.message_thread_id(tid);
                        }
                        if let Some(rp) = reply_params {
                            plain_req = plain_req.reply_parameters(rp.clone());
                        }
                        async move { plain_req.await }
                    })
                    .await
                    .channel_context("send message (plain)")?;
                Ok(message.id)
            },
        }
    }

    pub(crate) async fn run_telegram_request_with_retry<T, F, Fut>(
        &self,
        account_id: &str,
        to: &str,
        operation: &'static str,
        mut request: F,
    ) -> std::result::Result<T, RequestError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = std::result::Result<T, RequestError>>,
    {
        let mut retries = 0usize;

        loop {
            match request().await {
                Ok(value) => return Ok(value),
                Err(err) => {
                    let Some(wait) = retry_after_duration(&err) else {
                        return Err(err);
                    };

                    if retries >= TELEGRAM_RETRY_AFTER_MAX_RETRIES {
                        warn!(
                            account_id,
                            chat_id = to,
                            operation,
                            retries,
                            max_retries = TELEGRAM_RETRY_AFTER_MAX_RETRIES,
                            retry_after_secs = wait.as_secs(),
                            "telegram rate limit persisted after retries"
                        );
                        return Err(err);
                    }

                    retries += 1;
                    warn!(
                        account_id,
                        chat_id = to,
                        operation,
                        retries,
                        max_retries = TELEGRAM_RETRY_AFTER_MAX_RETRIES,
                        retry_after_secs = wait.as_secs(),
                        "telegram rate limited, waiting before retry"
                    );
                    tokio::time::sleep(wait).await;
                },
            }
        }
    }
}
