//! Telegram channel plugin for moltis.
//!
//! Implements `ChannelPlugin` using the teloxide library to receive and send
//! messages via the Telegram Bot API, including edit-in-place streaming.

pub mod access;
pub mod body;
pub mod bot;
pub mod config;
pub mod error;
pub mod handlers;
pub mod markdown;
mod md_renderer;
pub mod otp;
pub mod outbound;
pub mod plugin;
pub mod state;
pub(crate) mod topic;

pub use {
    config::TelegramAccountConfig,
    error::{Error, Result},
    plugin::TelegramPlugin,
};
