//! Telegram message bodies (home patch).
//!
//! Model output rendered by the external renderer (`MOLTIS_TELEGRAM_MD_RENDERER`,
//! telegram-md-entities) is plain text plus explicit `MessageEntity` values,
//! sent without any parse mode. Everything else (built-in converter fallback,
//! Moltis' own HTML notices) stays HTML. Send/edit paths carry a [`TgBody`] so
//! both forms flow through the same chunking, streaming and fallback logic.

use teloxide::{
    payloads::{EditMessageTextSetters, SendMessageSetters},
    prelude::*,
    types::{ChatId, MessageEntity, MessageEntityKind, MessageId, ParseMode},
};

use crate::outbound::telegram_html_to_plain_text;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TgBody {
    /// Telegram HTML, sent with `parse_mode = HTML`.
    Html(String),
    /// Plain text with explicit entities (UTF-16 offsets), sent without parse mode.
    Entities {
        text: String,
        entities: Vec<MessageEntity>,
    },
}

impl Default for TgBody {
    fn default() -> Self {
        Self::Html(String::new())
    }
}

impl From<String> for TgBody {
    fn from(html: String) -> Self {
        Self::Html(html)
    }
}

impl From<&str> for TgBody {
    fn from(html: &str) -> Self {
        Self::Html(html.to_string())
    }
}

pub(crate) fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}

fn shifted(entities: &[MessageEntity], shift: usize) -> impl Iterator<Item = MessageEntity> + '_ {
    entities.iter().map(move |entity| {
        let mut entity = entity.clone();
        entity.offset += shift;
        entity
    })
}

impl TgBody {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.as_str().is_empty()
    }

    /// Raw payload: HTML markup or the plain text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Html(html) => html,
            Self::Entities { text, .. } => text,
        }
    }

    /// Readable fallback used when Telegram rejects the formatted message.
    #[must_use]
    pub fn plain_text(&self) -> String {
        match self {
            Self::Html(html) => telegram_html_to_plain_text(html),
            Self::Entities { text, .. } => text.clone(),
        }
    }

    /// Size compared against Telegram limits: HTML bytes (conservative, as
    /// upstream does) or UTF-16 units of the visible text.
    #[must_use]
    pub fn budget_len(&self) -> usize {
        match self {
            Self::Html(html) => html.len(),
            Self::Entities { text, .. } => utf16_len(text),
        }
    }

    /// Prepend an italic notice line followed by a blank line.
    #[must_use]
    pub fn with_italic_prefix(self, notice: &str) -> Self {
        match self {
            Self::Html(html) => Self::Html(format!(
                "<i>{}</i>\n\n{html}",
                crate::markdown::escape_html(notice)
            )),
            Self::Entities { text, entities } => {
                let prefix = format!("{notice}\n\n");
                let shift = utf16_len(&prefix);
                let mut all = Vec::with_capacity(entities.len() + 1);
                all.push(MessageEntity::new(MessageEntityKind::Italic, 0, utf16_len(notice)));
                all.extend(shifted(&entities, shift));
                Self::Entities {
                    text: prefix + &text,
                    entities: all,
                }
            },
        }
    }

    pub(crate) fn send_request(
        &self,
        bot: &Bot,
        chat_id: ChatId,
    ) -> <Bot as Requester>::SendMessage {
        match self {
            Self::Html(html) => bot.send_message(chat_id, html).parse_mode(ParseMode::Html),
            Self::Entities { text, entities } => {
                bot.send_message(chat_id, text).entities(entities.clone())
            },
        }
    }

    pub(crate) fn edit_request(
        &self,
        bot: &Bot,
        chat_id: ChatId,
        message_id: MessageId,
    ) -> <Bot as Requester>::EditMessageText {
        match self {
            Self::Html(html) => bot
                .edit_message_text(chat_id, message_id, html)
                .parse_mode(ParseMode::Html),
            Self::Entities { text, entities } => bot
                .edit_message_text(chat_id, message_id, text)
                .entities(entities.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn italic_prefix_keeps_html_shape() {
        let body = TgBody::from("<b>x</b>").with_italic_prefix("Older progress hidden.");
        assert_eq!(
            body.as_str(),
            "<i>Older progress hidden.</i>\n\n<b>x</b>"
        );
    }

    #[test]
    fn italic_prefix_shifts_entities_by_utf16_units() {
        let body = TgBody::Entities {
            text: "\u{4f60}\u{1f600}x".into(),
            entities: vec![MessageEntity::new(MessageEntityKind::Bold, 3, 1)],
        }
        .with_italic_prefix("note");
        let TgBody::Entities { text, entities } = body else {
            panic!("expected entities");
        };
        assert_eq!(text, "note\n\n\u{4f60}\u{1f600}x");
        assert_eq!(entities[0], MessageEntity::new(MessageEntityKind::Italic, 0, 4));
        assert_eq!(entities[1], MessageEntity::new(MessageEntityKind::Bold, 9, 1));
    }
}
