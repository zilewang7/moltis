use crate::plugin::ChannelType;

pub(crate) fn classify_chat(channel_type: ChannelType, chat_id: &str) -> Option<String> {
    classify_chat_with_hint(channel_type, chat_id, false)
}

/// [`classify_chat`] plus the adapter's `direct_chat` evidence, which only
/// channel types whose chat IDs do not encode conversation kind consult.
pub(crate) fn classify_chat_with_hint(
    channel_type: ChannelType,
    chat_id: &str,
    direct_chat: bool,
) -> Option<String> {
    match channel_type {
        ChannelType::Telegram => {
            if chat_id.starts_with("-100") {
                Some("channel_or_supergroup".to_string())
            } else if chat_id.starts_with('-') {
                Some("group".to_string())
            } else {
                Some("private".to_string())
            }
        },
        ChannelType::Signal => Some(
            if chat_id.starts_with("group:") {
                "group"
            } else {
                "direct"
            }
            .to_string(),
        ),
        ChannelType::Slack => Some(
            if chat_id.starts_with('D') {
                "direct"
            } else {
                "channel"
            }
            .to_string(),
        ),
        ChannelType::Whatsapp => Some(
            if chat_id.ends_with("@g.us") {
                "group"
            } else {
                "direct"
            }
            .to_string(),
        ),
        ChannelType::Nostr => Some("dm".to_string()),
        ChannelType::Telephony => Some("call".to_string()),
        ChannelType::Discord if direct_chat => Some("direct".to_string()),
        ChannelType::MsTeams | ChannelType::Discord | ChannelType::Matrix => None,
    }
}

/// WhatsApp JID suffixes that identify a one-to-one conversation.
///
/// `@s.whatsapp.net` is the phone-number form and `@lid` the privacy-preserving
/// linked-ID form. Everything else — `@g.us` groups, `status@broadcast`,
/// `@newsletter` channels, and any suffix WhatsApp adds later — is treated as
/// shared.
const WHATSAPP_DIRECT_SUFFIXES: [&str; 2] = ["@s.whatsapp.net", "@lid"];

/// Whether a chat can carry messages from principals other than the sender.
///
/// This is the gate for privileged access, so it is an **allowlist of shapes
/// known to be one-to-one**, not a denylist of known group shapes. A chat id
/// whose form we do not recognise is shared, which costs an operator a denial;
/// getting it backwards would hand a room full of strangers the host.
///
/// Callers must not read this as "the platform proved the conversation is
/// direct" beyond what the id itself encodes. Telephony is deliberately shared:
/// its chat id is a caller number, and caller ID is trivially spoofable, so
/// there is nothing here to authenticate against.
pub(crate) fn is_shared_chat(channel_type: ChannelType, chat_id: &str) -> bool {
    is_shared_chat_with_hint(channel_type, chat_id, false)
}

/// [`is_shared_chat`] plus the adapter's `direct_chat` evidence.
///
/// The hint is honoured only for channel types whose chat IDs cannot be
/// classified on their own and whose adapter forwards the conversation kind.
/// Everywhere else it is ignored, so the ID-based allowlist above stays the
/// only way those channels reach "direct".
pub(crate) fn is_shared_chat_with_hint(
    channel_type: ChannelType,
    chat_id: &str,
    direct_chat: bool,
) -> bool {
    match channel_type {
        // Negative ids are groups, supergroups, and channels; users are positive.
        // Home patch: chats listed in MOLTIS_TELEGRAM_TRUSTED_CHATS count as direct.
        ChannelType::Telegram => {
            !is_trusted_telegram_chat(chat_id)
                && (!chat_id.chars().all(|c| c.is_ascii_digit()) || chat_id.is_empty())
        },
        // Signal direct ids are an E.164 number or an ACI UUID; groups carry a
        // `group:` prefix. Both forms are non-empty, so a blank id is unknown.
        ChannelType::Signal => chat_id.is_empty() || chat_id.starts_with("group:"),
        // Slack conversation ids are prefixed by kind: `D` is a 1:1 DM, `G` a
        // multi-person DM, `C` a channel.
        ChannelType::Slack => !chat_id.starts_with('D'),
        ChannelType::Whatsapp => !WHATSAPP_DIRECT_SUFFIXES
            .iter()
            .any(|suffix| chat_id.ends_with(suffix)),
        // Nostr DMs are addressed to a pubkey the sender must hold the key for.
        ChannelType::Nostr => false,
        // Discord channel ids do not encode conversation kind, so the adapter
        // forwards it: a message without a `guild_id` (bots cannot join group
        // DMs) or an interaction whose context is the bot's own DM.
        ChannelType::Discord => !direct_chat,
        // Telephony: caller-ID only, see above. Teams/Matrix ids do not encode
        // conversation kind and the adapters do not yet forward it.
        ChannelType::Telephony | ChannelType::MsTeams | ChannelType::Matrix => true,
    }
}

/// Home patch: Telegram chat ids listed (comma-separated) in the
/// `MOLTIS_TELEGRAM_TRUSTED_CHATS` environment variable are treated as
/// one-to-one chats, so an operator gets the full private context and
/// operator-direct commands (e.g. `/approve`) in that group and its forum
/// topics. Only list groups whose members are all operators: non-operator
/// senders are still untrusted through the separate sender-role check.
/// A forum-topic target (`chat_id:thread_id`) matches on its chat id.
fn is_trusted_telegram_chat(chat_id: &str) -> bool {
    let base_chat_id = chat_id.split_once(':').map_or(chat_id, |(chat, _)| chat);
    if base_chat_id.is_empty() {
        return false;
    }
    std::env::var("MOLTIS_TELEGRAM_TRUSTED_CHATS")
        .map(|list| list.split(',').map(str::trim).any(|id| id == base_chat_id))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_and_shared_shapes_are_classified_fail_closed() {
        assert!(!is_shared_chat(ChannelType::Telegram, "123"));
        assert!(is_shared_chat(ChannelType::Telegram, "-123"));
        assert!(!is_shared_chat(ChannelType::Slack, "D123"));
        assert!(is_shared_chat(ChannelType::Slack, "C123"));
        assert!(!is_shared_chat(
            ChannelType::Whatsapp,
            "15551234567@s.whatsapp.net"
        ));
        assert!(is_shared_chat(ChannelType::Whatsapp, "123@g.us"));
        assert!(is_shared_chat(ChannelType::Discord, "123"));
        assert!(is_shared_chat(ChannelType::Matrix, "!room:example.org"));
    }

    /// Unrecognised id shapes must be shared, not direct. An empty or
    /// unexpectedly-formatted id is exactly the case where we know least.
    #[test]
    fn unrecognised_id_shapes_are_shared() {
        for channel_type in [
            ChannelType::Telegram,
            ChannelType::Signal,
            ChannelType::Slack,
            ChannelType::Whatsapp,
        ] {
            assert!(
                is_shared_chat(channel_type, ""),
                "{channel_type:?} treated a blank chat id as direct"
            );
        }
        assert!(is_shared_chat(ChannelType::Telegram, "not-a-number"));
    }

    /// WhatsApp gained group-adjacent address kinds over time (status
    /// broadcasts, newsletter channels). Only the two one-to-one JID forms are
    /// direct; a new suffix must not silently read as a DM.
    #[test]
    fn whatsapp_direct_is_an_allowlist_of_jid_suffixes() {
        assert!(!is_shared_chat(
            ChannelType::Whatsapp,
            "15551234567@s.whatsapp.net"
        ));
        assert!(!is_shared_chat(
            ChannelType::Whatsapp,
            "259557842534599@lid"
        ));
        assert!(is_shared_chat(ChannelType::Whatsapp, "status@broadcast"));
        assert!(is_shared_chat(ChannelType::Whatsapp, "123@newsletter"));
        assert!(is_shared_chat(ChannelType::Whatsapp, "15551234567"));
    }

    /// Discord ids carry no conversation kind, so only the adapter's DM
    /// evidence makes a Discord chat direct; guild channels stay shared.
    #[test]
    fn discord_dm_is_direct_only_with_adapter_evidence() {
        assert!(!is_shared_chat_with_hint(ChannelType::Discord, "123", true));
        assert!(is_shared_chat_with_hint(ChannelType::Discord, "123", false));
        assert!(is_shared_chat(ChannelType::Discord, "123"));
        assert_eq!(
            classify_chat_with_hint(ChannelType::Discord, "123", true).as_deref(),
            Some("direct")
        );
        assert!(classify_chat_with_hint(ChannelType::Discord, "123", false).is_none());
    }

    /// The hint must not widen channels that classify by id, or channels whose
    /// adapters do not forward conversation kind.
    #[test]
    fn direct_hint_is_ignored_outside_discord() {
        assert!(is_shared_chat_with_hint(
            ChannelType::Telegram,
            "-123",
            true
        ));
        assert!(is_shared_chat_with_hint(ChannelType::Slack, "C123", true));
        assert!(is_shared_chat_with_hint(
            ChannelType::Signal,
            "group:abc",
            true
        ));
        assert!(is_shared_chat_with_hint(
            ChannelType::Whatsapp,
            "123@g.us",
            true
        ));
        assert!(is_shared_chat_with_hint(
            ChannelType::MsTeams,
            "19:abc",
            true
        ));
        assert!(is_shared_chat_with_hint(
            ChannelType::Matrix,
            "!room:example.org",
            true
        ));
        assert!(is_shared_chat_with_hint(
            ChannelType::Telephony,
            "+15551234567",
            true
        ));
        assert_eq!(
            classify_chat_with_hint(ChannelType::Slack, "C123", true).as_deref(),
            Some("channel")
        );
        assert!(classify_chat_with_hint(ChannelType::Matrix, "!room:example.org", true).is_none());
    }

    /// A phone number is a claim, not an authenticated identity: caller ID is
    /// spoofable, so a call can never be a proven direct chat.
    #[test]
    fn telephony_is_never_a_proven_direct_chat() {
        assert!(is_shared_chat(ChannelType::Telephony, "+15551234567"));
    }
}
