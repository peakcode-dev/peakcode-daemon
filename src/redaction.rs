use std::collections::HashMap;
use std::sync::Mutex;

use crate::ipc::WorkerEvent;

const REDACTED: &str = "[REDACTED]";
const PRIVATE_KEY_LABELS: [&str; 5] = [
    "PRIVATE KEY",
    "RSA PRIVATE KEY",
    "EC PRIVATE KEY",
    "OPENSSH PRIVATE KEY",
    "ENCRYPTED PRIVATE KEY",
];
const TOKEN_PREFIXES: [&str; 8] = [
    "github_pat_",
    "ghp_",
    "gho_",
    "ghu_",
    "ghs_",
    "xoxb-",
    "xoxp-",
    "sk-",
];

/// Redacts configured credentials without exposing them through `Debug` or errors.
pub(crate) struct Redactor {
    secrets: Vec<String>,
    identifiers: Mutex<IdentifierState>,
}

#[derive(Default)]
struct IdentifierState {
    aliases: HashMap<String, String>,
    next_alias: u64,
}

impl Redactor {
    pub(crate) fn empty() -> Self {
        Self {
            secrets: Vec::new(),
            identifiers: Mutex::new(IdentifierState::default()),
        }
    }

    pub(crate) fn from_env(provider_api_key: &str) -> Self {
        Self::from_named_values(provider_api_key, std::env::vars())
    }

    fn from_named_values<K, V, I>(provider_api_key: &str, values: I) -> Self
    where
        K: AsRef<str>,
        V: AsRef<str>,
        I: IntoIterator<Item = (K, V)>,
    {
        let mut secrets = Vec::new();
        if !provider_api_key.is_empty() {
            secrets.push(provider_api_key.to_owned());
        }
        secrets.extend(values.into_iter().filter_map(|(name, value)| {
            let value = value.as_ref();
            (is_sensitive_name(name.as_ref()) && value.len() >= 4).then(|| value.to_owned())
        }));
        secrets.sort_by(|left, right| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
        secrets.dedup();
        Self {
            secrets,
            identifiers: Mutex::new(IdentifierState::default()),
        }
    }

    pub(crate) fn redact_event(&self, event: WorkerEvent) -> WorkerEvent {
        match event {
            WorkerEvent::TextDelta { text } => WorkerEvent::TextDelta {
                text: self.redact_text(&text),
            },
            WorkerEvent::AssistantMessage { text } => WorkerEvent::AssistantMessage {
                text: self.redact_text(&text),
            },
            WorkerEvent::ToolStart {
                call_id,
                name,
                arguments_json,
            } => WorkerEvent::ToolStart {
                call_id: self.redact_identifier(&call_id),
                name: self.redact_text(&name),
                arguments_json: self.redact_text(&arguments_json),
            },
            WorkerEvent::ToolResult {
                call_id,
                name,
                content,
                is_error,
            } => WorkerEvent::ToolResult {
                call_id: self.redact_identifier(&call_id),
                name: self.redact_text(&name),
                content: self.redact_text(&content),
                is_error,
            },
            WorkerEvent::NeedsApproval {
                call_id,
                tool,
                arguments_json,
            } => WorkerEvent::NeedsApproval {
                call_id: self.redact_identifier(&call_id),
                tool: self.redact_text(&tool),
                arguments_json: self.redact_text(&arguments_json),
            },
            WorkerEvent::Crash { message } => WorkerEvent::Crash {
                message: self.redact_text(&message),
            },
            event @ (WorkerEvent::TurnFinished | WorkerEvent::Done { .. }) => event,
        }
    }

    pub(crate) fn redact_text(&self, input: &str) -> String {
        let mut redacted = input.to_owned();
        for secret in &self.secrets {
            redacted = redacted.replace(secret, REDACTED);
        }
        redacted = redact_private_keys(redacted);
        redacted = redact_bearer_tokens(redacted);
        for prefix in TOKEN_PREFIXES {
            redacted = redact_prefixed_tokens(redacted, prefix);
        }
        redacted
    }

    pub(crate) fn redact_identifier(&self, input: &str) -> String {
        let redacted = self.redact_text(input);
        if redacted == input {
            return redacted;
        }

        let mut identifiers = self
            .identifiers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(alias) = identifiers.aliases.get(input) {
            return alias.clone();
        }
        let alias = format!("redacted-call-{}", identifiers.next_alias);
        identifiers.next_alias = identifiers.next_alias.wrapping_add(1);
        identifiers.aliases.insert(input.to_owned(), alias.clone());
        alias
    }
}

fn is_sensitive_name(name: &str) -> bool {
    let name = name.to_ascii_uppercase();
    [
        "API_KEY",
        "TOKEN",
        "SECRET",
        "PASSWORD",
        "PASSWD",
        "PRIVATE_KEY",
        "CREDENTIAL",
        "AUTH",
    ]
    .iter()
    .any(|marker| name.contains(marker))
        || name == "DATABASE_URL"
        || name == "REDIS_URL"
        || name.ends_with("_DATABASE_URL")
        || name.ends_with("_DSN")
}

fn redact_private_keys(mut text: String) -> String {
    for label in PRIVATE_KEY_LABELS {
        let begin = format!("-----BEGIN {label}-----");
        let end = format!("-----END {label}-----");
        while let Some(start) = text.find(&begin) {
            let block_end = text[start..]
                .find(&end)
                .map(|offset| start + offset + end.len())
                .unwrap_or(text.len());
            text.replace_range(start..block_end, REDACTED);
        }
    }
    text
}

fn redact_bearer_tokens(mut text: String) -> String {
    let mut cursor = 0;
    loop {
        let lowercase = text.to_ascii_lowercase();
        let Some(offset) = lowercase[cursor..].find("bearer ") else {
            break;
        };
        let token_start = cursor + offset + "bearer ".len();
        let token_end = text[token_start..]
            .find(char::is_whitespace)
            .map(|offset| token_start + offset)
            .unwrap_or(text.len());
        if token_end > token_start {
            text.replace_range(token_start..token_end, REDACTED);
            cursor = token_start + REDACTED.len();
        } else {
            cursor = token_start;
        }
    }
    text
}

fn redact_prefixed_tokens(mut text: String, prefix: &str) -> String {
    let mut cursor = 0;
    while let Some(offset) = text[cursor..].find(prefix) {
        let start = cursor + offset;
        let end = text[start..]
            .char_indices()
            .take_while(|(_, character)| {
                character.is_ascii_alphanumeric() || matches!(character, '_' | '-' | '.')
            })
            .last()
            .map(|(offset, character)| start + offset + character.len_utf8())
            .unwrap_or(start);
        if end - start >= prefix.len() + 12 {
            text.replace_range(start..end, REDACTED);
            cursor = start + REDACTED.len();
        } else {
            cursor = start + prefix.len();
        }
    }
    text
}

#[cfg(test)]
mod tests {
    use super::Redactor;
    use crate::ipc::WorkerEvent;

    #[test]
    fn redacts_provider_sensitive_environment_and_private_key_material() {
        let redactor = Redactor::from_named_values(
            "provider-secret",
            [
                ("DATABASE_PASSWORD", "environment-secret"),
                ("PATH", "not-sensitive"),
            ],
        );
        let private_key = "-----BEGIN PRIVATE KEY-----\nprivate-body\n-----END PRIVATE KEY-----";
        let bearer = "Bearer bearer-token-value";
        let github_token = "ghp_1234567890abcdefghijklmnop";

        let redacted = redactor.redact_text(&format!(
            "provider-secret environment-secret not-sensitive {private_key} {bearer} {github_token}"
        ));

        assert!(!redacted.contains("provider-secret"));
        assert!(!redacted.contains("environment-secret"));
        assert!(!redacted.contains("private-body"));
        assert!(!redacted.contains("bearer-token-value"));
        assert!(!redacted.contains(github_token));
        assert!(redacted.contains("not-sensitive"));
        assert!(redacted.contains("[REDACTED]"));
    }

    #[test]
    fn redacts_every_string_bearing_worker_event_variant() {
        let redactor = Redactor::from_named_values("secret", std::iter::empty::<(&str, &str)>());
        let events = [
            WorkerEvent::TextDelta {
                text: "secret".to_owned(),
            },
            WorkerEvent::AssistantMessage {
                text: "secret".to_owned(),
            },
            WorkerEvent::ToolStart {
                call_id: "secret".to_owned(),
                name: "secret".to_owned(),
                arguments_json: "secret".to_owned(),
            },
            WorkerEvent::ToolResult {
                call_id: "secret".to_owned(),
                name: "secret".to_owned(),
                content: "secret".to_owned(),
                is_error: false,
            },
            WorkerEvent::NeedsApproval {
                call_id: "secret".to_owned(),
                tool: "secret".to_owned(),
                arguments_json: "secret".to_owned(),
            },
            WorkerEvent::Crash {
                message: "secret".to_owned(),
            },
        ];

        for event in events {
            let serialized = serde_json::to_string(&redactor.redact_event(event)).unwrap();
            assert!(!serialized.contains("secret"));
            assert!(serialized.contains("[REDACTED]"));
        }
    }

    #[test]
    fn secret_bearing_identifiers_get_stable_unique_aliases() {
        let redactor =
            Redactor::from_named_values("provider-secret", std::iter::empty::<(&str, &str)>());

        let first = redactor.redact_identifier("call-provider-secret-first");
        let repeated = redactor.redact_identifier("call-provider-secret-first");
        let second = redactor.redact_identifier("call-provider-secret-second");

        assert_eq!(first, repeated);
        assert_ne!(first, second);
        assert!(!first.contains("provider-secret"));
        assert!(!second.contains("provider-secret"));
    }
}
