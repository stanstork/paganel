use crate::hooks::Notification;
use crate::hooks::sign::signature;
use chrono::Utc;
use model::hook::delivery::Delivery;
use std::{fmt, sync::OnceLock, time::Duration};

/// A POST ready to send.
#[derive(Clone)]
pub(crate) struct Request {
    url: String,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
    timeout: Duration,
}

#[derive(Debug)]
pub(crate) struct PostError {
    pub(crate) status: Option<u16>,
    pub(crate) message: String,
    pub(crate) retry: bool,
}

impl Request {
    pub(crate) fn build(
        delivery: &Delivery,
        n: &Notification,
        timeout: Duration,
    ) -> Result<Self, String> {
        let url = delivery
            .url()
            .get()
            .map_err(|why| format!("`url` is not set: {why}"))?
            .to_string();

        let (body, headers) = match delivery {
            Delivery::Slack { .. } => {
                let body = serde_json::to_vec(&serde_json::json!({ "text": n.text }))
                    .map_err(|e| e.to_string())?;
                let headers = vec![("Content-Type", "application/json".to_string())];

                (body, headers)
            }

            Delivery::Webhook { secret, .. } => {
                let secret = secret
                    .get()
                    .map_err(|why| format!("`secret` is not set: {why}"))?;

                let timestamp = Utc::now().timestamp();
                let events = n
                    .events
                    .iter()
                    .map(|e| e.as_str())
                    .collect::<Vec<_>>()
                    .join(",");
                let sign_header = format!("sha256={}", signature(secret, timestamp, &n.json));

                let headers = vec![
                    ("Content-Type", "application/json".to_string()),
                    ("X-Paganel-Hook", n.hook.clone()),
                    ("X-Paganel-Event", events),
                    ("X-Paganel-Timestamp", timestamp.to_string()),
                    ("X-Paganel-Signature", sign_header),
                ];

                (n.json.clone(), headers)
            }
        };

        Ok(Self {
            url,
            headers,
            body,
            timeout,
        })
    }

    pub(crate) fn post(&self) -> Result<u16, PostError> {
        let request = self.headers.iter().fold(
            agent().post(&self.url).timeout(self.timeout),
            |req, (name, value)| req.set(name, value),
        );

        match request.send_bytes(&self.body) {
            Ok(response) => Ok(response.status()),
            Err(ureq::Error::Status(code, _)) => Err(PostError {
                status: Some(code),
                message: format!("HTTP {code}"),
                // 429 Too Many Requests or 5xx Server Errors
                retry: code == 429 || (500..=599).contains(&code),
            }),
            Err(ureq::Error::Transport(t)) => Err(PostError::transport(t.kind().to_string())),
        }
    }
}

impl PostError {
    pub(crate) fn transport(message: String) -> Self {
        Self {
            status: None,
            message,
            retry: true,
        }
    }
}

impl fmt::Display for PostError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

fn agent() -> &'static ureq::Agent {
    static AGENT: OnceLock<ureq::Agent> = OnceLock::new();
    AGENT.get_or_init(|| ureq::AgentBuilder::new().user_agent("paganel").build())
}
