use crate::{
    hooks::req::{PostError, Request},
    retry::{RetryDisposition, RetryPolicy},
};
use chrono::{DateTime, Utc};
use model::hook::{delivery::Delivery, event::HookEvent, result::HookDeliveryResult};
use serde::Serialize;
use std::time::Duration;
use tracing::{info, warn};

mod req;
mod sign;

pub use sign::signature;

/// Per attempt: a hook must never hold up the next run for long.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);

/// One hook's POST, rendered by the command.
#[derive(Debug, Clone)]
pub struct Notification {
    pub hook: String,
    pub events: Vec<HookEvent>,
    pub json: Vec<u8>,
    pub text: String,
}

/// The JSON body every command posts to a `webhook`.
#[derive(Debug, Serialize)]
pub struct Envelope<'a, R: Serialize> {
    pub hook: &'a str,
    pub events: &'a [HookEvent],
    pub command: &'static str,
    pub run_id: &'a str,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub target: Option<String>,
    /// Sent by `--notify-test`, not by a real run.
    pub test: bool,
    pub report: R,
}

pub async fn deliver(delivery: &Delivery, n: &Notification) -> HookDeliveryResult {
    let policy = RetryPolicy::new(3, Duration::from_secs(1), Duration::from_secs(16));
    deliver_with(delivery, n, &policy, ATTEMPT_TIMEOUT).await
}

async fn deliver_with(
    delivery: &Delivery,
    n: &Notification,
    policy: &RetryPolicy,
    timeout: Duration,
) -> HookDeliveryResult {
    let host = delivery.host().unwrap_or("?");

    let request = match Request::build(delivery, n, timeout) {
        Ok(request) => request,
        Err(e) => {
            warn!(hook = %n.hook, error = %e, "hook not sent");
            return HookDeliveryResult {
                hook: n.hook.clone(),
                events: n.events.clone(),
                ok: false,
                status: None,
                attempts: 0,
                error: Some(e),
            };
        }
    };

    let mut attempts = 0u32;
    let result = policy
        .run(
            || {
                attempts += 1;
                let request = request.clone();
                async move {
                    tokio::task::spawn_blocking(move || request.post())
                        .await
                        .unwrap_or_else(|e| Err(PostError::transport(e.to_string())))
                }
            },
            |e: &PostError| {
                if e.retry {
                    RetryDisposition::Retry
                } else {
                    RetryDisposition::Stop
                }
            },
        )
        .await;

    let (ok, status, error) = match result {
        Ok(status) => {
            info!(hook = %n.hook, %host, status, attempts, "hook delivered");
            (true, Some(status), None)
        }
        Err(e) => {
            let e = e.into_inner();
            warn!(hook = %n.hook, %host, attempts, error = %e, "hook delivery failed");
            (false, e.status, Some(e.message))
        }
    };

    HookDeliveryResult {
        hook: n.hook.clone(),
        events: n.events.clone(),
        ok,
        status,
        attempts,
        error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::sign::signature;
    use model::hook::delivery::Setting;
    use std::{
        io::{BufRead, BufReader, Read, Write},
        net::TcpListener,
        sync::mpsc,
        thread,
    };

    /// What the listener received: headers (lower-cased names) and body.
    struct Received {
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    impl Received {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.as_str())
        }
    }

    /// Answer each connection with the next of `statuses`; `0` never answers,
    /// so the attempt times out.
    fn serve(statuses: Vec<u16>) -> (String, mpsc::Receiver<Received>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/hook", listener.local_addr().unwrap());
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            for status in statuses {
                let (stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());

                let mut headers = Vec::new();
                let mut line = String::new();
                reader.read_line(&mut line).unwrap(); // request line
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    let Some((k, v)) = line.trim_end().split_once(": ") else {
                        break;
                    };
                    headers.push((k.to_ascii_lowercase(), v.to_string()));
                }
                let len: usize = headers
                    .iter()
                    .find(|(k, _)| k == "content-length")
                    .map_or(0, |(_, v)| v.parse().unwrap());
                let mut body = vec![0; len];
                reader.read_exact(&mut body).unwrap();
                tx.send(Received { headers, body }).unwrap();

                if status == 0 {
                    thread::sleep(Duration::from_secs(2));
                    continue;
                }
                let mut stream = stream;
                write!(
                    stream,
                    "HTTP/1.1 {status} X\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
            }
        });

        (url, rx)
    }

    fn notification() -> Notification {
        Notification {
            hook: "audit".into(),
            events: vec![HookEvent::DriftDiverged, HookEvent::DriftCompleted],
            json: br#"{"hook":"audit"}"#.to_vec(),
            text: "Paganel drift: 1 of 1 tables differ".into(),
        }
    }

    async fn send(delivery: &Delivery) -> HookDeliveryResult {
        let fast = RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(1));
        deliver_with(delivery, &notification(), &fast, Duration::from_millis(300)).await
    }

    fn webhook(url: String) -> Delivery {
        Delivery::Webhook {
            url: Setting::Value(url),
            secret: Setting::Value("s3cret".into()),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_webhook_is_signed_over_timestamp_and_body() {
        let (url, rx) = serve(vec![200]);
        let outcome = send(&webhook(url)).await;

        assert!(outcome.ok, "{outcome:?}");
        assert_eq!((outcome.status, outcome.attempts), (Some(200), 1));

        let got = rx.recv().unwrap();
        assert_eq!(got.body, br#"{"hook":"audit"}"#);
        assert_eq!(got.header("x-paganel-hook"), Some("audit"));
        assert_eq!(
            got.header("x-paganel-event"),
            Some("drift.diverged,drift.completed")
        );

        let timestamp: i64 = got.header("x-paganel-timestamp").unwrap().parse().unwrap();
        let expected = format!("sha256={}", signature("s3cret", timestamp, &got.body));
        assert_eq!(got.header("x-paganel-signature"), Some(expected.as_str()));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn slack_gets_text_and_no_signature() {
        let (url, rx) = serve(vec![200]);
        let outcome = send(&Delivery::Slack {
            url: Setting::Value(url),
        })
        .await;
        assert!(outcome.ok, "{outcome:?}");

        let got = rx.recv().unwrap();
        let body: serde_json::Value = serde_json::from_slice(&got.body).unwrap();
        assert_eq!(body["text"], "Paganel drift: 1 of 1 tables differ");
        assert_eq!(got.header("x-paganel-signature"), None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn retries_only_what_can_succeed_later() {
        let cases: [(Vec<u16>, bool, Option<u16>, u32); 4] = [
            (vec![400], false, Some(400), 1),
            (vec![503, 200], true, Some(200), 2),
            (vec![429, 429, 429], false, Some(429), 3),
            (vec![0, 0, 0], false, None, 3),
        ];

        for (statuses, ok, status, attempts) in cases {
            let (url, _rx) = serve(statuses.clone());
            let outcome = send(&webhook(url)).await;

            assert_eq!(
                (outcome.ok, outcome.status, outcome.attempts),
                (ok, status, attempts),
                "{statuses:?}: {outcome:?}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unset_url_is_reported_without_sending() {
        let outcome = send(&Delivery::Slack {
            url: Setting::Unset("Missing required environment variable: SLACK_URL".into()),
        })
        .await;

        assert!(!outcome.ok);
        assert_eq!(outcome.attempts, 0);
        assert!(outcome.error.unwrap().contains("SLACK_URL"));
    }
}
