#[cfg(test)]
mod tests {
    use crate::{harness::Direction, verify::support::drift_run};
    use engine_core::{context::env::EnvContext, plan::execution::ExecutionPlan};
    use engine_infra::hooks::{deliver, signature};
    use engine_verify::hooks::drift_notifications;
    use model::{hook::result::HookDeliveryResult, integrity::drift::DriftRun};
    use ppl_syntax::builder::parse;
    use serde_json::Value;
    use std::{
        io::{BufRead, BufReader, Read, Write},
        net::TcpListener,
        sync::{Arc, mpsc},
        thread,
    };
    use tracing_test::traced_test;

    const SECRET: &str = "e2e-secret";

    /// One POST as the receiver saw it.
    struct Received {
        path: String,
        headers: Vec<(String, String)>,
        body: Value,
        /// `None` without a signature header, else whether it verifies.
        verified: Option<bool>,
    }

    impl Received {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.as_str())
        }
    }

    /// A receiver that answers every POST with 200 and hands it back, with
    /// its signature checked against `SECRET`. Lives until the test ends.
    fn receiver() -> (String, mpsc::Receiver<Received>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (tx, rx) = mpsc::channel();

        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut reader = BufReader::new(stream.try_clone().unwrap());

                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let path = line.split_whitespace().nth(1).unwrap_or("").to_string();

                let mut headers = Vec::new();
                loop {
                    line.clear();
                    reader.read_line(&mut line).unwrap();
                    let Some((k, v)) = line.trim_end().split_once(": ") else {
                        break;
                    };
                    headers.push((k.to_ascii_lowercase(), v.to_string()));
                }
                let header = |name: &str| {
                    headers
                        .iter()
                        .find(|(k, _)| k == name)
                        .map(|(_, v)| v.clone())
                };
                let len: usize = header("content-length").map_or(0, |v| v.parse().unwrap());
                let mut body = vec![0; len];
                reader.read_exact(&mut body).unwrap();

                let verified = header("x-paganel-signature").map(|sig| {
                    let timestamp: i64 = header("x-paganel-timestamp").unwrap().parse().unwrap();
                    sig == format!("sha256={}", signature(SECRET, timestamp, &body))
                });

                // Hand it over before answering, so it is in the channel by
                // the time `deliver` returns.
                let _ = tx.send(Received {
                    path,
                    headers,
                    body: serde_json::from_slice(&body).unwrap(),
                    verified,
                });

                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
            }
        });

        (base, rx)
    }

    /// What `pag drift` does after a run: build the plan's hooks, match them
    /// against `run`, and post each one.
    async fn notify(ppl: &str, run: &DriftRun) -> Vec<HookDeliveryResult> {
        let doc = parse(ppl).expect("parse");
        let plan = ExecutionPlan::build(&doc, Arc::new(EnvContext::empty())).expect("plan");

        let mut results = Vec::new();
        for n in drift_notifications(run, &plan.hooks) {
            let hook = plan.hooks.iter().find(|h| h.name == n.hook).unwrap();
            results.push(deliver(&hook.delivery, &n).await);
        }
        results
    }

    /// Two copies of the same 100 rows, and a pair over them.
    async fn seed(dir: Direction) {
        dir.reset().await;
        for table in ["hooks_src", "hooks_dst"] {
            dir.dst_execute(&format!(
                "CREATE TABLE {table} (id INT PRIMARY KEY, amount NUMERIC(10,2), note TEXT);
                 INSERT INTO {table} SELECT g, g * 1.5, 'row ' || g FROM generate_series(1, 100) g"
            ))
            .await;
        }
    }

    fn config(dir: Direction, base: &str) -> String {
        dir.ppl(&format!(
            r#"
            pair "orders" {{
                source {{ connection = connection.dst table = "hooks_src" }}
                target {{ connection = connection.dst table = "hooks_dst" }}
                grace  = "0s"
            }}

            pair "broken" {{
                source {{ connection = connection.dst table = "hooks_src" }}
                target {{ connection = connection.dst table = "hooks_gone" }}
            }}

            hook "orders_alert" {{
                on     = ["drift.diverged", "drift.failed"]
                target = pair.orders
                slack {{ url = "{base}/slack" }}
            }}

            hook "broken_alert" {{
                on     = ["drift.failed"]
                target = pair.broken
                slack {{ url = "{base}/broken" }}
            }}

            hook "audit" {{
                on = ["drift.completed"]
                webhook {{
                    url    = "{base}/audit"
                    secret = "{SECRET}"
                }}
            }}
            "#
        ))
    }

    /// A real drift run fires exactly the hooks its results call for: on a
    /// clean pair only the heartbeat, after a hand edit the pair's alert too,
    /// naming the rows. A target that cannot be checked fires `drift.failed`.
    /// Every webhook body is signed, and each hook sees only its own tables.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn hooks_post_what_a_real_drift_run_found() {
        let dir = Direction::POSTGRES_TO_POSTGRES;
        seed(dir).await;
        let (base, rx) = receiver();
        let ppl = config(dir, &base);

        // Clean: `orders` matches, `broken` fails.
        let run = drift_run(&ppl, &[]).await;
        assert_eq!(run.exit_code(), 4, "broken cannot be checked");
        let results = notify(&ppl, &run).await;
        assert!(results.iter().all(|r| r.ok), "{results:?}");

        let mut got: Vec<Received> = rx.try_iter().collect();
        got.sort_by(|a, b| a.path.cmp(&b.path));
        let paths: Vec<&str> = got.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, ["/audit", "/broken"], "orders_alert stays quiet");

        let audit = &got[0];
        assert_eq!(audit.verified, Some(true));
        assert_eq!(audit.header("x-paganel-event"), Some("drift.completed"));
        assert_eq!(audit.body["target"], Value::Null);
        assert_eq!(audit.body["test"], false);
        let results: Vec<&str> = audit.body["report"]["tables"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["result"].as_str().unwrap())
            .collect();
        assert_eq!(results, ["match", "error"]);

        let broken = &got[1];
        assert_eq!(broken.verified, None, "Slack is not signed");
        let text = broken.body["text"].as_str().unwrap();
        assert!(
            text.contains("0 of 1 tables differ, 1 could not be checked"),
            "{text}"
        );
        assert!(
            text.contains(":warning: pair broken (hooks_gone)"),
            "{text}"
        );

        // Tamper: the pair now differs by two rows.
        dir.dst_execute("DELETE FROM hooks_dst WHERE id = 7").await;
        dir.dst_execute("UPDATE hooks_dst SET amount = 0 WHERE id = 42")
            .await;

        let run = drift_run(&ppl, &["orders"]).await;
        assert_eq!(run.exit_code(), 3);
        let results = notify(&ppl, &run).await;
        assert!(results.iter().all(|r| r.ok), "{results:?}");

        let mut got: Vec<Received> = rx.try_iter().collect();
        got.sort_by(|a, b| a.path.cmp(&b.path));
        let paths: Vec<&str> = got.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, ["/audit", "/slack"]);

        let audit = &got[0];
        assert_eq!(audit.verified, Some(true));
        let orders = &audit.body["report"]["tables"][0];
        assert_eq!(orders["pipeline"], "@pair/orders");
        assert_eq!(orders["result"], "mismatch");
        assert_eq!(
            (&orders["missing"], &orders["changed"]),
            (&1.into(), &1.into())
        );

        let text = got[1].body["text"].as_str().unwrap();
        assert!(
            text.contains(":red_circle: pair orders (hooks_dst) 1 changed, 1 missing"),
            "{text}"
        );
        assert!(text.contains("e.g. id=7, id=42"), "{text}");
    }

    /// A hook nobody answers fails on its own: three attempts, the error
    /// without the URL, and the run's result untouched.
    #[traced_test]
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unreachable_hook_does_not_change_the_run() {
        let dir = Direction::POSTGRES_TO_POSTGRES;
        seed(dir).await;

        // A port that was free a moment ago: connections are refused.
        let closed = {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            format!("http://{}", listener.local_addr().unwrap())
        };
        let ppl = config(dir, &closed);

        let run = drift_run(&ppl, &["orders"]).await;
        let results = notify(&ppl, &run).await;

        assert_eq!(run.exit_code(), 0);
        let [audit] = results.as_slice() else {
            panic!("only audit fires on a clean pair: {results:?}");
        };
        assert!(!audit.ok);
        assert_eq!((audit.status, audit.attempts), (None, 3));
        let error = audit.error.as_deref().unwrap_or_default();
        assert!(!error.contains("127.0.0.1"), "the URL leaked: {error}");
    }
}
