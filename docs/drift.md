# Drift Detection

`verify` checks a destination right after a migration. `pag drift` keeps
checking it: it re-reads tables on demand or on an interval, compares them with
what they should hold, names the rows that changed, and can post the result to
Slack or any webhook.

---

## Table of Contents

- [What drift checks](#what-drift-checks)
- [Quick start](#quick-start)
- [pag drift](#pag-drift)
  - [Output](#output)
  - [Exit codes](#exit-codes)
  - [Watching](#watching)
  - [Run history](#run-history)
  - [While an apply runs](#while-an-apply-runs)
- [Imports: pag state](#imports-pag-state)
- [Pairs](#pairs)
  - [How a pair is compared](#how-a-pair-is-compared)
  - [Grace: rows still being copied](#grace-rows-still-being-copied)
- [Hooks](#hooks)
  - [Events and targets](#events-and-targets)
  - [Slack](#slack)
  - [Webhook](#webhook)
  - [Verifying the signature](#verifying-the-signature)
  - [Delivery and failures](#delivery-and-failures)
  - [Testing a hook](#testing-a-hook)
- [Storage](#storage)
- [Limitations](#limitations)

---

## What drift checks

One drift run checks every target it finds for the config, in this order:

| Target | Comes from | Compared with | Use it for |
| --- | --- | --- | --- |
| **Pipeline receipt** | `pag apply --integrity` | The receipt written during apply: the live destination must still hold exactly what was migrated | Catching edits to tables Paganel migrated |
| **Pair** | A [`pair`](ppl-reference.md#pair) block in the config | The live source table, read in the same run | Copies another tool keeps in sync (replication, CDC, ELT) |
| **Import** | `pag state import` | A baseline you took by hand | Tables Paganel never migrated: archives, legacy data, a snapshot before cutover |

Receipts and imports are frozen baselines: any write after them is drift. That
is right for tables that should not change, and noisy for tables that do. A
pair compares two live tables, so it tolerates writes as long as the target
keeps up with the source.

Pipelines without a receipt and imports whose connection is not in the config
are listed as skipped and do not affect the result.

---

## Quick start

```bash
# A migration with receipts
pag apply -c migration.ppl --integrity

# A table Paganel did not migrate, as a baseline
pag state import -c migration.ppl --connection dst --table legacy_events

# Check everything once
pag drift -c migration.ppl

# Or keep checking, every 15 minutes
pag drift -c migration.ppl --watch --interval 15m
```

A complete, commented config with a receipt, a pair and two hooks is in
[`examples/configs/drift-checks.ppl`](../examples/configs/drift-checks.ppl).
A pair needs only the block in the config:

```ppl
pair "orders" {
  source { connection = connection.app  table = "orders" }
  target { connection = connection.dw   table = "orders" }
}
```

---

## pag drift

```bash
pag drift -c <config> [--filters NAME]... [--json] [--allow-extra] [--watch [--interval 24h]]
pag drift -c <config> --notify-test [HOOK]
```

| Flag | Effect |
| --- | --- |
| `-c, --config <FILE>` | The PPL config (auto-discovered if omitted). |
| `--filters <NAME>` | Check only this pipeline, pair or import. Repeatable. An unknown name is an error. |
| `--json` | Print the run as JSON. With `--watch`, one compact line per run (NDJSON). |
| `--allow-extra` | Accept destination rows a receipt or import does not cover, instead of reporting them as `extra`. Pairs ignore it. |
| `--watch` | Run again after every `--interval` until interrupted. |
| `--interval <DURATION>` | Time between runs with `--watch`: a whole number and `s`, `m`, `h` or `d` (`30s`, `15m`, `6h`, `1d`). Default `24h`; requires `--watch`. |
| `--notify-test [HOOK]` | Send a sample through every hook, or only `HOOK`, and exit. See [Testing a hook](#testing-a-hook). |

Global flags (`-e, --env-file`, `-v`, `-q`, `--no-color`, `--log-level`,
`--log-file`) apply as for every command.

### Output

```
drift-20261106T133632.090Z (2026-11-06 13:36:32 UTC)
✓ orders/orders - match (127,491 rows, 2,841ms)
✗ pair customers (customers_raw) - MISMATCH, new since the last run (1 missing, 1 changed, 0 extra, 2 settled after grace)
  customer_id=7 - missing
  customer_id=42 - changed
? import legacy_events (legacy_events) - table 'legacy_events' not found
- film_actor/film_actor: skipped, no receipt; run `apply --integrity` first
→ hook alerts delivered (200, 1 attempt)
1 matched, 1 mismatched, 1 failed, 1 skipped
```

- `✓` matched, `✗` mismatched, `?` not compared (an error, or a receipt whose
  row-hash log is gone), `-` skipped.
- Targets are named `pipeline/table`, `pair <name> (<target table>)` and
  `import <name> (<table>)`.
- A mismatch lists the first diverging rows by key; the counts are always
  complete. **New since the last run** marks a mismatch the previous run of the
  same config did not have.
- Hook results follow the tables (see [Hooks](#hooks)).

`--json` prints the run as one document:

```json
{
  "run_id": "drift-20261106T133632.090Z",
  "config": "/srv/paganel/migration.ppl",
  "started_at": "2026-11-06T13:36:32.090Z",
  "finished_at": "2026-11-06T13:36:35.012Z",
  "tables": [ { "pipeline": "@pair/customers", "table": "customers_raw", "result": "mismatch", "missing": 1, "changed": 1, "extra": 0, "...": "..." } ],
  "skipped": [ { "name": "film_actor/film_actor", "reason": "no receipt; run `apply --integrity` first" } ],
  "notifications": [ { "hook": "alerts", "events": ["drift.diverged"], "ok": true, "status": 200, "attempts": 1, "error": null } ]
}
```

Each entry in `tables` is the same `TableReport` that `verify --json` prints.
Pairs are reported under `@pair/<name>` and imports under `@import/<name>`.
Pairs add a `recheck` field (`{"status": "done", "settled": 2}` or
`{"status": "skipped", "reason": "..."}`). `notifications` is left out when no
hook was sent.

### Exit codes

The same codes as `verify`:

| Code | Meaning |
| --- | --- |
| 0 | Every checked table matched. Skipped targets don't count. |
| 3 | At least one table mismatched. |
| 4 | Nothing mismatched, but at least one table could not be compared: an error (table gone, connection refused) or a row-hash log that is missing. |
| 1 | The command itself failed: bad config, unknown `--filters` name, or the config is being applied (see below). |

With `--watch` the command runs until it is stopped, so the codes above don't
apply: each run's result is in its output (or its `--json` line).

### Watching

`--watch` runs once, waits `--interval`, and repeats. Each run re-reads every
table. A run that fails as a whole (a database down, the config
being applied) is logged as `drift run failed` and the next interval tries
again, so a watcher outlives short outages. Ctrl+C stops it at once (exit 130);
a run cut short saves nothing.

### Run history

Every completed run is saved in the state store, newest 100 per config. The
history is what **new since the last run** compares against, so the first run
after a hand edit says so and later runs report the same mismatch without the
mark. Hook results are not part of the saved run.

### While an apply runs

Drift takes the config's run lock, like `verify`. While an `apply`, `resume` or
`verify` of the same config holds it, drift refuses to run and exits 1, because
the tables are still being written. Under
`--watch` that run is skipped and the next interval tries again. Different
configs don't block each other.

---

## Imports: pag state

An import hashes a table that Paganel did not migrate and stores the result as a
baseline, the same way `apply --integrity` stores a receipt. Drift then checks
the live table against it.

```bash
pag state import -c <config> --table <TABLE> [--connection NAME] [--name NAME] [--key a,b]
pag state list [--json]
pag state rm <NAME>
```

| Flag | Effect |
| --- | --- |
| `--table` | The table to import. |
| `--connection` | The `connection` block to read it through. Optional when the config has only one. |
| `--name` | The import's name, used by `--filters`, `state rm` and in reports. Defaults to the table name when that is a valid name: 1-64 letters, digits, `_` or `-`. It can't be a pipeline's name. |
| `--key` | Key columns, comma-separated, for a table without a primary key. Rows are matched by key, so one is required. |

```
$ pag state import -c migration.ppl --connection dst --table legacy_events
imported legacy_events as 'legacy_events' (dst): 500 rows, root 9f2c…
check it with: pag drift -c migration.ppl

$ pag state list
legacy_events  dst/legacy_events  500 rows  imported 2026-11-06 13:20:11 UTC

$ pag state rm legacy_events
removed import 'legacy_events'
```

- Importing under an existing name **replaces** that baseline, even if it was of
  another table. Use it to accept the current contents as the new baseline.
- `state rm` deletes the import's record, its receipt and its row hashes.
- Imports live in the state directory, not in a config. Drift checks an import
  when the config has a connection **with the same name** as the one it was
  imported through; otherwise it is listed as skipped.

---

## Pairs

The block's syntax is in the [PPL reference](ppl-reference.md#pair).

### How a pair is compared

Each run reads both tables in full, through each side's own `where`:

1. Columns are lined up by name (case-insensitive) or through `map`, after each
   side's `ignore`. A column left over on one side fails the pair with the names
   to add to `ignore` or `map`, so a schema change on either side is reported.
2. Both sides are read and hashed. Each table is paged by its own primary
   key (or `key`), and each row is hashed with the same canonical encoding as
   receipts ([verification.md](verification.md#canonical-row-serialization)), so
   a MySQL `TINYINT(1)` and a Postgres `BOOLEAN` holding the same value hash
   alike. Two tables on different connections are read at the same time; two on
   one connection one after the other.
3. The row hashes are sorted on disk and merge-joined by key, so memory stays
   flat whatever the table size. Each key is `missing` (source only), `extra`
   (target only) or `changed` (both, different contents).
4. Keys that differ are read again after `grace` (next section).

Only the run's report is kept; the scratch row hashes are deleted when the pair
is done.

### Grace: rows still being copied

Something else is copying source to target, with some lag. A row written to
the source a moment before the read is not in the target yet, and would read as
`missing` though nothing is wrong. `grace` covers that:

1. After the full read, if anything differs, drift waits `grace` (default
   `30s`).
2. It re-reads only the differing keys, on both sides
   (`WHERE key IN (...)`, 500 values per query).
3. A key that now matches, or is gone from both sides, is **settled** and taken
   out of the counts. One that still differs is reported, with its kind as of
   the re-read.

The report says what happened: `2 settled after grace`, or
`not re-checked: <reason>`:

| Reason | When |
| --- | --- |
| `grace is 0` | `grace = "0s"`. Use it when nothing writes to either table. |
| `N keys differ; over 1000 is not lag` | More than 1,000 keys differ. Replication lag doesn't look like that, and re-reading them would only slow the run. |
| `only integer, text and UUID keys can be re-read` | A key column of another type (date, decimal, ...). |

Set `grace` a little above the copy's normal lag. For a sync that runs every
15 minutes no sensible grace will settle it; run drift between syncs instead.

---

## Hooks

A [`hook`](ppl-reference.md#hook) block posts the outcome of each drift run to
Slack or to any HTTP endpoint.

```ppl
hook "orders_alert" {
  on     = ["drift.diverged", "drift.failed"]
  target = pair.orders

  slack { url = env("ORDERS_SLACK_URL") }
}

hook "audit" {
  on = ["drift.completed"]

  webhook {
    url    = env("AUDIT_URL")
    secret = env("AUDIT_SECRET")
  }
}
```

### Events and targets

| Event | Fires when |
| --- | --- |
| `drift.completed` | Every drift run, whatever the result (a heartbeat). |
| `drift.diverged` | A table the hook watches mismatched. |
| `drift.failed` | A table the hook watches could not be compared (error or missing row-hash log). |

`target` narrows a hook to one block: `pipeline.<name>` or `pair.<name>`. The
hook then fires only on that block's tables, and posts only those. Without
`target` it watches every table in the run, imports included, and its payload
also lists the skipped targets.

The rules:

- One POST per hook per run, whatever number of tables or events matched.
  Ten diverged tables are one Slack message.
- A hook reports only the events in its own `on`. A heartbeat hook on
  `drift.completed` alone says `drift.completed` even in a run where a table
  failed; the failure is in its body. Add `drift.failed` to its `on` to have it
  named.
- A hook whose target was not checked this run (filtered out, or a pipeline
  without a receipt) sends nothing.
- Adding or editing a hook doesn't change the config's `run_id`, so it can't
  disturb a resumable run.

### Slack

`slack { url = ... }` posts to a Slack [incoming webhook](https://api.slack.com/messaging/webhooks)
as plain text: a header, one line per problem, then one line for the tables that
match.

```
Paganel drift, 2026-11-06 13:36 UTC (orders_alert): 1 of 3 tables differ, 1 could not be checked
:red_circle: pair orders (orders) 1 changed, 1 missing. New since last run. e.g. id=7, id=42
:warning: import legacy (events) connection refused
:large_green_circle: orders/orders match
```

Messages are capped at 30 lines (`... and N more`); Slack truncates longer ones.
They are unsigned, since Slack doesn't check signatures. The URL itself is the
credential, so keep it in an environment variable.

### Webhook

`webhook { url = ... secret = ... }` posts a signed JSON envelope. `secret` is
required.

```
POST <url>
Content-Type: application/json
User-Agent: paganel
X-Paganel-Hook: audit
X-Paganel-Event: drift.completed
X-Paganel-Timestamp: 1791590400
X-Paganel-Signature: sha256=<hex HMAC-SHA256 of "<timestamp>.<body>">
```

```json
{
  "hook": "audit",
  "events": ["drift.completed"],
  "command": "drift",
  "run_id": "drift-20261106T133632.090Z",
  "started_at": "2026-11-06T13:36:32.090Z",
  "finished_at": "2026-11-06T13:36:35.012Z",
  "target": null,
  "test": false,
  "report": {
    "tables": [ { "pipeline": "@pair/orders", "table": "orders", "result": "mismatch", "...": "..." } ],
    "skipped": [ { "name": "film_actor/film_actor", "reason": "no receipt; run `apply --integrity` first" } ]
  }
}
```

- `events` (and `X-Paganel-Event`, comma-joined) are the hook's events that
  fired in this run.
- `target` is the hook's target as written in the config (`"pair.orders"`), or
  `null`.
- `test` is `true` only for `--notify-test`.
- `report.tables` holds the watched tables, each the same `TableReport` as
  `pag drift --json`. `report.skipped` is only present for a hook without a
  target.
- `command` is `"drift"` today; other commands will use the same envelope with
  their own `report`.

### Verifying the signature

Recompute the HMAC over the timestamp, a `.`, and the **raw** body as received
(before any JSON parsing), compare in constant time, and reject timestamps more
than 5 minutes from your clock: a replayed request carries an old one.

Python:

```python
import hashlib, hmac, time

def verify(headers, body: bytes, secret: str, tolerance: int = 300) -> bool:
    timestamp = headers["X-Paganel-Timestamp"]
    if abs(time.time() - int(timestamp)) > tolerance:
        return False  # too old or from the future: a replay
    mac = hmac.new(secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256)
    return hmac.compare_digest("sha256=" + mac.hexdigest(), headers["X-Paganel-Signature"])
```

Node:

```js
const crypto = require("node:crypto");

function verify(headers, rawBody, secret, toleranceSec = 300) {
  const timestamp = headers["x-paganel-timestamp"];
  if (Math.abs(Date.now() / 1000 - Number(timestamp)) > toleranceSec) return false;
  const expected = Buffer.from(
    "sha256=" +
      crypto.createHmac("sha256", secret).update(`${timestamp}.`).update(rawBody).digest("hex"),
  );
  const received = Buffer.from(headers["x-paganel-signature"] ?? "");
  return received.length === expected.length && crypto.timingSafeEqual(received, expected);
}
```

Go:

```go
func verify(h http.Header, body []byte, secret string, tolerance time.Duration) bool {
	timestamp := h.Get("X-Paganel-Timestamp")
	sec, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return false
	}
	if age := time.Since(time.Unix(sec, 0)); age > tolerance || age < -tolerance {
		return false // too old or from the future: a replay
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "."))
	mac.Write(body)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(h.Get("X-Paganel-Signature")))
}
```

A known vector to test against: secret `s3cret`, timestamp `1791590400`, body
`{"hook":"audit"}` gives
`sha256=ab18fc6b00f35a1f70f8481d27024f0b5a06ef7445294b5b3b7a091127a02b88`.

### Delivery and failures

- Hooks are sent after the run is saved, all at once.
- Each POST gets up to 3 attempts, 10 seconds each, with 1s and 2s between
  them. Connection errors, timeouts, `429` and `5xx` are retried; any other
  status is final.
- A failed hook never changes the run's result or exit code. It is logged and
  shown after the tables:

  ```
  ✗ hook audit failed: Connection Failed after 3 attempts
  ✗ hook alerts not sent: `secret` is not set: Missing required environment variable: AUDIT_SECRET
  ```

- Logs and output show the host, never the URL, because a Slack URL is the
  credential.
- A hook setting whose `env(...)` is not set does not stop the config from
  loading. Only that hook fails, when it is sent, and `pag plan` flags it. So a
  Slack URL exported only on the machine that runs `pag drift --watch` doesn't
  break `pag apply` elsewhere.

### Testing a hook

```bash
pag drift -c migration.ppl --notify-test          # every hook
pag drift -c migration.ppl --notify-test audit    # one hook
```

`--notify-test` sends each hook a sample without reading any table: every event
in its `on`, `"test": true`, and a report with one matching and one mismatched
table named after the hook's target. Slack text starts with `[test]`. It prints one line per
hook and exits 1 if any could not be delivered. Use it to set up a channel or a
receiver without waiting for real drift.

---

## Storage

Everything lives in the state directory (`~/.paganel/state/`):

| What | Where | Size |
| --- | --- | --- |
| Imports | `imports` table and an `@import/<name>` receipt in `state.db` | A row each |
| Import baselines | `rowhash/baseline/@import_<name>/<table>/` | About 51 bytes per row, kept until `state rm` or a re-import |
| Drift history | `drift_runs` table in `state.db` | Newest 100 runs per config |
| Scratch during a run | `rowhash/verify/` (receipts, imports, pair targets) and `rowhash/source/` (pair sources) | About 51 bytes per row being checked; deleted when the table is done |

A pair on a 10M-row table needs roughly 1 GB of free disk while it runs (both
sides).

---

## Limitations

**Drift**

- **Every run is a full read.** Each target is re-read in full every time;
  there is no incremental mode, checksum by key range, or CDC. Check how long a
  run takes on your largest table before choosing `--interval`.
- **Receipts and imports flag any change.** They are frozen baselines, so every
  write after them counts. For a table that is written to, use a pair, or
  re-import to move the baseline.
- **One table at a time.** A run checks its tables one after another.
- **Ctrl+C stops at once.** A run cut short saves nothing and sends no hooks.
- **MySQL and PostgreSQL only.** Drift reads tables through those two drivers;
  an import or a pair can't use a CSV connection.

**Imports**

- **A key is required.** A table without a primary key needs `--key`; rows are
  matched by key, and without one a changed row would read as one missing and one
  extra.
- **Whole tables only.** There is no `where` for an import.
- **Matched to a config by connection name.** Drift checks an import against
  whatever the config's connection of that name points at. Two configs using
  the same name for different databases would both check it.
- **No lock between configs.** The run lock is per config; two configs checking
  the same import or pair at the same time share its scratch files.
- **An import can share a name with a pair.** `--filters` then selects both.

**Pairs**

- **No snapshot.** Each side is read in pages over time, not in one
  transaction, so rows written during the read can show as differences. `grace`
  settles those that catch up; on a busy table, expect some noise.
- **`map` renames only.** It lines up two columns; it doesn't convert values
  or compute them. Columns whose values differ by design (a sync timestamp, a
  surrogate key) go in both sides' `ignore`.
- **`where` is a plain filter.** Comparisons, `and`/`or`, grouping and
  `is [not] null` on the side's own columns. Function calls aren't supported.
- **Re-check limits.** At most 1,000 differing keys, integer, text and UUID keys
  only. A composite key is re-read by its first column, so more rows than needed
  are read back.
- **A changed row during the wait is missed.** The re-check only re-reads keys
  that differed in the first read.

**Hooks**

- **Drift only.** `apply.*` and `verify.*` events are reserved names, refused as
  "not supported yet".
- **Once per run, at the end.** No per-table or immediate alerts.
- **One target, one delivery.** `target` names one block (several tables of one
  team need several hooks), and a hook has one `slack` or `webhook` block. Slack
  plus an audit endpoint is two hooks.
- **Imports can't be targeted.** Only a hook without `target` covers them.
- **No custom headers or auth.** The HMAC signature is the only authentication.
- **Slack text is plain.** No Block Kit formatting, links or grouping.
- **Results are not kept.** They are in the run's output and `--json`, not in the
  saved history.
