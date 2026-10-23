# Output Modes

How `pag apply`, `pag verify`, and `pag receipt` report progress and results in the
terminal, and how to pick the mode that fits your context (CI, an interactive
terminal, or a live migration you want to watch).

---

## Table of Contents

- [apply: three ways to watch a migration](#apply-three-ways-to-watch-a-migration)
  - [Default (log) mode](#default-log-mode)
  - [`--pretty` mode](#--pretty-mode)
  - [`--tui` mode](#--tui-mode)
- [The TUI dashboard](#the-tui-dashboard)
  - [Estimated vs actual row counts](#estimated-vs-actual-row-counts)
  - [Throughput, peak, and ETA](#throughput-peak-and-eta)
  - [Keyboard controls](#keyboard-controls)
  - [Pause, resume, and cancel](#pause-resume-and-cancel)
  - [Responsive columns](#responsive-columns)
  - [Integrity finalization](#integrity-finalization)
  - [Resuming from a checkpoint](#resuming-from-a-checkpoint)
  - [Already-completed runs](#already-completed-runs)
- [verify output](#verify-output)
  - [Default output](#default-output)
  - [`--pretty` output](#--pretty-output)
  - [`--json` output](#--json-output)
- [receipt output](#receipt-output)
- [Colors and `--no-color`](#colors-and---no-color)
- [Choosing a mode](#choosing-a-mode)

---

## apply: three ways to watch a migration

`apply` has three mutually exclusive output modes. Passing both `--tui` and
`--pretty` is an error.

| Mode | Flag | Best for |
|------|------|----------|
| Log (default) | *(none)* | CI, cron, redirected output, `RUST_LOG` debugging |
| Pretty | `--pretty` | An interactive terminal where you want readable, colored progress |
| TUI | `--tui` | Watching a live migration with per-pipeline progress and controls |

### Default (log) mode

With no flag, `apply` emits structured `tracing` logs and nothing else. This is
the mode for CI, cron, and anything that redirects output: it's line-oriented,
greppable, and honors `RUST_LOG` / `--log-level`.

```bash
pag apply -c migration.ppl
```

```
INFO executing migration config=migration.ppl
INFO migration completed successfully
```

Turn up the detail with `-v` / `-vv` or `--log-level debug` for the full event
stream (per-batch progress, checkpoints, retries).

### `--pretty` mode

`--pretty` prints a colored, symbol-tagged line per event to stdout, each with an
elapsed-time stamp. It's meant for a human watching an interactive terminal.

```bash
pag apply -c migration.ppl --pretty
```

```
[  0.001s] ▶ Starting migration: run-1
[  0.002s] ◉ Pipeline 'migrate_actor' started (snapshot mode)
[  0.140s] -> migrate_actor 1,000 rows
[  1.320s] -> migrate_actor 500,000 rows, 12 skipped, 3 failed
[  2.500s] ✓ Pipeline 'migrate_actor' completed: 1,000,000 rows, 12 skipped, 3 failed in 2.50s (400,000/s)
[  2.505s] ✓ Migration completed!
   Total:      1,000,000 rows, 12 skipped, 3 failed
   Pipelines:  1
   Duration:   2.51s
   Throughput: 398,406/s
```

The line symbols: `▶` run start, `◉` pipeline start, `->` progress, `✓` success,
`✗` failure, `◆`/`⧗` integrity finalization (see below). Row counts and
throughput are thousands-separated.

In pretty mode the raw log stream is routed to `~/.paganel/pretty.log` instead of
the terminal, so the colored output stays clean. Tail that file if you need the
underlying logs.

### `--tui` mode

`--tui` takes over the terminal with a full-screen dashboard: a per-pipeline
table, an execution-stage map, and live aggregate panels for progress,
throughput, timing, and data volume.

```bash
pag apply -c migration.ppl --tui
```

See [The TUI dashboard](#the-tui-dashboard) for the full walkthrough.

---

## The TUI dashboard

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│ PAGANEL  RUNNING                                                                  View: Overview │
│                                                                                                  │
│Pipeline          Status        Progress                        Rows          Rate        ETA     │
│> migrate_actor   ✔ Done        [████████████████████]  100%    200           4.6K/s      0s      │
│  migrate_custom  ▶ Running     [█████████           ]   50%    298/599       305/s       0s      │
│  migrate_paymen  ○ Pending     [                    ]    0%    0/16.0K       --/s        --      │
│                                                                                                  │
│                                                                                                  │
│                                                                                                  │
│                                                                                                  │
│                                                                                                  │
│                                                                                                  │
│┌ Execution Stages ──────────────────────────────────────────────────────────────────────────────┐│
││Stage 0: migrate_actor                                                                          ││
││Stage 1: migrate_customer ●                                                                     ││
││Stage 2: migrate_payment                                                                        ││
││                                                                                                ││
││                                                                                                ││
││                                                                                                ││
││                                                                                                ││
│└────────────────────────────────────────────────────────────────────────────────────────────────┘│
│┌ Progress ─────────────┐┌ Throughput ──────────┐┌ Timing ───────────────┐┌ Data Volume ─────────┐│
││░░░░░░░░░░░░░░░░ 3.0%  ││Rate: 305/s           ││Elapsed: 1s            ││Volume: 59.58 KB      ││
││1 / 3 pipelines        ││Peak: 305/s           ││ETA:     52s           ││Rate:   29.82 KB/s    ││
││Rows: 498 / 16.7K      ││ █                    ││                       ││Peak:   33.79 KB/s    ││
│└───────────────────────┘└──────────────────────┘└───────────────────────┘└──────────────────────┘│
│                                                                                                  │
│                             [Q]uit  [Tab]View  [Space]Pause  [C]ancel                            │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
```

*A real run captured at 100x30: Sakila to PostgreSQL, three pipelines where
`migrate_payment` waits on `migrate_customer`, so one is done, one is mid-copy
and one is still queued.*

**Views** (cycle with `Tab`, or jump with `1`-`4`):

- **Overview** - the pipeline table plus the aggregate dashboard (shown above).
- **Pipeline Detail** - a single pipeline in depth; select it with `↑`/`↓` first.
- **Errors** - failed pipelines and their error messages.
- **Help** - the keyboard reference.

### Estimated vs actual row counts

The totals the dashboard shows progress *against* (the `/N` in the **Rows**
column, the denominator in the **Progress** panel, and every percentage and ETA
derived from them) are estimates. They come from the source's row-count
statistics gathered at plan time, not an exact `COUNT(*)`, so they can be off in
either direction (statistics lag; filtered or freshly-written tables are the
usual culprits). The processed-row counts are exact; only the target is an
estimate.

Two consequences to expect:

- A pipeline can reach or pass its estimated total before it actually finishes.
  The aggregate **Progress** bar caps at 100% so it never shows more than fully
  done, and a pipeline is marked **Completed** only when its producer genuinely
  finishes, not when it crosses the estimate.
- The final row count reported at completion is the real number written, which
  may differ from the estimate you watched during the run.

If you need proof that the destination holds exactly what was written, that's
what [`pag verify`](#verify-output) and `--integrity` are for.
The dashboard estimate is for progress feedback.

### Throughput, peak, and ETA

**Rate** is a sliding 30-second window over progress samples, so it tracks what a
pipeline is doing now rather than its average since the start. The window needs
two samples before it can divide anything, and a finished pipeline stops
producing samples at all, so in both cases the dashboard falls back to the
pipeline's own average: rows it moved this run over the time it ran. That is why
a completed pipeline keeps showing the rate it achieved instead of going blank,
and why a migration that finishes in a second still reports a number.

A pipeline that has not started shows `--/s`: nothing has moved, so there is
nothing to average. **ETA** is derived from the same rate and stays `--` until
there is one.

The aggregate **Throughput** panel sums the rates of the pipelines currently
running. When none are (the run has ended, or every pipeline finished before the
window could sample) it falls back to the whole run's average, so it agrees with
the per-pipeline column beside it.

### Keyboard controls

| Key | Action |
|-----|--------|
| `↑` / `↓` | Select a pipeline |
| `Tab` | Switch views |
| `1`-`4` | Jump to Overview / Detail / Errors / Help |
| `Space` | Pause the migration (drains the current batch, then checkpoints) |
| `c` | Cancel the migration (asks to confirm) |
| `q` | Quit the app |

### Pause, resume, and cancel

These three are deliberately distinct:

- **Pause** (`Space`) is graceful. The in-flight batch is allowed to drain, a
  checkpoint is written, and the run is marked paused. Re-running the same
  command later continues from that checkpoint. The dashboard freezes its stats
  and shows a notice explaining how to resume.
- **Cancel** (`c`) stops the migration and *stays* in the dashboard so you can
  read the final state. Data already written is checkpointed, so a later re-run
  still resumes. Because it ends the run, it asks for confirmation (`[y]`/`[n]`).
- **Quit** (`q`) leaves the application. If a migration is still running it first
  requests a graceful stop ("Stopping…") before exiting.

There is no live resume or per-pipeline retry inside the TUI; resuming is always
"re-run the same command", which picks up from the last checkpoint.

### Responsive columns

The pipeline table adapts to terminal width. The **Pipeline** name and **Status**
columns are always present; secondary columns drop as space runs out:

| Column | Shown when width ≥ |
|--------|--------------------|
| Progress bar | 74 |
| Rate | 86 |
| ETA | 96 |

At an 80×24 terminal (the classic minimum) you keep the name, status, progress
bar, and rows; rate and ETA are dropped.

The progress bars and row totals are measured against *estimated* source counts;
see [Estimated vs actual row counts](#estimated-vs-actual-row-counts) above.

### Integrity finalization

When you run with `--integrity`, each pipeline seals its keyed Merkle receipts
*after* its data is written. The TUI surfaces this as a modal so you don't mistake
the sealing pause for a hang, and so completion isn't declared prematurely:

```
┌────────────── ⧗ SEALING INTEGRITY RECEIPTS ──────────────┐
│                                                          │
│ Sealing 'orders'…                                        │
│                                                          │
│ Receipts committed:  2                                   │
│                                                          │
│ Committing verification receipts - please wait.          │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

The status line reads **FINALIZING** while this runs. When sealing finishes the
modal switches to "✓ INTEGRITY RECEIPTS COMMITTED" and waits for you to press
`Enter` before showing the completion summary. The receipt count is real, not a
spinner, and the run only reports **COMPLETED** once every pipeline's receipts are
committed.

In `--pretty` mode the same phases print inline:

```
[  2.500s] ✓ Pipeline 'migrate_actor' completed: 1,000,000 rows in 2.50s (400,000/s)
[  2.505s] ◆ Finalizing integrity for 'migrate_actor': 2 tables
[  2.505s] ⧗ Sealing 'actor' (sorting & merging row hashes)…
[  3.100s] ✓ Receipt 'actor': 1,000,000 rows, root a3f1b2c49d8c7b6a5e2d8a1c04b93f77c0ffee00112233445566778899aabbcc
[  3.101s] ✓ Migration completed!
```

In default (log) mode the same receipt is logged at `info` level with the full
root. `pag receipt -c migration.ppl` prints every stored receipt again later
(`--json` for tooling), so the root can be recorded somewhere outside the state
directory.

Once receipts are committed, verify the destination against them with
[`pag verify`](#verify-output). See [verification.md](verification.md) for
the cryptographic model.

### Resuming from a checkpoint

Checkpoints store the *cumulative* rows written across runs. When you re-run a
partially-completed migration, the TUI seeds each pipeline's progress from what
was already written: the bars start where the last run left off rather than
snapping back to zero, and pipelines that were already fully done show as
**Completed** immediately.

### Already-completed runs

If a run has already completed, `apply` doesn't reopen the dashboard (or re-run
the migration). It prints a short notice and exits successfully:

```
Migration for 'migration.ppl' already completed.
```

The same guard applies to `--pretty` and default modes; the default mode logs it
as an `info` line. Use `pag reset -c migration.ppl` to clear the state and
run again from scratch.

The guard is only for runs that finished cleanly. A run where a pipeline failed
is recorded as *failed*, not completed, so re-running it retries that pipeline
and leaves the ones that already finished alone; no `reset` needed.

---

## verify output

`verify` checks each pipeline's destination against its stored receipt and
reports one result per table. It has three output modes.

### Default output

The default output keeps the `✓` / `✗` / `?` status markers (they're the
documented result glyphs) but adds no color, header, phase lines, or summary.
It's stable, greppable, and identical to what `--output <file>` writes to disk.

```bash
pag verify -c migration.ppl
```

```
✓ migrate_actor/actor - match (200 rows, root abababababababababababababababababababababababababababababababab, 45ms)
? migrate_film/film - no integrity receipt (run `apply --integrity` first)
```

A mismatch expands to the divergences (missing / changed / extra), capped with a
"… and N more" line:

```
✗ migrate_orders/orders - MISMATCH (0 missing, 1 changed, 0 extra; 127,491 rows expected, 127,491 found; 2,841ms)
  expected root abababababababababababababababababababababababababababababababab
  actual   root cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd
  order_id=3412 - changed: expected abababababababab actual cdcdcdcdcdcdcdcd
```

**Marker meanings:** `✓` match · `✗` mismatch · `?` inconclusive or no receipt.

**Exit codes:**

| Code | Meaning |
| --- | --- |
| 0 | Every table with a receipt matched. Tables without a receipt don't fail the run. |
| 1 | Error: config, connection, state store, or another run of this config holds the lock. |
| 3 | At least one table mismatched. |
| 4 | Nothing mismatched, but at least one table was inconclusive (its row-hash log is missing or truncated). |

A mismatch outranks inconclusive: a run with both exits 3.

### `--pretty` output

`--pretty` adds a cyan header, per-table progress phases (`⧗ reading /
sorting / comparing`), color-coded result lines, and a final tally. The `✓`/`✗`/`?`
glyphs are the same: pretty mode layers decoration on top of the same
result lines.

```bash
pag verify -c migration.ppl --pretty
```

```
◆ Verifying: migration.ppl
⧗ migrate_actor/actor
    reading destination…
    sorting row hashes…
    comparing…
✓ migrate_actor/actor - match (200 rows, root abababababababababababababababababababababababababababababababab, 45ms)
? migrate_film/film - no integrity receipt (run `apply --integrity` first)
✓ 1 matched, 1 without a receipt
```

Like `apply --pretty`, this mode routes raw logs to `~/.paganel/pretty.log` to
keep the terminal output clean.

### `--json` output

`--json` prints one JSON document to stdout once every table is checked. It
prints no progress lines, and routine logs are suppressed as with `plan` (`-v`
sends them to stderr). It can't be combined with `--pretty`, and it exits with
the same code as the default output. `--output <file>` still writes the text
report.

```bash
pag verify -c migration.ppl --json | jq '.tables[] | select(.result != "match")'
```

```json
{
  "config": "migration.ppl",
  "ok": false,
  "tables": [
    {
      "pipeline": "migrate_actor",
      "table": "actor",
      "result": "mismatch",
      "expected_rows": 200,
      "actual_rows": 199,
      "missing": 1,
      "changed": 1,
      "extra": 0,
      "extra_ignored": 0,
      "expected_root": "2424b83c…",
      "actual_root": "137d7a95…",
      "divergences": [
        { "key": "actor_id=7", "kind": "changed", "expected_hash": "c110bb7d…", "actual_hash": "72120fc5…" },
        { "key": "actor_id=8", "kind": "missing", "expected_hash": "9b0e41d2…", "actual_hash": null }
      ],
      "duration_ms": 28,
      "message": null
    }
  ]
}
```

Roots and hashes are full 64-character hex strings (shortened above).

- `result` is one of `match`, `mismatch`, `no_prior_run` or `log_unavailable`.
- `ok` is false when any table is `mismatch` or `log_unavailable`.
- `divergences` holds at most the first 100 rows; `missing + changed + extra`
  is the full count.
- `actual_rows` is `null` when the table wasn't compared (`no_prior_run`,
  `log_unavailable`), and `message` says why for `log_unavailable`.
- `extra_ignored` counts the destination rows that `--allow-extra` accepted.

---

## receipt output

`receipt` prints the integrity receipts that `apply --integrity` stored, one
block per pipeline/table, with the **full 64-hex table root**. It is the command
to run when you want to keep the root somewhere outside `~/.paganel/state/` (a
ticket, a build log, a commit); see [verification.md](verification.md#trust-boundary)
for why that matters. It has no `--pretty` mode: stdout *is* the report, so
routine logs are suppressed (as with `plan`); add `-v` to see them.

```bash
pag receipt -c migration.ppl        # receipts for this config's pipelines
pag receipt                         # every receipt in the state store
```

```
migrate_actor/actor
  root:       2424b83ccd0a9852426c011ad58bd8af43696f295b03ed15c77dfdd9115b3553
  rows:       200
  skipped:    0
  key:        actor_id
  columns:    actor_id, first_name, last_name, last_update
  algorithm:  Sha256
  run:        run-15fd4e90351c775b
  created:    2026-09-14 16:40:39 UTC

migrate_payment/payment
  root:       9538f28f967218b7828418aff6c8934eb4c11ee291e7fcf10d20c88dfe089ee6
  rows:       16,044
  skipped:    0
  key:        payment_id
  columns:    amount, customer_id, last_update, payment_date, payment_id, rental_id, staff_id
  algorithm:  Sha256
  run:        run-15fd4e90351c775b
  created:    2026-09-14 16:40:39 UTC
```

`--json` emits the same receipts as a JSON array (root as hex, `created_at` as
RFC 3339), sorted by pipeline then table, so the file diffs cleanly between runs:

```bash
pag receipt -c migration.ppl --json > receipts.json
```

```json
[
  {
    "pipeline": "migrate_actor",
    "table": "actor",
    "root": "2424b83ccd0a9852426c011ad58bd8af43696f295b03ed15c77dfdd9115b3553",
    "rows": 200,
    "skipped_rows": 0,
    "key_columns": ["actor_id"],
    "columns": ["actor_id", "first_name", "last_name", "last_update"],
    "algorithm": "Sha256",
    "run_id": "run-15fd4e90351c775b",
    "created_at": "2026-09-14T16:40:39.123456789+00:00"
  }
]
```

With no receipts stored the text mode prints
`No integrity receipts found (run `apply --integrity` first).` and `--json`
prints `[]`; both exit 0.

---

## Colors and `--no-color`

Color is only applied when all of these hold: the mode opts into it
(`--pretty`, or the TUI), `--no-color` is not set, and stdout is a TTY. Piping to
a file or another process therefore yields plain text automatically. The `✓`/`✗`/
`?` glyphs and Unicode symbols are not color; they remain in the default,
uncolored output.

---

## Choosing a mode

- **CI, cron, redirected output** -> default log mode (add `--log-level debug` to
  trace). `verify` default output is stable and diffable.
- **Watching an interactive run** -> `--pretty` for a readable scrolling log, or
  `--tui` for a live dashboard with controls.
- **Long or large migrations you want to steer** -> `--tui`, so you can pause and
  checkpoint (`Space`) or cancel (`c`) without losing written data.
- **Scripting `verify`** -> `--json` for a parseable result, or the default
  output / `--output report.txt` for text; check the exit code for
  pass/fail/inconclusive.
- **Recording receipts** -> `pag receipt --json > receipts.json` right after
  `apply --integrity`, and commit or attach the file.
