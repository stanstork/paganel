# `pag plan`

`plan` is a **dry run**. It connects to the source and destination, introspects
their schemas, works out execution order, estimates cost, and prints a summary of
exactly what `apply` *would* do - without writing anything or moving any data.

```bash
pag plan -c migration.ppl
```

It's the first command most people run, so its default output is a concise,
human-readable summary. The full machine-readable report is one flag away
(`--json`), for CI and tooling.

While the plan is being built you'll see a spinner (`Analyzing migration plan…`).
Its routine progress logs are suppressed by default because every warning it finds
is already reported in the **Diagnostics** section; run with `-v` to see the full
log stream instead of the spinner.

---

## Reading the summary

```
pag plan · schema.ppl
2 pipelines · 2 connections · ~17,099 rows · 2 tables to create

EXECUTION
  ● stage 1   migrate_customer
  │
  ● stage 2   migrate_payment                   waits for stage 1

PIPELINES
  ▸ migrate_customer    customer → users           ▏           ~599 rows
    ├ + create table users                9 columns · pk customer_id
    ├ − excludes 5 columns                store_id, address_id, last_update, first_name, last_name
    └ ⧉ joins address, city

  ▸ migrate_payment     payment → payments         █████    ~16,500 rows
    ├ + create table payments             7 columns · pk payment_id
    ├ − excludes 3 columns                rental_id, last_update, staff_id
    └ ◷ after migrate_customer

DIAGNOSTICS
  · 2 notes hidden - see --json

ESTIMATES
  duration    5s   best 2s · worst 15s
              rough - a prior guess; run apply once to calibrate to this machine
  memory      ~32 MB peak
  transfer    ~13.4 MB   · 19 batches

────────────────────────────────────────────────────────────────────────
Plan: 2 tables to create · ~17,099 rows to copy
✓ Ready to apply  →  pag apply -c schema.ppl
```

The layout is grouped by pipeline, like `terraform plan`.

- **EXECUTION** - the DAG as a rail. Pipelines in the same stage run in parallel;
  later stages wait for earlier ones (`after = [...]` dependencies).
- **PIPELINES** - one card per pipeline: a header (`▸ name  source → destination
  bar  rows`) followed by a tree of what happens to it.
- **DIAGNOSTICS** - warnings and errors in full (with a fix suggestion under `↳`);
  routine informational notes are collapsed to a count (`--json` has them all).
- **ESTIMATES** - duration (with a best/worst range), peak memory, and transfer.
  The duration is self-calibrating - see [Duration estimates](#duration-estimates).
- **Verdict** - a rule, a totals line, and the exact next command to run.

### The change glyphs

Each card branch opens with one glyph. Diff glyphs describe *changes*; the dim
glyphs are structural facts.

| Glyph | Meaning | Color |
|-------|---------|-------|
| `+` | Creates something (table, index) | green |
| `~` | Safe type conversion | cyan |
| `⚠` | Lossy conversion - may lose data/precision | yellow |
| `−` | Source columns **not carried** to the destination (excluded from the projection, not deleted from the source) | red |
| `⧉` | Reads a `with { }` join (dim, structural) | dim |
| `◷` | Waits on a DAG dependency (dim, structural) | dim |

The verdict uses judgment glyphs - `✓` ready, `⚠` ready with warnings, `✗` not
executable. Color is always emphasis, never the only signal: every colored mark
is also a distinct character, so the output still reads under `--no-color`, when
piped to a file, or on a 16-color terminal. (With color off, `▸ ● ⧉ ◷` fall back
to `> * # @`.)

### Duration estimates

The duration comes from the row count divided by an expected throughput, plus
fixed overhead (connection setup, checkpoints). Throughput is self-calibrating
per machine, so the estimate sharpens as you use Paganel:

- **Before any run on this machine** the estimate rests on a conservative,
  built-in prior for the destination write path (PostgreSQL COPY, MySQL
  `LOAD DATA`, and a slower INSERT fallback are each different). Because a prior
  is a guess, the estimate is labelled `rough` and its best/worst band is
  deliberately wide.
- **Each `apply` records the throughput it actually achieved**, keyed by write
  path and normalized to a single lane, into a small store at
  `~/.paganel/calibration`. The next `plan` uses that measured rate: the `rough`
  label drops and the band tightens. The first real run replaces the prior
  outright; later runs move a rolling average.

The store is a regenerable cache - deleting `~/.paganel/calibration` just returns
estimates to their cold-start priors. `lanes` are accounted for (more lanes raise
the projected throughput sublinearly), so a `lanes = N` plan estimates faster than
a single-lane one.

### Graph / cascade pipelines

A pipeline that discovers its tables by walking the foreign-key graph
(`from { … with references { data = cascade } }`) is still one pipeline, but
it fans out into a whole closure of tables. `plan` shows a card per discovered
table under a cascade header, so you can see each table's row count, its
create-table, its type conversions, and - with `--sample` - its own transformed
rows:

```
PIPELINES
  ▸ graph_from_rental   cascade from rental · 15 tables · 1 pipeline
  ▸ customer            customer → customer      ▏        599 rows
    └ ▪ sample · 3 rows   given_name │ family_name │ …
  ▸ film                film → film              ▎      1,000 rows
    ├ ~ rental_rate → price               (named select "film")
    └ ▪ sample · 3 rows   price │ rent_days │ …
  …
```

Each table can carry its own field mapping: a named
`select "customer" { … }` block only rewrites the `customer` card, so the samples
reflect exactly what each table will be written as. The header row total, the
estimates and the verdict are summed across the whole closure (it all runs as one
pipeline). Tables that already exist on the destination are shown without a
`+ create table` line and don't count toward "tables to create".

### The magnitude bar

The 5-cell bar on each pipeline header shows that pipeline's row estimate relative
to the largest pipeline in the plan, so you can see where the time will go before
reading a number. The heaviest pipeline gets a full green bar (`█████`); smaller
ones show a proportional sliver (`▏`, `▍`). A plan with a single pipeline shows
no bar - there's nothing to compare it against.

Row counts are estimates when introspection can't get an exact figure; those are
prefixed with `~`.

### Pairs and hooks

[`pair`](ppl-reference.md#pair) and [`hook`](ppl-reference.md#hook) blocks
don't change what `apply` does; `pag drift` uses them. The plan lists them after
the pipelines and counts them in the header line
(`1 pipeline · 2 pairs · 2 hooks · 2 connections · …`):

```
PAIRS (SOURCE AND TARGET MUST HOLD THE SAME ROWS; CHECKED BY PAG DRIFT)
  > orders              app.orders → dw.orders_raw
    ├ source  where status = 'paid'
    └ target  where state = 'PAID'

HOOKS (POSTED AFTER EACH PAG DRIFT RUN)
  > orders_alert        drift.diverged, drift.failed → pair.orders · slack (hooks.slack.com)
  > audit               drift.completed → every target · webhook: expression evaluation error: Missing required environment variable: AUDIT_SECRET
```

- Each side's `where` is shown as it will be queried, cut at 60 characters.
- A hook shows where it posts by host only, never the URL or the secret. A
  setting that is not available (an `env(...)` not set) is shown in its place, in
  yellow: the config still loads, but that hook will fail when drift sends it.
- A config with only pairs and hooks has nothing for `apply` to plan, so `plan`
  prints just these sections and connects to nothing.

---

## Flags

| Flag | Effect |
|------|--------|
| `-c, --config <FILE>` | Path to the PPL config (auto-discovered if omitted). |
| `--json` | Emit the full machine-readable report instead of the summary. Stable, for CI/tooling. |
| `--ddl` | Append the exact `CREATE` / `ALTER` statements the migration would run (see below). |
| `--sample` | Collect and preview transformed rows per pipeline (see below). |
| `--sample-size <N>` | Rows to sample per pipeline (default `5`). |
| `--sample-method <first\|random\|id>` | How to pick rows (default `first`). |
| `--id-column <NAME>` | Key column for `--sample-method id` (default `id`). |
| `--sample-ids <a,b,…>` | Specific ids to sample (with `--sample-method id`). |
| `--exact-filter` | Use exact `COUNT` for filtered row estimates (slower, accurate) instead of `EXPLAIN`. |
| `-o, --output <FILE>` | Write the output to a file instead of stdout (honors `--json`). |

Global flags apply too: `-v`/`-vv` (show logs; disables the spinner), `-q`,
`--no-color`, `-e, --env-file <FILE>`.

`stdout` carries only the summary (or JSON) - logs go to `stderr` - so
`pag plan --json > plan.json` is always valid JSON.

---

## The `--json` report

The summary is a readable digest; `--json` emits the *complete* report it is
built from - nothing in the report is computed only for the screen. Reach for it
in CI (gate on `is_executable`, `summary.warning_count`, row counts, …) or to feed
another tool. Every URL is masked, so the report is safe to archive.

### What the report adds over the summary

| The summary shows… | `--json` also carries… |
|---|---|
| `9 columns · pk customer_id` | every column - `name`, `data_type`, nullability, length, PK / auto-increment flags - plus `indexes` and `size_bytes` |
| `~599 rows` | the row-count object: `value`, `is_estimated`, `confidence` |
| a card's conversion lines | every `mapping` - source/target types, `mapping_type`, join `source`, nullability |
| `2 notes hidden` | all diagnostics at every level, including the `info` notes the summary collapses |
| the execution rail | `execution_order` (the stages) and each pipeline's `execution_stage` / `depends_on` |
| the ESTIMATES block | `estimations` with `disk_usage_mb`, `network_transfer_mb`, `total_batches`, and per-pipeline breakdowns |
| - | run metadata: `plan_id`, `generated_at`, `engine_version`, `config_hash`, resolved `defines`, `execution_settings` |

For a graph/cascade pipeline each pipeline object additionally carries a
`cascade_tables` array - one entry per discovered table with its own `row_count`,
`columns`, `mappings` and `sample`.

### Full example

[**`schema-plan.json`**](schema-plan.json) is the complete, unedited report for
`pag plan -c schema.ppl --json` (the two-pipeline plan shown at the top of
this page) - all URLs masked. Open it alongside the summary to see how each line
of the digest maps onto the full structure.

With `--sample`, each pipeline's `sample` object fills in: `enabled: true`, the
chosen `sampling_method`, and a `rows` array where every row carries its `input`,
transformed `output`, and validation `status`. That is the complete sampled data,
without the 5-row / 6-column terminal cap.

### Pairs and hooks in the report

A config with `pair` or `hook` blocks adds two top-level arrays; without them
the report is unchanged.

```json
{
  "...": "the report as above",
  "pairs": [
    {
      "name": "orders",
      "source": { "connection": "app", "table": "orders", "where": "status = 'paid'" },
      "target": { "connection": "dw", "table": "orders_raw", "ignore": ["_fivetran_synced"] },
      "map": { "customer_id": "cust_id" },
      "key": ["id"],
      "grace_secs": 30
    }
  ],
  "hooks": [
    {
      "name": "audit",
      "on": ["drift.completed"],
      "target": null,
      "delivery": "webhook",
      "host": "audit.example.com",
      "unset": "expression evaluation error: Missing required environment variable: AUDIT_SECRET"
    }
  ]
}
```

`map`, `key`, `where` and `ignore` are left out when not set. A hook carries
`host` (where it posts) and, when a URL or secret is not available, `unset`
saying why; never the URL or the secret. For a
config with only pairs and hooks, the report is just these two arrays.

---

## How sampling works

With `--sample`, each pipeline is run as a genuine dry run over a few real rows -
so the preview reflects what will actually be written:

1. **Fetch** - selects the source table's columns, plus any `with { }` join
   columns, and applies the `where` filter so only rows that will migrate are
   sampled. `--sample-method` chooses *which* rows (`first` N, `random`, or a
   specific set of ids).
2. **Transform** - runs the pipeline's field mappings and computed columns, so a
   `full_name = concat(first, ' ', last)` shows its evaluated value.
3. **Validate** - applies the `validate { }` rules, tagging each row `ok` /
   `warning` / `skipped` / `failed`.

```
    └ ▪ sample · first 4 rows  3 ok · 1 warning
        film_id │ title            │ rental_duration │ rating
      ✓ 1       │ ACADEMY DINOSAUR │ 6               │ PG
      ⚠ 3       │ ADAPTATION HOLES │ 7               │ NC-17
      ✓ 5       │ AFRICAN EGG      │ 6               │ G
```

The preview appears inside the pipeline's card (as its last branch). A per-row
status glyph (`✓ ⚠ ⊘ ✗`) is shown only when some row isn't a clean pass.

**Display limits (the terminal view only):** the summary shows at most 6
columns (chosen in the pipeline's mapping order) and 5 rows, with cell values
truncated to ~22 characters; extra rows are noted as `… N more rows`. These caps
keep the summary readable - the full sampled data is always in `--json`
(`--sample-size` controls how many rows are collected, independent of the display
cap).

If a pipeline has no explicit `select { }` block there are no mapped columns to
preview, and the sample line says so.

> Sampling reads real data. Values from columns configured as sensitive are masked
> in the preview.

---

## The `--ddl` block

`--ddl` appends a **DDL** section: the exact `CREATE` / `ALTER` statements the
migration would run, verbatim and in execution order. Each statement is preceded
by a `-- <pipeline> · <what it does>` comment, and a footer counts them - so the
block can be pasted straight into a change ticket or reviewed before cutover.

```
DDL
  -- migrate_customer · Create new table 'users'
  CREATE TABLE "users" (
  	"id" smallserial PRIMARY KEY NOT NULL,
  	"email" varchar(50),
  	"is_active" boolean NOT NULL,
  	"registered_at" timestamp NOT NULL,
  	"full_name" varchar,
  	"city" varchar(50),
  	"address" varchar(50)
  );

  -- migrate_payment · Create new table 'payments'
  CREATE TABLE "payments" (
  	"id" smallserial PRIMARY KEY NOT NULL,
  	"customer_id" smallint NOT NULL,
  	"amount" numeric(5,2) NOT NULL,
  	"paid_at" timestamp NOT NULL
  );

  2 statements · verbatim - exactly what `pag apply` runs, in order
```

The comment carries a `· breaking` or `· irreversible` tag when a statement is
destructive or can't be rolled back, so the risk rides along with the SQL. For a
graph/cascade migration this is where the whole discovered closure shows up:
`CREATE TABLE` per table in dependency order, then `CREATE INDEX` and the
`ALTER TABLE … ADD CONSTRAINT` foreign keys as a second phase. If there's nothing
to create, the block says so instead.

---

## When it can't run

If the plan builds but is blocked (e.g. a required table is missing), the verdict
becomes `✗ Not executable · <reason>` and nothing is offered to apply. If a
connection is unreachable, `plan` fails outright with the connection error - there
is no report to show.

---

See also: [ppl-reference.md](ppl-reference.md) for the config language, and
[verification.md](verification.md) for verifying a completed migration.
