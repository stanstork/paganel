# Paganel Documentation

Paganel is a data migration engine that moves data and schema between
systems, then cryptographically verifies that the destination matches what was
written. For installation, the quick start, and the feature overview, start with
the [project README](../README.md).

## Start here

| Document | Description |
|----------|-------------|
| [plan.md](plan.md) | Reading `pag plan` - the summary layout, flags, sampling, and the magnitude bar |
| [output-modes.md](output-modes.md) | `apply`, `verify` and `receipt` output - default logs, `--pretty`, the `--tui` dashboard with its controls, and receipt text/JSON |
| [ppl-reference.md](ppl-reference.md) | PPL language reference with examples |
| [drift.md](drift.md) | `pag drift`, `pag state import`, `pair` blocks and `hook` notifications - checking tables after the migration, and the limits of each |

## Design

| Document | Description |
|----------|-------------|
| [verification.md](verification.md) | Cryptographic verification - canonical row serialization, Merkle construction, what a receipt does and does not prove |
| [architecture.md](architecture.md) | Full crate map, layer breakdown, design decisions |
| [why-ppl.md](why-ppl.md) | Why a purpose-built DSL instead of YAML/JSON/SQL - rationale and trade-offs |
| [plugins/](plugins/README.md) | WASM plugins - roles, runtimes (native Rust / JS-QuickJS), authoring, CLI |

## Background

| Document | Description |
|----------|-------------|
| [benchmarks.md](benchmarks.md) | Reproducible benchmark methodology, results, and the harness |
| [comparison.md](comparison.md) | How Paganel compares with other migration, CDC, and verification tools |

Runnable configs for every feature live in [`examples/configs/`](../examples/configs/).
