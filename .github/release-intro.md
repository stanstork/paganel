Paganel moves data and schema between systems, then proves the destination
holds what it wrote.

**Install.** Download the archive for your platform below, or follow the
[quickstart](https://github.com/stanstork/paganel#quick-start) for throwaway
databases and a first migration in about two minutes.

- **One binary.** Linux builds link OpenSSL statically and need only glibc 2.35
  or newer, so there is nothing to install alongside it.
- **Verified migrations.** `apply --integrity` records a keyed Merkle receipt per
  table while it writes; `verify` re-reads the destination later and names any
  row that differs, with no connection to the source.
- **Plugins.** Transforms, filters, sources and sinks, sandboxed in WASM. Write
  them in Rust against
  [paganel-plugin-sdk](https://crates.io/crates/paganel-plugin-sdk), or point a
  `plugin` block straight at a `.js` file; Paganel compiles it on first use
  (esbuild or Node on `PATH`) and caches the result.

Verify your download against `SHA256SUMS`. Pre-1.0: PPL and the internal APIs
still change between releases, so pin the version you tested against.
