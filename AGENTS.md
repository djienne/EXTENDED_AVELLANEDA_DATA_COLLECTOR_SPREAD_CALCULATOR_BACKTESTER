# Repository Guidelines

## Project Structure & Module Organization

- Core Rust crate lives in `src/`, with `lib.rs` exporting the public API and modules such as `websocket.rs`, `rest.rs`, `data_collector.rs`, `data_collection_task.rs`, `data_loader.rs`, `metrics.rs`, and `calibration.rs`.
- CLI binaries are in `src/bin/` (e.g. `collect_data.rs`, `backtest.rs`, `calculate_spread.rs`, `verify_orderbook.rs`); examples are under `examples/`.
- Collected market data is written to `data/{market}/` (e.g. `data/btc_usd/trades.csv`, `orderbook_depth.csv`, `state.json`); documentation lives in `DOC/full_doc.txt`.
- Configuration is via `config.json` and environment variables loaded from `.env` (never commit real secrets; use `.env.example` as a template).
- Intensity calibration now requires orderbook exposure points (see `calibration_engine.rs:add_orderbook`). Always feed snapshots with timestamps and mids; `fit_intensity_parameters` consumes trades + `OrderbookPoint` slice + window end ts.

## Build, Test, and Development Commands

- `cargo build` / `cargo build --release` — compile the crate (debug vs optimized release).
- `cargo run --bin collect_data` — run the main data collector using `config.json` and `.env`.
- `cargo run --bin backtest` / `calculate_spread` / `verify_orderbook` — run analysis and validation tools.
- `cargo test` — run the Rust unit tests (currently primarily in `src/lib.rs`); prefer adding focused tests near the code you change.

## Coding Style & Naming Conventions

- Use standard Rust 2021 style: modules and files `snake_case`, types and traits `PascalCase`, functions and variables `snake_case`, constants `SCREAMING_SNAKE_CASE`.
- Run `cargo fmt` before opening a PR; when possible also run `cargo clippy -- -D warnings` and fix any issues.
- Keep functions small and focused; reuse `Result<T>` and error types from `error.rs`, and prefer structured logging via `tracing` over ad-hoc `println!`.

## Testing Guidelines

- Prefer `#[test]` unit tests colocated with the module (see `src/lib.rs` for an example); use `#[tokio::test]` for async behavior.
- Before submitting changes, run `cargo test` and any relevant `cargo run --bin ...` commands that exercise your changes (e.g. short `collect_data` runs against test markets).
- When touching CSV or resume logic, add tests that cover edge cases such as duplicate trades, sequence gaps, and restart behavior.
- When touching intensity fitting, keep exposure-aware MLE intact (uses per-snapshot delta_min/delta_max durations). Avoid reverting to trade-only mean-delta estimates.

## Commit & Pull Request Guidelines

- Commit messages should be concise and imperative, e.g. `data_collector: fix duplicate trade handling` or `rest: add account info endpoint`; the existing history is minimal, so establish clear structure now.
- Each PR should include: a short overview, a bullet list of key changes, notes on any config/schema changes, and the exact commands you ran for verification (`cargo test`, `cargo run --bin collect_data`, etc.).
- For changes that affect output data or performance, include a brief summary of observed behavior (e.g. sample CSV row, approximate rows/sec, or log excerpt) rather than committing large data files.

## Security & Agent-Specific Instructions

- Never commit `.env` or real API keys; keep secrets local and scrub any sensitive values from logs and examples.
- Treat `data/` and `target/` as generated artifacts: do not commit large or temporary files and avoid editing collected CSVs by hand.
- When using automated tools or AI assistants, keep changes narrowly scoped, avoid broad reformatting of unrelated files, and prefer incremental, well-tested edits.

