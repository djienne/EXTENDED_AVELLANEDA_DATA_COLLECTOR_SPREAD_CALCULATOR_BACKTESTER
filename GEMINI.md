# Gemini Context: Extended DEX Data Collector & Backtester

This file provides context for Gemini when working with the Extended DEX data collector and backtesting suite.

## Project Overview

This is a **Rust-based** financial engineering project designed to:
1.  **Collect** high-frequency market data (orderbooks and trades) from Extended DEX via WebSocket.
2.  **Backtest** market-making strategies, specifically the **Avellaneda-Stoikov (AS)** model.
3.  **Calibrate** model parameters (volatility $\sigma$, intensity $\kappa$) dynamically.

**Strictly for research and backtesting.** Not for live execution.

## Key Technologies

*   **Language:** Rust (Edition 2021)
*   **Async Runtime:** `tokio`
*   **Networking:** `tokio-tungstenite` (WebSocket), `reqwest` (HTTP)
*   **Serialization:** `serde`, `csv`, `serde_json`
*   **Math:** `rust_decimal` for financial precision
*   **Configuration:** `config.json`

## Architecture & Workflow

The system operates in three distinct phases:

1.  **Data Collection (`collect_data`)**:
    *   Connects to WebSocket.
    *   Streams `SNAPSHOT` and `DELTA` updates for orderbooks.
    *   Streams public trades.
    *   Persists data to `data/{market}/orderbook_depth.csv` and `trades.csv`.
    *   Maintains state in `state.json` for resume capability.

2.  **Calibration (`calibration.rs`)**:
    *   Analyzes historical data to estimate market parameters.
    *   **Volatility ($\sigma$)**: Calculated from log returns.
    *   **Intensity ($\kappa, A$)**: Fitted using exponential regression on trade arrival rates.

3.  **Backtesting (`backtest`)**:
    *   **Event-Driven Engine**: Simulates the AS strategy over historical data.
    *   **No Look-ahead Bias**: Strictly chronological processing.
    *   **Features**: Inventory management, transaction costs, fill cooldowns, dynamic recalibration.

## Build & Run Commands

Run these commands from the project root.

### Build
```bash
cargo build --release
```

### 1. Collect Data
Start the data collector. Ensure `config.json` has the desired markets.
```bash
cargo run --bin collect_data
# With debug logging
RUST_LOG=debug cargo run --bin collect_data
```

### 2. Run Backtest
Run the AS strategy simulation on collected data.
```bash
cargo run --bin backtest
```
*   **Input**: `data/{market}/*.csv`
*   **Output**: `data/{market}/backtest_results.csv`

### 3. Analysis & Tools
Calculate optimal spreads based on current mock state:
```bash
cargo run --bin calculate_spread
```

Verify the integrity of collected orderbook data:
```bash
cargo run --bin verify_orderbook
```

## Configuration (`config.json`)

Key parameters controlling the system behavior:

| Category | Parameter | Description |
| :--- | :--- | :--- |
| **Data** | `markets` | List of pairs, e.g., `["ETH-USD", "BTC-USD"]` |
| **AS Model** | `risk_aversion_gamma` | Risk aversion ($\gamma$). Higher = wider spreads. |
| **AS Model** | `inventory_horizon_seconds` | Time horizon ($T$) for optimization. |
| **Backtest** | `fill_cooldown_seconds` | Min time between fills on the same side. |
| **Backtest** | `gap_threshold_seconds` | Max allowed data gap before triggering warm-up. |
| **Backtest** | `warmup_period_seconds` | Data accumulation time after start/gap. |

## Directory Structure

*   `src/bin/`: Entry points for each tool (`collect_data`, `backtest`, etc.).
*   `src/`: Core logic modules (`calibration.rs`, `spread_model.rs`, `websocket.rs`).
*   `data/`: Storage for CSVs. **Do not commit large CSV files.**
    *   `data/{market}/orderbook_depth.csv`: Timestamped orderbook snapshots.
    *   `data/{market}/trades.csv`: Public trades.

## Development Conventions

*   **Formatting**: Use `cargo fmt`.
*   **Linting**: Use `cargo clippy`.
*   **Error Handling**: Uses `anyhow` and `thiserror`.
*   **Logging**: Uses `tracing`. Use `RUST_LOG` env var to control verbosity.
*   **Precision**: Use `rust_decimal::Decimal` for all price/quantity calculations to avoid floating-point errors.
