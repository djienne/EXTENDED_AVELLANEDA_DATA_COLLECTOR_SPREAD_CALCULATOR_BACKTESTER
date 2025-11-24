# Extended DEX Data Collector & Backtester

## Objective
This project is designed to collect high-frequency market data from **Extended DEX**, **backtest market-making strategies**, and explore optimal spread calculations using the **Avellaneda-Stoikov (AS)** market-making model.

**Note**: The goal of this repository is **strictly for backtesting and research purposes**. It is **not** intended for live trading or execution strategies. It provides a robust framework for capturing real-time orderbook and trade data, calibrating model parameters ($\gamma$, $\kappa$, $\sigma$), and backtesting strategies with realistic constraints.

## Key Features

### 1. Data Collection
*   **Real-time Capture**: Connects to Extended DEX WebSocket to stream trades and orderbooks.
*   **High-Performance Storage**: Parquet format with ZSTD compression (11.8x compression ratio, 13% smaller and 26% faster than SNAPPY).
*   **Precision**: Timestamps preserved with millisecond precision.
*   **Resilience**: Automatic state saving and resume capability.
*   **Partitioned Files**: 25K rows per file for optimal read/write performance.

### 2. Avellaneda-Stoikov Backtesting
*   **Event-Driven Engine**: Simulates fills sequentially between orderbook updates to eliminate look-ahead bias.
*   **Realistic Constraints**: Enforces inventory limits, transaction costs (maker/taker fees), and cash balance.
*   **Configurable Cooldowns**: Supports separate bid/ask fill cooldowns to prevent rapid-fire trading while allowing two-way flow.
*   **Dynamic Calibration**: Continuously updates volatility ($\sigma$) and intensity parameters ($\kappa$, $A$) based on a rolling window.

### 3. Analysis Tools
*   **Spread Calculation**: Computes optimal bid/ask spreads based on current inventory and market volatility.
*   **Parameter Tuning**: Configurable risk aversion ($\gamma$) and time horizons.

## Usage

### Prerequisites
*   Rust toolchain installed (version >1.91 recommended; may work with older versions but not tested).
*   `config.json` configured (see below).

**Important**: You must collect data for a sufficient period before running backtests or spread calculations. The system requires historical data to:
- Calibrate volatility ($\sigma$) and intensity parameters ($\kappa$, $A$)
- Build a rolling window for dynamic parameter updates
- Provide realistic market conditions for backtesting

Recommended minimum: Several hours of data collection to ensure adequate calibration.

### 1. Collect Data
Start collecting market data to `data/{market}/orderbook_parts/`:
```bash
cargo run --release --bin collect_data
```
Data is stored in Parquet format with ZSTD(3) compression for optimal performance.

### 2. Run Backtest
Simulate the AS strategy using collected data:
```bash
cargo run --release --bin backtest
```
Results are saved to `data/{market}/backtest_results.csv` and `data/{market}/as_results.csv`.

### 3. Calculate Spreads
Compute optimal quotes based on current market state:
```bash
cargo run --release --bin calculate_spread
```

### 4. Verify Data Integrity
Check orderbook data quality and detect gaps:
```bash
cargo run --release --bin verify_orderbook
```

**Note**: Grid search and optimization tools are available after data collection. See section 5 below.

### 5. Grid Search Optimization
Find optimal parameters by testing multiple configurations:

**Time Horizon Grid Search:**
```bash
cargo run --release --bin grid_search
```
Tests multiple `inventory_horizon_seconds` values (1m to 4h).

**2D Grid Search (Horizon × Gamma):**
```bash
cargo run --release --bin grid_search_gamma
```
Tests combinations of time horizons and risk aversion (γ) values:
- Gamma values: 0.01 (low), 0.05, 0.1 (default), 0.2 (high)
- Provides best configuration for each parameter
- Analyzes parameter interactions

### 6. Migrate Legacy CSV Data
Convert old CSV files to Parquet format:
```bash
# Migrate all markets
cargo run --release --bin migrate_orderbook_to_parquet

# Migrate specific market
cargo run --release --bin migrate_orderbook_to_parquet -- data/eth_usd
```

## Configuration (`config.json`)

### Data Collection
*   `markets`: List of markets to collect (e.g., `["ETH-USD", "BTC-USD"]`).
*   `data_directory`: Output directory (default: `"data"`).
*   `collect_orderbook`: Enable orderbook depth collection (default: `true`).
*   `collect_trades`: Enable public trades collection (default: `true`).
*   `collect_full_orderbook`: Enable full depth snapshots (default: `true`).
*   `max_depth_levels`: Number of orderbook levels to collect (default: `20`).

### Backtesting
*   `inventory_horizon_seconds`: Time horizon $T$ for the AS model (default: `1800`).
*   `risk_aversion_gamma`: Risk aversion parameter $\gamma$ (default: `0.1`).
*   `gamma_mode`: Risk adjustment mode: `"constant"`, `"inventory_scaled"`, or `"max_shift"` (default: `"constant"`).
*   `fill_cooldown_seconds`: Minimum time between fills on the same side (default: `60`).
*   `maker_fee_bps`: Maker fee in basis points (default: `1.5`).
*   `taker_fee_bps`: Taker fee in basis points (default: `4.5`).
*   `gap_threshold_seconds`: Maximum gap before warm-up period (default: `1800`).
*   `warmup_period_seconds`: Warm-up duration after gaps (default: `900`).
*   `quote_validity_seconds`: Quote expiration time (default: `60`).
*   `calibration_window_seconds`: Window for parameter estimation (default: `3600`).
*   `recalibration_interval_seconds`: Frequency of parameter updates (default: `600`).
*   `min_spread_bps`: Minimum spread constraint (default: `3.0`).
*   `max_spread_bps`: Maximum spread constraint (default: `200.0`).

## Data Storage

### Parquet Format
Data is stored in efficient Parquet format with ZSTD(3) compression:
- **Orderbook**: `data/{market}/orderbook_parts/part_*.parquet` (25K rows per file)
- **Trades**: `data/{market}/trades.csv` (CSV format)
- **State**: `data/{market}/state.json` (resume tracking)

### Performance
- 11.8x compression ratio from raw data
- 13% smaller than SNAPPY compression
- 26% faster write performance
- Automatic compression detection on read

### Compression Options
To change compression level, see `COMPRESSION_UPGRADE.md`. Current options:
- **ZSTD(3)**: Best balance (default) ⭐
- **ZSTD(10)**: Better compression, slower writes
- **GZIP(6)**: Maximum compatibility
- **LZ4**: Maximum speed

## Architecture

### Core Components
- **WebSocket Client** (`websocket.rs`): Real-time market data streaming
- **Parquet Writer** (`storage/parquet_writer.rs`): High-performance data persistence
- **Data Loader** (`data_loader.rs`): Efficient historical data loading
- **Calibration** (`calibration.rs`): Parameter estimation ($\sigma$, $\kappa$, $A$)
- **Spread Model** (`spread_model.rs`): AS optimal quote calculation
- **Backtest Engine** (`bin/backtest.rs`): Event-driven strategy simulation

### Key Binaries
- `collect_data`: Real-time data collection
- `backtest`: AS strategy backtesting
- `calculate_spread`: Optimal spread computation
- `verify_orderbook`: Data integrity verification
- `grid_search`: Time horizon optimization
- `grid_search_gamma`: 2D grid search (horizon × gamma)
- `migrate_orderbook_to_parquet`: CSV to Parquet conversion
- `test_compression`: Compression algorithm benchmarking

## Documentation
- `CLAUDE.md`: Detailed architecture and development guide
- `COMPRESSION_UPGRADE.md`: Compression performance details and options

---
*Note: This project is for research and backtesting purposes only.*
