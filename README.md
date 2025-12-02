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
*   **Dynamic Calibration**: Continuously updates volatility ($\sigma$) and side-specific intensity parameters ($\kappa_{bid}$, $A_{bid}$, $\kappa_{ask}$, $A_{ask}$) based on a rolling window.
*   **Asymmetric Spreads**: Calculates separate optimal spreads for bid and ask sides using their respective fill intensities, allowing for more accurate pricing when liquidity differs across sides.

### 3. Analysis Tools
*   **Spread Calculation**: Computes optimal bid/ask spreads based on current inventory and market volatility.
*   **Parameter Tuning**: Configurable risk aversion ($\gamma$) and time horizons.

## Usage

### Prerequisites
*   Rust toolchain installed (version >1.91 recommended; may work with older versions but not tested).
*   `config.json` configured (see below).

**Important**: You must collect data for a sufficient period before running backtests or spread calculations. The system requires historical data to:
- Calibrate volatility ($\sigma$) and side-specific intensity parameters ($\kappa_{bid}$, $A_{bid}$, $\kappa_{ask}$, $A_{ask}$)
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
*   `num_threads`: Number of parallel threads for grid search (default: `4`).

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

## Avellaneda-Stoikov Model Details

### Asymmetric Spread Calculation

The system uses **side-specific intensity parameters** to calculate optimal spreads independently for bid and ask sides:

**Bid Spread Formula:**
$$\delta_{bid} = \gamma\sigma^2T + \frac{2}{\gamma}\ln\left(1 + \frac{\gamma}{\kappa_{bid}}\right)$$

**Ask Spread Formula:**
$$\delta_{ask} = \gamma\sigma^2T + \frac{2}{\gamma}\ln\left(1 + \frac{\gamma}{\kappa_{ask}}\right)$$

Where:
- $\sigma$ = Volatility (estimated from price returns)
- $\gamma$ = Risk aversion parameter
- $T$ = Time horizon (inventory_horizon_seconds)
- $\kappa_{bid}$ = Fill intensity for bid side (fitted from trades hitting bids)
- $\kappa_{ask}$ = Fill intensity for ask side (fitted from trades hitting asks)

**Quote Placement:**
- Reservation price: $r = mid - \gamma\sigma^2T \cdot q$ (where $q$ is inventory)
- Bid price: $r - \delta_{bid}/2$
- Ask price: $r + \delta_{ask}/2$

This approach captures asymmetries in market microstructure, where bid and ask liquidity may have different fill characteristics.

### Intensity Parameter Calibration

The system fits separate exponential decay models for each side:

$$\lambda_{bid}(\delta) = A_{bid} \cdot e^{-\kappa_{bid} \cdot \delta}$$
$$\lambda_{ask}(\delta) = A_{ask} \cdot e^{-\kappa_{ask} \cdot \delta}$$

Where $\lambda$ is the arrival rate of market orders at distance $\delta$ from the mid price. These parameters are estimated with a truncated exponential MLE that uses both trades and orderbook exposure:

- **Exposure-aware**: For each orderbook snapshot, the model integrates over the reachable range `[δ_min, δ_max]` (best to furthest level) and the time until the next snapshot.
- **Trade mapping**: Each trade delta is measured from the mid of the most recent snapshot at that timestamp.
- **Separate sides**: Bid and ask κ/A are fitted independently; if one side lacks data, the other side’s fit is reused; if neither fits, defaults are applied.

This avoids bias toward ambient spreads (uses exposure instead of a simple mean-delta) and respects left-truncation from the prevailing spread/tick.

## Architecture

### Core Components
- **WebSocket Client** (`websocket.rs`): Real-time market data streaming
- **Parquet Writer** (`storage/parquet_writer.rs`): High-performance data persistence
- **Data Loader** (`data_loader.rs`): Efficient historical data loading
- **Calibration** (`calibration.rs`): Parameter estimation ($\sigma$, $\kappa_{bid}$, $A_{bid}$, $\kappa_{ask}$, $A_{ask}$)
- **Spread Model** (`spread_model.rs`): AS optimal quote calculation with asymmetric spreads
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
