# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Extended DEX data collector and market-making backtester. Collects high-frequency orderbook/trade data via WebSocket and backtests Avellaneda-Stoikov (AS) market-making strategies.

## Quick Commands

```bash
# Build
cargo build --release

# Data collection
cargo run --bin collect_data
RUST_LOG=debug cargo run --bin collect_data

# Backtesting (requires collected data)
cargo run --bin backtest

# Spread calculation
cargo run --bin calculate_spread

# Verify orderbook integrity
cargo run --bin verify_orderbook
```

## Architecture Overview

**Three-Phase System**:
1. **Data Collection** (`collect_data`) - Stream and persist market data
2. **Calibration** (`calibration.rs`) - Calculate volatility (σ) and side-specific intensity parameters (κ_bid, A_bid, κ_ask, A_ask) from historical data
3. **Backtesting** (`backtest`) - Simulate AS market-making strategy with realistic constraints and asymmetric spreads

**Key Modules**:
- `websocket.rs` - WebSocket client for streaming orderbooks and trades
- `storage/parquet_writer.rs` - Parquet writers with deduplication, state persistence, and partitioned files
- `data_collector.rs` - Legacy CSV writers (deprecated, kept for backward compatibility)
- `data_loader.rs` - Load historical data (supports both Parquet and legacy CSV formats)
- `calibration.rs` - Volatility (σ) and side-specific intensity (κ_bid, A_bid, κ_ask, A_ask) parameter estimation
- `spread_model.rs` - AS optimal quote calculation with asymmetric spreads and inventory skew
- `model_types.rs` - Configuration types for AS model (ASConfig, GammaMode, etc.)
- `metrics.rs` - Performance tracking (PnL, Sharpe, fills, etc.)

## Core Binaries

- `src/bin/collect_data.rs` - Data collection service (WebSocket → Parquet)
- `src/bin/backtest.rs` - Event-driven AS backtester with fill simulation
- `src/bin/calculate_spread.rs` - Optimal spread calculator
- `src/bin/verify_orderbook.rs` - Orderbook data integrity checker

## Data Collection

**WebSocket Streaming**:
- Subscribes to orderbook depth (SNAPSHOT + DELTA updates) and public trades
- Maintains full orderbook state by merging deltas
- Deduplicates trades by ID, orderbook updates by sequence number
- Saves state every 30s and on Ctrl+C for resume capability

**Parquet Output Format**:
- **Orderbook**: Partitioned Parquet files with ZSTD(3) compression
  - Schema: `timestamp_ms, market, seq, bid_price_0, bid_qty_0, ask_price_0, ask_qty_0, ...` (up to N levels)
  - 25,000 rows per file for optimal I/O performance
- **Trades**: Partitioned Parquet files with ZSTD(3) compression
  - Schema: `timestamp_ms, price, quantity, is_buyer_maker`
  - 50,000 rows per file
- **State**: `state.json` - Resume state (last trade ID, orderbook seq, timestamps, part counters)

**Data Storage**:
```
data/{market}/
  orderbook_parts/part_TIMESTAMP_SEQ_PART.parquet    # Full depth (20 levels default)
  trades_parts/part_TIMESTAMP_PART.parquet           # Public trades
  state.json                                         # Resume state
```

**Performance**: 11.8x compression ratio, 13% smaller than SNAPPY, 26% faster writes than SNAPPY

## Avellaneda-Stoikov Model

**AS Quote Calculation** (`spread_model.rs:compute_optimal_quote`):
- Computes optimal bid/ask spreads based on inventory risk and market volatility with **asymmetric spreads**
- **Bid Spread Formula**: `δ_bid = γσ²T + (2/γ)ln(1 + γ/κ_bid)`
- **Ask Spread Formula**: `δ_ask = γσ²T + (2/γ)ln(1 + γ/κ_ask)`
  - `γ` (gamma): Risk aversion parameter (constant, inventory-scaled, or max-shift mode)
  - `σ` (sigma): Volatility (calculated from price returns)
  - `κ_bid` (bid kappa): Fill intensity for bid side (fitted from trades hitting bids)
  - `κ_ask` (ask kappa): Fill intensity for ask side (fitted from trades hitting asks)
  - `T`: Time horizon (inventory_horizon_seconds)
- Inventory skew: `reservation_price = mid - γσ²T·q` (q = current inventory)
- Quote placement:
  - `bid_price = reservation_price - δ_bid/2`
  - `ask_price = reservation_price + δ_ask/2`
- Enforces minimum spread (2× maker fee), maximum spread, and tick size rounding

**Gamma Modes** (`model_types.rs:GammaMode`):
- `Constant`: Fixed risk aversion
- `InventoryScaled`: Scales with inventory ratio (0 at center, max at limits)
- `MaxShift`: Derived from max_shift_ticks to cap price skew

**Parameter Calibration** (`calibration.rs`):
- `calculate_volatility`: Computes σ from log returns over rolling window
- `fit_intensity_parameters`: Fits separate bid/ask parameters (A_bid, κ_bid, A_ask, κ_ask) using exponential regression on trade arrival rates vs. price deltas
  - Bid trades: `λ_bid(δ) = A_bid · e^(-κ_bid·δ)` where δ is distance below mid
  - Ask trades: `λ_ask(δ) = A_ask · e^(-κ_ask·δ)` where δ is distance above mid
  - Returns 4 values: (bid_kappa, bid_a, ask_kappa, ask_a)

## Backtesting

**Event-Driven Engine** (`src/bin/backtest.rs`):
- Processes orderbooks chronologically, simulates fills between orderbook updates
- Enforces inventory limits, transaction costs (maker/taker fees), cash balance
- Supports fill cooldowns (separate bid/ask to allow two-way flow)
- Handles data gaps: warm-up period after gaps > gap_threshold_seconds
- Dynamic recalibration: Updates σ, κ_bid, A_bid, κ_ask, A_ask every recalibration_interval_seconds

**Fill Logic**:
- Quote valid for quote_validity_seconds after orderbook update
- Fill occurs if market crosses posted quote before next orderbook update
- Uses mid-point of crossing candle for fill price (conservative estimate)
- Respects fill_cooldown_seconds per side (last_bid_fill_ts, last_ask_fill_ts)

**Output**: `data/{market}/backtest_results.csv` and `as_results.csv` with detailed metrics

## Configuration (config.json)

**Data Collection**:
- `markets`: List of markets (e.g., ["ETH-USD", "BTC-USD"])
- `data_directory`: Output directory (default: "data")
- `collect_orderbook`, `collect_trades`, `collect_full_orderbook`: Enable/disable streams
- `max_depth_levels`: Orderbook depth (default: 20)

**AS Backtesting**:
- `inventory_horizon_seconds`: Time horizon T (default: 1800)
- `risk_aversion_gamma`: Risk aversion γ (default: 0.1)
- `fill_cooldown_seconds`: Min time between fills on same side (default: 60)
- `maker_fee_bps`, `taker_fee_bps`: Transaction costs (default: 1.5, 4.5)
- `gap_threshold_seconds`: Max gap before warm-up (default: 1800)
- `warmup_period_seconds`: Warm-up duration after gap (default: 900)
- `quote_validity_seconds`: Quote expiration time (default: 60)
- `calibration_window_seconds`: Window for σ/κ calculation (default: 3600)
- `recalibration_interval_seconds`: How often to recalibrate (default: 600)

**Advanced**:
- `gamma_mode`: "constant", "inventory_scaled", or "max_shift"
- `max_shift_ticks`: Max price skew from mid (used in max_shift mode)
- `min_spread_bps`, `max_spread_bps`: Spread bounds
- `min_volatility`, `max_volatility`: Volatility clamps

## Important Workflow Notes

1. **Collect data FIRST**: Backtesting requires historical data. Run `collect_data` for several hours minimum before backtesting.
2. **Warm-up periods**: Backtest skips the first `warmup_period_seconds` and after gaps > `gap_threshold_seconds` to ensure calibration convergence.
3. **No look-ahead bias**: Fill simulation uses only information available before the fill (orderbook seq, trade timestamps).
4. **Resume capability**: Data collection can be stopped/started - it resumes from last state.

## No API Key Required

Public data collection works without authentication. For authenticated WebSocket (account updates), set `API_KEY` in `.env`.
