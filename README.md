# Extended DEX Data Collector & Backtester

## Objective
This project is designed to collect high-frequency market data from **Extended DEX**, **backtest market-making strategies**, and explore optimal spread calculations using the **Avellaneda-Stoikov (AS)** market-making model.

**Note**: The goal of this repository is **strictly for backtesting and research purposes**. It is **not** intended for live trading or execution strategies. It provides a robust framework for capturing real-time orderbook and trade data, calibrating model parameters ($\gamma$, $\kappa$, $\sigma$), and backtesting strategies with realistic constraints.

## Key Features

### 1. Data Collection
*   **Real-time Capture**: Connects to Extended DEX WebSocket to stream trades and orderbooks.
*   **Precision**: Timestamps preserved with millisecond precision.
*   **Resilience**: Automatic state saving and resume capability.

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
Start collecting market data to `data/{market}/`:
```bash
cargo run --bin collect_data
```

### 2. Run Backtest
Simulate the AS strategy using collected data:
```bash
cargo run --bin backtest
```
Results are saved to `data/ETH_USD/backtest_results.csv`.

### 3. Calculate Spreads
Compute optimal quotes based on current market state:
```bash
cargo run --bin calculate_spread
```

## Configuration (`config.json`)
*   `markets`: List of markets to collect (e.g., `["ETH-USD", "BTC-USD"]`).
*   `inventory_horizon_seconds`: Time horizon for the AS model (e.g., `3600` for 1 hour).
*   `fill_cooldown_seconds`: Minimum time between fills on the same side (e.g., `30`).
*   `risk_aversion_gamma`: Risk aversion parameter (default `0.1`).

---
*Note: This project is for research and backtesting purposes.*
