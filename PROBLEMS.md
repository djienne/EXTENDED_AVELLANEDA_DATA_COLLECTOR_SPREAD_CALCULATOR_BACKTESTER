# Codebase Review: Problems & Flaws

This document outlines potential flaws and areas for improvement identified during the codebase review of the Extended DEX Data Collector & Backtester.

## 🔴 Critical Severity

### Irrecoverable Data Loss on Disconnect
**Location:** `src/websocket.rs`, `src/bin/collect_data.rs`

**The Issue:**
The data collection architecture currently guarantees data loss during any network interruption. The `collect_data` binary utilizes a loop that simply sleeps for 5 seconds when a WebSocket connection drops before attempting to reconnect.

**Impact:**
*   **Blind Spots:** There is no mechanism to "catch up" or backfill missing data (e.g., via REST API replay).
*   **Invalid Backtests:** A 5-second (or longer) gap invalidates the assumption of a continuous market. Backtesting strategies over these gaps without awareness will lead to incorrect results, as the engine will assume no price movement or trades occurred during the outage.

**Recommendation:**
*   **Gap Detection:** Implement a sequence number tracker to detect gaps immediately upon reconnection.
*   **Gap Logging:** Log the start and end timestamps of any disconnection to a `gaps.csv` file.
*   **Backfill Strategy:** If an API endpoint exists, fetch missing ranges. If not, the backtester *must* be updated to read `gaps.csv` and invalidate/skip those periods rather than treating them as quiet trading periods.

## 🟠 High Severity

### Precision Safety Violations
**Location:** `src/spread_model.rs`, `src/calibration.rs`

**The Issue:**
The project uses `rust_decimal` for financial types but defeats its purpose by immediately converting to standard `f64` floating-point numbers for critical calculations.

```rust
// src/spread_model.rs
let mid_f64 = mid_price.to_f64().unwrap_or(0.0);
// ... heavy math on floats ...
let mut spread = Decimal::from_f64(optimal_spread_f64).unwrap_or(Decimal::ZERO);
```

**Impact:**
*   **Floating Point Errors:** Representation errors (e.g., binary floating point cannot exactly represent 0.1) will accumulate.
*   **Penny Drift:** Over thousands of iterations, these small errors can lead to "penny drift," resulting in incorrect quote prices or inventory valuations that differ from a strictly precise ledger.

**Recommendation:**
*   **Isolate Floats:** Keep `mid_price`, `reservation_price`, and `spread` as `Decimal` types.
*   **Targeted Casting:** Only cast to `f64` for specific terms that require it (like `ln`, `sqrt`, `sigma`), and cast the result back to `Decimal` immediately before applying it to the price.

### Unscalable Memory Usage
**Location:** `src/data_loader.rs`

**The Issue:**
The `DataLoader` reads the entire historical dataset into RAM at startup.
```rust
let orderbooks = Self::load_orderbooks(orderbook_path)?; // Returns BTreeMap
```

**Impact:**
*   **OOM Crashes:** High-frequency data (10+ updates/sec) grows rapidly. Loading a full day or week of data will likely exceed available RAM, causing the backtester to crash with an Out of Memory error.
*   **Limited Backtest Scope:** This strictly limits the duration of backtests that can be run.

**Recommendation:**
*   **Streaming Iterator:** Refactor `DataLoader` to return an `Iterator` or `Stream`.
*   **Buffered Reading:** Read the CSV files line-by-line or in chunks. The backtester processes events chronologically and does not require random access to the full history.

## 🟡 Minor Issues & Code Quality

### Hardcoded Magic Numbers
*   **`src/bin/collect_data.rs`:** The 5-second retry delay is hardcoded. This prevents configuration of exponential backoff strategies, which are crucial to avoid "thundering herd" issues if multiple collectors restart at once.
*   **`src/spread_model.rs`:** Default values like `1e-6` and `10.0` (for kappa) are buried in the logic logic rather than defined as named constants or loaded from config.

### Silent Failures / Error Ambiguity
*   **`src/websocket.rs`:** The `handle_stream` function logs errors but ultimately just breaks the loop. The caller receives a generic `None` from the channel without knowing *why* the connection closed (e.g., Timeout vs. Authentication Error vs. parsing error). A specific `DisconnectReason` enum would improve debugging and automated recovery logic.
