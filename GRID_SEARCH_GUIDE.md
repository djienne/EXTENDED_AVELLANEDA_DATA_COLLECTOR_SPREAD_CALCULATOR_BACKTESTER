# Grid Search Optimization Guide

## Overview

Two grid search tools are available to find optimal parameters for the Avellaneda-Stoikov (AS) market-making strategy:

1. **`grid_search`**: Tests different time horizons
2. **`grid_search_gamma`**: Tests combinations of time horizons AND gamma values (2D search)

## Grid Search Gamma (NEW)

### What It Does

Tests all combinations of:
- **Time Horizons**: 5m, 15m, 30m, 1h, 2h, 4h
- **Gamma (Risk Aversion)**: 0.01, 0.05, 0.1, 0.2

Total: **6 horizons × 4 gammas = 24 configurations**

### Usage

```bash
cargo run --release --bin grid_search_gamma
```

### Gamma Parameter Interpretation

| Gamma | Risk Profile | Behavior |
|-------|-------------|----------|
| 0.01  | Very risk-tolerant | Minimal inventory penalty, allows large positions |
| 0.05  | Low risk aversion | Comfortable with moderate inventory |
| 0.10  | Moderate (default) | Balanced inventory management |
| 0.20  | High risk aversion | Strong preference for neutral inventory |

### Output Format

The tool provides three views of results:

#### 1. Main Results Table
Sorted by PnL (best to worst):
```
★ 30m      │   0.05 │       125.34 │      12.53 │      142 │      138 │        280 │       5600.00
  1h       │   0.10 │       118.72 │      11.87 │      156 │      152 │        308 │       6160.00
  ...
```

#### 2. Best Horizon for Each Gamma
Shows which time horizon works best with each gamma:
```
γ=0.01: Best horizon=1h → PnL=$112.45, Return=11.25%, Fills=285
γ=0.05: Best horizon=30m → PnL=$125.34, Return=12.53%, Fills=280
γ=0.10: Best horizon=1h → PnL=$118.72, Return=11.87%, Fills=308
γ=0.20: Best horizon=2h → PnL=$105.89, Return=10.59%, Fills=245
```

#### 3. Best Gamma for Each Horizon
Shows which gamma works best with each time horizon:
```
H=5m: Best gamma=0.05 → PnL=$98.45, Return=9.85%, Fills=380
H=15m: Best gamma=0.05 → PnL=$115.23, Return=11.52%, Fills=325
H=30m: Best gamma=0.05 → PnL=$125.34, Return=12.53%, Fills=280
...
```

### Understanding the Results

**High PnL with Low Gamma (0.01-0.05)**:
- Strategy benefits from holding inventory
- Market has strong directional trends
- Less inventory penalty allows capturing more upside

**High PnL with High Gamma (0.10-0.20)**:
- Strategy benefits from staying neutral
- Market is mean-reverting
- Strong inventory management prevents adverse selection

**Optimal Gamma Changes with Horizon**:
- Shorter horizons (5m-15m): Often prefer lower gamma (more flexibility)
- Medium horizons (30m-1h): Usually prefer moderate gamma (0.05-0.10)
- Longer horizons (2h-4h): May prefer higher gamma (more conservative)

## Original Grid Search

Tests only time horizons with your configured gamma:

```bash
cargo run --release --bin grid_search
```

Tests: 1m, 5m, 15m, 30m, 1h, 2h, 3h, 4h

## Performance Tips

1. **Use --release flag**: Grid searches run much faster in release mode
2. **Start with 2D search**: `grid_search_gamma` gives more comprehensive results
3. **Filter by fills**: Results with fewer than 5 fills are marked as low-confidence
4. **Look for consistency**: Best parameters should be robust across similar values

## Customization

To test different ranges, edit the source files:

**`src/bin/grid_search_gamma.rs`**:
```rust
// Line 71-72: Modify time horizons (in seconds)
let horizons = vec![300, 900, 1800, 3600, 7200, 14400];

// Line 74-75: Modify gamma values
let gammas = vec![0.01, 0.05, 0.1, 0.2];
```

Example modifications:
```rust
// More granular gamma search
let gammas = vec![0.02, 0.05, 0.08, 0.10, 0.15, 0.20];

// Longer horizons
let horizons = vec![1800, 3600, 7200, 14400, 21600, 28800]; // 30m to 8h

// Very aggressive (low gamma)
let gammas = vec![0.005, 0.01, 0.02, 0.05];

// Very conservative (high gamma)
let gammas = vec![0.10, 0.20, 0.35, 0.50];
```

## Next Steps

After finding optimal parameters:

1. **Update config.json** with best values:
   ```json
   {
     "inventory_horizon_seconds": 1800,
     "risk_aversion_gamma": 0.05
   }
   ```

2. **Run full backtest** with detailed output:
   ```bash
   cargo run --release --bin backtest
   ```

3. **Analyze results** in `data/eth_usd/backtest_results.csv`

4. **Validate robustness**: Test on different time periods or markets

## Theoretical Background

### Time Horizon (T)
- Controls how far ahead the strategy optimizes
- Shorter T: More reactive, tighter spreads, more fills
- Longer T: More patient, wider spreads, fewer fills

### Gamma (γ)
- Risk aversion parameter in AS model
- Determines inventory penalty in optimal quote calculation
- Formula: `reservation_price = mid - γσ²T·q`
  - Higher γ → stronger push to neutral inventory
  - Lower γ → more tolerance for inventory risk

### Interaction
The optimal gamma depends on the time horizon:
- Short horizons need flexibility → lower gamma
- Long horizons need discipline → higher gamma

The 2D grid search reveals this interaction automatically.
