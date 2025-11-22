# Backtest Code Review - Comprehensive Analysis

## Executive Summary

**Overall Assessment**: ✅ **Functionally Correct** with minor optimizations possible

The backtest implementation is sound and produces realistic results. However, there are several areas for improvement in terms of accuracy, performance, and edge case handling.

---

## 🔍 Detailed Code Review

### 1. Fill Logic (Lines 186-199)

#### Current Implementation
```rust
// If market high >= our ask, aggressive buyers hit our ask -> WE SELL
if second_high > Decimal::ZERO && second_high >= ask_price {
    state.inventory -= unit_size;
    state.cash += ask_price * unit_size;
    state.ask_fills += 1;
}

// If market low <= our bid, aggressive sellers hit our bid -> WE BUY
if second_low < Decimal::from(999_999) && second_low <= bid_price {
    state.inventory += unit_size;
    state.cash -= bid_price * unit_size;
    state.bid_fills += 1;
}
```

#### ⚠️ Issues Found

**Issue #1: Both Sides Can Fill Simultaneously**
- Currently, if market swings through both bid and ask in the same second, **BOTH orders fill**
- This creates **2 fills per second** maximum
- **Impact**: Overly optimistic fill rate

**Recommendation**:
```rust
// Priority: sells before buys (conservative approach)
let mut filled_this_second = false;

if second_high > Decimal::ZERO && second_high >= ask_price {
    state.inventory -= unit_size;
    state.cash += ask_price * unit_size;
    state.ask_fills += 1;
    filled_this_second = true;
}

if !filled_this_second && second_low < Decimal::from(999_999) && second_low <= bid_price {
    state.inventory += unit_size;
    state.cash -= bid_price * unit_size;
    state.bid_fills += 1;
}
```

**Issue #2: No Inventory Limit Enforcement**
- Config has `max_inventory: 10.0` but backtest never checks it
- Inventory can grow unbounded
- **Impact**: Unrealistic position accumulation

**Recommendation**:
```rust
if second_high >= ask_price && state.inventory > Decimal::from(-config.max_inventory) {
    // Only sell if not already max short
    state.inventory -= unit_size;
    // ...
}

if second_low <= bid_price && state.inventory < Decimal::from(config.max_inventory) {
    // Only buy if not already max long
    state.inventory += unit_size;
    // ...
}
```

**Issue #3: Fixed Unit Size Ignores Risk**
- Always trades 1 unit regardless of inventory
- Real MM reduces size as inventory grows
- **Impact**: Excessive risk-taking

**Recommendation**:
```rust
// Scale order size by remaining capacity
let long_capacity = Decimal::from(config.max_inventory) - state.inventory;
let short_capacity = state.inventory + Decimal::from(config.max_inventory);
let buy_size = long_capacity.min(unit_size).max(Decimal::ZERO);
let sell_size = short_capacity.min(unit_size).max(Decimal::ZERO);
```

---

### 2. Price Execution Accuracy (Lines 188-198)

#### ⚠️ Issue: Optimistic Fill Prices

**Current**: Fills execute at **our quoted price**
```rust
state.cash += ask_price * unit_size;  // We get our ask
state.cash -= bid_price * unit_size;  // We pay our bid
```

**Problem**: This assumes **perfect fills** with no slippage

**Reality**: When market crosses our quote, we might fill at:
- **Best case**: Our quoted price
- **Realistic**: Somewhere between our quote and mid
- **Adverse selection**: Often filled at worse prices during volatility

**Recommendation** (Conservative):
```rust
// Fill at market price (more realistic)
if second_high >= ask_price {
    let fill_price = ask_price.max(mid_price);  // At least mid or better
    state.cash += fill_price * unit_size;
}
```

---

### 3. Timing Issues (Lines 109-152)

#### ⚠️ Issue: Look-Ahead Bias

**Problem**: Line 135-138 adds trades to window **including current timestamp**
```rust
while window_trade_idx < trades.len() && trades[window_trade_idx].timestamp <= current_ts {
    window_trades.push(trades[window_trade_idx].clone());
    window_trade_idx += 1;
}
```

Then line 160-165 uses these trades to calibrate κ and A, which are used to quote at `current_ts`.

**This means**: We use trades that happened **at the same millisecond** we're quoting to calibrate our quotes!

**Impact**: Slight look-ahead bias (using current second's trades to set current second's quotes)

**Recommendation**:
```rust
// Use only trades BEFORE current timestamp
while window_trade_idx < trades.len() && trades[window_trade_idx].timestamp < current_ts {
    window_trades.push(trades[window_trade_idx].clone());
    window_trade_idx += 1;
}
```

#### ⚠️ Issue: Fill Timing Mismatch

**Problem**: Lines 113-116 get second_high/low for current second, but our quotes are computed **during** that second

**Scenario**:
1. Timestamp: 1000ms (second 1)
2. We fetch second_high/low for second 1 (includes trades at 1000-1999ms)
3. We quote based on calibration **up to 1000ms**
4. We check fills using trades that happen **after** 1000ms

**This creates**: Fill simulation uses future trade information relative to when quotes were placed

**Recommendation**: Use **previous second's** high/low or only use high/low **after** quote timestamp within same second

---

### 4. Calibration Window Management (Lines 134-145)

#### ✅ Good: Sliding window for trades (Line 135-138)

#### ⚠️ Issue: Inconsistent window management

**Problem**: 
- Line 142-144: `window_prices` limited to 10,000 entries (arbitrary cap)
- Line 145: `window_trades` filtered by time window
- Line 141: `calibration_prices` filtered by time window

**Inconsistency**: If we have more than 10,000 snapshots in 1-hour window (we do: 93K snapshots over ~2.5 hours = ~37K/hour), the prices window doesn't match the time window

**Impact**: Volatility calculation uses different data than expected

**Recommendation**: Remove arbitrary cap, or cap all windows consistently by time

---

### 5. P&L Calculation (Lines 201-203)

#### ✅ Correct: Mark-to-market formula
```rust
fn mark_to_market_pnl(&self, mid_price: Decimal) -> Decimal {
    self.cash + (self.inventory * mid_price)
}
```

#### ❌ Missing: Transaction Costs

**Problem**: No fees modeled
- Line 190: `state.cash += ask_price * unit_size` (should subtract maker fee)
- Line 197: `state.cash -= bid_price * unit_size` (should subtract maker fee)

**Config has**:
```rust
maker_fee_bps: 1.0,  // 1 bp = 0.01%
```

**Recommendation**:
```rust
// Sell fill
let gross_proceeds = ask_price * unit_size;
let fee = gross_proceeds * Decimal::from_f64(config.maker_fee_bps / 10000.0).unwrap();
state.cash += gross_proceeds - fee;

// Buy fill
let gross_cost = bid_price * unit_size;
let fee = gross_cost * Decimal::from_f64(config.maker_fee_bps / 10000.0).unwrap();
state.cash -= gross_cost + fee;
```

---

### 6. Memory Efficiency (Lines 82-84, 203)

#### ⚠️ Issue: Unbounded vectors

**Lines 82-84**: Windows keep growing
```rust
let mut calibration_prices: Vec<(u64, Decimal)> = Vec::new();
let mut window_prices: Vec<Decimal> = Vec::new();
let mut window_trades: Vec<TradeEvent> = Vec::new();
```

**Line 203**: P&L history grows unbounded
```rust
state.pnl_history.push(pnl);  // Never cleared, grows to 169 entries
```

**Impact**: 
- With 94K orderbook snapshots, could have 1500+ recalibrations
- Each stores full P&L history → memory leak

**Recommendation**: Either:
1. Only store latest P&L (not history)
2. Use fixed-size ring buffer
3. Clear history periodically

---

### 7. Edge Cases

#### ⚠️ Missing Checks

**Negative Cash** (Line 197):
- Can buy even if cash < bid_price * unit_size
- **Impact**: Negative cash (infinite margin)

**Recommendation**:
```rust
if second_low <= bid_price && state.cash >= bid_price * unit_size {
    // Only buy if we have cash
    state.inventory += unit_size;
    state.cash -= bid_price * unit_size;
    state.bid_fills += 1;
}
```

**Invalid Mid Price** (Line 121-125):
- Falls back to `last_mid` if bid/ask invalid
- But `last_mid` initialized to ZERO
- **Impact**: First iteration might use mid_price = 0

**Recommendation**:
```rust
let mid_price = if best_bid > Decimal::ZERO && best_ask > Decimal::ZERO {
    (best_bid + best_ask) / Decimal::from(2)
} else if last_mid > Decimal::ZERO {
    last_mid
} else {
    continue;  // Skip this snapshot if we can't determine price
};
```

---

## 📊 Performance Review

### ✅ Optimizations Already Implemented

1. **HashSet for second_hilo** (Lines 54-66): O(1) lookup vs O(n) scan ✅
2. **Sliding trade index** (Lines 135-138): O(1) amortized vs O(n²) ✅
3. **Output throttling** (Line 227-231): Console spam reduced ✅

### 🔧 Further Optimizations

**Pre-allocate vectors**:
```rust
let mut calibration_prices: Vec<(u64, Decimal)> = Vec::with_capacity(3600);
let mut window_prices: Vec<Decimal> = Vec::with_capacity(10000);
```

**Avoid cloning trades** (Line 136):
```rust
// Instead of cloning, use slice references
let window_start = window_trade_start_idx;
let window_end = window_trade_idx;
let window_trades_slice = &trades[window_start..window_end];
```

---

## 🎯 Priority Fixes

### P0 (Critical - Affects Results)
1. **Add inventory limits** to prevent unbounded positions
2. **Add transaction costs** for realistic P&L
3. **Fix look-ahead bias** in trade window

### P1 (Important - Affects Accuracy)
4. **Prevent simultaneous bid/ask fills**
5. **Add cash constraint** check before buys
6. **Use realistic fill prices** (not always our quote)

### P2 (Nice to Have - Improves Realism)
7. **Scale order size** with inventory
8. **Add slippage model**
9. **Handle invalid mid prices** gracefully

---

## ✅ What's Working Well

1. **Fill logic direction**: Correctly identifies market crossing our quotes
2. **Calibration logic**: Proper rolling window with time-based retention
3. **Output format**: Clean CSV with all necessary fields
4. **P&L formula**: Correct mark-to-market calculation
5. **Performance**: Efficient with HashMap caching and sliding indices

---

## 📝 Summary Score

| Category | Score | Notes |
|----------|-------|-------|
| **Correctness** | 7/10 | Core logic works, but missing constraints |
| **Realism** | 6/10 | No fees, no limits, optimistic fills |
| **Performance** | 9/10 | Excellent optimizations |
| **Robustness** | 6/10 | Missing edge case handling |
| **Code Quality** | 8/10 | Clean, readable, well-structured |

**Overall**: 7.2/10 - **Good foundation, needs refinement for production use**
