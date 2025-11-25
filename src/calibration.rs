use crate::model_types::TradeEvent;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::collections::HashMap;

// ============================================================================
// Constants
// ============================================================================

/// Minimum samples required for variance calculation (need at least 2 for sample variance)
const MIN_SAMPLES_FOR_VARIANCE: usize = 2;

/// Minimum deltas required for regression
const MIN_DELTAS_FOR_REGRESSION: usize = 10;

/// Minimum data points for linear regression
const MIN_POINTS_FOR_REGRESSION: usize = 2;

/// Default kappa when calibration fails
const DEFAULT_KAPPA: f64 = 10.0;

/// Default A when calibration fails
const DEFAULT_A: f64 = 10.0;

/// Epsilon for floating point comparisons
const EPSILON: f64 = 1e-9;

// ============================================================================
// Volatility Calculation
// ============================================================================

/// Calculate volatility (sigma) from price updates
/// Returns volatility scaled to per-second
pub fn calculate_volatility(
    prices: &[Decimal],
    window_duration_seconds: f64,
) -> f64 {
    if prices.len() < 2 || window_duration_seconds <= 0.0 {
        return 0.0;
    }

    // Pre-allocate for expected size
    let mut log_returns = Vec::with_capacity(prices.len() - 1);
    
    for i in 1..prices.len() {
        let p1 = prices[i - 1].to_f64().unwrap_or(0.0);
        let p2 = prices[i].to_f64().unwrap_or(0.0);

        if p1 > 0.0 && p2 > 0.0 {
            log_returns.push((p2 / p1).ln());
        }
    }

    // Need at least 2 samples for sample variance (n-1 denominator)
    if log_returns.len() < MIN_SAMPLES_FOR_VARIANCE {
        return 0.0;
    }

    let n = log_returns.len() as f64;
    let mean: f64 = log_returns.iter().sum::<f64>() / n;
    
    // Sample variance with Bessel's correction (n-1 denominator)
    let variance: f64 = log_returns
        .iter()
        .map(|r| (r - mean).powi(2))
        .sum::<f64>()
        / (log_returns.len() - 1) as f64;
    
    let std_dev = variance.sqrt();

    // Handle edge case where std_dev is NaN or infinite
    if !std_dev.is_finite() {
        return 0.0;
    }

    // Scale to per-second volatility
    // We have `n` samples over `window_duration_seconds`
    // Average time between samples: dt = window / n
    // sigma_per_second = sigma_sample / sqrt(dt) = sigma_sample * sqrt(n / window)
    let count = log_returns.len() as f64;
    let scaled_sigma = std_dev * (count / window_duration_seconds).sqrt();

    if scaled_sigma.is_finite() {
        scaled_sigma
    } else {
        0.0
    }
}

// ============================================================================
// Intensity Parameter Fitting
// ============================================================================

/// Perform linear regression on one side (bid or ask) to extract kappa and A
///
/// Model: λ(δ) = A * exp(-κ * δ)
/// Taking log: ln(λ) = ln(A) - κ * δ
/// Linear regression: y = α + β * x, where y = ln(rate), x = delta
/// Therefore: A = exp(α), κ = -β
fn regress_side(
    deltas: &[f64],
    window_duration_seconds: f64,
    tick_size: f64,
) -> Option<(f64, f64)> {
    if deltas.len() < MIN_DELTAS_FOR_REGRESSION 
        || window_duration_seconds <= 0.0 
        || tick_size <= 0.0 
    {
        return None;
    }

    // Bin deltas by tick size
    let mut bins: HashMap<i64, usize> = HashMap::with_capacity(deltas.len() / 2);

    for delta in deltas {
        let ticks = (delta / tick_size).round() as i64;
        if ticks >= 0 {
            *bins.entry(ticks).or_insert(0) += 1;
        }
    }

    // Prepare regression data
    let mut x_data = Vec::with_capacity(bins.len());
    let mut y_data = Vec::with_capacity(bins.len());

    for (tick_idx, count) in bins {
        let delta_price = tick_idx as f64 * tick_size;
        let rate = count as f64 / window_duration_seconds;

        // Only include positive rates (ln(0) is undefined)
        if rate > 0.0 {
            let ln_rate = rate.ln();
            // Skip if ln produced non-finite value
            if ln_rate.is_finite() {
                x_data.push(delta_price);
                y_data.push(ln_rate);
            }
        }
    }

    if x_data.len() < MIN_POINTS_FOR_REGRESSION {
        return None;
    }

    // Ordinary least squares regression
    let n = x_data.len() as f64;
    let sum_x: f64 = x_data.iter().sum();
    let sum_y: f64 = y_data.iter().sum();
    let sum_xy: f64 = x_data.iter().zip(y_data.iter()).map(|(x, y)| x * y).sum();
    let sum_xx: f64 = x_data.iter().map(|x| x * x).sum();

    let denominator = n * sum_xx - sum_x * sum_x;
    if denominator.abs() < EPSILON {
        return None;
    }

    let beta = (n * sum_xy - sum_x * sum_y) / denominator;
    let alpha = (sum_y - beta * sum_x) / n;

    // Extract parameters: κ = -β, A = exp(α)
    let kappa = -beta;
    let a = alpha.exp();

    // Validate results
    if !kappa.is_finite() || !a.is_finite() || kappa <= 0.0 || a <= 0.0 {
        return None;
    }

    Some((kappa, a))
}

/// Find the mid-price index for a given timestamp using binary search
/// Returns the index of the last mid-price with timestamp <= target
#[inline]
fn find_mid_price_index(mid_prices: &[(u64, Decimal)], target_ts: u64, hint: usize) -> usize {
    // Start from hint and scan forward (common case: timestamps are roughly in order)
    let mut idx = hint;
    
    while idx + 1 < mid_prices.len() && mid_prices[idx + 1].0 <= target_ts {
        idx += 1;
    }
    
    idx
}

/// Calibrates intensity parameters A and kappa using regression on trade arrival rates
///
/// # Arguments
/// * `trades` - List of trades in the window
/// * `mid_prices` - History of mid-prices (timestamp, price) sorted by timestamp
/// * `window_duration_seconds` - The duration of the window in seconds
/// * `tick_size` - The tick size for binning (e.g., 0.01)
///
/// # Returns
/// Tuple of (kappa, A) parameters
pub fn fit_intensity_parameters(
    trades: &[TradeEvent],
    mid_prices: &[(u64, Decimal)],
    window_duration_seconds: f64,
    tick_size: f64,
) -> (f64, f64) {
    if trades.is_empty() || mid_prices.is_empty() || window_duration_seconds <= 0.0 {
        return (DEFAULT_KAPPA, DEFAULT_A);
    }

    // Pre-allocate with reasonable estimates
    let estimated_per_side = trades.len() / 2;
    let mut bid_deltas = Vec::with_capacity(estimated_per_side);
    let mut ask_deltas = Vec::with_capacity(estimated_per_side);

    let mut mid_idx = 0;

    for trade in trades {
        // Find the appropriate mid-price for this trade's timestamp
        mid_idx = find_mid_price_index(mid_prices, trade.timestamp, mid_idx);

        let mid = mid_prices[mid_idx].1;
        let price = trade.price;

        if trade.is_buyer_maker {
            // Buyer is maker = sell market order hit the bid
            // Delta = how far below mid the trade occurred
            let delta = mid - price;
            if delta > Decimal::ZERO {
                if let Some(d) = delta.to_f64() {
                    if d.is_finite() && d > 0.0 {
                        bid_deltas.push(d);
                    }
                }
            }
        } else {
            // Seller is maker = buy market order hit the ask
            // Delta = how far above mid the trade occurred
            let delta = price - mid;
            if delta > Decimal::ZERO {
                if let Some(d) = delta.to_f64() {
                    if d.is_finite() && d > 0.0 {
                        ask_deltas.push(d);
                    }
                }
            }
        }
    }

    // Fit both sides
    let bid_fit = regress_side(&bid_deltas, window_duration_seconds, tick_size);
    let ask_fit = regress_side(&ask_deltas, window_duration_seconds, tick_size);

    // Combine results
    match (bid_fit, ask_fit) {
        (Some((k_b, a_b)), Some((k_a, a_a))) => {
            // Average both sides
            let kappa = (k_b + k_a) / 2.0;
            let a = (a_b + a_a) / 2.0;
            (kappa, a)
        }
        (Some((k, a)), None) | (None, Some((k, a))) => (k, a),
        (None, None) => (DEFAULT_KAPPA, DEFAULT_A),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_volatility_empty() {
        assert_eq!(calculate_volatility(&[], 100.0), 0.0);
    }

    #[test]
    fn test_volatility_single_price() {
        let prices = vec![Decimal::from(100)];
        assert_eq!(calculate_volatility(&prices, 100.0), 0.0);
    }

    #[test]
    fn test_volatility_two_prices() {
        // With only 2 prices, we get 1 log return, which is < MIN_SAMPLES_FOR_VARIANCE
        let prices = vec![Decimal::from(100), Decimal::from(101)];
        assert_eq!(calculate_volatility(&prices, 100.0), 0.0);
    }

    #[test]
    fn test_volatility_three_prices() {
        // With 3 prices, we get 2 log returns, which meets MIN_SAMPLES_FOR_VARIANCE
        let prices = vec![
            Decimal::from(100),
            Decimal::from(101),
            Decimal::from(102),
        ];
        let vol = calculate_volatility(&prices, 100.0);
        assert!(vol >= 0.0);
        assert!(vol.is_finite());
    }

    #[test]
    fn test_volatility_constant_prices() {
        let prices: Vec<Decimal> = (0..10).map(|_| Decimal::from(100)).collect();
        let vol = calculate_volatility(&prices, 100.0);
        assert_eq!(vol, 0.0); // No variance in constant prices
    }

    #[test]
    fn test_fit_intensity_empty() {
        let result = fit_intensity_parameters(&[], &[], 100.0, 0.01);
        assert_eq!(result, (DEFAULT_KAPPA, DEFAULT_A));
    }

    #[test]
    fn test_default_constants() {
        // Ensure defaults are reasonable
        assert!(DEFAULT_KAPPA > 0.0);
        assert!(DEFAULT_A > 0.0);
    }
}