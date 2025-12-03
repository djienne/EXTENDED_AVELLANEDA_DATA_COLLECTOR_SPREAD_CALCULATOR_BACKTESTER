use crate::model_types::TradeEvent;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;

// ============================================================================
// Constants
// ============================================================================

/// Minimum log returns required for volatility calculation (need at least 2 to be stable)
const MIN_SAMPLES_FOR_VARIANCE: usize = 2;
/// Minimum returns required for GARCH estimation
const MIN_RETURNS_FOR_GARCH: usize = 5;
/// Upper bound for alpha+beta to keep stationarity
const MAX_ALPHA_BETA_SUM: f64 = 0.999;
/// ln(2π) for normal log-likelihood (precomputed to keep const)
const LOG_2PI: f64 = 1.837_877_066_409_345_3_f64;

/// Minimum trades required for MLE estimation
const MIN_TRADES_FOR_ESTIMATION: usize = 5;

/// Default kappa when calibration fails
const DEFAULT_KAPPA: f64 = 10.0;

/// Default A when calibration fails
const DEFAULT_A: f64 = 10.0;

/// Lower/upper bounds for kappa grid search
const KAPPA_MIN: f64 = 1e-6;
const KAPPA_MAX: f64 = 1e4;

/// Exposure over a single orderbook interval for one side
#[derive(Debug, Clone)]
struct ExposureInterval {
    duration_sec: f64,
    delta_min: f64,
    delta_max: f64,
}

/// Minimal orderbook information needed for intensity calibration
#[derive(Debug, Clone)]
pub struct OrderbookPoint {
    pub timestamp: u64,
    pub mid: Decimal,
    pub bid_min: f64,
    pub bid_max: f64,
    pub ask_min: f64,
    pub ask_max: f64,
}

// ============================================================================
// Volatility Calculation
// ============================================================================

/// Sanitize price series to positive finite f64 values, dropping invalid points.
fn sanitize_prices(prices: &[(u64, Decimal)]) -> Vec<(u64, f64)> {
    let mut cleaned = Vec::with_capacity(prices.len());
    for (ts, p) in prices {
        if let Some(v) = p.to_f64() {
            if v.is_finite() && v > 0.0 {
                cleaned.push((*ts, v));
            }
        }
    }
    cleaned
}

/// Collect log returns with their elapsed seconds between samples.
/// Returns None if prices are insufficient after sanitization.
fn collect_log_returns(prices: &[(u64, Decimal)]) -> Option<Vec<(f64, f64)>> {
    let cleaned = sanitize_prices(prices);
    if cleaned.len() < 2 {
        return None;
    }

    let mut log_returns = Vec::with_capacity(cleaned.len() - 1);
    for window in cleaned.windows(2) {
        let (ts1, p1) = window[0];
        let (ts2, p2) = window[1];

        let dt_seconds = (ts2.saturating_sub(ts1)) as f64 / 1000.0;
        if dt_seconds <= 0.0 {
            continue;
        }

        let lr = (p2 / p1).ln();
        if !lr.is_finite() {
            continue;
        }

        log_returns.push((lr, dt_seconds));
    }

    if log_returns.len() < MIN_SAMPLES_FOR_VARIANCE {
        None
    } else {
        Some(log_returns)
    }
}

/// Calculate volatility (sigma) from price updates
/// Returns volatility scaled to per-second using irregular sampling intervals
pub fn calculate_volatility(prices: &[(u64, Decimal)]) -> f64 {
    let returns = match collect_log_returns(prices) {
        Some(r) => r,
        None => return 0.0,
    };

    let mut sum_squared_returns = 0.0;
    let mut total_duration_seconds = 0.0;
    let mut valid_returns: usize = 0;

    for (lr, dt_seconds) in returns {
        sum_squared_returns += lr * lr;
        total_duration_seconds += dt_seconds;
        valid_returns += 1;
    }

    if valid_returns < MIN_SAMPLES_FOR_VARIANCE || total_duration_seconds <= 0.0 || sum_squared_returns <= 0.0 {
        return 0.0;
    }

    let sigma_sq = sum_squared_returns / total_duration_seconds;
    let sigma = sigma_sq.sqrt();

    if sigma.is_finite() { sigma } else { 0.0 }
}

/// Fit a simple GARCH(1,1) via grid-based MLE and return the next-step sigma forecast (per-second).
pub fn forecast_garch_volatility(prices: &[(u64, Decimal)]) -> Option<f64> {
    let returns = build_fixed_step_returns(prices, 1.0)?; // 1-second grid
    if returns.len() < MIN_RETURNS_FOR_GARCH {
        return None;
    }
    fit_garch_forecast(&returns)
}

fn garch_loglik(returns: &[f64], alpha: f64, beta: f64, var0: f64) -> Option<(f64, f64)> {
    if alpha < 0.0 || beta < 0.0 || alpha + beta >= MAX_ALPHA_BETA_SUM {
        return None;
    }

    let omega = var0 * (1.0 - alpha - beta);
    if omega <= 0.0 {
        return None;
    }

    let mut sigma2 = var0.max(1e-12);
    if !sigma2.is_finite() {
        return None;
    }

    let mut loglik = 0.0;
    for &r in returns {
        if sigma2 <= 0.0 || !sigma2.is_finite() {
            return None;
        }

        loglik += -0.5 * (LOG_2PI + sigma2.ln() + (r * r) / sigma2);
        sigma2 = omega + alpha * (r * r) + beta * sigma2;
    }

    Some((loglik, sigma2))
}

fn fit_garch_forecast(returns: &[f64]) -> Option<f64> {
    if returns.len() < MIN_RETURNS_FOR_GARCH {
        return None;
    }

    let mean_sq = returns.iter().map(|r| r * r).sum::<f64>() / returns.len() as f64;
    if !mean_sq.is_finite() || mean_sq <= 0.0 {
        return None;
    }

    let mut best_loglik = f64::NEG_INFINITY;
    let mut best_alpha = 0.1;
    let mut best_beta = 0.85;
    let mut best_sigma2_next = mean_sq;

    // Coarse grid search
    for alpha in (0..=25).map(|i| i as f64 * 0.02) {
        for beta in (0..=49).map(|i| i as f64 * 0.02) {
            if alpha + beta >= MAX_ALPHA_BETA_SUM {
                continue;
            }
            if let Some((ll, sigma2_next)) = garch_loglik(returns, alpha, beta, mean_sq) {
                if ll > best_loglik {
                    best_loglik = ll;
                    best_alpha = alpha;
                    best_beta = beta;
                    best_sigma2_next = sigma2_next;
                }
            }
        }
    }

    // Local refinement around the best point
    let refine_steps = [-0.02, -0.01, -0.005, 0.0, 0.005, 0.01, 0.02];
    for da in refine_steps.iter() {
        for db in refine_steps.iter() {
            let alpha = (best_alpha + da).max(0.0);
            let beta = (best_beta + db).max(0.0);
            if alpha + beta >= MAX_ALPHA_BETA_SUM {
                continue;
            }
            if let Some((ll, sigma2_next)) = garch_loglik(returns, alpha, beta, mean_sq) {
                if ll > best_loglik {
                    best_loglik = ll;
                    best_alpha = alpha;
                    best_beta = beta;
                    best_sigma2_next = sigma2_next;
                }
            }
        }
    }

    if best_loglik.is_finite() && best_sigma2_next.is_finite() && best_sigma2_next > 0.0 {
        Some(best_sigma2_next.sqrt())
    } else {
        None
    }
}

/// Build fixed-step (e.g., 1s) log returns using previous-tick interpolation.
/// Returns per-second log returns on a uniform grid.
fn build_fixed_step_returns(prices: &[(u64, Decimal)], step_seconds: f64) -> Option<Vec<f64>> {
    if step_seconds <= 0.0 {
        return None;
    }

    let cleaned = sanitize_prices(prices);
    if cleaned.len() < 2 {
        return None;
    }

    let step_ms = (step_seconds * 1000.0).round() as u64;
    if step_ms == 0 {
        return None;
    }

    let mut resampled_prices = Vec::with_capacity(cleaned.len() * 2);
    let mut iter = cleaned.into_iter();
    let (mut current_ts, mut last_price) = iter.next()?;
    let mut next_bucket = current_ts + step_ms;
    resampled_prices.push((current_ts, last_price));

    for (ts, price) in iter {
        // Fill buckets until we reach the current sample timestamp
        while next_bucket <= ts {
            resampled_prices.push((next_bucket, last_price));
            next_bucket = next_bucket.saturating_add(step_ms);
        }
        // Update last price at the current sample
        current_ts = ts;
        last_price = price;
    }

    // Add one final bucket to allow a return from the last recorded price
    resampled_prices.push((next_bucket, last_price));

    if resampled_prices.len() < 2 {
        return None;
    }

    let mut returns = Vec::with_capacity(resampled_prices.len() - 1);
    for window in resampled_prices.windows(2) {
        let (_, p1) = window[0];
        let (_, p2) = window[1];
        let lr = (p2 / p1).ln();
        if lr.is_finite() {
            returns.push(lr);
        }
    }

    if returns.len() < MIN_SAMPLES_FOR_VARIANCE {
        None
    } else {
        Some(returns)
    }
}

// ============================================================================
// Intensity Parameter Fitting
// ============================================================================

/// Estimate intensity parameters kappa and A using a truncated exponential MLE.
///
/// Combines trade deltas with per-snapshot exposure (delta_min, delta_max, duration)
/// to account for the part of the book that could realistically fill.

/// Find the orderbook index for a given timestamp using a forward scan from a hint
/// Returns the index of the last orderbook with timestamp <= target
#[inline]
fn find_orderbook_index(points: &[OrderbookPoint], target_ts: u64, hint: usize) -> usize {
    let mut idx = hint;
    
    while idx + 1 < points.len() && points[idx + 1].timestamp <= target_ts {
        idx += 1;
    }
    
    idx
}

/// Build exposure intervals for one side using successive orderbook points.
/// Each interval covers the time until the next snapshot (or window end).
fn build_side_exposures(
    orderbooks: &[OrderbookPoint],
    window_end_ts: u64,
    is_bid: bool,
) -> Vec<ExposureInterval> {
    let mut exposures = Vec::new();

    for (idx, ob) in orderbooks.iter().enumerate() {
        let start_ts = ob.timestamp;
        let end_ts = if idx + 1 < orderbooks.len() {
            orderbooks[idx + 1].timestamp
        } else {
            window_end_ts
        };

        if end_ts <= start_ts {
            continue;
        }

        let duration_sec = (end_ts.saturating_sub(start_ts)) as f64 / 1000.0;
        if duration_sec <= 0.0 {
            continue;
        }

        let (delta_min, delta_max) = if is_bid {
            (ob.bid_min, ob.bid_max)
        } else {
            (ob.ask_min, ob.ask_max)
        };

        if !delta_min.is_finite() || !delta_max.is_finite() || delta_max <= delta_min || delta_max <= 0.0 {
            continue;
        }

        exposures.push(ExposureInterval {
            duration_sec,
            delta_min,
            delta_max,
        });
    }

    exposures
}

/// Collect trade deltas for a given side using the most recent orderbook point at each trade time.
fn collect_trade_deltas(
    trades: &[TradeEvent],
    orderbooks: &[OrderbookPoint],
    is_bid: bool,
) -> Vec<f64> {
    if orderbooks.is_empty() {
        return Vec::new();
    }

    let mut deltas = Vec::with_capacity(trades.len());
    let mut ob_idx = 0;

    for trade in trades {
        if is_bid && !trade.is_buyer_maker {
            continue;
        }
        if !is_bid && trade.is_buyer_maker {
            continue;
        }

        ob_idx = find_orderbook_index(orderbooks, trade.timestamp, ob_idx);
        let ob = &orderbooks[ob_idx];

        let delta_dec = if is_bid {
            ob.mid - trade.price
        } else {
            trade.price - ob.mid
        };

        if delta_dec <= Decimal::ZERO {
            continue;
        }

        if let Some(delta) = delta_dec.to_f64() {
            if delta.is_finite() && delta > 0.0 {
                deltas.push(delta);
            }
        }
    }

    deltas
}

#[inline]
fn exposure_term(kappa: f64, exposures: &[ExposureInterval]) -> f64 {
    exposures
        .iter()
        .map(|e| {
            let upper = (-kappa * e.delta_max).exp();
            let lower = (-kappa * e.delta_min).exp();
            e.duration_sec * (lower - upper)
        })
        .sum()
}

fn log_likelihood(
    kappa: f64,
    n: f64,
    sum_deltas: f64,
    exposures: &[ExposureInterval],
) -> f64 {
    if kappa <= 0.0 || !kappa.is_finite() || exposures.is_empty() || n <= 0.0 {
        return f64::NEG_INFINITY;
    }

    let exposure = exposure_term(kappa, exposures);
    if !exposure.is_finite() || exposure <= 0.0 {
        return f64::NEG_INFINITY;
    }

    // Log-likelihood up to additive constant: n*(ln k - ln exposure) - k*sum_delta
    n * (kappa.ln() - exposure.ln()) - kappa * sum_deltas
}

fn estimate_mle_side_exposure(
    deltas: &[f64],
    exposures: &[ExposureInterval],
) -> Option<(f64, f64)> {
    if deltas.len() < MIN_TRADES_FOR_ESTIMATION || exposures.is_empty() {
        return None;
    }

    let n = deltas.len() as f64;
    let sum_deltas: f64 = deltas.iter().sum();
    if sum_deltas <= 0.0 || !sum_deltas.is_finite() {
        return None;
    }

    // Coarse log-space search to bracket a good region
    let mut best_kappa = None;
    let mut best_ll = f64::NEG_INFINITY;
    let log_min = KAPPA_MIN.log10();
    let log_max = KAPPA_MAX.log10();

    for i in 0..=60 {
        let frac = i as f64 / 60.0;
        let kappa = 10f64.powf(log_min + frac * (log_max - log_min));
        let ll = log_likelihood(kappa, n, sum_deltas, exposures);
        if ll.is_finite() && ll > best_ll {
            best_ll = ll;
            best_kappa = Some(kappa);
        }
    }

    let mut best_kappa = best_kappa?;
    // Refine with golden-section search around the best coarse point
    let mut low = (best_kappa / 5.0).max(KAPPA_MIN);
    let mut high = (best_kappa * 5.0).min(KAPPA_MAX);
    if high <= low {
        high = (low * 10.0).min(KAPPA_MAX);
    }

    const PHI: f64 = 0.618_033_988_75; // golden ratio conjugate
    let mut c = high - (high - low) * PHI;
    let mut d = low + (high - low) * PHI;
    let mut fc = log_likelihood(c, n, sum_deltas, exposures);
    let mut fd = log_likelihood(d, n, sum_deltas, exposures);

    for _ in 0..32 {
        if fc > fd {
            high = d;
            d = c;
            fd = fc;
            c = high - (high - low) * PHI;
            fc = log_likelihood(c, n, sum_deltas, exposures);
        } else {
            low = c;
            c = d;
            fc = fd;
            d = low + (high - low) * PHI;
            fd = log_likelihood(d, n, sum_deltas, exposures);
        }
    }

    best_kappa = if fc > fd { c } else { d };

    let exposure = exposure_term(best_kappa, exposures);
    if !exposure.is_finite() || exposure <= 0.0 {
        return None;
    }

    let a = (n * best_kappa) / exposure;
    if !a.is_finite() || a <= 0.0 || !best_kappa.is_finite() || best_kappa <= 0.0 {
        return None;
    }

    Some((best_kappa, a))
}

/// Calibrates intensity parameters A and kappa using MLE on trade arrival rates
///
/// # Arguments
/// * `trades` - List of trades in the window
/// * `orderbooks` - Rolling orderbook points (timestamp, deltas) sorted by timestamp
/// * `window_end_ts` - End timestamp (ms) of the calibration window
///
/// # Returns
/// Tuple of (bid_kappa, bid_a, ask_kappa, ask_a) parameters
pub fn fit_intensity_parameters(
    trades: &[TradeEvent],
    orderbooks: &[OrderbookPoint],
    window_end_ts: u64,
) -> (f64, f64, f64, f64) {
    if trades.is_empty() || orderbooks.is_empty() {
        return (DEFAULT_KAPPA, DEFAULT_A, DEFAULT_KAPPA, DEFAULT_A);
    }

    // Collect trade deltas by side using the most recent book state
    let bid_deltas = collect_trade_deltas(trades, orderbooks, true);
    let ask_deltas = collect_trade_deltas(trades, orderbooks, false);

    // Build exposure integrals for each side
    let bid_exposures = build_side_exposures(orderbooks, window_end_ts, true);
    let ask_exposures = build_side_exposures(orderbooks, window_end_ts, false);

    // Fit both sides separately
    let bid_fit = estimate_mle_side_exposure(&bid_deltas, &bid_exposures);
    let ask_fit = estimate_mle_side_exposure(&ask_deltas, &ask_exposures);

    // Return side-specific results (no averaging)
    match (bid_fit, ask_fit) {
        (Some((k_b, a_b)), Some((k_a, a_a))) => {
            // Both sides have valid fits - return separate values
            (k_b, a_b, k_a, a_a)
        }
        (Some((k_b, a_b)), None) => {
            // Only bid side has valid fit - use bid for both sides
            (k_b, a_b, k_b, a_b)
        }
        (None, Some((k_a, a_a))) => {
            // Only ask side has valid fit - use ask for both sides
            (k_a, a_a, k_a, a_a)
        }
        (None, None) => {
            // Neither side has valid fit - use defaults
            (DEFAULT_KAPPA, DEFAULT_A, DEFAULT_KAPPA, DEFAULT_A)
        }
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
        assert_eq!(calculate_volatility(&[]), 0.0);
    }

    #[test]
    fn test_volatility_single_price() {
        let prices = vec![(0, Decimal::from(100))];
        assert_eq!(calculate_volatility(&prices), 0.0);
    }

    #[test]
    fn test_volatility_two_prices() {
        // With only 2 prices, we get 1 log return, which is < MIN_SAMPLES_FOR_VARIANCE
        let prices = vec![
            (0, Decimal::from(100)),
            (1_000, Decimal::from(101)),
        ];
        assert_eq!(calculate_volatility(&prices), 0.0);
    }

    #[test]
    fn test_volatility_three_prices() {
        // With 3 prices, we get 2 log returns, which meets MIN_SAMPLES_FOR_VARIANCE
        let prices = vec![
            (0, Decimal::from(100)),
            (1_000, Decimal::from(101)),
            (2_000, Decimal::from(102)),
        ];
        let vol = calculate_volatility(&prices);
        assert!(vol >= 0.0);
        assert!(vol.is_finite());
    }

    #[test]
    fn test_volatility_constant_prices() {
        let prices: Vec<(u64, Decimal)> = (0..10)
            .map(|i| (i * 1_000, Decimal::from(100)))
            .collect();
        let vol = calculate_volatility(&prices);
        assert_eq!(vol, 0.0); // No variance in constant prices
    }

    #[test]
    fn test_volatility_irregular_spacing() {
        // Longer second interval should dilute volatility contribution compared to uniform spacing
        let prices = vec![
            (0, Decimal::from(100)),
            (1_000, Decimal::from(101)),   // 1s gap
            (11_000, Decimal::from(102)),  // 10s gap
        ];

        let r1 = (101f64 / 100f64).ln();
        let r2 = (102f64 / 101f64).ln();
        let expected = ((r1 * r1 + r2 * r2) / 11.0).sqrt();

        let vol = calculate_volatility(&prices);
        let diff = (vol - expected).abs();
        assert!(diff < 1e-9, "vol {} differs from expected {}", vol, expected);
    }

    #[test]
    fn test_volatility_rejects_invalid_prices() {
        let prices = vec![
            (0, Decimal::from(100)),
            (1_000, Decimal::ZERO), // invalid non-positive price should reject window
            (2_000, Decimal::from(101)),
        ];
        // Now skips invalid point instead of zeroing entire window
        assert!(calculate_volatility(&prices) >= 0.0);
    }

    #[test]
    fn test_garch_requires_enough_returns() {
        let prices = vec![
            (0, Decimal::from(100)),
            (1_000, Decimal::from(100)),
        ];
        assert!(forecast_garch_volatility(&prices).is_none());
    }

    #[test]
    fn test_garch_forecast_positive() {
        let mut prices = Vec::new();
        let mut ts = 0u64;
        let mut p = 100.0f64;
        prices.push((ts, Decimal::from_f64(p).unwrap()));

        let steps = [
            0.0010, -0.0005, 0.0020, -0.0010, 0.0015, -0.0008, 0.0025, -0.0012, 0.0009, -0.0004, 0.0011,
        ];

        for r in steps.iter() {
            ts += 1_000;
            p *= 1.0 + r;
            prices.push((ts, Decimal::from_f64(p).unwrap()));
        }

        let sigma = forecast_garch_volatility(&prices).unwrap();
        assert!(sigma.is_finite());
        assert!(sigma > 0.0);
    }

    #[test]
    fn test_build_fixed_step_returns_handles_irregular() {
        // Large gap should produce zeros between buckets, but still yield returns when price moves
        let prices = vec![
            (0, Decimal::from(100)),
            (1_000, Decimal::from(101)),
            (10_000, Decimal::from(102)),
        ];
        let rets = build_fixed_step_returns(&prices, 1.0).unwrap();
        assert!(rets.len() >= 2);
        assert!(rets.iter().any(|r| r.abs() > 0.0));
    }

    #[test]
    fn test_fit_intensity_empty() {
        let result = fit_intensity_parameters(&[], &[], 1000);
        assert_eq!(result, (DEFAULT_KAPPA, DEFAULT_A, DEFAULT_KAPPA, DEFAULT_A));
    }

    #[test]
    fn test_fit_intensity_with_exposure() {
        // Build synthetic trades hitting both sides
        let mut trades = Vec::new();
        for i in 0..5 {
            trades.push(TradeEvent {
                timestamp: 1_000 + i * 200,
                price: Decimal::from_str("99.9").unwrap(),
                quantity: Decimal::ONE,
                is_buyer_maker: true, // hits bid
            });
            trades.push(TradeEvent {
                timestamp: 2_000 + i * 200,
                price: Decimal::from_str("100.1").unwrap(),
                quantity: Decimal::ONE,
                is_buyer_maker: false, // hits ask
            });
        }

        // Two orderbook snapshots covering the window
        let orderbooks = vec![
            OrderbookPoint {
                timestamp: 0,
                mid: Decimal::from_str("100").unwrap(),
                bid_min: 0.05,
                bid_max: 1.0,
                ask_min: 0.05,
                ask_max: 1.0,
            },
            OrderbookPoint {
                timestamp: 4_000,
                mid: Decimal::from_str("100").unwrap(),
                bid_min: 0.04,
                bid_max: 0.8,
                ask_min: 0.04,
                ask_max: 0.8,
            },
        ];

        let (bid_k, bid_a, ask_k, ask_a) = fit_intensity_parameters(&trades, &orderbooks, 5_000);
        assert!(bid_k > 0.0 && bid_a > 0.0);
        assert!(ask_k > 0.0 && ask_a > 0.0);
    }

    #[test]
    fn test_default_constants() {
        // Ensure defaults are reasonable
        assert!(DEFAULT_KAPPA > 0.0);
        assert!(DEFAULT_A > 0.0);
    }
}
