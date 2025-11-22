use crate::model_types::TradeEvent;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::collections::HashMap;

/// Calculate volatility (sigma) from price updates
/// Returns annualized volatility (or scaled to seconds)
pub fn calculate_volatility(
    prices: &[Decimal],
    window_duration_seconds: f64,
) -> f64 {
    if prices.len() < 2 || window_duration_seconds <= 0.0 {
        return 0.0;
    }

    let mut log_returns = Vec::new();
    for i in 1..prices.len() {
        let p1 = prices[i-1].to_f64().unwrap_or(0.0);
        let p2 = prices[i].to_f64().unwrap_or(0.0);
        
        if p1 > 0.0 && p2 > 0.0 {
            log_returns.push((p2 / p1).ln());
        }
    }

    if log_returns.is_empty() {
        return 0.0;
    }

    let mean: f64 = log_returns.iter().sum::<f64>() / log_returns.len() as f64;
    let variance: f64 = log_returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (log_returns.len() - 1) as f64;
    let std_dev = variance.sqrt();

    // Scale to seconds.
    // We have `log_returns.len()` samples over `window_duration_seconds`.
    // Average time between samples dt = window / count
    // sigma_sec = sigma_sample / sqrt(dt) = sigma_sample * sqrt(count / window)
    let count = log_returns.len() as f64;
    let scaled_sigma = std_dev * (count / window_duration_seconds).sqrt();
    
    scaled_sigma
}

/// Calibrates intensity parameters A and kappa using regression on trade arrival rates.
/// 
/// # Arguments
/// * `trades` - List of trades in the window
/// * `mid_prices` - History of mid-prices (timestamp, price) sorted by timestamp
/// * `window_duration_seconds` - The duration of the window in seconds (for rate calculation)
/// * `tick_size` - The tick size for binning (e.g. 0.01)
fn regress_side(
    deltas: &[f64],
    window_duration_seconds: f64,
    tick_size: f64,
) -> Option<(f64, f64)> {
    if deltas.len() < 10 || window_duration_seconds <= 0.0 || tick_size <= 0.0 {
        return None;
    }

    let mut bins: HashMap<i64, usize> = HashMap::new();

    for delta in deltas {
        let ticks = (delta / tick_size).round() as i64;
        if ticks >= 0 {
            *bins.entry(ticks).or_insert(0) += 1;
        }
    }

    let mut x_data = Vec::new();
    let mut y_data = Vec::new();

    for (tick_idx, count) in bins {
        let delta_price = tick_idx as f64 * tick_size;
        let rate = count as f64 / window_duration_seconds;

        if rate > 0.0 {
            x_data.push(delta_price);
            y_data.push(rate.ln());
        }
    }

    if x_data.len() < 2 {
        return None;
    }

    let n = x_data.len() as f64;
    let sum_x: f64 = x_data.iter().sum();
    let sum_y: f64 = y_data.iter().sum();
    let sum_xy: f64 = x_data.iter().zip(y_data.iter()).map(|(x, y)| x * y).sum();
    let sum_xx: f64 = x_data.iter().map(|x| x * x).sum();

    let denominator = n * sum_xx - sum_x * sum_x;
    if denominator.abs() < 1e-9 {
        return None;
    }

    let beta = (n * sum_xy - sum_x * sum_y) / denominator;
    let alpha = (sum_y - beta * sum_x) / n;

    let kappa = -beta;
    let a = alpha.exp();

    if kappa <= 0.0 || a <= 0.0 {
        return None;
    }

    Some((kappa, a))
}

pub fn fit_intensity_parameters(
    trades: &[TradeEvent],
    mid_prices: &[(u64, Decimal)],
    window_duration_seconds: f64,
    tick_size: f64,
) -> (f64, f64) {
    if trades.is_empty() || mid_prices.is_empty() || window_duration_seconds <= 0.0 {
        return (10.0, 10.0);
    }

    let mut bid_deltas = Vec::new();
    let mut ask_deltas = Vec::new();

    let mut mid_idx = 0;

    for trade in trades {
        while mid_idx + 1 < mid_prices.len() && mid_prices[mid_idx + 1].0 <= trade.timestamp {
            mid_idx += 1;
        }

        let mid = mid_prices[mid_idx].1;
        let price = trade.price;

        if trade.is_buyer_maker {
            let delta = mid - price;
            if delta > Decimal::ZERO {
                bid_deltas.push(delta.to_f64().unwrap_or(0.0));
            }
        } else {
            let delta = price - mid;
            if delta > Decimal::ZERO {
                ask_deltas.push(delta.to_f64().unwrap_or(0.0));
            }
        }
    }

    let bid_fit = regress_side(&bid_deltas, window_duration_seconds, tick_size);
    let ask_fit = regress_side(&ask_deltas, window_duration_seconds, tick_size);

    match (bid_fit, ask_fit) {
        (Some((k_b, a_b)), Some((k_a, a_a))) => {
            let kappa = (k_b + k_a) / 2.0;
            let a = (a_b + a_a) / 2.0;
            (kappa, a)
        }
        (Some((k_b, a_b)), None) => (k_b, a_b),
        (None, Some((k_a, a_a))) => (k_a, a_a),
        _ => (10.0, 10.0),
    }
}
