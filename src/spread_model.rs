use crate::model_types::{ASConfig, GammaMode, OptimalQuote};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;

/// Maximum allowed gamma to prevent numerical instability
const MAX_GAMMA_LIMIT: f64 = 1e6;

/// Minimum gamma to prevent division issues
const MIN_GAMMA: f64 = 1e-6;

fn clamp_sigma(sigma_pct: f64, config: &ASConfig) -> f64 {
    let min_v = config.min_volatility;
    let max_v = config.max_volatility;
    if max_v > min_v {
        sigma_pct.clamp(min_v, max_v)
    } else {
        sigma_pct
    }
}

fn round_down_to_tick(price: Decimal, tick: Decimal) -> Decimal {
    if tick <= Decimal::ZERO {
        return price;
    }
    let ratio = price / tick;
    let ticks = ratio.to_i64().unwrap_or(0);
    Decimal::from(ticks) * tick
}

fn round_up_to_tick(price: Decimal, tick: Decimal) -> Decimal {
    if tick <= Decimal::ZERO {
        return price;
    }
    let ratio = price / tick;
    let ticks_down = ratio.to_i64().unwrap_or(0);
    let down_price = Decimal::from(ticks_down) * tick;
    if down_price < price {
        Decimal::from(ticks_down + 1) * tick
    } else {
        down_price
    }
}

pub fn compute_optimal_quote(
    timestamp: u64,
    mid_price: Decimal,
    inventory: Decimal,
    sigma_pct_raw: f64,
    bid_kappa: f64,
    ask_kappa: f64,
    config: &ASConfig,
) -> OptimalQuote {
    let sigma_pct = clamp_sigma(sigma_pct_raw, config);
    let mid_f64 = mid_price.to_f64().unwrap_or(0.0);
    let sigma_abs = mid_f64 * sigma_pct;

    let t_horizon = config.inventory_horizon_seconds as f64;
    let sigma_sq = sigma_abs.powi(2);

    let inv_abs = inventory.abs().to_f64().unwrap_or(0.0);
    let inv_ratio = if config.max_inventory > 0.0 {
        (inv_abs / config.max_inventory).min(1.0)
    } else {
        0.0
    };
    
    let gamma_constant = config.risk_aversion_gamma.max(MIN_GAMMA);
    
    // Calculate gamma_from_shift with additional safeguards
    let gamma_from_shift = {
        let denominator = sigma_sq * t_horizon * config.max_inventory;
        if sigma_sq > 1e-12 && t_horizon > 0.0 && config.max_inventory > 0.0 && denominator > 1e-12 {
            let raw_gamma = (config.max_shift_ticks * config.tick_size) / denominator;
            // Clamp to prevent extremely large values
            raw_gamma.min(MAX_GAMMA_LIMIT)
        } else {
            gamma_constant
        }
    };

    let mut gamma = match config.gamma_mode {
        GammaMode::Constant => gamma_constant,
        GammaMode::InventoryScaled => (gamma_from_shift * inv_ratio).max(MIN_GAMMA),
        GammaMode::MaxShift => gamma_from_shift.max(MIN_GAMMA),
    };

    // Apply gamma bounds from config
    if config.gamma_max > config.gamma_min && config.gamma_max > 0.0 {
        let min_g = config.gamma_min.max(MIN_GAMMA);
        let max_g = config.gamma_max.min(MAX_GAMMA_LIMIT);
        gamma = gamma.clamp(min_g, max_g);
    }

    // Calculate separate bid and ask spreads using side-specific kappa values
    let vol_risk_term = gamma * sigma_sq * t_horizon;

    // Bid spread calculation
    let bid_kappa_eff = if bid_kappa > 0.0 { bid_kappa } else { 10.0 };
    let bid_term = (1.0 + (gamma / bid_kappa_eff)).max(MIN_GAMMA);
    let bid_spread_f64 = vol_risk_term + (2.0 / gamma) * bid_term.ln();

    // Ask spread calculation
    let ask_kappa_eff = if ask_kappa > 0.0 { ask_kappa } else { 10.0 };
    let ask_term = (1.0 + (gamma / ask_kappa_eff)).max(MIN_GAMMA);
    let ask_spread_f64 = vol_risk_term + (2.0 / gamma) * ask_term.ln();

    // Convert to Decimal and validate
    let mut bid_spread = if bid_spread_f64.is_finite() && bid_spread_f64 > 0.0 {
        Decimal::from_f64(bid_spread_f64).unwrap_or(Decimal::ZERO)
    } else {
        Decimal::ZERO
    };

    let mut ask_spread = if ask_spread_f64.is_finite() && ask_spread_f64 > 0.0 {
        Decimal::from_f64(ask_spread_f64).unwrap_or(Decimal::ZERO)
    } else {
        Decimal::ZERO
    };

    // Apply spread bounds in basis points for bid spread
    if bid_spread > Decimal::ZERO && mid_price > Decimal::ZERO {
        let mut spread_bps = (bid_spread / mid_price) * Decimal::from(10000);
        let spread_bps_f64 = spread_bps.to_f64().unwrap_or(0.0);

        let fee_floor_bps = config.maker_fee_bps.max(0.0);
        let min_bps = config.min_spread_bps.max(2.0 * fee_floor_bps);
        let max_bps = if config.max_spread_bps > 0.0 {
            config.max_spread_bps
        } else {
            spread_bps_f64.max(min_bps)
        };

        if max_bps > 0.0 {
            let clamped_bps = spread_bps_f64.clamp(min_bps, max_bps);
            spread_bps = Decimal::from_f64(clamped_bps).unwrap_or(spread_bps);
            bid_spread = (spread_bps * mid_price) / Decimal::from(10000);
        }
    }

    // Apply spread bounds in basis points for ask spread
    if ask_spread > Decimal::ZERO && mid_price > Decimal::ZERO {
        let mut spread_bps = (ask_spread / mid_price) * Decimal::from(10000);
        let spread_bps_f64 = spread_bps.to_f64().unwrap_or(0.0);

        let fee_floor_bps = config.maker_fee_bps.max(0.0);
        let min_bps = config.min_spread_bps.max(2.0 * fee_floor_bps);
        let max_bps = if config.max_spread_bps > 0.0 {
            config.max_spread_bps
        } else {
            spread_bps_f64.max(min_bps)
        };

        if max_bps > 0.0 {
            let clamped_bps = spread_bps_f64.clamp(min_bps, max_bps);
            spread_bps = Decimal::from_f64(clamped_bps).unwrap_or(spread_bps);
            ask_spread = (spread_bps * mid_price) / Decimal::from(10000);
        }
    }

    // Calculate reservation price adjustment
    let risk_adjustment_f64 = inventory.to_f64().unwrap_or(0.0) * gamma * sigma_sq * t_horizon;
    let risk_adjustment = if risk_adjustment_f64.is_finite() {
        Decimal::from_f64(risk_adjustment_f64).unwrap_or(Decimal::ZERO)
    } else {
        Decimal::ZERO
    };
    
    let mut reservation_price = mid_price - risk_adjustment;

    if reservation_price <= Decimal::ZERO {
        reservation_price = mid_price;
    }

    // Calculate bid and ask prices using side-specific spreads
    let half_bid_spread = bid_spread / Decimal::TWO;
    let half_ask_spread = ask_spread / Decimal::TWO;
    let raw_bid = reservation_price - half_bid_spread;
    let raw_ask = reservation_price + half_ask_spread;

    let tick = Decimal::from_f64(config.tick_size).unwrap_or(Decimal::ZERO);
    let mut bid_price = if raw_bid > Decimal::ZERO {
        round_down_to_tick(raw_bid, tick)
    } else {
        raw_bid
    };
    let mut ask_price = if raw_ask > Decimal::ZERO {
        round_up_to_tick(raw_ask, tick)
    } else {
        raw_ask
    };

    // Ensure bid <= ask
    if bid_price > ask_price {
        let mid = (bid_price + ask_price) / Decimal::TWO;
        bid_price = mid;
        ask_price = mid;
    }

    // Final spread is the actual distance between bid and ask
    let final_spread = if ask_price > bid_price {
        ask_price - bid_price
    } else {
        // If prices crossed, use average of theoretical spreads
        ((bid_spread + ask_spread) / Decimal::TWO).max(Decimal::ZERO)
    };

    OptimalQuote {
        timestamp,
        reservation_price,
        optimal_spread: final_spread,
        bid_price,
        ask_price,
        inventory_level: inventory,
        gamma,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::prelude::FromStr;

    #[test]
    fn compute_quote_basic() {
        let config = ASConfig::default();
        let mid = Decimal::from_str("100.0").unwrap();
        let q = Decimal::ZERO;
        let quote = compute_optimal_quote(0, mid, q, 0.01, 10.0, 10.0, &config);
        assert!(quote.bid_price < quote.ask_price);
        assert!(quote.optimal_spread > Decimal::ZERO);
    }

    #[test]
    fn compute_quote_with_inventory() {
        let config = ASConfig::default();
        let mid = Decimal::from_str("100.0").unwrap();
        let inv = Decimal::from_str("5.0").unwrap();
        let quote = compute_optimal_quote(0, mid, inv, 0.01, 10.0, 10.0, &config);
        // With positive inventory, reservation price should be below mid
        assert!(quote.reservation_price <= mid);
    }

    #[test]
    fn compute_quote_zero_volatility() {
        let config = ASConfig::default();
        let mid = Decimal::from_str("100.0").unwrap();
        let quote = compute_optimal_quote(0, mid, Decimal::ZERO, 0.0, 10.0, 10.0, &config);
        // Should still produce valid quotes even with zero volatility
        assert!(quote.bid_price <= quote.ask_price);
    }

    #[test]
    fn compute_quote_extreme_gamma() {
        let mut config = ASConfig::default();
        config.gamma_max = 1e10; // Very high gamma max
        let mid = Decimal::from_str("100.0").unwrap();
        let quote = compute_optimal_quote(0, mid, Decimal::ZERO, 0.01, 10.0, 10.0, &config);
        // Should still produce valid quotes due to internal clamping
        assert!(quote.gamma <= MAX_GAMMA_LIMIT);
    }
}
