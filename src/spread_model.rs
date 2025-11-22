use crate::model_types::{ASConfig, GammaMode, OptimalQuote};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;

fn clamp_sigma(sigma_pct: f64, config: &ASConfig) -> f64 {
    let min_v = config.min_volatility;
    let max_v = config.max_volatility;
    if max_v > min_v {
        sigma_pct.max(min_v).min(max_v)
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
    kappa: f64,
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
    let gamma_constant = config.risk_aversion_gamma.max(1e-6);
    let gamma_from_shift = if sigma_sq > 1e-12 && t_horizon > 0.0 && config.max_inventory > 0.0 {
        (config.max_shift_ticks * config.tick_size) / (sigma_sq * t_horizon * config.max_inventory)
    } else {
        gamma_constant
    };

    let mut gamma = match config.gamma_mode {
        GammaMode::Constant => gamma_constant,
        GammaMode::InventoryScaled => (gamma_from_shift * inv_ratio).max(1e-6),
        GammaMode::MaxShift => gamma_from_shift.max(1e-6),
    };

    if config.gamma_max > config.gamma_min && config.gamma_max > 0.0 {
        let min_g = config.gamma_min.max(1e-6);
        let max_g = config.gamma_max;
        gamma = gamma.max(min_g).min(max_g);
    }

    let kappa_eff = if kappa > 0.0 { kappa } else { 10.0 };
    let term = 1.0 + (gamma / kappa_eff);
    if term <= 0.0 {
        gamma = gamma.max(1e-6);
    }

    let vol_risk_term = gamma * sigma_sq * t_horizon;
    let optimal_spread_f64 = vol_risk_term + (2.0 / gamma) * term.ln();
    let mut spread = Decimal::from_f64(optimal_spread_f64).unwrap_or(Decimal::ZERO);
    if spread <= Decimal::ZERO || mid_price <= Decimal::ZERO {
        spread = Decimal::ZERO;
    }

    if spread > Decimal::ZERO {
        let mut spread_bps = (spread / mid_price) * Decimal::from(10000);
        let spread_bps_f64 = spread_bps.to_f64().unwrap_or(0.0);

        let fee_floor_bps = config.maker_fee_bps.max(0.0);
        let min_bps = config.min_spread_bps.max(2.0 * fee_floor_bps);
        let max_bps = if config.max_spread_bps > 0.0 {
            config.max_spread_bps
        } else {
            spread_bps_f64.max(min_bps)
        };

        if max_bps > 0.0 {
            let clamped_bps = spread_bps_f64.max(min_bps).min(max_bps);
            spread_bps = Decimal::from_f64(clamped_bps).unwrap_or(spread_bps);
            spread = (spread_bps * mid_price) / Decimal::from(10000);
        }
    }

    let risk_adjustment_f64 = inventory.to_f64().unwrap_or(0.0) * gamma * sigma_sq * t_horizon;
    let risk_adjustment = Decimal::from_f64(risk_adjustment_f64).unwrap_or(Decimal::ZERO);
    let mut reservation_price = mid_price - risk_adjustment;

    if reservation_price <= Decimal::ZERO {
        reservation_price = mid_price;
    }

    let half_spread = spread / Decimal::from(2);
    let raw_bid = reservation_price - half_spread;
    let raw_ask = reservation_price + half_spread;

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

    if bid_price > ask_price {
        let mid = (bid_price + ask_price) / Decimal::from(2);
        bid_price = mid;
        ask_price = mid;
    }

    let final_spread = if ask_price > bid_price {
        ask_price - bid_price
    } else {
        spread.max(Decimal::ZERO)
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
        let quote = compute_optimal_quote(0, mid, q, 0.01, 10.0, &config);
        assert!(quote.bid_price < quote.ask_price);
        assert!(quote.optimal_spread > Decimal::ZERO);
    }
}
