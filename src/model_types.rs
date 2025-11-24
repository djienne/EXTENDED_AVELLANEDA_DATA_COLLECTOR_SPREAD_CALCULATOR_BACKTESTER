use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum GammaMode {
    #[serde(rename = "constant")]
    Constant,
    #[serde(rename = "inventory_scaled")]
    InventoryScaled,
    #[serde(rename = "max_shift")]
    MaxShift,
}

/// Configuration for the Avellaneda-Stoikov calculator
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ASConfig {
    pub risk_aversion_gamma: f64,
    pub effective_volume_threshold: Decimal,
    pub calibration_window_seconds: u64,
    pub recalibration_interval_seconds: u64,
    pub inventory_horizon_seconds: u64,
    pub gamma_min: f64,
    pub gamma_max: f64,
    pub max_inventory: f64,
    pub tick_size: f64,
    pub max_shift_ticks: f64,
    pub gamma_mode: GammaMode,
    pub min_spread_bps: f64,
    pub max_spread_bps: f64,
    pub maker_fee_bps: f64,
    pub taker_fee_bps: f64,
    pub min_volatility: f64,
    pub max_volatility: f64,
    pub fill_cooldown_seconds: u64,
    #[serde(default = "default_quote_validity")]
    pub quote_validity_seconds: u64,
    #[serde(default = "default_gap_threshold")]
    pub gap_threshold_seconds: u64,
    #[serde(default = "default_warmup_period")]
    pub warmup_period_seconds: u64,
    #[serde(default = "default_num_threads")]
    pub num_threads: usize,
}

fn default_gap_threshold() -> u64 {
    1800 // 30 minutes
}

fn default_warmup_period() -> u64 {
    900 // 15 minutes
}

pub fn default_quote_validity() -> u64 {
    60
}

fn default_num_threads() -> usize {
    4 // Default to 4 threads for parallel grid search
}

impl Default for ASConfig {
    fn default() -> Self {
        use rust_decimal::prelude::FromStr;
        Self {
            risk_aversion_gamma: 0.5,
            effective_volume_threshold: Decimal::from_str("1000.0").unwrap(),
            calibration_window_seconds: 3600, // 1 hour
            recalibration_interval_seconds: 60, // 1 minute
            inventory_horizon_seconds: 60, // 1 minute (matches tight crypto market spreads)
            gamma_min: 0.1,
            gamma_max: 5.0,
            max_inventory: 10.0,
            tick_size: 0.01,
            max_shift_ticks: 100.0,
            gamma_mode: GammaMode::InventoryScaled,
            min_spread_bps: 2.0,
            max_spread_bps: 100.0,
            maker_fee_bps: 1.0,
            taker_fee_bps: 5.0,
            min_volatility: 0.0,
            max_volatility: 0.02,
            fill_cooldown_seconds: 0,
            quote_validity_seconds: 60,
            gap_threshold_seconds: 1800,
            warmup_period_seconds: 900,
            num_threads: 4,
        }
    }
}

/// Represents an "Effective" Quote based on depth
#[derive(Debug, Clone, Copy)]
pub struct EffectiveQuote {
    pub bid: Decimal,
    pub ask: Decimal,
    pub mid: Decimal,
    pub weighted_bid: Decimal, // VWAP of bid depth
    pub weighted_ask: Decimal, // VWAP of ask depth
}

/// Calibrated model parameters
#[derive(Debug, Clone, Copy)]
pub struct ModelParams {
    /// Volatility (sigma)
    pub sigma: f64,
    /// Order arrival intensity decay (kappa)
    pub kappa: f64,
    /// Base order arrival intensity (A)
    pub A: f64,
    /// Risk aversion (gamma) from config
    pub gamma: f64,
}

/// Calculated optimal quotes
#[derive(Debug, Clone)]
pub struct OptimalQuote {
    pub timestamp: u64,
    pub reservation_price: Decimal,
    pub optimal_spread: Decimal,
    pub bid_price: Decimal,
    pub ask_price: Decimal,
    pub inventory_level: Decimal, // The inventory level this quote is for (e.g. 0)
    pub gamma: f64,
}

/// A trade event for calibration
#[derive(Debug, Clone)]
pub struct TradeEvent {
    pub timestamp: u64,
    pub price: Decimal,
    pub quantity: Decimal,
    pub is_buyer_maker: bool, // true if buyer was maker (sell side aggressor)
}
