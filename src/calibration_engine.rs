/// Calibration engine for Avellaneda-Stoikov model parameters
///
/// This module provides a stateful calibration engine that manages rolling windows
/// of prices and trades, and performs periodic recalibration of volatility (σ) and
/// intensity parameters (κ, A).

use crate::calibration::{calculate_volatility, fit_intensity_parameters};
use crate::model_types::{ASConfig, TradeEvent};
use rust_decimal::Decimal;

/// Minimum number of price observations required for calibration
const MIN_PRICES_FOR_CALIBRATION: usize = 10;

/// Result of a calibration run
#[derive(Debug, Clone)]
pub struct CalibrationResult {
    /// Timestamp when calibration was performed
    pub timestamp: u64,
    /// Volatility (sigma) as percentage (e.g., 0.02 = 2%)
    pub volatility: f64,
    /// Intensity parameter kappa for bid side
    pub bid_kappa: f64,
    /// Intensity parameter A for bid side
    pub bid_a: f64,
    /// Intensity parameter kappa for ask side
    pub ask_kappa: f64,
    /// Intensity parameter A for ask side
    pub ask_a: f64,
}

/// Stateful calibration engine for AS model
///
/// Manages rolling windows of prices and trades, and performs periodic recalibration.
pub struct CalibrationEngine {
    /// Rolling window of prices for volatility calculation (timestamp, price)
    calibration_prices: Vec<(u64, Decimal)>,
    /// Rolling window of trades for intensity parameter fitting
    window_trades: Vec<TradeEvent>,
    /// Current kappa parameter for bid side
    bid_kappa: f64,
    /// Current A parameter for bid side
    bid_a: f64,
    /// Current kappa parameter for ask side
    ask_kappa: f64,
    /// Current A parameter for ask side
    ask_a: f64,
    /// Timestamp of last calibration
    last_calibration_ts: Option<u64>,
    /// Calibration window size in milliseconds
    calibration_window_ms: u64,
    /// Recalibration interval in milliseconds
    recalibration_interval_ms: u64,
}

impl CalibrationEngine {
    /// Create a new calibration engine
    pub fn new(config: &ASConfig) -> Self {
        // Cast before multiply to prevent overflow
        let calibration_window_ms = (config.calibration_window_seconds as u64).saturating_mul(1000);
        let recalibration_interval_ms = (config.recalibration_interval_seconds as u64).saturating_mul(1000);

        // Estimate capacity: ~1 price per second for the window duration
        let estimated_prices = (config.calibration_window_seconds as usize).min(10_000);
        let estimated_trades = estimated_prices * 10; // Rough estimate

        Self {
            calibration_prices: Vec::with_capacity(estimated_prices),
            window_trades: Vec::with_capacity(estimated_trades),
            bid_kappa: 10.0,  // Default starting value
            bid_a: 100.0,     // Default starting value
            ask_kappa: 10.0,  // Default starting value
            ask_a: 100.0,     // Default starting value
            last_calibration_ts: None,
            calibration_window_ms,
            recalibration_interval_ms,
        }
    }

    /// Add a price observation to the calibration window
    #[inline]
    pub fn add_price(&mut self, timestamp: u64, price: Decimal) {
        self.calibration_prices.push((timestamp, price));
    }

    /// Add a trade to the calibration window
    /// 
    /// Note: Takes ownership of the trade. Clone before calling if you need to retain it.
    #[inline]
    pub fn add_trade(&mut self, trade: TradeEvent) {
        self.window_trades.push(trade);
    }

    /// Prune old data from windows based on current timestamp
    pub fn prune_windows(&mut self, current_ts: u64) {
        // Remove prices older than calibration window
        self.calibration_prices.retain(|(ts, _)| {
            current_ts.saturating_sub(*ts) <= self.calibration_window_ms
        });

        // Remove trades older than calibration window
        self.window_trades.retain(|t| {
            current_ts.saturating_sub(t.timestamp) <= self.calibration_window_ms
        });
    }

    /// Check if it's time to recalibrate
    #[inline]
    pub fn should_recalibrate(&self, current_ts: u64) -> bool {
        if let Some(last_cal_ts) = self.last_calibration_ts {
            current_ts >= last_cal_ts + self.recalibration_interval_ms
        } else {
            // First calibration: wait until we have enough data
            self.calibration_prices.len() >= MIN_PRICES_FOR_CALIBRATION
        }
    }

    /// Extract prices from calibration_prices for volatility calculation
    /// Avoids storing duplicate data
    #[inline]
    fn get_prices(&self) -> Vec<Decimal> {
        self.calibration_prices.iter().map(|(_, p)| *p).collect()
    }

    /// Perform calibration and return results
    ///
    /// Returns None if insufficient data for calibration
    pub fn calibrate(
        &mut self,
        current_ts: u64,
        tick_size: f64,
    ) -> Option<CalibrationResult> {
        if self.calibration_prices.is_empty() {
            return None;
        }

        // Calculate actual window duration
        let start_ts = self.calibration_prices.first().map(|(ts, _)| *ts).unwrap_or(current_ts);
        let actual_duration_sec = ((current_ts.saturating_sub(start_ts)) as f64 / 1000.0).max(1.0);

        // Extract prices for volatility calculation
        let prices = self.get_prices();

        // Calculate volatility
        let volatility = calculate_volatility(&prices, actual_duration_sec);

        // Fit intensity parameters (returns separate bid/ask values)
        let (new_bid_kappa, new_bid_a, new_ask_kappa, new_ask_a) = fit_intensity_parameters(
            &self.window_trades,
            &self.calibration_prices,
            actual_duration_sec,
            tick_size,
        );

        // Update stored parameters if valid
        if new_bid_kappa > 0.0 && new_bid_a > 0.0 {
            self.bid_kappa = new_bid_kappa;
            self.bid_a = new_bid_a;
        }
        if new_ask_kappa > 0.0 && new_ask_a > 0.0 {
            self.ask_kappa = new_ask_kappa;
            self.ask_a = new_ask_a;
        }

        // Update last calibration timestamp
        self.last_calibration_ts = Some(current_ts);

        Some(CalibrationResult {
            timestamp: current_ts,
            volatility,
            bid_kappa: self.bid_kappa,
            bid_a: self.bid_a,
            ask_kappa: self.ask_kappa,
            ask_a: self.ask_a,
        })
    }

    /// Get current bid kappa parameter
    #[inline]
    pub fn bid_kappa(&self) -> f64 {
        self.bid_kappa
    }

    /// Get current bid A parameter
    #[inline]
    pub fn bid_a(&self) -> f64 {
        self.bid_a
    }

    /// Get current ask kappa parameter
    #[inline]
    pub fn ask_kappa(&self) -> f64 {
        self.ask_kappa
    }

    /// Get current ask A parameter
    #[inline]
    pub fn ask_a(&self) -> f64 {
        self.ask_a
    }

    /// Get last calibration timestamp
    #[inline]
    pub fn last_calibration_ts(&self) -> Option<u64> {
        self.last_calibration_ts
    }

    /// Get number of prices in calibration window
    #[inline]
    pub fn price_count(&self) -> usize {
        self.calibration_prices.len()
    }

    /// Get number of trades in calibration window
    #[inline]
    pub fn trade_count(&self) -> usize {
        self.window_trades.len()
    }

    /// Reset the engine state (useful for reusing across multiple backtests)
    pub fn reset(&mut self) {
        self.calibration_prices.clear();
        self.window_trades.clear();
        self.bid_kappa = 10.0;
        self.bid_a = 100.0;
        self.ask_kappa = 10.0;
        self.ask_a = 100.0;
        self.last_calibration_ts = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::prelude::*;

    fn make_test_config() -> ASConfig {
        ASConfig {
            calibration_window_seconds: 3600,
            recalibration_interval_seconds: 60,
            ..ASConfig::default()
        }
    }

    #[test]
    fn test_calibration_engine_basic() {
        let config = make_test_config();
        let mut engine = CalibrationEngine::new(&config);

        assert_eq!(engine.price_count(), 0);
        assert_eq!(engine.trade_count(), 0);
        assert!(!engine.should_recalibrate(1000));

        // Add some prices
        for i in 0..20 {
            engine.add_price(1000 + i * 1000, Decimal::from(2800 + i));
        }

        assert_eq!(engine.price_count(), 20);
        assert!(engine.should_recalibrate(20000));
    }

    #[test]
    fn test_window_pruning() {
        let config = make_test_config();
        let mut engine = CalibrationEngine::new(&config);

        // Add prices spanning 2 hours (every minute)
        for i in 0..120 {
            engine.add_price(i * 60_000, Decimal::from(2800 + i));
        }

        assert_eq!(engine.price_count(), 120);

        // Prune at t=2 hours; should keep prices from t >= 1 hour
        let current_ts = 120 * 60_000; // 7,200,000 ms
        engine.prune_windows(current_ts);

        // Prices kept: timestamps >= 3,600,000 (minute 60 to 119 inclusive = 60 prices)
        // Plus minute 120 if it existed, but we only added up to 119
        assert!(engine.price_count() <= 61);
        assert!(engine.price_count() >= 59); // Allow some tolerance
    }

    #[test]
    fn test_reset() {
        let config = make_test_config();
        let mut engine = CalibrationEngine::new(&config);

        for i in 0..20 {
            engine.add_price(1000 + i * 1000, Decimal::from(2800 + i));
        }

        assert_eq!(engine.price_count(), 20);

        engine.reset();

        assert_eq!(engine.price_count(), 0);
        assert_eq!(engine.trade_count(), 0);
        assert_eq!(engine.last_calibration_ts(), None);
    }
}