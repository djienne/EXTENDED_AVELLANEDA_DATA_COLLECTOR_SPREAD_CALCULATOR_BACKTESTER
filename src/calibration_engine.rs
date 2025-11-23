/// Calibration engine for Avellaneda-Stoikov model parameters
///
/// This module provides a stateful calibration engine that manages rolling windows
/// of prices and trades, and performs periodic recalibration of volatility (σ) and
/// intensity parameters (κ, A).
///
/// Eliminates code duplication across backtest, calculate_spread, and other binaries.

use crate::calibration::{calculate_volatility, fit_intensity_parameters};
use crate::model_types::{ASConfig, TradeEvent};
use rust_decimal::Decimal;

/// Result of a calibration run
#[derive(Debug, Clone)]
pub struct CalibrationResult {
    /// Timestamp when calibration was performed
    pub timestamp: u64,
    /// Volatility (sigma) as percentage (e.g., 0.02 = 2%)
    pub volatility: f64,
    /// Intensity parameter kappa
    pub kappa: f64,
    /// Intensity parameter A
    pub a: f64,
}

/// Stateful calibration engine for AS model
///
/// Manages rolling windows of prices and trades, and performs periodic recalibration.
pub struct CalibrationEngine {
    /// Rolling window of prices for volatility calculation (timestamp, price)
    calibration_prices: Vec<(u64, Decimal)>,
    /// Rolling window of just prices (for volatility calculation)
    window_prices: Vec<Decimal>,
    /// Rolling window of trades for intensity parameter fitting
    window_trades: Vec<TradeEvent>,
    /// Current kappa parameter
    kappa: f64,
    /// Current A parameter
    a: f64,
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
        let calibration_window_ms = (config.calibration_window_seconds * 1000) as u64;
        let recalibration_interval_ms = (config.recalibration_interval_seconds * 1000) as u64;

        Self {
            calibration_prices: Vec::new(),
            window_prices: Vec::new(),
            window_trades: Vec::new(),
            kappa: 10.0,  // Default starting value
            a: 100.0,     // Default starting value
            last_calibration_ts: None,
            calibration_window_ms,
            recalibration_interval_ms,
        }
    }

    /// Add a price observation to the calibration windows
    pub fn add_price(&mut self, timestamp: u64, price: Decimal) {
        self.calibration_prices.push((timestamp, price));
        self.window_prices.push(price);
    }

    /// Add a trade to the calibration window
    pub fn add_trade(&mut self, trade: TradeEvent) {
        self.window_trades.push(trade);
    }

    /// Prune old data from windows based on current timestamp
    pub fn prune_windows(&mut self, current_ts: u64) {
        // Remove prices older than calibration window
        self.calibration_prices.retain(|(ts, _)| {
            current_ts.saturating_sub(*ts) <= self.calibration_window_ms
        });

        // Keep window_prices in sync (same length as calibration_prices)
        let expected_len = self.calibration_prices.len();
        if self.window_prices.len() > expected_len {
            let to_remove = self.window_prices.len() - expected_len;
            self.window_prices.drain(0..to_remove);
        }

        // Remove trades older than calibration window
        self.window_trades.retain(|t| {
            current_ts.saturating_sub(t.timestamp) <= self.calibration_window_ms
        });
    }

    /// Check if it's time to recalibrate
    pub fn should_recalibrate(&self, current_ts: u64) -> bool {
        if let Some(last_cal_ts) = self.last_calibration_ts {
            current_ts >= last_cal_ts + self.recalibration_interval_ms
        } else {
            // First calibration: wait until we have enough data
            self.calibration_prices.len() >= 10
        }
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
        let actual_duration_sec = ((current_ts - start_ts) as f64 / 1000.0).max(1.0);

        // Calculate volatility
        let volatility = calculate_volatility(&self.window_prices, actual_duration_sec);

        // Fit intensity parameters
        let (new_kappa, new_a) = fit_intensity_parameters(
            &self.window_trades,
            &self.calibration_prices,
            actual_duration_sec,
            tick_size,
        );

        // Update stored parameters if valid
        if new_kappa > 0.0 && new_a > 0.0 {
            self.kappa = new_kappa;
            self.a = new_a;
        }

        // Update last calibration timestamp
        self.last_calibration_ts = Some(current_ts);

        Some(CalibrationResult {
            timestamp: current_ts,
            volatility,
            kappa: self.kappa,
            a: self.a,
        })
    }

    /// Get current kappa parameter
    pub fn kappa(&self) -> f64 {
        self.kappa
    }

    /// Get current A parameter
    pub fn a(&self) -> f64 {
        self.a
    }

    /// Get last calibration timestamp
    pub fn last_calibration_ts(&self) -> Option<u64> {
        self.last_calibration_ts
    }

    /// Get number of prices in calibration window
    pub fn price_count(&self) -> usize {
        self.calibration_prices.len()
    }

    /// Get number of trades in calibration window
    pub fn trade_count(&self) -> usize {
        self.window_trades.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::prelude::*;

    #[test]
    fn test_calibration_engine_basic() {
        let config = ASConfig::default();
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
        let config = ASConfig::default();
        let mut engine = CalibrationEngine::new(&config);

        // Add prices spanning 2 hours
        for i in 0..120 {
            engine.add_price(i * 60_000, Decimal::from(2800 + i)); // Every minute
        }

        assert_eq!(engine.price_count(), 120);

        // Prune to last hour (default window is 3600s)
        let current_ts = 120 * 60_000;
        engine.prune_windows(current_ts);

        // Should keep only last hour
        assert!(engine.price_count() <= 61); // 60 minutes + possibly 1 more
    }
}
