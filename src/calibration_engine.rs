/// Calibration engine for Avellaneda-Stoikov model parameters
///
/// This module provides a stateful calibration engine that manages rolling windows
/// of prices and trades, and performs periodic recalibration of volatility (σ) and
/// intensity parameters (κ, A).

use crate::calibration::{calculate_volatility, fit_intensity_parameters, forecast_garch_volatility, CalibrationTrade, OrderbookPoint};
use crate::data_loader::OrderbookSnapshot;
use crate::model_types::{ASConfig, TradeEvent};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;

/// Minimum number of price observations required for calibration
const MIN_PRICES_FOR_CALIBRATION: usize = 10;

/// Result of a calibration run
#[derive(Debug, Clone)]
pub struct CalibrationResult {
    /// Timestamp when calibration was performed
    pub timestamp: u64,
    /// Volatility (sigma) in units of 1/√seconds. σ²T is dimensionless.
    pub volatility: f64,
    /// Intensity decay kappa for bid side (dimensionless, calibrated in return space)
    pub bid_kappa: f64,
    /// Intensity scale A for bid side (trades per second at δ=0)
    pub bid_a: f64,
    /// Intensity decay kappa for ask side (dimensionless, calibrated in return space)
    pub ask_kappa: f64,
    /// Intensity scale A for ask side (trades per second at δ=0)
    pub ask_a: f64,
}

/// Stateful calibration engine for AS model
///
/// Manages rolling windows of prices and trades, and performs periodic recalibration.
pub struct CalibrationEngine {
    /// Rolling window of prices for volatility calculation (timestamp, price)
    calibration_prices: Vec<(u64, Decimal)>,
    /// Growing history of prices for GARCH forecasting (no pruning to avoid forward-looking bias)
    full_price_history: Vec<(u64, Decimal)>,
    /// Rolling window of orderbook exposure points for intensity fitting
    orderbook_points: Vec<OrderbookPoint>,
    /// Rolling window of trades for intensity parameter fitting (lightweight struct)
    window_trades: Vec<CalibrationTrade>,
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
        let calibration_window_ms = config.calibration_window_seconds.saturating_mul(1000);
        let recalibration_interval_ms = config.recalibration_interval_seconds.saturating_mul(1000);

        // Estimate capacity: ~1 price per second for the window duration
        let estimated_prices = (config.calibration_window_seconds as usize).min(10_000);
        let estimated_trades = estimated_prices * 10; // Rough estimate

        Self {
            calibration_prices: Vec::with_capacity(estimated_prices),
            full_price_history: Vec::with_capacity(estimated_prices),
            orderbook_points: Vec::with_capacity(estimated_prices),
            window_trades: Vec::with_capacity(estimated_trades),
            bid_kappa: 100.0,  // Default starting value (dimensionless, in return space)
            bid_a: 10.0,       // Default starting value (trades per second at δ=0)
            ask_kappa: 100.0,  // Default starting value (dimensionless, in return space)
            ask_a: 10.0,       // Default starting value (trades per second at δ=0)
            last_calibration_ts: None,
            calibration_window_ms,
            recalibration_interval_ms,
        }
    }

    /// Add a price observation to the calibration window
    #[inline]
    pub fn add_price(&mut self, timestamp: u64, price: Decimal) {
        self.calibration_prices.push((timestamp, price));
        self.full_price_history.push((timestamp, price));
    }

    /// Add a full orderbook snapshot to the calibration window (captures exposure and mid).
    ///
    /// Deltas are stored in return space (relative to mid) so that kappa is dimensionless.
    #[inline]
    pub fn add_orderbook(&mut self, snapshot: &OrderbookSnapshot, mid_price: Decimal) {
        let timestamp = snapshot.timestamp;
        self.calibration_prices.push((timestamp, mid_price));
        self.full_price_history.push((timestamp, mid_price));

        let mid_f64 = mid_price.to_f64().unwrap_or(0.0);
        if mid_f64 <= 0.0 {
            return; // Can't compute return-space deltas without valid mid
        }

        let best_bid = snapshot.bids.first().map(|(p, _)| *p).unwrap_or(Decimal::ZERO);
        let best_ask = snapshot.asks.first().map(|(p, _)| *p).unwrap_or(Decimal::ZERO);
        let far_bid = snapshot.bids.last().map(|(p, _)| *p).unwrap_or(best_bid);
        let far_ask = snapshot.asks.last().map(|(p, _)| *p).unwrap_or(best_ask);

        // Compute deltas in return space: δ = |price - mid| / mid
        let bid_min = (mid_price - best_bid)
            .max(Decimal::ZERO)
            .to_f64()
            .unwrap_or(0.0) / mid_f64;
        let bid_max = (mid_price - far_bid)
            .max(Decimal::ZERO)
            .to_f64()
            .unwrap_or(0.0) / mid_f64;
        let ask_min = (best_ask - mid_price)
            .max(Decimal::ZERO)
            .to_f64()
            .unwrap_or(0.0) / mid_f64;
        let ask_max = (far_ask - mid_price)
            .max(Decimal::ZERO)
            .to_f64()
            .unwrap_or(0.0) / mid_f64;

        self.orderbook_points.push(OrderbookPoint {
            timestamp,
            mid: mid_price,
            bid_min,
            bid_max,
            ask_min,
            ask_max,
        });
    }

    /// Add a trade to the calibration window.
    ///
    /// Takes a reference and copies only the fields needed for calibration (timestamp, price,
    /// is_buyer_maker), avoiding the need to clone the entire TradeEvent including quantity.
    #[inline]
    pub fn add_trade(&mut self, trade: &TradeEvent) {
        self.window_trades.push(CalibrationTrade {
            timestamp: trade.timestamp,
            price: trade.price,
            is_buyer_maker: trade.is_buyer_maker,
        });
    }

    /// Prune old data from windows based on current timestamp
    pub fn prune_windows(&mut self, current_ts: u64) {
        // Remove prices older than calibration window
        self.calibration_prices.retain(|(ts, _)| {
            current_ts.saturating_sub(*ts) <= self.calibration_window_ms
        });

        // Remove orderbooks older than calibration window
        self.orderbook_points.retain(|p| {
            current_ts.saturating_sub(p.timestamp) <= self.calibration_window_ms
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

    /// Perform calibration and return results
    ///
    /// Returns None if insufficient data for calibration
    pub fn calibrate(
        &mut self,
        current_ts: u64,
        _tick_size: f64,
    ) -> Option<CalibrationResult> {
        if self.calibration_prices.is_empty() {
            return None;
        }

        // Calculate volatility (GARCH forecast if available, otherwise realized)
        let volatility = forecast_garch_volatility(&self.full_price_history)
            .unwrap_or_else(|| calculate_volatility(&self.calibration_prices));

        // Fit intensity parameters (returns separate bid/ask values)
        let (new_bid_kappa, new_bid_a, new_ask_kappa, new_ask_a) = fit_intensity_parameters(
            &self.window_trades,
            &self.orderbook_points,
            current_ts,
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
        self.full_price_history.clear();
        self.orderbook_points.clear();
        self.window_trades.clear();
        self.bid_kappa = 100.0;  // Default (dimensionless, in return space)
        self.bid_a = 10.0;
        self.ask_kappa = 100.0;  // Default (dimensionless, in return space)
        self.ask_a = 10.0;
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
