/// Backtest engine module - reusable backtesting logic for AS market-making strategy
///
/// This module provides the core backtesting functionality that can be used by both
/// the standalone backtest binary and grid search binary without code duplication.

use crate::calibration_engine::CalibrationEngine;
use crate::data_loader::DataEvent;
use crate::model_types::ASConfig;
use crate::spread_model::compute_optimal_quote;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use chrono::{DateTime, Utc};

/// Input parameters for backtest
pub struct BacktestParams<I> {
    /// Stream of data events (trades and orderbooks)
    pub data_stream: I,
    /// AS model configuration
    pub config: ASConfig,
    /// Starting capital in dollars
    pub initial_capital: Decimal,
    /// Order size in dollars
    pub order_notional: Decimal,
    /// Optional path to write detailed CSV results
    pub output_csv_path: Option<String>,
    /// Enable verbose console output during backtest
    pub verbose: bool,
}

/// Results from backtest run
#[derive(Debug, Clone)]
pub struct BacktestResults {
    /// Starting capital
    pub initial_capital: Decimal,
    /// Final mark-to-market PnL (after closing positions)
    pub final_pnl: Decimal,
    /// Total return percentage
    pub total_return_pct: Decimal,
    /// Number of bid fills
    pub bid_fills: u64,
    /// Number of ask fills
    pub ask_fills: u64,
    /// Total trading volume in units
    pub total_volume: Decimal,
    /// Total notional trading volume in quote currency (e.g. USD)
    pub total_notional_volume: Decimal,
    /// Final inventory (should be zero after position close)
    pub final_inventory: Decimal,
    /// Final cash balance
    pub final_cash: Decimal,
    /// Configuration used
    pub config: ASConfig,
}

impl BacktestResults {
    /// Get total number of fills (bids + asks)
    pub fn total_fills(&self) -> u64 {
        self.bid_fills + self.ask_fills
    }
}

/// Internal state tracking during backtest
#[derive(Debug, Clone)]
struct BacktestState {
    inventory: Decimal,
    cash: Decimal,
    bid_fills: u64,
    ask_fills: u64,
    total_volume: Decimal,
    total_notional_volume: Decimal,
    last_bid_fill_ts: u64,
    last_ask_fill_ts: u64,
}

impl BacktestState {
    fn new(initial_capital: Decimal) -> Self {
        Self {
            inventory: Decimal::ZERO,
            cash: initial_capital,
            bid_fills: 0,
            ask_fills: 0,
            total_volume: Decimal::ZERO,
            total_notional_volume: Decimal::ZERO,
            last_bid_fill_ts: 0,
            last_ask_fill_ts: 0,
        }
    }

    fn mark_to_market_pnl(&self, mid_price: Decimal) -> Decimal {
        self.cash + (self.inventory * mid_price)
    }
}

/// Format timestamp (epoch milliseconds) as human-readable string
fn format_timestamp(timestamp_ms: u64) -> String {
    let seconds = (timestamp_ms / 1000) as i64;
    let nanos = ((timestamp_ms % 1000) * 1_000_000) as u32;

    match DateTime::<Utc>::from_timestamp(seconds, nanos) {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string(),
        None => "N/A".to_string(),
    }
}

/// Run backtest simulation with given parameters
///
/// Returns BacktestResults with performance metrics, or error if simulation fails
pub fn run_backtest<I>(params: BacktestParams<I>) -> Result<BacktestResults, Box<dyn Error>>
where
    I: Iterator<Item = Result<DataEvent, Box<dyn Error>>>,
{
    let BacktestParams {
        data_stream,
        config,
        initial_capital,
        order_notional,
        output_csv_path,
        verbose,
    } = params;

    if verbose {
        println!("Running backtest with horizon={}s, gamma={}",
            config.inventory_horizon_seconds, config.risk_aversion_gamma);
    }

    // Initialize state
    let mut state = BacktestState::new(initial_capital);

    // Initialize calibration engine
    let mut calibration_engine = CalibrationEngine::new(&config);

    // Output file (optional)
    let mut output_file = if let Some(ref path) = output_csv_path {
        let file = File::create(path)?;
        let mut f = Some(file);
        if let Some(ref mut file) = f {
            writeln!(
                file,
                "timestamp,datetime,mid_price,inventory,cash,pnl,spread_bps,bid_price,ask_price,bid_fills,ask_fills,gamma,kappa"
            )?;
        }
        f
    } else {
        None
    };

    // Print header
    if verbose {
        println!("\n{:<15} | {:<24} | {:>12} | {:>10} | {:>10} | {:>12} | {:>12} | {:>8} | {:>8}",
            "Timestamp", "DateTime", "Mid Price", "Inventory", "PnL", "Bid", "Ask", "BidFill", "AskFill");
        println!("{:-<145}", "");
    }

    let mut last_mid = Decimal::ZERO;
    let mut row_count = 0;

    // Event-driven loop state
    let mut active_bid_price: Option<Decimal> = None;
    let mut active_ask_price: Option<Decimal> = None;
    let mut active_quote_ts: u64 = 0;
    let mut last_orderbook_ts: u64 = 0;
    let mut warmup_end_ts: u64 = 0;

    // Constants for simulation
    let max_inventory_decimal = Decimal::from_f64(config.max_inventory).unwrap_or(Decimal::from(10));
    let fee_bps = Decimal::from_f64(config.maker_fee_bps).unwrap_or(Decimal::from(1));
    let fee_multiplier = fee_bps / Decimal::from(10000);
    let quote_validity_ms = config.quote_validity_seconds * 1000;
    let gap_threshold_ms = config.gap_threshold_seconds * 1000;
    let warmup_period_ms = config.warmup_period_seconds * 1000;

    for event_result in data_stream {
        let event = event_result?;

        match event {
            DataEvent::Trade(trade) => {
                let current_ts = trade.timestamp;

                // Add trade to calibration engine (for intensity parameter fitting)
                calibration_engine.add_trade(trade.clone());

                // Check if we are in warm-up period
                let is_warming_up = current_ts < warmup_end_ts;

                // Skip trading if warming up
                if is_warming_up {
                    continue;
                }

                // Check cooldown
                let cooldown_ms = config.fill_cooldown_seconds * 1000;

                // Check quote validity
                let is_quote_valid = active_quote_ts > 0 && trade.timestamp < active_quote_ts + quote_validity_ms;

                // Only check for fills if we have active quotes AND they are valid
                if let (Some(bid), Some(ask)) = (active_bid_price, active_ask_price) {
                    if is_quote_valid {
                        // SELL FILL: Market trade price >= our ask
                        if trade.price >= ask {
                            if state.last_ask_fill_ts > 0 && trade.timestamp < state.last_ask_fill_ts + cooldown_ms {
                                // Cooldown active, skip
                            } else if state.inventory > -max_inventory_decimal {
                                let unit_size = order_notional / trade.price;
                                let short_capacity = state.inventory + max_inventory_decimal;
                                let sell_size = short_capacity.min(unit_size).max(Decimal::ZERO);

                                if sell_size > Decimal::ZERO {
                                    let fill_price = ask;
                                    let gross_proceeds = fill_price * sell_size;
                                    let fee = gross_proceeds * fee_multiplier;

                                    state.inventory -= sell_size;
                                    state.cash += gross_proceeds - fee;
                                    state.ask_fills += 1;
                                    state.total_volume += sell_size;
                                    state.total_notional_volume += gross_proceeds;
                                    state.last_ask_fill_ts = trade.timestamp;
                                }
                            }
                        }
                        // BUY FILL: Market trade price <= our bid
                        else if trade.price <= bid {
                            if state.last_bid_fill_ts > 0 && trade.timestamp < state.last_bid_fill_ts + cooldown_ms {
                                // Cooldown active, skip
                            } else if state.inventory < max_inventory_decimal {
                                let unit_size = order_notional / trade.price;
                                let long_capacity = max_inventory_decimal - state.inventory;
                                let buy_size = long_capacity.min(unit_size).max(Decimal::ZERO);

                                if buy_size > Decimal::ZERO {
                                    let fill_price = bid;
                                    let gross_cost = fill_price * buy_size;
                                    let fee = gross_cost * fee_multiplier;
                                    let total_cost = gross_cost + fee;

                                    if state.cash >= total_cost {
                                        state.inventory += buy_size;
                                        state.cash -= total_cost;
                                        state.bid_fills += 1;
                                        state.total_volume += buy_size;
                                        state.total_notional_volume += gross_cost;
                                        state.last_bid_fill_ts = trade.timestamp;
                                    }
                                }
                            }
                        }
                    }
                }
            },
            DataEvent::Orderbook(quote) => {
                let current_ts = quote.timestamp;

                // Check for data gaps
                if last_orderbook_ts > 0 {
                    let time_delta = current_ts.saturating_sub(last_orderbook_ts);
                    if time_delta > gap_threshold_ms {
                        warmup_end_ts = current_ts + warmup_period_ms;
                        if verbose {
                            println!("Gap detected ({}s). Entering warm-up until {}",
                                time_delta / 1000,
                                format_timestamp(warmup_end_ts));
                        }

                        // Invalidate quotes during gap/warmup
                        active_bid_price = None;
                        active_ask_price = None;
                        active_quote_ts = 0;
                    }
                } else {
                    // Initial warm-up
                    warmup_end_ts = current_ts + warmup_period_ms;
                    if verbose {
                        println!("Starting initial warm-up until {}", format_timestamp(warmup_end_ts));
                    }
                }
                last_orderbook_ts = current_ts;

                // Update Market State with current orderbook
                let best_bid = quote.bids.first().map(|(p, _)| *p).unwrap_or(Decimal::ZERO);
                let best_ask = quote.asks.first().map(|(p, _)| *p).unwrap_or(Decimal::ZERO);
                let mid_price = if best_bid > Decimal::ZERO && best_ask > Decimal::ZERO {
                    (best_bid + best_ask) / Decimal::from(2)
                } else {
                    last_mid
                };

                last_mid = mid_price;

                // Calibration & Quoting for NEXT interval
                // Add price to calibration engine
                calibration_engine.add_price(current_ts, mid_price);

                // Prune old data from calibration windows
                calibration_engine.prune_windows(current_ts);

                // Check if we should recalibrate
                if calibration_engine.should_recalibrate(current_ts) {
                    if let Some(cal_result) = calibration_engine.calibrate(current_ts, config.tick_size) {
                        let optimal = compute_optimal_quote(
                            current_ts,
                            mid_price,
                            state.inventory,
                            cal_result.volatility,
                            cal_result.kappa,
                            &config,
                        );

                        // Set active quotes for the NEXT interval
                        active_bid_price = Some(optimal.bid_price);
                        active_ask_price = Some(optimal.ask_price);
                        active_quote_ts = current_ts;

                        let pnl = state.mark_to_market_pnl(mid_price);
                        let spread_bps = (optimal.optimal_spread / mid_price) * Decimal::from(10000);
                        let inventory_display = state.inventory.round_dp(6);

                        // Write to CSV if enabled
                        if let Some(ref mut file) = output_file {
                            writeln!(
                                file,
                                "{},{},{},{},{},{},{:.2},{},{},{},{},{:.6},{:.2}",
                                current_ts,
                                format_timestamp(current_ts),
                                mid_price,
                                inventory_display,
                                state.cash,
                                pnl,
                                spread_bps.to_f64().unwrap_or(0.0),
                                optimal.bid_price,
                                optimal.ask_price,
                                state.bid_fills,
                                state.ask_fills,
                                optimal.gamma,
                                cal_result.kappa
                            ).unwrap_or(()); // Ignore write errors for now
                        }

                        if verbose && row_count % 10 == 0 {
                            println!(
                                "{:<15} | {:<24} | {:>12.2} | {:>10} | {:>10.2} | {:>12.2} | {:>12.2} | {:>8} | {:>8}",
                                current_ts, format_timestamp(current_ts), mid_price, inventory_display, pnl, optimal.bid_price, optimal.ask_price, state.bid_fills, state.ask_fills
                            );
                        }
                        row_count += 1;
                    }
                }
            }
        }
    }

    // Force close any remaining position at final mid price
    if state.inventory != Decimal::ZERO && last_mid > Decimal::ZERO {
        let closing_fee_bps = Decimal::from_f64(config.taker_fee_bps).unwrap_or(Decimal::from(5));
        let closing_fee_multiplier = closing_fee_bps / Decimal::from(10000);

        if state.inventory > Decimal::ZERO {
            // Long position - sell to close
            let gross_proceeds = last_mid * state.inventory;
            let fee = gross_proceeds * closing_fee_multiplier;
            state.cash += gross_proceeds - fee;
            state.total_volume += state.inventory;
            state.total_notional_volume += gross_proceeds;
            if verbose {
                println!("\nClosing long position: Sold {} units at {} (fee: {})",
                    state.inventory, last_mid, fee);
            }
            state.inventory = Decimal::ZERO;
        } else {
            // Short position - buy to close
            let abs_inventory = state.inventory.abs();
            let gross_cost = last_mid * abs_inventory;
            let fee = gross_cost * closing_fee_multiplier;
            state.cash -= gross_cost + fee;
            state.total_volume += abs_inventory;
            state.total_notional_volume += gross_cost;
            if verbose {
                println!("\nClosing short position: Bought {} units at {} (fee: {})",
                    abs_inventory, last_mid, fee);
            }
            state.inventory = Decimal::ZERO;
        }
    }

    // Calculate final P&L
    let final_pnl = state.mark_to_market_pnl(last_mid);
    let total_return_pct = ((final_pnl - initial_capital) / initial_capital) * Decimal::from(100);

    Ok(BacktestResults {
        initial_capital,
        final_pnl,
        total_return_pct,
        bid_fills: state.bid_fills,
        ask_fills: state.ask_fills,
        total_volume: state.total_volume,
        total_notional_volume: state.total_notional_volume,
        final_inventory: state.inventory,
        final_cash: state.cash,
        config,
    })
}
