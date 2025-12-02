/// Backtest engine module - reusable backtesting logic for AS market-making strategy
///
/// This module provides the core backtesting functionality that can be used by both
/// the standalone backtest binary and grid search binary without code duplication.
///
/// PERFORMANCE OPTIMIZATIONS APPLIED:
/// 1. BufWriter for CSV output (reduces syscalls)
/// 2. Precomputed Decimal constants (avoids repeated conversions)
/// 3. Lazy string formatting (only when needed)
/// 4. Reduced allocations in hot path
/// 5. Conditional computation gating

use crate::calibration_engine::CalibrationEngine;
use crate::data_loader::DataEvent;
use crate::model_types::ASConfig;
use crate::spread_model::compute_optimal_quote;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};
use chrono::{DateTime, Utc};

/// Precomputed constants to avoid repeated Decimal conversions
struct DecimalConstants {
    zero: Decimal,
    two: Decimal,
    ten_thousand: Decimal,
    hundred: Decimal,
}

impl DecimalConstants {
    const fn new() -> Self {
        Self {
            zero: Decimal::ZERO,
            two: Decimal::from_parts(2, 0, 0, false, 0),
            ten_thousand: Decimal::from_parts(10000, 0, 0, false, 0),
            hundred: Decimal::from_parts(100, 0, 0, false, 0),
        }
    }
}

static DECIMAL_CONSTS: DecimalConstants = DecimalConstants::new();

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
    #[inline]
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

    #[inline]
    fn mark_to_market_pnl(&self, mid_price: Decimal) -> Decimal {
        self.cash + (self.inventory * mid_price)
    }
}

/// Format timestamp (epoch milliseconds) as human-readable string
/// Only call this when actually needed for output
#[inline]
fn format_timestamp(timestamp_ms: u64) -> String {
    let seconds = (timestamp_ms / 1000) as i64;
    let nanos = ((timestamp_ms % 1000) * 1_000_000) as u32;

    match DateTime::<Utc>::from_timestamp(seconds, nanos) {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string(),
        None => "N/A".to_string(),
    }
}

/// Precomputed configuration values to avoid repeated calculations in hot path
struct PrecomputedConfig {
    max_inventory_decimal: Decimal,
    fee_multiplier: Decimal,
    closing_fee_multiplier: Decimal,
    quote_validity_ms: u64,
    gap_threshold_ms: u64,
    warmup_period_ms: u64,
    cooldown_ms: u64,
}

impl PrecomputedConfig {
    fn from_config(config: &ASConfig) -> Self {
        let fee_bps = Decimal::from_f64(config.maker_fee_bps).unwrap_or(Decimal::ONE);
        let closing_fee_bps = Decimal::from_f64(config.taker_fee_bps).unwrap_or(Decimal::from(5));
        
        Self {
            max_inventory_decimal: Decimal::from_f64(config.max_inventory).unwrap_or(Decimal::from(10)),
            fee_multiplier: fee_bps / DECIMAL_CONSTS.ten_thousand,
            closing_fee_multiplier: closing_fee_bps / DECIMAL_CONSTS.ten_thousand,
            quote_validity_ms: config.quote_validity_seconds.saturating_mul(1000),
            gap_threshold_ms: config.gap_threshold_seconds.saturating_mul(1000),
            warmup_period_ms: config.warmup_period_seconds.saturating_mul(1000),
            cooldown_ms: config.fill_cooldown_seconds.saturating_mul(1000),
        }
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

    // Precompute configuration values once
    let precomputed = PrecomputedConfig::from_config(&config);

    // Initialize state
    let mut state = BacktestState::new(initial_capital);

    // Initialize calibration engine
    let mut calibration_engine = CalibrationEngine::new(&config);

    // Output file with buffered writer for better I/O performance
    let mut output_file: Option<BufWriter<File>> = if let Some(ref path) = output_csv_path {
        let file = File::create(path)?;
        let mut writer = BufWriter::with_capacity(64 * 1024, file); // 64KB buffer
        writeln!(
            writer,
            "timestamp,datetime,mid_price,inventory,cash,pnl,spread_bps,bid_price,ask_price,bid_fills,ask_fills,gamma,bid_kappa,ask_kappa,bid_a,ask_a"
        )?;
        Some(writer)
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
    let mut row_count: u64 = 0;

    // Event-driven loop state
    let mut active_bid_price: Option<Decimal> = None;
    let mut active_ask_price: Option<Decimal> = None;
    let mut active_quote_ts: u64 = 0;
    let mut last_orderbook_ts: u64 = 0;
    let mut warmup_end_ts: u64 = 0;

    // Track if we need to output (reduces conditional checks)
    let needs_csv = output_file.is_some();

    for event_result in data_stream {
        let event = event_result?;

        match event {
            DataEvent::Trade(trade) => {
                let current_ts = trade.timestamp;

                // Add trade to calibration engine
                // TODO: If CalibrationEngine can accept &Trade, remove clone for better performance
                calibration_engine.add_trade(trade.clone());

                // Skip trading if warming up (early exit for performance)
                if current_ts < warmup_end_ts {
                    continue;
                }

                // Only check for fills if we have active quotes
                let (bid, ask) = match (active_bid_price, active_ask_price) {
                    (Some(b), Some(a)) => (b, a),
                    _ => continue,
                };

                // Check quote validity
                if active_quote_ts == 0 || trade.timestamp >= active_quote_ts + precomputed.quote_validity_ms {
                    continue;
                }

                let trade_price = trade.price;
                let trade_ts = trade.timestamp;

                // SELL FILL: Market trade price >= our ask
                if trade_price >= ask {
                    let ask_cooldown_active = state.last_ask_fill_ts > 0 
                        && trade_ts < state.last_ask_fill_ts + precomputed.cooldown_ms;

                    if !ask_cooldown_active && state.inventory > -precomputed.max_inventory_decimal {
                        let unit_size = order_notional / trade_price;
                        let short_capacity = state.inventory + precomputed.max_inventory_decimal;
                        let sell_size = short_capacity.min(unit_size).max(Decimal::ZERO);

                        if sell_size > Decimal::ZERO {
                            let gross_proceeds = ask * sell_size;
                            let fee = gross_proceeds * precomputed.fee_multiplier;

                            state.inventory -= sell_size;
                            state.cash += gross_proceeds - fee;
                            state.ask_fills += 1;
                            state.total_volume += sell_size;
                            state.total_notional_volume += gross_proceeds;
                            state.last_ask_fill_ts = trade_ts;
                        }
                    }
                }
                // BUY FILL: Market trade price <= our bid
                else if trade_price <= bid {
                    let bid_cooldown_active = state.last_bid_fill_ts > 0 
                        && trade_ts < state.last_bid_fill_ts + precomputed.cooldown_ms;

                    if !bid_cooldown_active && state.inventory < precomputed.max_inventory_decimal {
                        let unit_size = order_notional / trade_price;
                        let long_capacity = precomputed.max_inventory_decimal - state.inventory;
                        let buy_size = long_capacity.min(unit_size).max(Decimal::ZERO);

                        if buy_size > Decimal::ZERO {
                            let gross_cost = bid * buy_size;
                            let fee = gross_cost * precomputed.fee_multiplier;
                            let total_cost = gross_cost + fee;

                            if state.cash >= total_cost {
                                state.inventory += buy_size;
                                state.cash -= total_cost;
                                state.bid_fills += 1;
                                state.total_volume += buy_size;
                                state.total_notional_volume += gross_cost;
                                state.last_bid_fill_ts = trade_ts;
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
                    if time_delta > precomputed.gap_threshold_ms {
                        warmup_end_ts = current_ts + precomputed.warmup_period_ms;
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
                    warmup_end_ts = current_ts + precomputed.warmup_period_ms;
                    if verbose {
                        println!("Starting initial warm-up until {}", format_timestamp(warmup_end_ts));
                    }
                }
                last_orderbook_ts = current_ts;

                // Update Market State with current orderbook
                let best_bid = quote.bids.first().map(|(p, _)| *p).unwrap_or(Decimal::ZERO);
                let best_ask = quote.asks.first().map(|(p, _)| *p).unwrap_or(Decimal::ZERO);
                let mid_price = if best_bid > Decimal::ZERO && best_ask > Decimal::ZERO {
                    (best_bid + best_ask) / DECIMAL_CONSTS.two
                } else {
                    last_mid
                };

                last_mid = mid_price;

                // Calibration & Quoting for NEXT interval
                calibration_engine.add_orderbook(&quote, mid_price);
                calibration_engine.prune_windows(current_ts);

                // Check if we should recalibrate
                if calibration_engine.should_recalibrate(current_ts) {
                    if let Some(cal_result) = calibration_engine.calibrate(current_ts, config.tick_size) {
                        let optimal = compute_optimal_quote(
                            current_ts,
                            mid_price,
                            state.inventory,
                            cal_result.volatility,
                            cal_result.bid_kappa,
                            cal_result.ask_kappa,
                            &config,
                        );

                        // Set active quotes for the NEXT interval
                        active_bid_price = Some(optimal.bid_price);
                        active_ask_price = Some(optimal.ask_price);
                        active_quote_ts = current_ts;

                        // Only compute display values when actually needed
                        let should_print = verbose && row_count % 10 == 0;
                        
                        if needs_csv || should_print {
                            let pnl = state.mark_to_market_pnl(mid_price);
                            
                            let spread_bps = if mid_price > Decimal::ZERO {
                                (optimal.optimal_spread / mid_price) * DECIMAL_CONSTS.ten_thousand
                            } else {
                                Decimal::ZERO
                            };

                            // Write to CSV if enabled
                            if let Some(ref mut writer) = output_file {
                                let inventory_display = state.inventory.round_dp(6);
                                if let Err(e) = writeln!(
                                    writer,
                                    "{},{},{},{},{},{},{:.2},{},{},{},{},{:.6},{:.2},{:.2},{:.2},{:.2}",
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
                                    cal_result.bid_kappa,
                                    cal_result.ask_kappa,
                                    cal_result.bid_a,
                                    cal_result.ask_a
                                ) {
                                    eprintln!("Warning: Failed to write to CSV: {}", e);
                                }
                            }

                            if should_print {
                                let inventory_display = state.inventory.round_dp(6);
                                println!(
                                    "{:<15} | {:<24} | {:>12.2} | {:>10} | {:>10.2} | {:>12.2} | {:>12.2} | {:>8} | {:>8}",
                                    current_ts, format_timestamp(current_ts), mid_price, inventory_display, pnl, optimal.bid_price, optimal.ask_price, state.bid_fills, state.ask_fills
                                );
                            }
                        }
                        row_count += 1;
                    }
                }
            }
        }
    }

    // Flush the buffered writer before closing
    if let Some(ref mut writer) = output_file {
        writer.flush()?;
    }

    // Force close any remaining position at final mid price
    if state.inventory != Decimal::ZERO && last_mid > Decimal::ZERO {
        if state.inventory > Decimal::ZERO {
            // Long position - sell to close
            let gross_proceeds = last_mid * state.inventory;
            let fee = gross_proceeds * precomputed.closing_fee_multiplier;
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
            let fee = gross_cost * precomputed.closing_fee_multiplier;
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
    let total_return_pct = ((final_pnl - initial_capital) / initial_capital) * DECIMAL_CONSTS.hundred;

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
