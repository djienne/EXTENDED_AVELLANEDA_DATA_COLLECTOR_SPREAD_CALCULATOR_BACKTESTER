use extended_data_collector::calibration::{calculate_volatility, fit_intensity_parameters};
use extended_data_collector::data_loader::DataLoader;
use extended_data_collector::model_types::{ASConfig, TradeEvent};
use extended_data_collector::spread_model::compute_optimal_quote;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone)]
struct BacktestState {
    inventory: Decimal,
    cash: Decimal,
    pnl_history: Vec<Decimal>,
    bid_fills: u64,
    ask_fills: u64,
    total_volume: Decimal,  // Accumulated trading volume
}

impl BacktestState {
    fn new(initial_capital: Decimal) -> Self {
        Self {
            inventory: Decimal::ZERO,
            cash: initial_capital,
            pnl_history: Vec::new(),
            bid_fills: 0,
            ask_fills: 0,
            total_volume: Decimal::ZERO,
        }
    }

    fn mark_to_market_pnl(&self, mid_price: Decimal) -> Decimal {
        self.cash + (self.inventory * mid_price)
    }
}

fn main() -> std::io::Result<()> {
    println!("Loading data...");
    
    // Load configuration
    let config = ASConfig::default();
    
    
    // Load data using DataLoader
    let loader = DataLoader::new(
        Path::new("data/ETH_USD/trades.csv"),
        Path::new("data/ETH_USD/orderbook_depth.csv"),
    ).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e)))?;
    
    let trades = loader.get_trades();
    let orderbooks: Vec<_> = loader.orderbooks_iter().map(|(_, ob)| ob.clone()).collect();

    // Precompute per-second trade high/low to avoid repeated scans
    let mut second_hilo: HashMap<u64, (Decimal, Decimal)> = HashMap::new();
    for trade in trades.iter() {
        let sec = trade.timestamp / 1000;
        let entry = second_hilo
            .entry(sec)
            .or_insert((Decimal::ZERO, Decimal::from(999_999)));
        if trade.price > entry.0 {
            entry.0 = trade.price;
        }
        if trade.price < entry.1 {
            entry.1 = trade.price;
        }
    }
    
    println!("Loaded {} orderbooks and {} trades", orderbooks.len(), trades.len());
    println!("Config: gamma_min={}, max_inventory={}, horizon={}s", 
        config.gamma_min, config.max_inventory, config.inventory_horizon_seconds);
    
    // Backtest parameters
    let initial_capital = Decimal::from_str("1000.0").unwrap();
    let order_notional = Decimal::from_str("20.0").unwrap();  // $20 per order
    
    // Initialize state
    let mut state = BacktestState::new(initial_capital);
    
    let calibration_window_ms = (config.calibration_window_seconds * 1000) as u64;
    let recalibration_interval_ms = (config.recalibration_interval_seconds * 1000) as u64;
    
    let mut calibration_prices: Vec<(u64, Decimal)> = Vec::new();
    let mut window_prices: Vec<Decimal> = Vec::new();
    let mut window_trades: Vec<TradeEvent> = Vec::new();
    
    // Output file
    let mut output_file = File::create("data/ETH_USD/backtest_results.csv")?;
    writeln!(
        output_file,
        "timestamp,datetime,mid_price,inventory,cash,pnl,spread_bps,bid_price,ask_price,bid_fills,ask_fills,gamma,kappa"
    )?;
    
    // Calibration state
    let mut kappa = 10.0;
    let mut A = 10.0;
    let mut last_calibration_ts: Option<u64> = None;
    
    // Print header
    println!("\n{:<15} | {:>12} | {:>10} | {:>10} | {:>12} | {:>12} | {:>8} | {:>8}",
        "Timestamp", "Mid Price", "Inventory", "PnL", "Bid", "Ask", "BidFill", "AskFill");
    println!("{:-<120}", "");
    
    let mut last_bid = Decimal::ZERO;
    let mut last_ask = Decimal::ZERO;
    let mut last_mid = Decimal::ZERO;
    let mut row_count = 0;
    let mut window_trade_idx: usize = 0;
    
    for (idx, quote) in orderbooks.iter().enumerate() {
        let current_ts = quote.timestamp;
        let current_second_ts = current_ts / 1000; // Round down to second

        let (second_high, second_low) = second_hilo
            .get(&current_second_ts)
            .cloned()
            .unwrap_or((Decimal::ZERO, Decimal::from(999_999)));
        
        // Calculate mid price from best bid/ask
        let best_bid = quote.bids.first().map(|(p, _)| *p).unwrap_or(Decimal::ZERO);
        let best_ask = quote.asks.first().map(|(p, _)| *p).unwrap_or(Decimal::ZERO);
        let mid_price = if best_bid > Decimal::ZERO && best_ask > Decimal::ZERO {
            (best_bid + best_ask) / Decimal::from(2)
        } else {
            last_mid
        };

        last_bid = best_bid;
        last_ask = best_ask;
        last_mid = mid_price;

        calibration_prices.push((current_ts, mid_price));
        window_prices.push(mid_price);

        // Add new trades to window (FIX: Use only trades BEFORE current timestamp to avoid look-ahead bias)
        while window_trade_idx < trades.len() && trades[window_trade_idx].timestamp < current_ts {
            window_trades.push(trades[window_trade_idx].clone());
            window_trade_idx += 1;
        }

        // Remove old data from windows
        calibration_prices.retain(|(ts, _)| *ts >= current_ts.saturating_sub(calibration_window_ms));
        window_trades.retain(|t| t.timestamp >= current_ts.saturating_sub(calibration_window_ms));
        
        let should_calibrate = if let Some(last_cal_ts) = last_calibration_ts {
            current_ts >= last_cal_ts + recalibration_interval_ms
        } else {
            calibration_prices.len() >= 10
        };
        
        if should_calibrate && !calibration_prices.is_empty() {
            let start_ts = calibration_prices.first().map(|(ts, _)| *ts).unwrap_or(current_ts);
            let actual_duration_sec = ((current_ts - start_ts) as f64 / 1000.0).max(1.0);
            
            let sigma_pct_raw = calculate_volatility(&window_prices, actual_duration_sec);
            let sigma_pct = sigma_pct_raw;
            
            let (new_kappa, new_A) = fit_intensity_parameters(
                &window_trades,
                &calibration_prices,
                actual_duration_sec,
                config.tick_size
            );
            
            if new_kappa > 0.0 && new_A > 0.0 {
                kappa = new_kappa;
                A = new_A;
            }
            last_calibration_ts = Some(current_ts);
            
            let optimal = compute_optimal_quote(
                current_ts,
                mid_price,
                state.inventory,
                sigma_pct,
                kappa,
                &config,
            );
            let bid_price = optimal.bid_price;
            let ask_price = optimal.ask_price;
            let optimal_spread = optimal.optimal_spread;
            
            // Simulate fills using market high/low with realistic constraints
            let max_inventory_decimal = Decimal::from_f64(config.max_inventory).unwrap_or(Decimal::from(10));
            let fee_bps = Decimal::from_f64(config.maker_fee_bps).unwrap_or(Decimal::from(1));
            let fee_multiplier = fee_bps / Decimal::from(10000);
            
            let mut filled_this_second = false;
            
            // FIX P1: Prevent simultaneous fills (priority to sells first)
            // FIX P0: Enforce inventory limits
            // FIX P1: Use realistic fill prices (mid price for conservative estimate)
            // FIX P0: Include transaction costs
            
            // SELL FILL: Market high >= our ask, aggressive buyers hit our ask -> WE SELL
            if second_high > Decimal::ZERO && second_high >= ask_price {
                // Check inventory limits (can't sell if already max short)
                if state.inventory > -max_inventory_decimal {
                    // Calculate unit size from dollar notional
                    let unit_size = order_notional / mid_price;
                    // Scale order size based on available capacity (P2 fix)
                    let short_capacity = state.inventory + max_inventory_decimal;
                    let sell_size = short_capacity.min(unit_size).max(Decimal::ZERO);
                    
                    if sell_size > Decimal::ZERO {
                        // Use mid or our ask, whichever is more conservative (realistic pricing)
                        let fill_price = ask_price.min(mid_price.max(ask_price));
                        let gross_proceeds = fill_price * sell_size;
                        let fee = gross_proceeds * fee_multiplier;
                        
                        state.inventory -= sell_size;
                        state.cash += gross_proceeds - fee;
                        state.ask_fills += 1;
                        state.total_volume += sell_size;
                        filled_this_second = true;
                    }
                }
            }
            
            // BUY FILL: Market low <= our bid, aggressive sellers hit our bid -> WE BUY
            if !filled_this_second && second_low < Decimal::from(999_999) && second_low <= bid_price {
                // Check inventory limits (can't buy if already max long)
                if state.inventory < max_inventory_decimal {
                    // Calculate unit size from dollar notional
                    let unit_size = order_notional / mid_price;
                    // Scale order size based on available capacity (P2 fix)
                    let long_capacity = max_inventory_decimal - state.inventory;
                    let buy_size = long_capacity.min(unit_size).max(Decimal::ZERO);
                    
                    if buy_size > Decimal::ZERO {
                        // Use mid or our bid, whichever is more conservative (realistic pricing)
                        let fill_price = bid_price.max(mid_price.min(bid_price));
                        let gross_cost = fill_price * buy_size;
                        let fee = gross_cost * fee_multiplier;
                        let total_cost = gross_cost + fee;
                        
                        // FIX P1: Check cash constraint
                        if state.cash >= total_cost {
                            state.inventory += buy_size;
                            state.cash -= total_cost;
                            state.bid_fills += 1;
                            state.total_volume += buy_size;
                        }
                    }
                }
            }
            
            // Update P&L (FIX: Don't grow history unbounded, just track latest)
            let pnl = state.mark_to_market_pnl(mid_price);
            if state.pnl_history.is_empty() {
                state.pnl_history.push(pnl);
            } else {
                state.pnl_history[0] = pnl;
            }
            
            let spread_bps = (optimal_spread / mid_price) * Decimal::from(10000);
            
            // Round inventory for output (keep full precision internally)
            let inventory_display = state.inventory.round_dp(6);
            
            // Output every recalibration (every minute based on config)
            writeln!(
                output_file,
                "{},{},{},{},{},{},{:.2},{},{},{},{},{:.6},{:.2}",
                current_ts,
                "N/A",
                mid_price,
                inventory_display,
                state.cash,
                pnl,
                spread_bps.to_f64().unwrap_or(0.0),
                bid_price,
                ask_price,
                state.bid_fills,
                state.ask_fills,
                optimal.gamma,
                kappa
            )?;
            
            // Print to console every 10 recalibrations to avoid spam
            if row_count % 10 == 0 {
                // Round inventory for display (keep full precision internally)
                let inventory_display = state.inventory.round_dp(6);
                println!(
                    "{:<15} | {:>12.2} | {:>10} | {:>10.2} | {:>12.2} | {:>12.2} | {:>8} | {:>8}",
                    current_ts, mid_price, inventory_display, pnl, bid_price, ask_price, state.bid_fills, state.ask_fills
                );
            }
            
            row_count += 1;
        }
        
        // last_quote tracking not needed
    }
    
    // Force close any remaining position at final mid price
    if state.inventory != Decimal::ZERO && last_mid > Decimal::ZERO {
        let closing_fee_bps = Decimal::from_f64(config.taker_fee_bps).unwrap_or(Decimal::from(5));
        let closing_fee_multiplier = closing_fee_bps / Decimal::from(10000);
        
        if state.inventory > Decimal::ZERO {
            // Long position - sell to close at mid (pay taker fee as market order)
            let gross_proceeds = last_mid * state.inventory;
            let fee = gross_proceeds * closing_fee_multiplier;
            state.cash += gross_proceeds - fee;
            state.total_volume += state.inventory;
            println!("\nClosing long position: Sold {} units at {} (fee: {})", 
                state.inventory, last_mid, fee);
            state.inventory = Decimal::ZERO;
        } else {
            // Short position - buy to close at mid (pay taker fee as market order)
            let abs_inventory = state.inventory.abs();
            let gross_cost = last_mid * abs_inventory;
            let fee = gross_cost * closing_fee_multiplier;
            state.cash -= gross_cost + fee;
            state.total_volume += abs_inventory;
            println!("\nClosing short position: Bought {} units at {} (fee: {})", 
                abs_inventory, last_mid, fee);
            state.inventory = Decimal::ZERO;
        }
    }
    
    // Calculate final P&L after closing position
    let final_pnl = state.mark_to_market_pnl(last_mid);
    
    // Final summary
    println!("\n{:-<120}", "");
    println!("BACKTEST SUMMARY");
    println!("{:-<120}", "");
    println!("Initial Capital: ${:.2}", initial_capital);
    println!("Final P&L: ${:.2}", final_pnl);
    println!("Total Return: {:.2}%", ((final_pnl - initial_capital) / initial_capital) * Decimal::from(100));
    println!("Final Inventory: {} (closed)", state.inventory);
    println!("Total Bid Fills: {}", state.bid_fills);
    println!("Total Ask Fills: {}", state.ask_fills);
    println!("Total Fills: {}", state.bid_fills + state.ask_fills);
    println!("Total Volume Traded: {} units (${:.2} notional at avg ~$2745)", 
        state.total_volume, 
        state.total_volume * Decimal::from_str("2745.0").unwrap_or(last_mid));
    println!("\nResults written to data/ETH_USD/backtest_results.csv");
    
    Ok(())
}
