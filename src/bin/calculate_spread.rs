use extended_data_collector::model_types::ASConfig;
use extended_data_collector::data_loader::DataLoader;
use extended_data_collector::metrics::calculate_effective_price;
use extended_data_collector::calibration::{calculate_volatility, fit_intensity_parameters};
use extended_data_collector::spread_model::compute_optimal_quote;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::path::Path;
use std::fs::File;
use std::io::Write;
// use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Load Config
    let config = ASConfig::default(); // Use defaults for now, or load from file
    println!("Using Config: {:?}", config);

    // 2. Load Data
    println!("Loading data...");
    let loader = DataLoader::new(
        Path::new("data/ETH_USD/trades.csv"),
        Path::new("data/ETH_USD/orderbook_depth.csv"),
    )?;
    println!("Loaded {} trades and {} orderbooks", loader.get_trades().len(), loader.orderbooks_iter().count());

    // 3. Prepare Output
    let mut output_file = File::create("data/ETH_USD/as_results.csv")?;
    writeln!(output_file, "timestamp,datetime,mid_price,volatility,kappa,A,gamma,optimal_spread_bps,bid_spread_bps,ask_spread_bps,bid_price,ask_price,reservation_price")?;

    // Print Header to Terminal
    println!("{:-<155}", "");
    println!(
        "{:<15} | {:>12} | {:>10} | {:>8} | {:>8} | {:>6} | {:>12} | {:>12} | {:>12} | {:>12} | {:>12}",
        "Timestamp", "Mid Price", "Volatility", "Kappa", "A", "Gamma", "Total(bps)", "Bid(bps)", "Ask(bps)", "Bid", "Ask"
    );
    println!("{:-<155}", "");

    // 4. Sliding Window Loop
    let mut window_trades: Vec<extended_data_collector::model_types::TradeEvent> = Vec::new();
    let mut window_prices: Vec<Decimal> = Vec::new(); // For volatility (just prices)
    let mut calibration_prices: Vec<(u64, Decimal)> = Vec::new(); // For intensity (ts, price)
    
    // Parameters
    let calibration_window_seconds = config.calibration_window_seconds as u64; 
    let calibration_window_ms = calibration_window_seconds * 1000;
    
    let mut row_count = 0;
    let mut last_calibration_ts: Option<u64> = None;
    let recalibration_interval_ms = (config.recalibration_interval_seconds as u64) * 1000;

    // Current calibrated parameters (start with defaults)
    let mut kappa = 10.0;
    let mut A = 100.0;

    // Pre-load all trades to easily find them in window
    // In a real streaming app, we'd process them as they come.
    // Here we have a list of trades. We can use an index to track where we are.
    let all_trades = loader.trades.clone(); // Clone for simplicity in this loop
    let mut trade_idx = 0;
    
    // Store last quote for final output
    let mut last_quote: Option<extended_data_collector::model_types::EffectiveQuote> = None;
    let mut last_ts = 0;

    for (ts, snapshot) in loader.orderbooks_iter() {
        let current_ts = *ts;
        last_ts = current_ts;
        
        // Initialize start time
        if last_calibration_ts.is_none() {
            last_calibration_ts = Some(current_ts);
        }
        
        // Calculate Effective Price
        let effective_quote = calculate_effective_price(snapshot, config.effective_volume_threshold);
        
        if let Some(quote) = effective_quote {
            last_quote = Some(quote);
            // Update Price History for Volatility
            window_prices.push(quote.mid);
            calibration_prices.push((current_ts, quote.mid));

            // Prune old prices
            while let Some((first_ts, _)) = calibration_prices.first() {
                if current_ts - first_ts > calibration_window_ms {
                    calibration_prices.remove(0);
                    if !window_prices.is_empty() { window_prices.remove(0); }
                } else {
                    break;
                }
            }

            // Update Trades Window
            while trade_idx < all_trades.len() {
                let trade = &all_trades[trade_idx];
                if trade.timestamp <= current_ts {
                    window_trades.push(trade.clone());
                    trade_idx += 1;
                } else {
                    break;
                }
            }

            // Prune old trades
            window_trades.retain(|t| current_ts - t.timestamp <= calibration_window_ms);
            
            // 5. Calibrate (Periodically - Every 1 Hour)
            if let Some(last_cal_ts) = last_calibration_ts {
                if current_ts >= last_cal_ts + recalibration_interval_ms {
                    // Calculate actual window duration
                    let start_ts = calibration_prices.first().map(|(ts, _)| *ts).unwrap_or(current_ts);
                    let actual_duration_sec = ((current_ts - start_ts) as f64 / 1000.0).max(1.0);

                    let sigma_pct_raw = calculate_volatility(&window_prices, actual_duration_sec);
                    let sigma_pct = sigma_pct_raw;
                    
                    // B. Intensity (Kappa, A)
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
                    
                    let inventory = Decimal::ZERO;
                    let optimal = compute_optimal_quote(
                        current_ts,
                        quote.mid,
                        inventory,
                        sigma_pct,
                        kappa,
                        &config,
                    );
                    let spread = optimal.optimal_spread;
                    let reservation_price = optimal.reservation_price;
                    let half_spread = spread / Decimal::from(2);
                    let bid_price = optimal.bid_price;
                    let ask_price = optimal.ask_price;
                    let total_spread_bps = (spread / reservation_price) * Decimal::from(10000);
                    let bid_spread_bps = ((reservation_price - bid_price) / reservation_price) * Decimal::from(10000);
                    let ask_spread_bps = ((ask_price - reservation_price) / reservation_price) * Decimal::from(10000);

                    // 7. Output
                    writeln!(
                        output_file,
                        "{},{},{},{:.6},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2}",
                        current_ts,
                        "N/A", // DateTime
                        quote.mid,
                        sigma_pct, // Keep logging pct volatility for familiarity
                        kappa,
                        A,
                        optimal.gamma,
                        total_spread_bps,
                        bid_spread_bps,
                        ask_spread_bps,
                        bid_price,
                        ask_price,
                        reservation_price
                    )?;

                    println!(
                        "{:<15} | {:>12.2} | {:>10.6} | {:>8.2} | {:>8.2} | {:>6.2} | {:>12.2} | {:>12.2} | {:>12.2} | {:>12.2} | {:>12.2}",
                        current_ts, quote.mid, sigma_pct, kappa, A, optimal.gamma, total_spread_bps, bid_spread_bps, ask_spread_bps, bid_price, ask_price
                    );
                    
                    row_count += 1;
                }
            }
        }
    }

    // Final Output (if we haven't printed recently or at all)
    // This ensures we see the result of the partial window at the end
    if let Some(quote) = last_quote {
        let current_ts = last_ts;
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
        
        let inventory = Decimal::ZERO;
        let optimal = compute_optimal_quote(
            current_ts,
            quote.mid,
            inventory,
            sigma_pct,
            new_kappa,
            &config,
        );
        let spread = optimal.optimal_spread;
        let reservation_price = optimal.reservation_price;
        let half_spread = spread / Decimal::from(2);
        let bid_price = optimal.bid_price;
        let ask_price = optimal.ask_price;
        
        let total_spread_bps = (spread / reservation_price) * Decimal::from(10000);
        let bid_spread_bps = ((reservation_price - bid_price) / reservation_price) * Decimal::from(10000);
        let ask_spread_bps = ((ask_price - reservation_price) / reservation_price) * Decimal::from(10000);

        writeln!(
            output_file,
            "{},{},{},{:.6},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2}",
            current_ts, "N/A", quote.mid, sigma_pct, new_kappa, new_A, optimal.gamma, total_spread_bps, bid_spread_bps, ask_spread_bps, bid_price, ask_price, reservation_price
        )?;
        println!(
            "{:<15} | {:>12.2} | {:>10.6} | {:>8.2} | {:>8.2} | {:>6.2} | {:>12.2} | {:>12.2} | {:>12.2} | {:>12.2} | {:>12.2}",
            current_ts, quote.mid, sigma_pct, new_kappa, new_A, optimal.gamma, total_spread_bps, bid_spread_bps, ask_spread_bps, bid_price, ask_price
        );
    }

    println!("Done! Results written to data/ETH_USD/as_results.csv");
    Ok(())
}
