use extended_data_collector::model_types::ASConfig;
use extended_data_collector::data_loader::DataLoader;
use extended_data_collector::metrics::calculate_effective_price;
use extended_data_collector::calibration_engine::CalibrationEngine;
use extended_data_collector::spread_model::compute_optimal_quote;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::path::Path;
use std::fs::File;
use std::io::Write;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Load Config
    let config_path = "config.json";
    let config = match std::fs::read_to_string(config_path) {
        Ok(contents) => {
            match serde_json::from_str::<ASConfig>(&contents) {
                Ok(cfg) => {
                    println!("Loaded config from {}", config_path);
                    cfg
                },
                Err(e) => {
                    eprintln!("Error parsing config.json: {}. Using defaults.", e);
                    ASConfig::default()
                }
            }
        },
        Err(_) => {
            println!("config.json not found. Using defaults.");
            ASConfig::default()
        }
    };
    println!("Using Config: {:?}", config);

    // 2. Load Data
    println!("Loading data...");
    let loader = DataLoader::new(
        Path::new("data/ETH_USD/trades.csv"),
        Path::new("data/ETH_USD/orderbook_depth.csv"),
    )?;
    println!("Loaded {} trades and {} orderbooks", loader.get_trades().len(), loader.orderbooks_iter().count());

    // 3. Initialize Calibration Engine
    let mut calibration_engine = CalibrationEngine::new(&config);

    // 4. Prepare Output
    let mut output_file = File::create("data/ETH_USD/as_results.csv")?;
    writeln!(output_file, "timestamp,datetime,mid_price,volatility,kappa,A,gamma,optimal_spread_bps,bid_spread_bps,ask_spread_bps,bid_price,ask_price,reservation_price")?;

    // Print Header to Terminal
    println!("{:-<155}", "");
    println!(
        "{:<15} | {:>12} | {:>10} | {:>8} | {:>8} | {:>6} | {:>12} | {:>12} | {:>12} | {:>12} | {:>12}",
        "Timestamp", "Mid Price", "Volatility", "Kappa", "A", "Gamma", "Total(bps)", "Bid(bps)", "Ask(bps)", "Bid", "Ask"
    );
    println!("{:-<155}", "");

    // 5. Pre-load all trades for window processing
    let all_trades = loader.trades.clone();
    let mut trade_idx = 0;

    // Track last quote for final output
    let mut last_quote: Option<extended_data_collector::model_types::EffectiveQuote> = None;
    let mut last_ts = 0;

    // 6. Process Orderbook Snapshots
    for (ts, snapshot) in loader.orderbooks_iter() {
        let current_ts = *ts;
        last_ts = current_ts;

        // Calculate Effective Price
        let effective_quote = calculate_effective_price(snapshot, config.effective_volume_threshold);

        if let Some(quote) = effective_quote {
            last_quote = Some(quote);

            // Add price to calibration engine
            calibration_engine.add_price(current_ts, quote.mid);

            // Add trades that occurred since last update
            while trade_idx < all_trades.len() && all_trades[trade_idx].timestamp <= current_ts {
                calibration_engine.add_trade(all_trades[trade_idx].clone());
                trade_idx += 1;
            }

            // Prune old data from windows
            calibration_engine.prune_windows(current_ts);

            // Check if we should recalibrate
            if calibration_engine.should_recalibrate(current_ts) {
                if let Some(cal_result) = calibration_engine.calibrate(current_ts, config.tick_size) {
                    // Compute optimal quote with zero inventory
                    let inventory = Decimal::ZERO;
                    let optimal = compute_optimal_quote(
                        current_ts,
                        quote.mid,
                        inventory,
                        cal_result.volatility,
                        cal_result.kappa,
                        &config,
                    );

                    // Calculate spread metrics
                    let spread = optimal.optimal_spread;
                    let reservation_price = optimal.reservation_price;
                    let bid_price = optimal.bid_price;
                    let ask_price = optimal.ask_price;

                    let total_spread_bps = (spread / reservation_price) * Decimal::from(10000);
                    let bid_spread_bps = ((reservation_price - bid_price) / reservation_price) * Decimal::from(10000);
                    let ask_spread_bps = ((ask_price - reservation_price) / reservation_price) * Decimal::from(10000);

                    // Output to CSV
                    writeln!(
                        output_file,
                        "{},{},{},{:.6},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2}",
                        current_ts,
                        "N/A",
                        quote.mid,
                        cal_result.volatility,
                        cal_result.kappa,
                        cal_result.a,
                        optimal.gamma,
                        total_spread_bps,
                        bid_spread_bps,
                        ask_spread_bps,
                        bid_price,
                        ask_price,
                        reservation_price
                    )?;

                    // Output to terminal
                    println!(
                        "{:<15} | {:>12.2} | {:>10.6} | {:>8.2} | {:>8.2} | {:>6.2} | {:>12.2} | {:>12.2} | {:>12.2} | {:>12.2} | {:>12.2}",
                        current_ts, quote.mid, cal_result.volatility, cal_result.kappa, cal_result.a,
                        optimal.gamma, total_spread_bps, bid_spread_bps, ask_spread_bps, bid_price, ask_price
                    );
                }
            }
        }
    }

    // 7. Final Output (for partial window at end)
    if let Some(quote) = last_quote {
        if let Some(cal_result) = calibration_engine.calibrate(last_ts, config.tick_size) {
            let inventory = Decimal::ZERO;
            let optimal = compute_optimal_quote(
                last_ts,
                quote.mid,
                inventory,
                cal_result.volatility,
                cal_result.kappa,
                &config,
            );

            let spread = optimal.optimal_spread;
            let reservation_price = optimal.reservation_price;
            let bid_price = optimal.bid_price;
            let ask_price = optimal.ask_price;

            let total_spread_bps = (spread / reservation_price) * Decimal::from(10000);
            let bid_spread_bps = ((reservation_price - bid_price) / reservation_price) * Decimal::from(10000);
            let ask_spread_bps = ((ask_price - reservation_price) / reservation_price) * Decimal::from(10000);

            writeln!(
                output_file,
                "{},{},{},{:.6},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2}",
                last_ts, "N/A", quote.mid, cal_result.volatility, cal_result.kappa, cal_result.a,
                optimal.gamma, total_spread_bps, bid_spread_bps, ask_spread_bps, bid_price, ask_price, reservation_price
            )?;

            println!(
                "{:<15} | {:>12.2} | {:>10.6} | {:>8.2} | {:>8.2} | {:>6.2} | {:>12.2} | {:>12.2} | {:>12.2} | {:>12.2} | {:>12.2}",
                last_ts, quote.mid, cal_result.volatility, cal_result.kappa, cal_result.a,
                optimal.gamma, total_spread_bps, bid_spread_bps, ask_spread_bps, bid_price, ask_price
            );
        }
    }

    println!("Done! Results written to data/ETH_USD/as_results.csv");
    Ok(())
}
