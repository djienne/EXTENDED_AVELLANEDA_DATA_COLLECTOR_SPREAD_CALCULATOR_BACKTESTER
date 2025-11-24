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
        Path::new("data/eth_usd/trades.csv"),
        Path::new("data/eth_usd/orderbook_parts"),
    );
    let trades = loader.get_trades()?;
    // let orderbooks_count = loader.orderbooks_iter()?.count(); // Removed to prevent freezing
    println!("Loaded {} trades", trades.len());

    // 3. Initialize Calibration Engine
    let mut calibration_engine = CalibrationEngine::new(&config);

    // 4. Prepare Output
    let mut output_file = File::create("data/eth_usd/as_results.csv")?;
    writeln!(output_file, "timestamp,datetime,mid_price,volatility,kappa,A,gamma,optimal_spread_bps,bid_spread_bps,ask_spread_bps,bid_price,ask_price,reservation_price")?;

    // Print Header to Terminal
    println!("{:-<155}", "");
    println!(
        "{:<15} | {:>12} | {:>10} | {:>8} | {:>8} | {:>6} | {:>12} | {:>12} | {:>12} | {:>12} | {:>12}",
        "Timestamp", "Mid Price", "Volatility", "Kappa", "A", "Gamma", "Total(bps)", "Bid(bps)", "Ask(bps)", "Bid", "Ask"
    );
    println!("{:-<155}", "");

    // 5. Pre-load all trades for window processing
    let all_trades = trades;
    let mut trade_idx = 0;

    // Track last quote for final output
    let mut last_quote: Option<extended_data_collector::model_types::EffectiveQuote> = None;
    let mut last_ts = 0;

    // Collect statistics
    let mut total_spread_bps_vec: Vec<f64> = Vec::new();
    let mut bid_spread_bps_vec: Vec<f64> = Vec::new();
    let mut ask_spread_bps_vec: Vec<f64> = Vec::new();
    let mut volatility_vec: Vec<f64> = Vec::new();
    let mut kappa_vec: Vec<f64> = Vec::new();
    let mut a_vec: Vec<f64> = Vec::new();
    let mut gamma_vec: Vec<f64> = Vec::new();

    // 6. Process Orderbook Snapshots
    for result in loader.orderbooks_iter()? {
        let (ts, snapshot) = result?;
        let current_ts = ts;
        last_ts = current_ts;

        // Calculate Effective Price
        let effective_quote = calculate_effective_price(&snapshot, config.effective_volume_threshold);

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

                    // Collect statistics
                    total_spread_bps_vec.push(total_spread_bps.to_f64().unwrap_or(0.0));
                    bid_spread_bps_vec.push(bid_spread_bps.to_f64().unwrap_or(0.0));
                    ask_spread_bps_vec.push(ask_spread_bps.to_f64().unwrap_or(0.0));
                    volatility_vec.push(cal_result.volatility.to_f64().unwrap_or(0.0));
                    kappa_vec.push(cal_result.kappa.to_f64().unwrap_or(0.0));
                    a_vec.push(cal_result.a.to_f64().unwrap_or(0.0));
                    gamma_vec.push(optimal.gamma.to_f64().unwrap_or(0.0));

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

            // Collect statistics for final calculation
            total_spread_bps_vec.push(total_spread_bps.to_f64().unwrap_or(0.0));
            bid_spread_bps_vec.push(bid_spread_bps.to_f64().unwrap_or(0.0));
            ask_spread_bps_vec.push(ask_spread_bps.to_f64().unwrap_or(0.0));
            volatility_vec.push(cal_result.volatility.to_f64().unwrap_or(0.0));
            kappa_vec.push(cal_result.kappa.to_f64().unwrap_or(0.0));
            a_vec.push(cal_result.a.to_f64().unwrap_or(0.0));
            gamma_vec.push(optimal.gamma.to_f64().unwrap_or(0.0));

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

    println!("{:-<155}", "");
    println!("Done! Results written to data/eth_usd/as_results.csv");

    // Display Statistics
    if !total_spread_bps_vec.is_empty() {
        println!("\n{:=<80}", "");
        println!("STATISTICS SUMMARY (n = {})", total_spread_bps_vec.len());
        println!("{:=<80}", "");

        println!("\n{:<20} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10}",
                 "Metric", "Mean", "Median", "Std Dev", "Min", "Max");
        println!("{:-<80}", "");

        print_stats("Total Spread (bps)", &total_spread_bps_vec);
        print_stats("Bid Spread (bps)", &bid_spread_bps_vec);
        print_stats("Ask Spread (bps)", &ask_spread_bps_vec);
        print_stats("Volatility", &volatility_vec);
        print_stats("Kappa", &kappa_vec);
        print_stats("A Parameter", &a_vec);
        print_stats("Gamma", &gamma_vec);

        println!("{:=<80}", "");
    }

    Ok(())
}

fn print_stats(label: &str, data: &[f64]) {
    if data.is_empty() {
        return;
    }

    let mean = data.iter().sum::<f64>() / data.len() as f64;

    let mut sorted = data.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let median = if sorted.len() % 2 == 0 {
        (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
    } else {
        sorted[sorted.len() / 2]
    };

    let variance = data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / data.len() as f64;
    let std_dev = variance.sqrt();

    let min = sorted.first().unwrap();
    let max = sorted.last().unwrap();

    println!("{:<20} | {:>10.4} | {:>10.4} | {:>10.4} | {:>10.4} | {:>10.4}",
             label, mean, median, std_dev, min, max);
}
