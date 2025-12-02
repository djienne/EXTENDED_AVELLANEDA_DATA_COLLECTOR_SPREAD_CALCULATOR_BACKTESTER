use extended_data_collector::model_types::ASConfig;
use extended_data_collector::data_loader::DataLoader;
use extended_data_collector::metrics::calculate_effective_price;
use extended_data_collector::calibration_engine::CalibrationEngine;
use extended_data_collector::spread_model::compute_optimal_quote;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::path::Path;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::env;

/// Default paths
const DEFAULT_CONFIG_PATH: &str = "config.json";
const DEFAULT_TRADES_PATH: &str = "data/eth_usd/trades_parts";
const DEFAULT_ORDERBOOK_PATH: &str = "data/eth_usd/orderbook_parts";
const DEFAULT_OUTPUT_PATH: &str = "data/eth_usd/as_results.csv";

fn print_usage(program: &str) {
    eprintln!("Usage: {} [OPTIONS]", program);
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --config <path>      Path to config file (default: {})", DEFAULT_CONFIG_PATH);
    eprintln!("  --trades <path>      Path to trades CSV (default: {})", DEFAULT_TRADES_PATH);
    eprintln!("  --orderbook <path>   Path to orderbook directory (default: {})", DEFAULT_ORDERBOOK_PATH);
    eprintln!("  --output <path>      Path to output CSV (default: {})", DEFAULT_OUTPUT_PATH);
    eprintln!("  --help               Show this help message");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    // Parse command-line arguments
    let mut config_path = DEFAULT_CONFIG_PATH.to_string();
    let mut trades_path = DEFAULT_TRADES_PATH.to_string();
    let mut orderbook_path = DEFAULT_ORDERBOOK_PATH.to_string();
    let mut output_path = DEFAULT_OUTPUT_PATH.to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                i += 1;
                config_path = args.get(i).cloned().unwrap_or_default();
            }
            "--trades" => {
                i += 1;
                trades_path = args.get(i).cloned().unwrap_or_default();
            }
            "--orderbook" => {
                i += 1;
                orderbook_path = args.get(i).cloned().unwrap_or_default();
            }
            "--output" => {
                i += 1;
                output_path = args.get(i).cloned().unwrap_or_default();
            }
            "--help" | "-h" => {
                print_usage(&args[0]);
                return Ok(());
            }
            _ => {
                eprintln!("Unknown argument: {}", args[i]);
                print_usage(&args[0]);
                return Err("Invalid arguments".into());
            }
        }
        i += 1;
    }

    // 1. Load Config
    let config = match std::fs::read_to_string(&config_path) {
        Ok(contents) => {
            match serde_json::from_str::<ASConfig>(&contents) {
                Ok(cfg) => {
                    println!("Loaded config from {}", config_path);
                    cfg
                }
                Err(e) => {
                    eprintln!("Error parsing {}: {}. Using defaults.", config_path, e);
                    ASConfig::default()
                }
            }
        }
        Err(_) => {
            println!("{} not found. Using defaults.", config_path);
            ASConfig::default()
        }
    };
    println!("Using Config: {:?}", config);

    // 2. Load Data
    println!("Loading data...");
    let loader = DataLoader::new(
        Path::new(&trades_path),
        Path::new(&orderbook_path),
    );
    let all_trades = loader.get_trades()?;
    println!("Loaded {} trades", all_trades.len());

    // 3. Initialize Calibration Engine
    let mut calibration_engine = CalibrationEngine::new(&config);

    // 4. Prepare Output with buffered writer
    let file = File::create(&output_path)?;
    let mut output_file = BufWriter::new(file);
    writeln!(output_file, "timestamp,datetime,mid_price,volatility,bid_kappa,ask_kappa,bid_a,ask_a,gamma,optimal_spread_bps,bid_spread_bps,ask_spread_bps,bid_price,ask_price,reservation_price")?;

    // Print Header to Terminal
    println!("{:-<155}", "");
    println!(
        "{:<15} | {:>12} | {:>10} | {:>8} | {:>8} | {:>6} | {:>12} | {:>12} | {:>12} | {:>12} | {:>12}",
        "Timestamp", "Mid Price", "Volatility", "Kappa", "A", "Gamma", "Total(bps)", "Bid(bps)", "Ask(bps)", "Bid", "Ask"
    );
    println!("{:-<155}", "");

    // 5. State tracking
    let mut trade_idx = 0;
    let mut last_quote: Option<extended_data_collector::model_types::EffectiveQuote> = None;
    let mut last_ts: u64 = 0;

    // Collect statistics
    let mut stats = SpreadStats::new();

    // Precompute constant
    let bps_multiplier = Decimal::from(10000);

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
            calibration_engine.add_orderbook(&snapshot, quote.mid);

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
                    let optimal = compute_optimal_quote(
                        current_ts,
                        quote.mid,
                        Decimal::ZERO,
                        cal_result.volatility,
                        cal_result.bid_kappa,
                        cal_result.ask_kappa,
                        &config,
                    );

                    // Calculate spread metrics with division-by-zero guard
                    let reservation_price = optimal.reservation_price;
                    
                    if reservation_price > Decimal::ZERO {
                        let spread = optimal.optimal_spread;
                        let bid_price = optimal.bid_price;
                        let ask_price = optimal.ask_price;

                        let total_spread_bps = (spread / reservation_price) * bps_multiplier;
                        let bid_spread_bps = ((reservation_price - bid_price) / reservation_price) * bps_multiplier;
                        let ask_spread_bps = ((ask_price - reservation_price) / reservation_price) * bps_multiplier;

                        // Collect statistics (these are already f64, no conversion needed)
                        stats.add_sample(
                            total_spread_bps.to_f64().unwrap_or(0.0),
                            bid_spread_bps.to_f64().unwrap_or(0.0),
                            ask_spread_bps.to_f64().unwrap_or(0.0),
                            cal_result.volatility,
                            cal_result.bid_kappa,
                            cal_result.ask_kappa,
                            cal_result.bid_a,
                            cal_result.ask_a,
                            optimal.gamma,
                        );

                        // Output to CSV
                        writeln!(
                            output_file,
                            "{},{},{},{:.6},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2}",
                            current_ts,
                            format_timestamp(current_ts),
                            quote.mid,
                            cal_result.volatility,
                            cal_result.bid_kappa,
                            cal_result.ask_kappa,
                            cal_result.bid_a,
                            cal_result.ask_a,
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
                            "{:<15} | {:>12.2} | {:>10.6} | {:>8.2} | {:>8.2} | {:>8.2} | {:>8.2} | {:>6.2} | {:>12.2} | {:>12.2} | {:>12.2} | {:>12.2} | {:>12.2}",
                            current_ts, quote.mid, cal_result.volatility, cal_result.bid_kappa, cal_result.ask_kappa,
                            cal_result.bid_a, cal_result.ask_a, optimal.gamma, total_spread_bps, bid_spread_bps, ask_spread_bps, bid_price, ask_price
                        );
                    }
                }
            }
        }
    }

    // 7. Final Output (for partial window at end)
    if let Some(quote) = last_quote {
        if let Some(cal_result) = calibration_engine.calibrate(last_ts, config.tick_size) {
            let optimal = compute_optimal_quote(
                last_ts,
                quote.mid,
                Decimal::ZERO,
                cal_result.volatility,
                cal_result.bid_kappa,
                cal_result.ask_kappa,
                &config,
            );

            let reservation_price = optimal.reservation_price;
            
            if reservation_price > Decimal::ZERO {
                let spread = optimal.optimal_spread;
                let bid_price = optimal.bid_price;
                let ask_price = optimal.ask_price;

                let total_spread_bps = (spread / reservation_price) * bps_multiplier;
                let bid_spread_bps = ((reservation_price - bid_price) / reservation_price) * bps_multiplier;
                let ask_spread_bps = ((ask_price - reservation_price) / reservation_price) * bps_multiplier;

                stats.add_sample(
                    total_spread_bps.to_f64().unwrap_or(0.0),
                    bid_spread_bps.to_f64().unwrap_or(0.0),
                    ask_spread_bps.to_f64().unwrap_or(0.0),
                    cal_result.volatility,
                    cal_result.bid_kappa,
                    cal_result.ask_kappa,
                    cal_result.bid_a,
                    cal_result.ask_a,
                    optimal.gamma,
                );

                writeln!(
                    output_file,
                    "{},{},{},{:.6},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2}",
                    last_ts,
                    format_timestamp(last_ts),
                    quote.mid,
                    cal_result.volatility,
                    cal_result.bid_kappa,
                    cal_result.ask_kappa,
                    cal_result.bid_a,
                    cal_result.ask_a,
                    optimal.gamma,
                    total_spread_bps,
                    bid_spread_bps,
                    ask_spread_bps,
                    bid_price,
                    ask_price,
                    reservation_price
                )?;

                println!(
                    "{:<15} | {:>12.2} | {:>10.6} | {:>8.2} | {:>8.2} | {:>8.2} | {:>8.2} | {:>6.2} | {:>12.2} | {:>12.2} | {:>12.2} | {:>12.2} | {:>12.2}",
                    last_ts, quote.mid, cal_result.volatility, cal_result.bid_kappa, cal_result.ask_kappa,
                    cal_result.bid_a, cal_result.ask_a, optimal.gamma, total_spread_bps, bid_spread_bps, ask_spread_bps, bid_price, ask_price
                );
            }
        }
    }

    // Flush the buffer
    output_file.flush()?;

    println!("{:-<155}", "");
    println!("Done! Results written to {}", output_path);

    // Display Statistics
    stats.print_summary();

    Ok(())
}

/// Format timestamp as human-readable string
fn format_timestamp(timestamp_ms: u64) -> String {
    use chrono::{DateTime, Utc};
    let seconds = (timestamp_ms / 1000) as i64;
    let nanos = ((timestamp_ms % 1000) * 1_000_000) as u32;

    match DateTime::<Utc>::from_timestamp(seconds, nanos) {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        None => "N/A".to_string(),
    }
}

/// Statistics collector for spread metrics
struct SpreadStats {
    total_spread_bps: Vec<f64>,
    bid_spread_bps: Vec<f64>,
    ask_spread_bps: Vec<f64>,
    volatility: Vec<f64>,
    bid_kappa: Vec<f64>,
    ask_kappa: Vec<f64>,
    bid_a: Vec<f64>,
    ask_a: Vec<f64>,
    gamma: Vec<f64>,
}

impl SpreadStats {
    fn new() -> Self {
        Self {
            total_spread_bps: Vec::new(),
            bid_spread_bps: Vec::new(),
            ask_spread_bps: Vec::new(),
            volatility: Vec::new(),
            bid_kappa: Vec::new(),
            ask_kappa: Vec::new(),
            bid_a: Vec::new(),
            ask_a: Vec::new(),
            gamma: Vec::new(),
        }
    }

    fn add_sample(
        &mut self,
        total_spread: f64,
        bid_spread: f64,
        ask_spread: f64,
        vol: f64,
        bid_kappa: f64,
        ask_kappa: f64,
        bid_a: f64,
        ask_a: f64,
        gamma: f64,
    ) {
        self.total_spread_bps.push(total_spread);
        self.bid_spread_bps.push(bid_spread);
        self.ask_spread_bps.push(ask_spread);
        self.volatility.push(vol);
        self.bid_kappa.push(bid_kappa);
        self.ask_kappa.push(ask_kappa);
        self.bid_a.push(bid_a);
        self.ask_a.push(ask_a);
        self.gamma.push(gamma);
    }

    fn print_summary(&self) {
        if self.total_spread_bps.is_empty() {
            println!("\nNo data collected for statistics.");
            return;
        }

        println!("\n{:=<80}", "");
        println!("STATISTICS SUMMARY (n = {})", self.total_spread_bps.len());
        println!("{:=<80}", "");

        println!(
            "\n{:<20} | {:>10} | {:>10} | {:>10} | {:>10} | {:>10}",
            "Metric", "Mean", "Median", "Std Dev", "Min", "Max"
        );
        println!("{:-<80}", "");

        Self::print_stats_row("Total Spread (bps)", &self.total_spread_bps);
        Self::print_stats_row("Bid Spread (bps)", &self.bid_spread_bps);
        Self::print_stats_row("Ask Spread (bps)", &self.ask_spread_bps);
        Self::print_stats_row("Volatility", &self.volatility);
        Self::print_stats_row("Bid Kappa", &self.bid_kappa);
        Self::print_stats_row("Ask Kappa", &self.ask_kappa);
        Self::print_stats_row("Bid A Parameter", &self.bid_a);
        Self::print_stats_row("Ask A Parameter", &self.ask_a);
        Self::print_stats_row("Gamma", &self.gamma);

        println!("{:=<80}", "");
    }

    fn print_stats_row(label: &str, data: &[f64]) {
        if data.is_empty() {
            return;
        }

        let n = data.len() as f64;
        let mean = data.iter().sum::<f64>() / n;

        let mut sorted = data.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let median = if sorted.len() % 2 == 0 {
            (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
        } else {
            sorted[sorted.len() / 2]
        };

        // Sample standard deviation (n-1 denominator)
        let variance = if data.len() > 1 {
            data.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (data.len() - 1) as f64
        } else {
            0.0
        };
        let std_dev = variance.sqrt();

        let min = sorted.first().copied().unwrap_or(0.0);
        let max = sorted.last().copied().unwrap_or(0.0);

        println!(
            "{:<20} | {:>10.4} | {:>10.4} | {:>10.4} | {:>10.4} | {:>10.4}",
            label, mean, median, std_dev, min, max
        );
    }
}
