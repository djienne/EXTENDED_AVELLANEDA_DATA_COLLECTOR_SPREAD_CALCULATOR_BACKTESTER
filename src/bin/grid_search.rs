/// Grid search over time horizons for AS market-making backtest
///
/// Tests multiple inventory_horizon_seconds values to find the optimal horizon
/// for the given market conditions and parameters.

use extended_data_collector::backtest_engine::{run_backtest, BacktestParams, BacktestResults};
use extended_data_collector::data_loader::{DataLoader, DataEvent};
use extended_data_collector::model_types::ASConfig;
use rayon::prelude::*;
use rust_decimal::Decimal;
use std::env;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_CONFIG_PATH: &str = "config.json";
const DEFAULT_TRADES_PATH: &str = "data/eth_usd/trades.csv";
const DEFAULT_ORDERBOOK_PATH: &str = "data/eth_usd/orderbook_parts";
const DEFAULT_OUTPUT_PATH: &str = "data/eth_usd/grid_search_results.csv";

const DEFAULT_INITIAL_CAPITAL: i64 = 1000;
const DEFAULT_ORDER_NOTIONAL: i64 = 20;
const DEFAULT_MIN_FILLS: u64 = 5;

/// Default horizons to test (seconds): 1m, 5m, 15m, 30m, 1h, 2h, 3h, 4h
const DEFAULT_HORIZONS: &[u64] = &[60, 300, 900, 1800, 3600, 7200, 10800, 14400];

// =============================================================================
// Helper Functions
// =============================================================================

fn format_duration(seconds: u64) -> String {
    if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3600 {
        format!("{}m", seconds / 60)
    } else {
        let hours = seconds / 3600;
        let mins = (seconds % 3600) / 60;
        if mins > 0 {
            format!("{}h{}m", hours, mins)
        } else {
            format!("{}h", hours)
        }
    }
}

fn print_usage(program: &str) {
    eprintln!("Usage: {} [OPTIONS]", program);
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --config <path>       Path to config file (default: {})", DEFAULT_CONFIG_PATH);
    eprintln!("  --trades <path>       Path to trades CSV (default: {})", DEFAULT_TRADES_PATH);
    eprintln!("  --orderbook <path>    Path to orderbook directory (default: {})", DEFAULT_ORDERBOOK_PATH);
    eprintln!("  --output <path>       Path to output CSV (default: {})", DEFAULT_OUTPUT_PATH);
    eprintln!("  --capital <amount>    Initial capital in dollars (default: {})", DEFAULT_INITIAL_CAPITAL);
    eprintln!("  --notional <amount>   Order notional in dollars (default: {})", DEFAULT_ORDER_NOTIONAL);
    eprintln!("  --min-fills <n>       Minimum fills to include in results (default: {})", DEFAULT_MIN_FILLS);
    eprintln!("  --horizons <list>     Comma-separated list of horizons in seconds");
    eprintln!("                        (default: {})", DEFAULT_HORIZONS.iter()
        .map(|h| h.to_string()).collect::<Vec<_>>().join(","));
    eprintln!("  --parallel            Run backtests in parallel");
    eprintln!("  --help                Show this help message");
}

fn parse_horizons(s: &str) -> Result<Vec<u64>, String> {
    s.split(',')
        .map(|part| {
            part.trim()
                .parse::<u64>()
                .map_err(|e| format!("Invalid horizon '{}': {}", part, e))
        })
        .collect()
}

// =============================================================================
// Main
// =============================================================================

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    // Parse command-line arguments
    let mut config_path = DEFAULT_CONFIG_PATH.to_string();
    let mut trades_path = DEFAULT_TRADES_PATH.to_string();
    let mut orderbook_path = DEFAULT_ORDERBOOK_PATH.to_string();
    let mut output_path = DEFAULT_OUTPUT_PATH.to_string();
    let mut initial_capital = DEFAULT_INITIAL_CAPITAL;
    let mut order_notional = DEFAULT_ORDER_NOTIONAL;
    let mut min_fills = DEFAULT_MIN_FILLS;
    let mut horizons: Vec<u64> = DEFAULT_HORIZONS.to_vec();
    let mut parallel = false;

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
            "--capital" => {
                i += 1;
                initial_capital = args.get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(DEFAULT_INITIAL_CAPITAL);
            }
            "--notional" => {
                i += 1;
                order_notional = args.get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(DEFAULT_ORDER_NOTIONAL);
            }
            "--min-fills" => {
                i += 1;
                min_fills = args.get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(DEFAULT_MIN_FILLS);
            }
            "--horizons" => {
                i += 1;
                if let Some(s) = args.get(i) {
                    horizons = parse_horizons(s)?;
                }
            }
            "--parallel" => {
                parallel = true;
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

    println!("============================================================");
    println!("       AS Market-Making Strategy - Grid Search");
    println!("============================================================\n");

    // Load configuration (as base config)
    let base_config = match std::fs::read_to_string(&config_path) {
        Ok(contents) => {
            match serde_json::from_str::<ASConfig>(&contents) {
                Ok(cfg) => {
                    println!("[OK] Loaded base config from {}", config_path);
                    cfg
                }
                Err(e) => {
                    eprintln!("[WARN] Error parsing {}: {}. Using defaults.", config_path, e);
                    ASConfig::default()
                }
            }
        }
        Err(_) => {
            println!("[WARN] {} not found. Using defaults.", config_path);
            ASConfig::default()
        }
    };

    // Load data loader
    println!("Initializing data loader...");
    let loader = DataLoader::new(
        Path::new(&trades_path),
        Path::new(&orderbook_path),
    );

    println!("Loading all data into memory for performance...");
    let start_load = Instant::now();
    let events = loader.load_all_events()?;
    let duration = start_load.elapsed();
    println!("Loaded {} events in {:.2}s", events.len(), duration.as_secs_f64());

    println!("Testing {} time horizons...", horizons.len());
    println!(
        "Horizons: {}\n",
        horizons
            .iter()
            .map(|h| format_duration(*h))
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Backtest parameters
    let initial_capital_dec = Decimal::from(initial_capital);
    let order_notional_dec = Decimal::from(order_notional);

    // Run backtests
    let results: Vec<(u64, Result<BacktestResults, String>)> = if parallel {
        println!("Running in parallel mode...\n");
        run_parallel_backtests(
            &horizons,
            &base_config,
            &events,
            initial_capital_dec,
            order_notional_dec,
        )
    } else {
        run_sequential_backtests(
            &horizons,
            &base_config,
            &events,
            initial_capital_dec,
            order_notional_dec,
        )?
    };

    // Collect successful results
    let successful_results: Vec<(u64, BacktestResults)> = results
        .into_iter()
        .filter_map(|(h, r)| match r {
            Ok(result) => Some((h, result)),
            Err(e) => {
                eprintln!("[ERROR] Horizon {}: {}", format_duration(h), e);
                None
            }
        })
        .collect();

    // Print and save results
    print_results_summary(&successful_results, min_fills);
    save_results_to_csv(&successful_results, &output_path)?;

    println!("\nResults saved to {}", output_path);

    Ok(())
}

// =============================================================================
// Backtest Runners
// =============================================================================

fn run_sequential_backtests(
    horizons: &[u64],
    base_config: &ASConfig,
    events: &[DataEvent],
    initial_capital: Decimal,
    order_notional: Decimal,
) -> io::Result<Vec<(u64, Result<BacktestResults, String>)>> {
    let mut results = Vec::with_capacity(horizons.len());

    for (i, &horizon) in horizons.iter().enumerate() {
        print!(
            "[{}/{}] Testing horizon={}... ",
            i + 1,
            horizons.len(),
            format_duration(horizon)
        );
        io::stdout().flush()?;

        let mut config = base_config.clone();
        config.inventory_horizon_seconds = horizon;

        let data_stream = events.iter().cloned().map(Ok);
        let params = BacktestParams {
            data_stream,
            config,
            initial_capital,
            order_notional,
            output_csv_path: None,
            verbose: false,
        };

        let result = match run_backtest(params) {
            Ok(result) => {
                println!(
                    "[OK] PnL: ${:.2}, Fills: {}, Vol: ${:.2}",
                    result.final_pnl,
                    result.total_fills(),
                    result.total_notional_volume
                );
                Ok(result)
            }
            Err(e) => {
                println!("[FAIL] Error: {}", e);
                Err(e.to_string())
            }
        };

        results.push((horizon, result));
    }

    Ok(results)
}

fn run_parallel_backtests(
    horizons: &[u64],
    base_config: &ASConfig,
    events: &[DataEvent],
    initial_capital: Decimal,
    order_notional: Decimal,
) -> Vec<(u64, Result<BacktestResults, String>)> {
    let progress = Arc::new(Mutex::new(0usize));
    let total = horizons.len();

    // Note: We don't clone the entire events vector, just the slice reference is passed
    // But par_iter works on horizons. Inside the closure, we need to iterate over events.
    // events is a &Vec<DataEvent>. We can read it from multiple threads safely.

    horizons
        .par_iter()
        .map(|&horizon| {
            let mut config = base_config.clone();
            config.inventory_horizon_seconds = horizon;

            let data_stream = events.iter().cloned().map(Ok);
            let params = BacktestParams {
                data_stream,
                config,
                initial_capital,
                order_notional,
                output_csv_path: None,
                verbose: false,
            };

            let result = run_backtest(params).map_err(|e| e.to_string());

            // Update progress
            let mut prog = progress.lock().unwrap();
            *prog += 1;
            let status = if result.is_ok() { "OK" } else { "FAIL" };
            println!(
                "[{}/{}] Horizon {} - {}",
                *prog,
                total,
                format_duration(horizon),
                status
            );

            (horizon, result)
        })
        .collect()
}

// =============================================================================
// Results Display
// =============================================================================

fn print_results_summary(results: &[(u64, BacktestResults)], min_fills: u64) {
    println!("\n============================================================");
    println!("                    RESULTS SUMMARY");
    println!("============================================================\n");

    // Filter results with minimum fills
    let filtered_results: Vec<_> = results
        .iter()
        .filter(|(_, r)| r.total_fills() >= min_fills)
        .collect();

    if filtered_results.is_empty() {
        println!(
            "[WARN] No results with at least {} fills. Showing all results.\n",
            min_fills
        );
    } else {
        println!("Showing results with at least {} fills:\n", min_fills);
    }

    // Sort by best PnL (descending)
    let mut sorted_results = if filtered_results.is_empty() {
        results.iter().collect::<Vec<_>>()
    } else {
        filtered_results
    };
    sorted_results.sort_by(|a, b| b.1.final_pnl.cmp(&a.1.final_pnl));

    // Display table header
    println!(
        "{:<10} | {:>12} | {:>10} | {:>8} | {:>8} | {:>10} | {:>12} | {:>12}",
        "Horizon", "Final PnL", "Return %", "Bid Fill", "Ask Fill", "Tot Fills", "Volume", "Notional"
    );
    println!("{:-<10}-+-{:-<12}-+-{:-<10}-+-{:-<8}-+-{:-<8}-+-{:-<10}-+-{:-<12}-+-{:-<12}",
        "", "", "", "", "", "", "", "");

    // Display rows
    for (rank, (horizon, result)) in sorted_results.iter().enumerate() {
        let marker = if rank == 0 { "*" } else { " " };
        println!(
            "{} {:<8} | {:>12.2} | {:>10.2} | {:>8} | {:>8} | {:>10} | {:>12.4} | {:>12.2}",
            marker,
            format_duration(*horizon),
            result.final_pnl,
            result.total_return_pct,
            result.bid_fills,
            result.ask_fills,
            result.total_fills(),
            result.total_volume,
            result.total_notional_volume
        );
    }

    println!("\n============================================================");

    // Best result info
    if let Some((best_horizon, best_result)) = sorted_results.first() {
        println!("\n[BEST] Best Configuration:");
        println!(
            "   Time Horizon: {} ({}s)",
            format_duration(*best_horizon),
            best_horizon
        );
        println!("   Final PnL: ${:.2}", best_result.final_pnl);
        println!("   Return: {:.2}%", best_result.total_return_pct);
        println!(
            "   Total Fills: {} ({} bid, {} ask)",
            best_result.total_fills(),
            best_result.bid_fills,
            best_result.ask_fills
        );
        println!(
            "   Total Volume: {:.4} units (${:.2})",
            best_result.total_volume, best_result.total_notional_volume
        );
    }
}

fn save_results_to_csv(
    results: &[(u64, BacktestResults)],
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(path)?;

    // Write header
    writeln!(
        file,
        "horizon_seconds,horizon_formatted,initial_capital,final_pnl,total_return_pct,bid_fills,ask_fills,total_fills,total_volume,total_notional_volume,final_inventory,final_cash"
    )?;

    // Write rows
    for (horizon, result) in results {
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{},{},{},{}",
            horizon,
            format_duration(*horizon),
            result.initial_capital,
            result.final_pnl,
            result.total_return_pct,
            result.bid_fills,
            result.ask_fills,
            result.total_fills(),
            result.total_volume,
            result.total_notional_volume,
            result.final_inventory,
            result.final_cash
        )?;
    }

    Ok(())
}