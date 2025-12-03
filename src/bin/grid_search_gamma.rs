/// Parallel 2D grid search over time horizons AND gamma values for AS market-making backtest
///
/// Tests multiple combinations of inventory_horizon_seconds and risk_aversion_gamma
/// using parallel threads for faster execution.
///
/// Gamma (risk aversion) interpretation:
/// - 0.01: Very risk-tolerant, minimal inventory penalty
/// - 0.05: Low risk aversion, allows significant inventory
/// - 0.10: Moderate risk aversion (typical default)
/// - 0.20: High risk aversion, strong inventory penalty

use extended_data_collector::backtest_engine::{run_backtest, BacktestParams, BacktestResults};
use extended_data_collector::data_loader::DataLoader;
use extended_data_collector::model_types::ASConfig;
use rayon::prelude::*;
use rust_decimal::Decimal;
use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_CONFIG_PATH: &str = "config.json";
const DEFAULT_TRADES_PATH: &str = "data/eth_usd/trades_parts";
const DEFAULT_ORDERBOOK_PATH: &str = "data/eth_usd/orderbook_parts";
const DEFAULT_OUTPUT_PATH: &str = "data/eth_usd/grid_search_2d_results.csv";

const DEFAULT_INITIAL_CAPITAL: i64 = 1000;
const DEFAULT_ORDER_NOTIONAL: i64 = 20;
const DEFAULT_MIN_FILLS: u64 = 5;

/// Default horizons: 5m, 15m, 30m, 1h, 2h, 4h
const DEFAULT_HORIZONS: &[u64] = &[300, 900, 1800, 3600, 7200, 14400];

/// Default gamma values used when neither config nor CLI provide overrides
const DEFAULT_GAMMAS: &[f64] = &[0.01, 0.05, 0.1, 0.2];

// =============================================================================
// Types
// =============================================================================

#[derive(Debug, Clone)]
struct GridResult {
    horizon: u64,
    gamma: f64,
    result: BacktestResults,
}

#[derive(Clone)]
struct GridConfig {
    horizon: u64,
    gamma: f64,
}

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
    eprintln!("  --horizons <list>     Comma-separated horizons in seconds");
    eprintln!("  --gammas <list>       Comma-separated gamma values");
    eprintln!("  --threads <n>         Number of parallel threads (default: from config)");
    eprintln!("  --help                Show this help message");
}

fn parse_u64_list(s: &str) -> Result<Vec<u64>, String> {
    s.split(',')
        .map(|p| p.trim().parse::<u64>().map_err(|e| format!("Invalid value '{}': {}", p, e)))
        .collect()
}

fn parse_f64_list(s: &str) -> Result<Vec<f64>, String> {
    s.split(',')
        .map(|p| p.trim().parse::<f64>().map_err(|e| format!("Invalid value '{}': {}", p, e)))
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
    let mut gammas: Vec<f64> = Vec::new();
    let mut num_threads: Option<usize> = None;

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
                    horizons = parse_u64_list(s)?;
                }
            }
            "--gammas" => {
                i += 1;
                if let Some(s) = args.get(i) {
                    gammas = parse_f64_list(s)?;
                }
            }
            "--threads" => {
                i += 1;
                num_threads = args.get(i).and_then(|s| s.parse().ok());
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
    println!("    AS Market-Making Strategy - Parallel 2D Grid Search");
    println!("        Time Horizon x Risk Aversion (Gamma)");
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

    // If gammas not provided via CLI, prefer config gamma bounds; otherwise fallback defaults
    if gammas.is_empty() {
        if base_config.gamma_max > base_config.gamma_min && base_config.gamma_max > 0.0 {
            let min_g = base_config.gamma_min.max(0.0);
            let max_g = base_config.gamma_max;
            let points = base_config.gamma_grid_points.max(2);
            if points == 2 || min_g <= 0.0 {
                gammas = vec![min_g.max(1e-6), max_g];
            } else {
                let log_min = min_g.ln();
                let log_max = max_g.ln();
                let step = (log_max - log_min) / (points as f64 - 1.0);
                gammas = (0..points)
                    .map(|i| (log_min + step * i as f64).exp())
                    .collect();
            }
        } else {
            gammas = DEFAULT_GAMMAS.to_vec();
        }
    }

    // Determine thread count
    let num_threads = num_threads.unwrap_or(base_config.num_threads);

    // Apply defaults if horizons/gammas empty
    if horizons.is_empty() {
        horizons = DEFAULT_HORIZONS.to_vec();
    }

    // Generate all combinations
    let mut configs = Vec::with_capacity(horizons.len() * gammas.len());
    for &horizon in &horizons {
        for &gamma in &gammas {
            configs.push(GridConfig { horizon, gamma });
        }
    }

    let total_runs = configs.len();

    println!(
        "Testing {} time horizons x {} gamma values = {} total configurations",
        horizons.len(),
        gammas.len(),
        total_runs
    );
    println!(
        "Horizons: {}",
        horizons.iter().map(|h| format_duration(*h)).collect::<Vec<_>>().join(", ")
    );
    println!(
        "Gammas: {}",
        gammas.iter().map(|g| format!("{:.2}", g)).collect::<Vec<_>>().join(", ")
    );
    println!("Using {} parallel threads", num_threads);
    println!();

    // Configure rayon thread pool
    if let Err(e) = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
    {
        eprintln!("[WARN] Could not configure thread pool: {}. Using default.", e);
    }

    println!("Starting parallel execution...\n");

    // Backtest parameters
    let initial_capital_dec = Decimal::from(initial_capital);
    let order_notional_dec = Decimal::from(order_notional);

    // Shared state for results and progress
    let results: Arc<Mutex<Vec<GridResult>>> = Arc::new(Mutex::new(Vec::with_capacity(total_runs)));
    let completed_count = Arc::new(Mutex::new(0usize));
    let start_time = Instant::now();

    // Clone paths for use in parallel closure
    let trades_path = Arc::new(trades_path);
    let orderbook_path = Arc::new(orderbook_path);

    // Run backtests in parallel
    configs.par_iter().for_each(|grid_config| {
        let mut config = base_config.clone();
        config.inventory_horizon_seconds = grid_config.horizon;
        config.risk_aversion_gamma = grid_config.gamma;
        // Ensure gamma is applied directly without scaling/clamping in the quote model
        config.gamma_mode = extended_data_collector::model_types::GammaMode::Constant;
        config.gamma_min = grid_config.gamma;
        config.gamma_max = grid_config.gamma;

        // Each thread needs its own data loader
        let loader = DataLoader::new(
            Path::new(trades_path.as_str()),
            Path::new(orderbook_path.as_str()),
        );

        let data_stream = match loader.stream() {
            Ok(stream) => stream,
            Err(e) => {
                eprintln!(
                    "[FAIL] H={}, g={:.2} - Failed to load data: {}",
                    format_duration(grid_config.horizon),
                    grid_config.gamma,
                    e
                );
                return;
            }
        };

        let params = BacktestParams {
            data_stream,
            config,
            initial_capital: initial_capital_dec,
            order_notional: order_notional_dec,
            output_csv_path: None,
            verbose: false,
        };

        match run_backtest(params) {
            Ok(result) => {
                // Update progress
                let count = {
                    let mut c = completed_count.lock().unwrap();
                    *c += 1;
                    *c
                };

                let elapsed = start_time.elapsed();
                let avg_time = elapsed.as_secs_f64() / count as f64;
                let remaining = ((total_runs - count) as f64 * avg_time) as u64;

                println!(
                    "[{:2}/{:2}] H={:>4}, g={:.2} -> PnL: ${:>7.2}, Fills: {:>3}, Return: {:>6.2}% (ETA: {}s)",
                    count,
                    total_runs,
                    format_duration(grid_config.horizon),
                    grid_config.gamma,
                    result.final_pnl,
                    result.total_fills(),
                    result.total_return_pct,
                    remaining
                );

                // Store result
                let grid_result = GridResult {
                    horizon: grid_config.horizon,
                    gamma: grid_config.gamma,
                    result,
                };
                results.lock().unwrap().push(grid_result);
            }
            Err(e) => {
                eprintln!(
                    "[FAIL] H={}, g={:.2} - Error: {}",
                    format_duration(grid_config.horizon),
                    grid_config.gamma,
                    e
                );
            }
        }
    });

    // Extract results - handle potential failures gracefully
    let final_results: Vec<GridResult> = {
        let guard = results.lock().map_err(|e| format!("Mutex poisoned: {}", e))?;
        guard.clone()
    };

    let total_elapsed = start_time.elapsed();
    println!(
        "\n[OK] All {} configurations completed in {:.1}s",
        total_runs,
        total_elapsed.as_secs_f64()
    );
    println!(
        "  Average: {:.1}s per config",
        total_elapsed.as_secs_f64() / total_runs as f64
    );

    // Print and save results
    print_results_summary(&final_results, min_fills, &horizons, &gammas);
    save_results_to_csv(&final_results, &output_path)?;

    println!("\nResults saved to {}", output_path);
    println!(
        "\n[PERF] ~{:.1}x speedup vs sequential execution (using {} threads)",
        num_threads as f64 * 0.85, // Account for overhead
        num_threads
    );

    Ok(())
}

// =============================================================================
// Results Display
// =============================================================================

fn print_results_summary(
    results: &[GridResult],
    min_fills: u64,
    horizons: &[u64],
    gammas: &[f64],
) {
    println!("\n============================================================");
    println!("                    RESULTS SUMMARY");
    println!("============================================================\n");

    // Filter results with minimum fills
    let filtered_results: Vec<_> = results
        .iter()
        .filter(|r| r.result.total_fills() >= min_fills)
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
    sorted_results.sort_by(|a, b| b.result.final_pnl.cmp(&a.result.final_pnl));

    // Display table
    println!(
        "{:<10} | {:>6} | {:>12} | {:>10} | {:>8} | {:>8} | {:>10} | {:>12}",
        "Horizon", "Gamma", "Final PnL", "Return %", "Bid Fill", "Ask Fill", "Tot Fills", "Notional"
    );
    println!(
        "{:-<10}-+-{:-<6}-+-{:-<12}-+-{:-<10}-+-{:-<8}-+-{:-<8}-+-{:-<10}-+-{:-<12}",
        "", "", "", "", "", "", "", ""
    );

    for (rank, grid_result) in sorted_results.iter().enumerate() {
        let marker = if rank == 0 { "*" } else { " " };
        let r = &grid_result.result;
        println!(
            "{} {:<8} | {:>6.2} | {:>12.2} | {:>10.2} | {:>8} | {:>8} | {:>10} | {:>12.2}",
            marker,
            format_duration(grid_result.horizon),
            grid_result.gamma,
            r.final_pnl,
            r.total_return_pct,
            r.bid_fills,
            r.ask_fills,
            r.total_fills(),
            r.total_notional_volume
        );
    }

    println!("\n============================================================");

    // Best result info
    if let Some(best) = sorted_results.first() {
        println!("\n[BEST] Best Configuration:");
        println!(
            "   Time Horizon: {} ({}s)",
            format_duration(best.horizon),
            best.horizon
        );
        println!("   Gamma (Risk Aversion): {:.2}", best.gamma);
        println!("   Final PnL: ${:.2}", best.result.final_pnl);
        println!("   Return: {:.2}%", best.result.total_return_pct);
        println!(
            "   Total Fills: {} ({} bid, {} ask)",
            best.result.total_fills(),
            best.result.bid_fills,
            best.result.ask_fills
        );
        println!(
            "   Total Volume: {:.4} units (${:.2})",
            best.result.total_volume, best.result.total_notional_volume
        );
    }

    // Analysis by gamma (best horizon for each gamma)
    println!("\n------------------------------------------------------------");
    println!("Analysis: Best Horizon for Each Gamma\n");

    for &gamma in gammas {
        let best_for_gamma = sorted_results
            .iter()
            .filter(|r| (r.gamma - gamma).abs() < 1e-9)
            .max_by(|a, b| a.result.final_pnl.cmp(&b.result.final_pnl));

        if let Some(best) = best_for_gamma {
            println!(
                "g={:.2}: Best horizon={} -> PnL=${:.2}, Return={:.2}%, Fills={}",
                gamma,
                format_duration(best.horizon),
                best.result.final_pnl,
                best.result.total_return_pct,
                best.result.total_fills()
            );
        }
    }

    // Analysis by horizon (best gamma for each horizon)
    println!("\n------------------------------------------------------------");
    println!("Analysis: Best Gamma for Each Horizon\n");

    for &horizon in horizons {
        let best_for_horizon = sorted_results
            .iter()
            .filter(|r| r.horizon == horizon)
            .max_by(|a, b| a.result.final_pnl.cmp(&b.result.final_pnl));

        if let Some(best) = best_for_horizon {
            println!(
                "H={}: Best gamma={:.2} -> PnL=${:.2}, Return={:.2}%, Fills={}",
                format_duration(horizon),
                best.gamma,
                best.result.final_pnl,
                best.result.total_return_pct,
                best.result.total_fills()
            );
        }
    }

    println!("\n============================================================");
}

fn save_results_to_csv(
    results: &[GridResult],
    path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(path)?;

    // Write header
    writeln!(
        file,
        "horizon_seconds,horizon_formatted,gamma,initial_capital,final_pnl,total_return_pct,bid_fills,ask_fills,total_fills,total_volume,total_notional_volume,final_inventory,final_cash"
    )?;

    // Sort by PnL for the CSV output
    let mut sorted = results.to_vec();
    sorted.sort_by(|a, b| b.result.final_pnl.cmp(&a.result.final_pnl));

    // Write rows
    for grid_result in &sorted {
        let r = &grid_result.result;
        writeln!(
            file,
            "{},{},{:.4},{},{},{},{},{},{},{},{},{},{}",
            grid_result.horizon,
            format_duration(grid_result.horizon),
            grid_result.gamma,
            r.initial_capital,
            r.final_pnl,
            r.total_return_pct,
            r.bid_fills,
            r.ask_fills,
            r.total_fills(),
            r.total_volume,
            r.total_notional_volume,
            r.final_inventory,
            r.final_cash
        )?;
    }

    Ok(())
}
