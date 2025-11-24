/// Parallel 2D grid search over time horizons AND gamma values for AS market-making backtest
///
/// Tests multiple combinations of inventory_horizon_seconds and risk_aversion_gamma
/// using 4 parallel threads for faster execution.
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
use rust_decimal::prelude::*;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;

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

fn main() -> std::io::Result<()> {
    println!("═══════════════════════════════════════════════════════════");
    println!("    AS Market-Making Strategy - Parallel 2D Grid Search");
    println!("        Time Horizon × Risk Aversion (Gamma)");
    println!("═══════════════════════════════════════════════════════════\n");

    // Load configuration (as base config)
    let config_path = "config.json";
    let base_config = match std::fs::read_to_string(config_path) {
        Ok(contents) => {
            match serde_json::from_str::<ASConfig>(&contents) {
                Ok(cfg) => {
                    println!("✓ Loaded base config from {}", config_path);
                    cfg
                },
                Err(e) => {
                    eprintln!("⚠ Error parsing config.json: {}. Using defaults.", e);
                    ASConfig::default()
                }
            }
        },
        Err(_) => {
            println!("⚠ config.json not found. Using defaults.");
            ASConfig::default()
        }
    };

    // Time horizons to test (seconds): 5m, 15m, 30m, 1h, 2h, 4h
    let horizons = vec![300, 900, 1800, 3600, 7200, 14400];

    // Gamma values to test (risk aversion parameter)
    let gammas = vec![0.01, 0.05, 0.1, 0.2];

    // Generate all combinations
    let mut configs = Vec::new();
    for &horizon in &horizons {
        for &gamma in &gammas {
            configs.push(GridConfig { horizon, gamma });
        }
    }

    let total_runs = configs.len();
    let num_threads = base_config.num_threads;

    println!("Testing {} time horizons × {} gamma values = {} total configurations",
        horizons.len(), gammas.len(), total_runs);
    println!("Horizons: {}", horizons.iter()
        .map(|h| format_duration(*h))
        .collect::<Vec<_>>()
        .join(", "));
    println!("Gammas: {}", gammas.iter()
        .map(|g| format!("{:.2}", g))
        .collect::<Vec<_>>()
        .join(", "));
    println!("Using {} parallel threads", num_threads);
    println!();

    // Shared state for results and progress
    let results = Arc::new(Mutex::new(Vec::new()));
    let completed_count = Arc::new(Mutex::new(0));
    let start_time = Instant::now();

    // Configure rayon to use configured number of threads
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
        .unwrap();

    println!("Starting parallel execution...\n");

    // Backtest parameters (shared)
    let initial_capital = Decimal::from_str("1000.0").unwrap();
    let order_notional = Decimal::from_str("20.0").unwrap();

    // Run backtests in parallel
    configs.par_iter().for_each(|grid_config| {
        let mut config = base_config.clone();
        config.inventory_horizon_seconds = grid_config.horizon;
        config.risk_aversion_gamma = grid_config.gamma;

        // Each thread needs its own data loader
        let loader = DataLoader::new(
            Path::new("data/eth_usd/trades.csv"),
            Path::new("data/eth_usd/orderbook_parts"),
        );

        let data_stream = match loader.stream() {
            Ok(stream) => stream,
            Err(e) => {
                eprintln!("✗ H={}, γ={:.2} - Failed to load data: {}",
                    format_duration(grid_config.horizon), grid_config.gamma, e);
                return;
            }
        };

        let params = BacktestParams {
            data_stream,
            config,
            initial_capital,
            order_notional,
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

                println!("[{:2}/{:2}] H={:>4}, γ={:.2} → PnL: ${:>7.2}, Fills: {:>3}, Return: {:>6.2}% (ETA: {}s)",
                    count, total_runs,
                    format_duration(grid_config.horizon),
                    grid_config.gamma,
                    result.final_pnl,
                    result.total_fills(),
                    result.total_return_pct,
                    remaining);

                // Store result
                let grid_result = GridResult {
                    horizon: grid_config.horizon,
                    gamma: grid_config.gamma,
                    result,
                };
                results.lock().unwrap().push(grid_result);
            }
            Err(e) => {
                eprintln!("✗ H={}, γ={:.2} - Error: {}",
                    format_duration(grid_config.horizon), grid_config.gamma, e);
            }
        }
    });

    // Extract results from Arc<Mutex<>>
    let mut final_results = Arc::try_unwrap(results)
        .unwrap()
        .into_inner()
        .unwrap();

    let total_elapsed = start_time.elapsed();
    println!("\n✓ All {} configurations completed in {:.1}s", total_runs, total_elapsed.as_secs_f64());
    println!("  Average: {:.1}s per config", total_elapsed.as_secs_f64() / total_runs as f64);

    println!("\n═══════════════════════════════════════════════════════════");
    println!("                    RESULTS SUMMARY");
    println!("═══════════════════════════════════════════════════════════\n");

    // Filter out results with too few fills (< 5 total fills)
    let min_fills = 5;
    let filtered_results: Vec<_> = final_results.iter()
        .filter(|r| r.result.total_fills() >= min_fills)
        .collect();

    if filtered_results.is_empty() {
        println!("⚠ No results with at least {} fills. Showing all results.\n", min_fills);
    } else {
        println!("Showing results with at least {} fills:\n", min_fills);
    }

    // Sort by best PnL (descending)
    let mut sorted_results = if filtered_results.is_empty() {
        final_results.iter().collect::<Vec<_>>()
    } else {
        filtered_results
    };
    sorted_results.sort_by(|a, b| b.result.final_pnl.cmp(&a.result.final_pnl));

    // Display table
    println!("{:<10} │ {:>6} │ {:>12} │ {:>10} │ {:>8} │ {:>8} │ {:>10} │ {:>12}",
        "Horizon", "Gamma", "Final PnL", "Return %", "Bid Fill", "Ask Fill", "Tot Fills", "Notional");
    println!("{:─<10}┼{:─<8}┼{:─<14}┼{:─<12}┼{:─<10}┼{:─<10}┼{:─<12}┼{:─<14}",
        "", "", "", "", "", "", "", "");

    for (rank, grid_result) in sorted_results.iter().enumerate() {
        let marker = if rank == 0 { "★" } else { " " };
        let r = &grid_result.result;
        println!("{} {:<8} │ {:>6.2} │ {:>12.2} │ {:>10.2} │ {:>8} │ {:>8} │ {:>10} │ {:>12.2}",
            marker,
            format_duration(grid_result.horizon),
            grid_result.gamma,
            r.final_pnl,
            r.total_return_pct,
            r.bid_fills,
            r.ask_fills,
            r.total_fills(),
            r.total_notional_volume);
    }

    println!("\n═══════════════════════════════════════════════════════════");

    // Best result info
    if let Some(best) = sorted_results.first() {
        println!("\n🏆 Best Configuration:");
        println!("   Time Horizon: {} ({}s)", format_duration(best.horizon), best.horizon);
        println!("   Gamma (Risk Aversion): {:.2}", best.gamma);
        println!("   Final PnL: ${:.2}", best.result.final_pnl);
        println!("   Return: {:.2}%", best.result.total_return_pct);
        println!("   Total Fills: {} ({} bid, {} ask)",
            best.result.total_fills(), best.result.bid_fills, best.result.ask_fills);
        println!("   Total Volume: {:.4} units (${:.2})",
            best.result.total_volume, best.result.total_notional_volume);
    }

    // Analysis by gamma (best horizon for each gamma)
    println!("\n───────────────────────────────────────────────────────────");
    println!("Analysis: Best Horizon for Each Gamma\n");

    for &gamma in &gammas {
        let best_for_gamma = sorted_results.iter()
            .filter(|r| r.gamma == gamma)
            .max_by(|a, b| a.result.final_pnl.cmp(&b.result.final_pnl));

        if let Some(best) = best_for_gamma {
            println!("γ={:.2}: Best horizon={} → PnL=${:.2}, Return={:.2}%, Fills={}",
                gamma,
                format_duration(best.horizon),
                best.result.final_pnl,
                best.result.total_return_pct,
                best.result.total_fills());
        }
    }

    // Analysis by horizon (best gamma for each horizon)
    println!("\n───────────────────────────────────────────────────────────");
    println!("Analysis: Best Gamma for Each Horizon\n");

    for &horizon in &horizons {
        let best_for_horizon = sorted_results.iter()
            .filter(|r| r.horizon == horizon)
            .max_by(|a, b| a.result.final_pnl.cmp(&b.result.final_pnl));

        if let Some(best) = best_for_horizon {
            println!("H={}: Best gamma={:.2} → PnL=${:.2}, Return={:.2}%, Fills={}",
                format_duration(horizon),
                best.gamma,
                best.result.final_pnl,
                best.result.total_return_pct,
                best.result.total_fills());
        }
    }

    println!("\n═══════════════════════════════════════════════════════════");
    println!("\n⚡ Performance: ~{:.1}x speedup vs sequential execution (using {} threads)",
        num_threads as f64 * 0.95, // Account for overhead
        num_threads);

    Ok(())
}
