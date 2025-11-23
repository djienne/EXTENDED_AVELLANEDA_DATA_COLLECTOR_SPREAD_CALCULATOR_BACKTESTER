/// Grid search over time horizons for AS market-making backtest
///
/// Tests multiple inventory_horizon_seconds values to find the optimal horizon
/// for the given market conditions and parameters.

use extended_data_collector::backtest_engine::{run_backtest, BacktestParams, BacktestResults};
use extended_data_collector::data_loader::DataLoader;
use extended_data_collector::model_types::ASConfig;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::path::Path;

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

fn main() -> std::io::Result<()> {
    println!("═══════════════════════════════════════════════════════════");
    println!("       AS Market-Making Strategy - Grid Search");
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

    // Load data loader (shared paths)
    println!("Initializing data loader...");
    let loader = DataLoader::new(
        Path::new("data/eth_usd/trades.csv"),
        Path::new("data/eth_usd/orderbook_parts"),
    );

    // Time horizons to test (seconds): 1m, 5m, 15m, 30m, 1h, 2h, 3h, 4h
    let horizons = vec![60, 300, 900, 1800, 3600, 7200, 10800, 14400];

    println!("Testing {} time horizons...", horizons.len());
    println!("Horizons: {}\n", horizons.iter()
        .map(|h| format_duration(*h))
        .collect::<Vec<_>>()
        .join(", "));

    // Backtest parameters (shared)
    let initial_capital = Decimal::from_str("1000.0").unwrap();
    let order_notional = Decimal::from_str("20.0").unwrap();

    // Run backtests for each horizon
    let mut results: Vec<(u64, BacktestResults)> = Vec::new();

    for (i, &horizon) in horizons.iter().enumerate() {
        print!("[{}/{}] Testing horizon={}... ", i + 1, horizons.len(), format_duration(horizon));
        std::io::Write::flush(&mut std::io::stdout()).unwrap();

        let mut config = base_config.clone();
        config.inventory_horizon_seconds = horizon;

        let data_stream = loader.stream()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e)))?;

        let params = BacktestParams {
            data_stream,
            config,
            initial_capital,
            order_notional,
            output_csv_path: None,  // No CSV output for grid search
            verbose: false,          // Silent run
        };

        match run_backtest(params) {
            Ok(result) => {
                println!("✓ PnL: ${:.2}, Fills: {}, Vol: ${:.2}", result.final_pnl, result.total_fills(), result.total_notional_volume);
                results.push((horizon, result));
            }
            Err(e) => {
                println!("✗ Error: {}", e);
            }
        }
    }

    println!("\n═══════════════════════════════════════════════════════════");
    println!("                    RESULTS SUMMARY");
    println!("═══════════════════════════════════════════════════════════\n");

    // Filter out results with too few fills (< 5 total fills)
    let min_fills = 5;
    let filtered_results: Vec<_> = results.iter()
        .filter(|(_, r)| r.total_fills() >= min_fills)
        .collect();

    if filtered_results.is_empty() {
        println!("⚠ No results with at least {} fills. Showing all results.\n", min_fills);
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

    // Display table
    println!("{:<10} │ {:>12} │ {:>10} │ {:>8} │ {:>8} │ {:>10} │ {:>12} │ {:>12}",
        "Horizon", "Final PnL", "Return %", "Bid Fill", "Ask Fill", "Tot Fills", "Volume", "Notional");
    println!("{:─<10}┼{:─<14}┼{:─<12}┼{:─<10}┼{:─<10}┼{:─<12}┼{:─<14}┼{:─<14}",
        "", "", "", "", "", "", "", "");

    for (rank, (horizon, result)) in sorted_results.iter().enumerate() {
        let marker = if rank == 0 { "★" } else { " " };
        println!("{} {:<8} │ {:>12.2} │ {:>10.2} │ {:>8} │ {:>8} │ {:>10} │ {:>12.4} │ {:>12.2}",
            marker,
            format_duration(*horizon),
            result.final_pnl,
            result.total_return_pct,
            result.bid_fills,
            result.ask_fills,
            result.total_fills(),
            result.total_volume,
            result.total_notional_volume);
    }

    println!("\n═══════════════════════════════════════════════════════════");

    // Best result info
    if let Some((best_horizon, best_result)) = sorted_results.first() {
        println!("\n🏆 Best Configuration:");
        println!("   Time Horizon: {} ({}s)", format_duration(*best_horizon), best_horizon);
        println!("   Final PnL: ${:.2}", best_result.final_pnl);
        println!("   Return: {:.2}%", best_result.total_return_pct);
        println!("   Total Fills: {} ({} bid, {} ask)",
            best_result.total_fills(), best_result.bid_fills, best_result.ask_fills);
        println!("   Total Volume: {:.4} units (${:.2})", best_result.total_volume, best_result.total_notional_volume);
    }

    println!();

    Ok(())
}
