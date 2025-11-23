use extended_data_collector::backtest_engine::{run_backtest, BacktestParams};
use extended_data_collector::data_loader::DataLoader;
use extended_data_collector::model_types::ASConfig;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use std::path::Path;

fn main() -> std::io::Result<()> {
    println!("Loading data...");

    // Load configuration
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

    // Load data using DataLoader
    let loader = DataLoader::new(
        Path::new("data/ETH_USD/trades.csv"),
        Path::new("data/ETH_USD/orderbook_depth.csv"),
    );

    let data_stream = loader.stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e)))?;

    println!("Config: gamma_min={}, max_inventory={}, horizon={}s",
        config.gamma_min, config.max_inventory, config.inventory_horizon_seconds);

    // Backtest parameters
    let initial_capital = Decimal::from_str("1000.0").unwrap();
    let order_notional = Decimal::from_str("20.0").unwrap();

    // Run backtest
    let params = BacktestParams {
        data_stream,
        config,
        initial_capital,
        order_notional,
        output_csv_path: Some("data/ETH_USD/backtest_results.csv".to_string()),
        verbose: true,
    };

    let results = run_backtest(params)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e)))?;

    // Final summary
    println!("\n{:-<120}", "");
    println!("BACKTEST SUMMARY");
    println!("{:-<120}", "");
    println!("Initial Capital: ${:.2}", results.initial_capital);
    println!("Final P&L: ${:.2}", results.final_pnl);
    println!("Total Return: {:.2}%", results.total_return_pct);
    println!("Final Inventory: {} (closed)", results.final_inventory);
    println!("Total Bid Fills: {}", results.bid_fills);
    println!("Total Ask Fills: {}", results.ask_fills);
    println!("Total Fills: {}", results.total_fills());
    println!("Total Volume Traded: {} units", results.total_volume);
    println!("\nResults written to data/ETH_USD/backtest_results.csv");

    Ok(())
}
