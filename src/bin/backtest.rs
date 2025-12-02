use extended_data_collector::backtest_engine::{run_backtest, BacktestParams};
use extended_data_collector::data_loader::DataLoader;
use extended_data_collector::model_types::ASConfig;
use rust_decimal::Decimal;
use std::env;
use std::path::Path;

/// Default values
const DEFAULT_INITIAL_CAPITAL: i64 = 1000;
const DEFAULT_ORDER_NOTIONAL: i64 = 20;
const DEFAULT_CONFIG_PATH: &str = "config.json";
const DEFAULT_TRADES_PATH: &str = "data/eth_usd/trades_parts";
const DEFAULT_ORDERBOOK_PATH: &str = "data/eth_usd/orderbook_parts";
const DEFAULT_OUTPUT_PATH: &str = "data/eth_usd/backtest_results.csv";

fn print_usage(program: &str) {
    eprintln!("Usage: {} [OPTIONS]", program);
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --config <path>      Path to config file (default: {})", DEFAULT_CONFIG_PATH);
    eprintln!("  --trades <path>      Path to trades CSV (default: {})", DEFAULT_TRADES_PATH);
    eprintln!("  --orderbook <path>   Path to orderbook directory (default: {})", DEFAULT_ORDERBOOK_PATH);
    eprintln!("  --output <path>      Path to output CSV (default: {})", DEFAULT_OUTPUT_PATH);
    eprintln!("  --capital <amount>   Initial capital in dollars (default: {})", DEFAULT_INITIAL_CAPITAL);
    eprintln!("  --notional <amount>  Order notional in dollars (default: {})", DEFAULT_ORDER_NOTIONAL);
    eprintln!("  --quiet              Disable verbose output");
    eprintln!("  --help               Show this help message");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    
    // Parse command-line arguments
    let mut config_path = DEFAULT_CONFIG_PATH.to_string();
    let mut trades_path = DEFAULT_TRADES_PATH.to_string();
    let mut orderbook_path = DEFAULT_ORDERBOOK_PATH.to_string();
    let mut output_path = DEFAULT_OUTPUT_PATH.to_string();
    let mut initial_capital = DEFAULT_INITIAL_CAPITAL;
    let mut order_notional = DEFAULT_ORDER_NOTIONAL;
    let mut verbose = true;

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
            "--quiet" => {
                verbose = false;
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

    println!("Loading data...");

    // Load configuration
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

    // Load data using DataLoader
    let loader = DataLoader::new(
        Path::new(&trades_path),
        Path::new(&orderbook_path),
    );

    let data_stream = loader.stream()?;

    println!("Config: gamma_min={}, max_inventory={}, horizon={}s",
        config.gamma_min, config.max_inventory, config.inventory_horizon_seconds);
    println!("Capital: ${}, Order Notional: ${}", initial_capital, order_notional);

    // Backtest parameters - use Decimal::from() for integers (no panic risk)
    let params = BacktestParams {
        data_stream,
        config,
        initial_capital: Decimal::from(initial_capital),
        order_notional: Decimal::from(order_notional),
        output_csv_path: Some(output_path.clone()),
        verbose,
    };

    let results = run_backtest(params)?;

    // Final summary
    println!("\n{:-<120}", "");
    println!("BACKTEST SUMMARY");
    println!("{:-<120}", "");
    println!("Initial Capital:       ${:.2}", results.initial_capital);
    println!("Final P&L:             ${:.2}", results.final_pnl);
    println!("Total Return:          {:.2}%", results.total_return_pct);
    println!("Final Inventory:       {} (closed)", results.final_inventory);
    println!("Total Bid Fills:       {}", results.bid_fills);
    println!("Total Ask Fills:       {}", results.ask_fills);
    println!("Total Fills:           {}", results.total_fills());
    println!("Total Volume Traded:   {} units", results.total_volume);
    println!("Total Notional Volume: ${:.2}", results.total_notional_volume);
    println!("\nResults written to {}", output_path);

    Ok(())
}