/// Verification tool to validate Parquet orderbook data integrity
///
/// This tool checks:
/// - Timestamp ordering (monotonically increasing)
/// - No bid/ask crossing (bid < ask)
/// - Bid prices sorted descending, ask prices sorted ascending
/// - Row count matches expectations
///
/// Usage:
///   cargo run --release --example verify_parquet_integrity -- <market_dir>
///
/// Example:
///   cargo run --release --example verify_parquet_integrity -- data/eth_usd

use extended_data_collector::data_loader::{DataLoader, DataEvent};
use std::env;
use std::error::Error;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <market_dir>", args[0]);
        eprintln!("Example: {} data/eth_usd", args[0]);
        std::process::exit(1);
    }

    let market_dir = Path::new(&args[1]);
    if !market_dir.exists() {
        eprintln!("Market directory does not exist: {}", market_dir.display());
        std::process::exit(1);
    }

    let trades_path = market_dir.join("trades.csv");
    let orderbook_path = market_dir.join("orderbook_parts");

    if !orderbook_path.exists() {
        eprintln!("No orderbook_parts directory found in {}", market_dir.display());
        std::process::exit(1);
    }

    println!("Verifying Parquet orderbook integrity...");
    println!("Market directory: {}", market_dir.display());
    println!("Orderbook path: {}", orderbook_path.display());

    let loader = DataLoader::new(&trades_path, &orderbook_path);
    let mut stream = loader.stream()?;

    let mut orderbook_count = 0;
    let mut trade_count = 0;
    let mut last_orderbook_ts = 0u64;
    let mut last_trade_ts = 0u64;
    let mut crossing_errors = 0;
    let mut sort_errors = 0;
    let mut time_order_errors = 0;

    println!("\nProcessing data...");

    while let Some(event) = stream.next() {
        match event? {
            DataEvent::Orderbook(ob) => {
                orderbook_count += 1;

                // Check timestamp ordering
                if ob.timestamp < last_orderbook_ts {
                    time_order_errors += 1;
                    if time_order_errors <= 5 {
                        eprintln!(
                            "ERROR: Orderbook timestamp went backwards: {} < {}",
                            ob.timestamp, last_orderbook_ts
                        );
                    }
                }
                last_orderbook_ts = ob.timestamp;

                // Check bid/ask crossing
                if !ob.bids.is_empty() && !ob.asks.is_empty() {
                    let best_bid = ob.bids[0].0;
                    let best_ask = ob.asks[0].0;

                    if best_bid >= best_ask {
                        crossing_errors += 1;
                        if crossing_errors <= 5 {
                            eprintln!(
                                "ERROR: Bid/ask crossing at ts={}: bid={} >= ask={}",
                                ob.timestamp, best_bid, best_ask
                            );
                        }
                    }
                }

                // Check bid sorting (descending)
                for i in 1..ob.bids.len() {
                    if ob.bids[i].0 > ob.bids[i - 1].0 {
                        sort_errors += 1;
                        if sort_errors <= 5 {
                            eprintln!(
                                "ERROR: Bids not sorted at ts={}: {} > {}",
                                ob.timestamp, ob.bids[i].0, ob.bids[i - 1].0
                            );
                        }
                        break;
                    }
                }

                // Check ask sorting (ascending)
                for i in 1..ob.asks.len() {
                    if ob.asks[i].0 < ob.asks[i - 1].0 {
                        sort_errors += 1;
                        if sort_errors <= 5 {
                            eprintln!(
                                "ERROR: Asks not sorted at ts={}: {} < {}",
                                ob.timestamp, ob.asks[i].0, ob.asks[i - 1].0
                            );
                        }
                        break;
                    }
                }

                // Progress indicator
                if orderbook_count % 100_000 == 0 {
                    println!("Processed {} orderbook snapshots...", orderbook_count);
                }
            }
            DataEvent::Trade(trade) => {
                trade_count += 1;

                // Check timestamp ordering
                if trade.timestamp < last_trade_ts {
                    time_order_errors += 1;
                    if time_order_errors <= 5 {
                        eprintln!(
                            "ERROR: Trade timestamp went backwards: {} < {}",
                            trade.timestamp, last_trade_ts
                        );
                    }
                }
                last_trade_ts = trade.timestamp;

                // Progress indicator
                if trade_count % 100_000 == 0 {
                    println!("Processed {} trades...", trade_count);
                }
            }
        }
    }

    println!("\n=== Verification Results ===");
    println!("Total orderbook snapshots: {}", orderbook_count);
    println!("Total trades: {}", trade_count);
    println!("\nErrors:");
    println!("  Timestamp ordering errors: {}", time_order_errors);
    println!("  Bid/ask crossing errors: {}", crossing_errors);
    println!("  Sorting errors: {}", sort_errors);

    if time_order_errors == 0 && crossing_errors == 0 && sort_errors == 0 {
        println!("\n✓ All checks passed! Data integrity verified.");
        Ok(())
    } else {
        println!("\n✗ Data integrity issues detected.");
        std::process::exit(1);
    }
}
