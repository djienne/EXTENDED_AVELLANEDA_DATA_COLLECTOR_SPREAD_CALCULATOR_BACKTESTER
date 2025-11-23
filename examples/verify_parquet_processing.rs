use extended_data_collector::data_loader::{DataLoader, DataEvent};
use std::error::Error;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    println!("Verifying Parquet processing pipeline...");

    // Use data/eth_usd which has been migrated
    let loader = DataLoader::new(
        Path::new("data/eth_usd/trades.csv"),
        Path::new("data/eth_usd/orderbook_parts"),
    );

    let mut last_ts = 0;
    let mut count = 0;
    let limit = 5000; // Check first 5000 events

    for event_result in loader.stream()?.take(limit) {
        let event = event_result?;
        let current_ts = event.timestamp();

        // 1. Time Ordering Check
        if current_ts < last_ts {
            return Err(format!("Time regression detected! Last: {}, Current: {}", last_ts, current_ts).into());
        }
        last_ts = current_ts;

        match event {
            DataEvent::Trade(t) => {
                // Minimal verification for trades
                if t.price.is_zero() || t.quantity.is_zero() {
                     return Err(format!("Invalid trade data: {:?}", t).into());
                }
            },
            DataEvent::Orderbook(ob) => {
                // 2. Top-of-Book Integrity
                if ob.bids.is_empty() || ob.asks.is_empty() {
                    // It's possible to have empty side if market is illiquid or connection just started, 
                    // but unusual for major pairs.
                    println!("Warning: Empty orderbook side at {}", current_ts);
                    continue;
                }

                let best_bid = ob.bids[0].0;
                let best_ask = ob.asks[0].0;

                if best_bid >= best_ask {
                    println!("Warning: Crossed book at {}: Bid {} >= Ask {}", current_ts, best_bid, best_ask);
                    // Not strictly an error in raw data, but good to know
                }

                // 3. Sorting Integrity (Critical Assumption)
                // Bids should be DESCENDING
                for w in ob.bids.windows(2) {
                    if w[0].0 < w[1].0 {
                        return Err(format!("Bids not sorted descending at {}: {} < {}", current_ts, w[0].0, w[1].0).into());
                    }
                }

                // Asks should be ASCENDING
                for w in ob.asks.windows(2) {
                    if w[0].0 > w[1].0 {
                        return Err(format!("Asks not sorted ascending at {}: {} > {}", current_ts, w[0].0, w[1].0).into());
                    }
                }
            }
        }
        count += 1;
    }

    println!("✅ Verified {} events. Data is correctly time-ordered, sorted, and parsed.", count);
    Ok(())
}
