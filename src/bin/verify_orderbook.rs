use extended_data_collector::types::WsOrderBookMessage;
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::json;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, tungstenite::client::IntoClientRequest};
use url::Url;

const MARKET: &str = "SOL-USD";
const WS_URL: &str = "wss://api.starknet.extended.exchange/v1/stream"; // Based on docs: wss://api.starknet.extended.exchange/stream.extended.exchange/v1 ?
// Docs say: "Connect to the WebSocket streams using wss://api.starknet.extended.exchange as the host."
// And "Order book stream HTTP Request GET /stream.extended.exchange/v1/orderbooks/{market}"
// Usually this means the path is /stream.extended.exchange/v1/orderbooks/{market} if using a direct connection or maybe a subscription message.
// Let's check how data_collector does it.

// Re-checking data_collector.rs might be wise to get the exact URL structure if I missed it.
// But I'll assume the standard subscription model or the URL path model.
// The docs said: "Subscribe to the orderbooks stream... GET /stream.extended.exchange/v1/orderbooks/{market}"
// This usually implies a websocket connection to a base URL and then sending a message, OR connecting directly to that path.
// Let's try connecting to the specific path as a stream URL.
// Docs: "Connect to the WebSocket streams using wss://api.starknet.extended.exchange as the host."
// Docs: "stream_url": "wss://api.starknet.extended.exchange/stream.extended.exchange/v1" (from SDK config)
// So the base stream URL is wss://api.starknet.extended.exchange/stream.extended.exchange/v1
// And we probably append /orderbooks/SOL-USD

const BASE_WS_URL: &str = "wss://api.starknet.extended.exchange/stream.extended.exchange/v1/orderbooks";

#[derive(Debug, Clone, Default)]
struct Orderbook {
    bids: BTreeMap<Decimal, Decimal>, // Price -> Qty
    asks: BTreeMap<Decimal, Decimal>, // Price -> Qty
    seq: u64,
}

impl Orderbook {
    fn apply_snapshot(&mut self, msg: &WsOrderBookMessage) {
        self.seq = msg.seq;
        self.bids.clear();
        self.asks.clear();

        for level in &msg.data.b {
            let price = Decimal::from_str(&level.p).unwrap();
            let qty = Decimal::from_str(&level.q).unwrap();
            if qty > Decimal::ZERO {
                self.bids.insert(price, qty);
            }
        }

        for level in &msg.data.a {
            let price = Decimal::from_str(&level.p).unwrap();
            let qty = Decimal::from_str(&level.q).unwrap();
            if qty > Decimal::ZERO {
                self.asks.insert(price, qty);
            }
        }
    }

    fn apply_delta(&mut self, msg: &WsOrderBookMessage) {
        self.seq = msg.seq;

        for level in &msg.data.b {
            let price = Decimal::from_str(&level.p).unwrap();
            let qty_delta = Decimal::from_str(&level.q).unwrap();
            
            let entry = self.bids.entry(price).or_insert(Decimal::ZERO);
            *entry += qty_delta;

            if *entry <= Decimal::ZERO {
                self.bids.remove(&price);
            }
        }

        for level in &msg.data.a {
            let price = Decimal::from_str(&level.p).unwrap();
            let qty_delta = Decimal::from_str(&level.q).unwrap();

            let entry = self.asks.entry(price).or_insert(Decimal::ZERO);
            *entry += qty_delta;

            if *entry <= Decimal::ZERO {
                self.asks.remove(&price);
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let url = format!("{}/{}", BASE_WS_URL, MARKET);
    println!("Connecting to {}", url);

    let request = url.as_str().into_client_request().unwrap();
    let mut request = request;
    request.headers_mut().insert("User-Agent", "Extended-Data-Collector/0.1.0".parse().unwrap());

    let (ws_stream, _) = connect_async(request).await.expect("Failed to connect");
    let (_, mut read) = ws_stream.split();

    let mut snapshot_ob = Orderbook::default();
    let mut delta_ob = Orderbook::default();
    let mut first_snapshot_received = false;
    let mut snapshots_processed = 0;

    let start_time = Instant::now();
    let run_duration = Duration::from_secs(70); // Run slightly longer than 1 minute to catch 2 snapshots

    println!("Listening for messages...");

    let mut delta_count = 0;

    while let Some(msg) = read.next().await {
        if start_time.elapsed() > run_duration {
            println!("Run duration exceeded. Stopping.");
            break;
        }

        match msg {
            Ok(Message::Text(text)) => {
                if let Ok(parsed) = serde_json::from_str::<WsOrderBookMessage>(&text) {
                    if parsed.message_type == "SNAPSHOT" {
                        println!("Received SNAPSHOT (seq: {})", parsed.seq);
                        
                        snapshot_ob.apply_snapshot(&parsed);

                        if !first_snapshot_received {
                            delta_ob = snapshot_ob.clone();
                            first_snapshot_received = true;
                            println!("Initialized delta_ob from first SNAPSHOT.");
                            print_top_levels("Initial SNAPSHOT", &snapshot_ob);
                        } else {
                            snapshots_processed += 1;
                            println!("Comparing orderbooks after {} deltas...", delta_count);
                            
                            if !verify_ordering(&delta_ob) {
                                println!("FAILURE: Delta OB ordering invalid!");
                            }
                            if !verify_ordering(&snapshot_ob) {
                                println!("FAILURE: Snapshot OB ordering invalid!");
                            }

                            print_top_levels("Constructed Delta OB", &delta_ob);
                            print_top_levels("Fresh SNAPSHOT", &snapshot_ob);
                            compare_orderbooks(&delta_ob, &snapshot_ob);
                        }
                    } else if parsed.message_type == "DELTA" {
                        if first_snapshot_received {
                            delta_ob.apply_delta(&parsed);
                            delta_count += 1;
                            if delta_count % 100 == 0 {
                                println!("Processed {} DELTA messages...", delta_count);
                                if !verify_ordering(&delta_ob) {
                                    println!("FAILURE: Delta OB ordering invalid at {} deltas!", delta_count);
                                }
                                print_top_levels(&format!("Delta OB at {}", delta_count), &delta_ob);
                            }
                        }
                    }
                }
            }
            Ok(Message::Ping(_)) => {} // Auto-handled usually, but good to ignore
            Ok(Message::Pong(_)) => {}
            Err(e) => {
                eprintln!("Error reading message: {}", e);
                break;
            }
            _ => {}
        }
    }
}

fn compare_orderbooks(delta: &Orderbook, snapshot: &Orderbook) {
    let mut consistent = true;

    // Compare Bids
    if delta.bids.len() != snapshot.bids.len() {
        println!("FAILURE: Bid count mismatch! Delta: {}, Snapshot: {}", delta.bids.len(), snapshot.bids.len());
        consistent = false;
    }
    for (price, qty) in &snapshot.bids {
        match delta.bids.get(price) {
            Some(delta_qty) => {
                if delta_qty != qty {
                    println!("FAILURE: Bid qty mismatch at {}: Delta={}, Snapshot={}", price, delta_qty, qty);
                    consistent = false;
                }
            }
            None => {
                println!("FAILURE: Bid missing in Delta at {}", price);
                consistent = false;
            }
        }
    }
    for (price, _) in &delta.bids {
        if !snapshot.bids.contains_key(price) {
            println!("FAILURE: Extra Bid in Delta at {}", price);
            consistent = false;
        }
    }

    // Compare Asks
    if delta.asks.len() != snapshot.asks.len() {
        println!("FAILURE: Ask count mismatch! Delta: {}, Snapshot: {}", delta.asks.len(), snapshot.asks.len());
        consistent = false;
    }
    for (price, qty) in &snapshot.asks {
        match delta.asks.get(price) {
            Some(delta_qty) => {
                if delta_qty != qty {
                    println!("FAILURE: Ask qty mismatch at {}: Delta={}, Snapshot={}", price, delta_qty, qty);
                    consistent = false;
                }
            }
            None => {
                println!("FAILURE: Ask missing in Delta at {}", price);
                consistent = false;
            }
        }
    }
    for (price, _) in &delta.asks {
        if !snapshot.asks.contains_key(price) {
            println!("FAILURE: Extra Ask in Delta at {}", price);
            consistent = false;
        }
    }

    if consistent {
        println!("SUCCESS: Orderbooks match perfectly.");
    } else {
        println!("FAILURE: Orderbooks inconsistent.");
    }
}

fn verify_ordering(ob: &Orderbook) -> bool {
    // Bids: Iterate from Best (Highest) to Worst (Lowest)
    let mut prev_price: Option<Decimal> = None;
    for (price, _) in ob.bids.iter().rev() {
        if let Some(prev) = prev_price {
            if *price >= prev {
                println!("FAILURE: Bids not strictly decreasing! {} >= {}", price, prev);
                return false;
            }
        }
        prev_price = Some(*price);
    }

    // Asks: Iterate from Best (Lowest) to Worst (Highest)
    prev_price = None;
    for (price, _) in ob.asks.iter() {
        if let Some(prev) = prev_price {
            if *price <= prev {
                println!("FAILURE: Asks not strictly increasing! {} <= {}", price, prev);
                return false;
            }
        }
        prev_price = Some(*price);
    }
    
    true
}

fn print_top_levels(label: &str, ob: &Orderbook) {
    println!("--- {} (Top 20 Levels) ---", label);
    
    // Print top 20 Asks (Lowest Prices)
    println!("Asks (Increasing):");
    for (price, qty) in ob.asks.iter().take(20) {
        println!("  {} @ {}", qty, price);
    }

    // Print top 20 Bids (Highest Prices)
    println!("Bids (Decreasing):");
    for (price, qty) in ob.bids.iter().rev().take(20) {
        println!("  {} @ {}", qty, price);
    }
    println!("----------------");
}
