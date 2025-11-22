use crate::model_types::TradeEvent;
use csv::ReaderBuilder;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde::Deserialize;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
struct RawTrade {
    timestamp_ms: u64,
    price: String,
    quantity: String,
    side: String,
}

pub struct DataLoader {
    pub trades: Vec<TradeEvent>,
    pub orderbooks: BTreeMap<u64, OrderbookSnapshot>,
}

#[derive(Debug, Clone)]
pub struct OrderbookSnapshot {
    pub timestamp: u64,
    pub bids: Vec<(Decimal, Decimal)>, // (price, qty)
    pub asks: Vec<(Decimal, Decimal)>, // (price, qty)
}

impl DataLoader {
    pub fn new(trades_path: &Path, orderbook_path: &Path) -> Result<Self, Box<dyn Error>> {
        let trades = Self::load_trades(trades_path)?;
        let orderbooks = Self::load_orderbooks(orderbook_path)?;
        
        Ok(Self { trades, orderbooks })
    }

    fn load_trades(path: &Path) -> Result<Vec<TradeEvent>, Box<dyn Error>> {
        let file = File::open(path)?;
        let mut rdr = ReaderBuilder::new().from_reader(file);
        let mut trades = Vec::new();

        for result in rdr.deserialize() {
            let raw: RawTrade = result?;
            let price = Decimal::from_str(&raw.price)?;
            let quantity = Decimal::from_str(&raw.quantity)?;
            
            // If side is "sell", the aggressor is selling, so the maker is the buyer.
            // is_buyer_maker = true means the maker was the buyer (aggressor sold into bid).
            let is_buyer_maker = raw.side.to_lowercase() == "sell";

            trades.push(TradeEvent {
                timestamp: raw.timestamp_ms,
                price,
                quantity,
                is_buyer_maker,
            });
        }
        
        // Sort by timestamp just in case
        trades.sort_by_key(|t| t.timestamp);
        
        Ok(trades)
    }

    fn load_orderbooks(path: &Path) -> Result<BTreeMap<u64, OrderbookSnapshot>, Box<dyn Error>> {
        let file = File::open(path)?;
        let mut rdr = ReaderBuilder::new().from_reader(file);
        let mut orderbooks = BTreeMap::new();
        
        // Get headers to determine depth
        let headers = rdr.headers()?.clone();
        let mut max_levels = 0;
        for field in headers.iter() {
            if field.starts_with("bid_price") {
                max_levels += 1;
            }
        }

        for result in rdr.records() {
            let record = result?;
            let timestamp: u64 = record[0].parse()?;
            
            let mut bids = Vec::new();
            let mut asks = Vec::new();

            // Columns: timestamp,datetime,market,seq (0-3)
            // Then bid_price0, bid_qty0, ask_price0, ask_qty0 (4, 5, 6, 7)
            // Then bid_price1, ...
            
            for i in 0..max_levels {
                let base_idx = 4 + (i * 4);
                if base_idx + 3 >= record.len() {
                    break;
                }

                let bid_price = Decimal::from_str(&record[base_idx])?;
                let bid_qty = Decimal::from_str(&record[base_idx + 1])?;
                let ask_price = Decimal::from_str(&record[base_idx + 2])?;
                let ask_qty = Decimal::from_str(&record[base_idx + 3])?;

                if bid_price > Decimal::ZERO {
                    bids.push((bid_price, bid_qty));
                }
                if ask_price > Decimal::ZERO {
                    asks.push((ask_price, ask_qty));
                }
            }

            orderbooks.insert(timestamp, OrderbookSnapshot {
                timestamp,
                bids,
                asks,
            });
        }

        Ok(orderbooks)
    }

    pub fn get_trades(&self) -> &Vec<TradeEvent> {
        &self.trades
    }

    /// Get the orderbook snapshot closest to (but not after) the given timestamp
    pub fn get_closest_orderbook(&self, timestamp: u64) -> Option<&OrderbookSnapshot> {
        self.orderbooks.range(..=timestamp).next_back().map(|(_, v)| v)
    }
    
    /// Get iterator over orderbooks
    pub fn orderbooks_iter(&self) -> std::collections::btree_map::Iter<u64, OrderbookSnapshot> {
        self.orderbooks.iter()
    }
}
