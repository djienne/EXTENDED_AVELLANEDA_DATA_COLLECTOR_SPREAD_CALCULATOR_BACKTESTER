use crate::model_types::TradeEvent;
use csv::{ReaderBuilder, StringRecord};
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde::Deserialize;
use std::error::Error;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::iter::Peekable;

#[derive(Debug, Clone)]
pub enum DataEvent {
    Trade(TradeEvent),
    Orderbook(OrderbookSnapshot),
}

impl DataEvent {
    pub fn timestamp(&self) -> u64 {
        match self {
            DataEvent::Trade(t) => t.timestamp,
            DataEvent::Orderbook(o) => o.timestamp,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderbookSnapshot {
    pub timestamp: u64,
    pub bids: Vec<(Decimal, Decimal)>, // (price, qty)
    pub asks: Vec<(Decimal, Decimal)>, // (price, qty)
}

#[derive(Debug, Deserialize)]
struct RawTrade {
    timestamp_ms: u64,
    price: String,
    quantity: String,
    side: String,
}

pub struct DataLoader {
    trades_path: PathBuf,
    orderbook_path: PathBuf,
}

impl DataLoader {
    pub fn new(trades_path: &Path, orderbook_path: &Path) -> Self {
        Self {
            trades_path: trades_path.to_path_buf(),
            orderbook_path: orderbook_path.to_path_buf(),
        }
    }

    pub fn stream(&self) -> Result<MergedDataIterator, Box<dyn Error>> {
        let trades_file = File::open(&self.trades_path)?;
        // Use a large buffer (1MB) to read chunks from disk, improving performance
        let trades_buf = std::io::BufReader::with_capacity(1024 * 1024, trades_file);
        let trades_rdr = ReaderBuilder::new().from_reader(trades_buf);
        let trades_iter = trades_rdr.into_deserialize::<RawTrade>();

        let orderbook_file = File::open(&self.orderbook_path)?;
        let orderbook_buf = std::io::BufReader::with_capacity(1024 * 1024, orderbook_file);
        let mut orderbook_rdr = ReaderBuilder::new().from_reader(orderbook_buf);
        
        // Get headers to determine depth
        let headers = orderbook_rdr.headers()?.clone();
        let mut max_levels = 0;
        for field in headers.iter() {
            if field.starts_with("bid_price") {
                max_levels += 1;
            }
        }
        
        let orderbook_iter = orderbook_rdr.into_records();

        Ok(MergedDataIterator {
            trades_iter: trades_iter.peekable(),
            orderbook_iter: orderbook_iter.peekable(),
            max_levels,
            next_trade: None,
            next_orderbook: None,
        })
    }
}

pub struct MergedDataIterator {
    trades_iter: Peekable<csv::DeserializeRecordsIntoIter<std::io::BufReader<File>, RawTrade>>,
    orderbook_iter: Peekable<csv::StringRecordsIntoIter<std::io::BufReader<File>>>,
    max_levels: usize,
    // Buffers to hold the parsed next event to allow peeking/comparison
    next_trade: Option<TradeEvent>,
    next_orderbook: Option<OrderbookSnapshot>,
}

impl Iterator for MergedDataIterator {
    type Item = Result<DataEvent, Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        // Ensure we have the next trade buffered if available
        if self.next_trade.is_none() {
            match self.trades_iter.next() {
                Some(Ok(raw)) => {
                    match parse_trade(raw) {
                        Ok(trade) => self.next_trade = Some(trade),
                        Err(e) => return Some(Err(e)),
                    }
                },
                Some(Err(e)) => return Some(Err(Box::new(e))),
                None => {} // End of trades
            }
        }

        // Ensure we have the next orderbook buffered if available
        if self.next_orderbook.is_none() {
            match self.orderbook_iter.next() {
                Some(Ok(record)) => {
                    match parse_orderbook(record, self.max_levels) {
                        Ok(ob) => self.next_orderbook = Some(ob),
                        Err(e) => return Some(Err(e)),
                    }
                },
                Some(Err(e)) => return Some(Err(Box::new(e))),
                None => {} // End of orderbooks
            }
        }

        // Compare timestamps and yield the smaller one
        match (&self.next_trade, &self.next_orderbook) {
            (Some(t), Some(ob)) => {
                if t.timestamp <= ob.timestamp {
                    let trade = self.next_trade.take().unwrap();
                    Some(Ok(DataEvent::Trade(trade)))
                } else {
                    let ob = self.next_orderbook.take().unwrap();
                    Some(Ok(DataEvent::Orderbook(ob)))
                }
            },
            (Some(_), None) => {
                let trade = self.next_trade.take().unwrap();
                Some(Ok(DataEvent::Trade(trade)))
            },
            (None, Some(_)) => {
                let ob = self.next_orderbook.take().unwrap();
                Some(Ok(DataEvent::Orderbook(ob)))
            },
            (None, None) => None,
        }
    }
}

fn parse_trade(raw: RawTrade) -> Result<TradeEvent, Box<dyn Error>> {
    let price = Decimal::from_str(&raw.price)?;
    let quantity = Decimal::from_str(&raw.quantity)?;
    let is_buyer_maker = raw.side.to_lowercase() == "sell";

    Ok(TradeEvent {
        timestamp: raw.timestamp_ms,
        price,
        quantity,
        is_buyer_maker,
    })
}

fn parse_orderbook(record: StringRecord, max_levels: usize) -> Result<OrderbookSnapshot, Box<dyn Error>> {
    let timestamp: u64 = record[0].parse()?;
    let mut bids = Vec::new();
    let mut asks = Vec::new();

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

    Ok(OrderbookSnapshot {
        timestamp,
        bids,
        asks,
    })
}
