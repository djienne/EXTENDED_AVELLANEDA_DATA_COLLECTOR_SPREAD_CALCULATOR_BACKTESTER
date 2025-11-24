use crate::model_types::TradeEvent;
use arrow::array::{Array, Float64Array, TimestampMillisecondArray};
use arrow::record_batch::RecordBatch;
use csv::{ReaderBuilder, StringRecord};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use serde::Deserialize;
use std::error::Error;
use std::fs::{self, File};
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

    /// Load all trades into memory (backward compatibility)
    pub fn get_trades(&self) -> Result<Vec<TradeEvent>, Box<dyn Error>> {
        let file = File::open(&self.trades_path)?;
        let buf = std::io::BufReader::with_capacity(1024 * 1024, file);
        let rdr = ReaderBuilder::new().from_reader(buf);

        let mut trades = Vec::new();
        for result in rdr.into_deserialize::<RawTrade>() {
            let raw = result?;
            trades.push(parse_trade(raw)?);
        }

        Ok(trades)
    }

    /// Iterator over orderbook snapshots (backward compatibility)
    pub fn orderbooks_iter(&self) -> Result<OrderbookIterator, Box<dyn Error>> {
        // Detect format
        let use_parquet = self.orderbook_path.is_dir() ||
                          self.orderbook_path.extension().and_then(|s| s.to_str()) == Some("parquet");

        if use_parquet {
            let parquet_iter = ParquetOrderbookIterator::new(&self.orderbook_path)?;
            Ok(OrderbookIterator::Parquet(parquet_iter))
        } else {
            let file = File::open(&self.orderbook_path)?;
            let buf = std::io::BufReader::with_capacity(1024 * 1024, file);
            let mut rdr = ReaderBuilder::new().from_reader(buf);

            let headers = rdr.headers()?.clone();
            let mut max_levels = 0;
            for field in headers.iter() {
                if field.starts_with("bid_price") {
                    max_levels += 1;
                }
            }

            Ok(OrderbookIterator::Csv {
                iter: rdr.into_records(),
                max_levels,
            })
        }
    }

    pub fn stream(&self) -> Result<MergedDataIterator, Box<dyn Error>> {
        let trades_file = File::open(&self.trades_path)?;
        // Use a large buffer (1MB) to read chunks from disk, improving performance
        let trades_buf = std::io::BufReader::with_capacity(1024 * 1024, trades_file);
        let trades_rdr = ReaderBuilder::new().from_reader(trades_buf);
        let trades_iter = trades_rdr.into_deserialize::<RawTrade>();

        // Detect format: directory with Parquet files or CSV file
        let use_parquet = self.orderbook_path.is_dir() ||
                          self.orderbook_path.extension().and_then(|s| s.to_str()) == Some("parquet");

        if use_parquet {
            // Read from Parquet files
            let parquet_iter = ParquetOrderbookIterator::new(&self.orderbook_path)?;
            let max_levels = parquet_iter.max_levels;

            Ok(MergedDataIterator {
                trades_iter: trades_iter.peekable(),
                orderbook_source: OrderbookSource::Parquet(parquet_iter),
                max_levels,
                next_trade: None,
                next_orderbook: None,
            })
        } else {
            // Read from CSV file
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
                orderbook_source: OrderbookSource::Csv(orderbook_iter.peekable()),
                max_levels,
                next_trade: None,
                next_orderbook: None,
            })
        }
    }
}

/// Backward compatibility iterator for orderbook snapshots
pub enum OrderbookIterator {
    Csv {
        iter: csv::StringRecordsIntoIter<std::io::BufReader<File>>,
        max_levels: usize,
    },
    Parquet(ParquetOrderbookIterator),
}

impl Iterator for OrderbookIterator {
    type Item = Result<(u64, OrderbookSnapshot), Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            OrderbookIterator::Csv { iter, max_levels } => {
                match iter.next() {
                    Some(Ok(record)) => {
                        match parse_orderbook(record, *max_levels) {
                            Ok(snapshot) => {
                                let ts = snapshot.timestamp;
                                Some(Ok((ts, snapshot)))
                            }
                            Err(e) => Some(Err(e)),
                        }
                    }
                    Some(Err(e)) => Some(Err(Box::new(e))),
                    None => None,
                }
            }
            OrderbookIterator::Parquet(iter) => {
                match iter.next() {
                    Some(Ok(snapshot)) => {
                        let ts = snapshot.timestamp;
                        Some(Ok((ts, snapshot)))
                    }
                    Some(Err(e)) => Some(Err(e)),
                    None => None,
                }
            }
        }
    }
}

enum OrderbookSource {
    Csv(Peekable<csv::StringRecordsIntoIter<std::io::BufReader<File>>>),
    Parquet(ParquetOrderbookIterator),
}

pub struct MergedDataIterator {
    trades_iter: Peekable<csv::DeserializeRecordsIntoIter<std::io::BufReader<File>, RawTrade>>,
    orderbook_source: OrderbookSource,
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
            match &mut self.orderbook_source {
                OrderbookSource::Csv(ref mut iter) => {
                    match iter.next() {
                        Some(Ok(record)) => {
                            match parse_orderbook(record, self.max_levels) {
                                Ok(ob) => self.next_orderbook = Some(ob),
                                Err(e) => return Some(Err(e)),
                            }
                        },
                        Some(Err(e)) => return Some(Err(Box::new(e))),
                        None => {} // End of orderbooks
                    }
                },
                OrderbookSource::Parquet(ref mut iter) => {
                    match iter.next() {
                        Some(Ok(ob)) => self.next_orderbook = Some(ob),
                        Some(Err(e)) => return Some(Err(e)),
                        None => {} // End of orderbooks
                    }
                }
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

/// Iterator over Parquet orderbook files
pub struct ParquetOrderbookIterator {
    files: Vec<PathBuf>,
    current_file_idx: usize,
    current_reader: Option<Box<dyn Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>>>>,
    current_batch: Option<RecordBatch>,
    current_row_idx: usize,
    max_levels: usize,
}

impl ParquetOrderbookIterator {
    pub fn new(path: &Path) -> Result<Self, Box<dyn Error>> {
        // If path is a directory, collect all .parquet files
        let files = if path.is_dir() {
            let mut files: Vec<PathBuf> = fs::read_dir(path)?
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.path())
                .filter(|path| path.extension().and_then(|s| s.to_str()) == Some("parquet"))
                .collect();
            files.sort(); // Sort by filename for time ordering

            // Check if the last file is valid (it might be the active file being written)
            if let Some(last_file) = files.last() {
                if let Ok(file) = File::open(last_file) {
                    if ParquetRecordBatchReaderBuilder::try_new(file).is_err() {
                        eprintln!("\nWarning: Skipping incomplete/active parquet file: {:?}", last_file.file_name().unwrap());
                        files.pop();
                    }
                }
            }
            
            files
        } else {
            vec![path.to_path_buf()]
        };

        if files.is_empty() {
            return Err("No Parquet files found".into());
        }

        // Read first file to determine max_levels
        let first_file = File::open(&files[0])?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(first_file)?;
        let schema = builder.schema();

        // Count bid_price fields to determine max_levels
        let mut max_levels = 0;
        for field in schema.fields() {
            if field.name().starts_with("bid_price_") {
                max_levels += 1;
            }
        }

        let mut iter = Self {
            files,
            current_file_idx: 0,
            current_reader: None,
            current_batch: None,
            current_row_idx: 0,
            max_levels,
        };

        // Initialize first file
        iter.advance_file()?;

        Ok(iter)
    }

    fn advance_file(&mut self) -> Result<bool, Box<dyn Error>> {
        loop {
            if self.current_file_idx >= self.files.len() {
                return Ok(false);
            }

            let file_path = &self.files[self.current_file_idx];
            self.current_file_idx += 1;

            match File::open(file_path) {
                Ok(file) => {
                    match ParquetRecordBatchReaderBuilder::try_new(file) {
                        Ok(builder) => {
                            match builder.build() {
                                Ok(reader) => {
                                    self.current_reader = Some(Box::new(reader));
                                    self.current_batch = None;
                                    self.current_row_idx = 0;
                                    return Ok(true);
                                }
                                Err(e) => {
                                    eprintln!("Warning: Skipping corrupt parquet file (build failed) {:?}: {}", file_path, e);
                                    continue;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Warning: Skipping corrupt parquet file (invalid footer/metadata) {:?}: {}", file_path, e);
                            continue;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Warning: Skipping unreadable parquet file {:?}: {}", file_path, e);
                    continue;
                }
            }
        }
    }

    fn read_snapshot_from_batch(&self, batch: &RecordBatch, row_idx: usize) -> Result<OrderbookSnapshot, Box<dyn Error>> {
        // Extract timestamp
        let timestamp_col = batch.column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or("Invalid timestamp column")?;
        let timestamp = timestamp_col.value(row_idx) as u64;

        let mut bids = Vec::new();
        let mut asks = Vec::new();

        // Extract bid/ask prices and quantities
        for i in 0..self.max_levels {
            let bid_price_idx = 3 + (i * 4);
            let bid_qty_idx = bid_price_idx + 1;
            let ask_price_idx = bid_price_idx + 2;
            let ask_qty_idx = bid_price_idx + 3;

            if bid_price_idx >= batch.num_columns() {
                break;
            }

            let bid_price_col = batch.column(bid_price_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("Invalid bid_price column")?;
            let bid_qty_col = batch.column(bid_qty_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("Invalid bid_qty column")?;
            let ask_price_col = batch.column(ask_price_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("Invalid ask_price column")?;
            let ask_qty_col = batch.column(ask_qty_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("Invalid ask_qty column")?;

            if !bid_price_col.is_null(row_idx) && !bid_qty_col.is_null(row_idx) {
                let bid_price = Decimal::from_f64(bid_price_col.value(row_idx))
                    .ok_or("Failed to convert bid price")?;
                let bid_qty = Decimal::from_f64(bid_qty_col.value(row_idx))
                    .ok_or("Failed to convert bid qty")?;

                if bid_price > Decimal::ZERO {
                    bids.push((bid_price, bid_qty));
                }
            }

            if !ask_price_col.is_null(row_idx) && !ask_qty_col.is_null(row_idx) {
                let ask_price = Decimal::from_f64(ask_price_col.value(row_idx))
                    .ok_or("Failed to convert ask price")?;
                let ask_qty = Decimal::from_f64(ask_qty_col.value(row_idx))
                    .ok_or("Failed to convert ask qty")?;

                if ask_price > Decimal::ZERO {
                    asks.push((ask_price, ask_qty));
                }
            }
        }

        Ok(OrderbookSnapshot {
            timestamp,
            bids,
            asks,
        })
    }
}

impl Iterator for ParquetOrderbookIterator {
    type Item = Result<OrderbookSnapshot, Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If we have a current batch, try to read from it
            if let Some(ref batch) = self.current_batch {
                if self.current_row_idx < batch.num_rows() {
                    let snapshot = self.read_snapshot_from_batch(batch, self.current_row_idx);
                    self.current_row_idx += 1;
                    return Some(snapshot);
                } else {
                    // Batch exhausted, try next batch
                    self.current_batch = None;
                    self.current_row_idx = 0;
                }
            }

            // Try to get next batch from current reader
            if let Some(ref mut reader) = self.current_reader {
                match reader.next() {
                    Some(Ok(batch)) => {
                        self.current_batch = Some(batch);
                        self.current_row_idx = 0;
                        continue;
                    }
                    Some(Err(e)) => return Some(Err(Box::new(e))),
                    None => {
                        // Reader exhausted, try next file
                        self.current_reader = None;
                    }
                }
            }

            // Try to advance to next file
            if self.current_reader.is_none() {
                match self.advance_file() {
                    Ok(true) => continue,
                    Ok(false) => return None, // No more files
                    Err(e) => return Some(Err(e)),
                }
            }
        }
    }
}
