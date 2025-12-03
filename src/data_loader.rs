use crate::model_types::TradeEvent;
use arrow::array::{Array, BooleanArray, Float64Array, TimestampMillisecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use csv::{ReaderBuilder, StringRecord};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashSet;
use std::error::Error;
use std::fs::{self, File};
use std::iter::Peekable;
use std::path::{Path, PathBuf};

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
    suppress_warnings: bool,
}

impl DataLoader {
    pub fn new(trades_path: &Path, orderbook_path: &Path) -> Self {
        Self {
            trades_path: trades_path.to_path_buf(),
            orderbook_path: orderbook_path.to_path_buf(),
            suppress_warnings: true,
        }
    }

    /// Load all trades into memory (backward compatibility)
    pub fn get_trades(&self) -> Result<Vec<TradeEvent>, Box<dyn Error>> {
        // Detect format
        let use_parquet = self.trades_path.is_dir()
            || self.trades_path.extension().and_then(|s| s.to_str()) == Some("parquet");

        if use_parquet {
            let mut trades = Vec::new();
            let iter = ParquetTradeIterator::new(&self.trades_path, self.suppress_warnings)?;
            for result in iter {
                trades.push(result?);
            }
            Ok(trades)
        } else {
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
    }

    /// Iterator over orderbook snapshots (backward compatibility)
    pub fn orderbooks_iter(&self) -> Result<OrderbookIterator, Box<dyn Error>> {
        // Detect format
        let use_parquet = self.orderbook_path.is_dir()
            || self.orderbook_path.extension().and_then(|s| s.to_str()) == Some("parquet");

        if use_parquet {
            let parquet_iter = ParquetOrderbookIterator::new(&self.orderbook_path, self.suppress_warnings)?;
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
        // Setup Trade Source
        let use_parquet_trades = self.trades_path.is_dir()
            || self.trades_path.extension().and_then(|s| s.to_str()) == Some("parquet");

        let trade_source = if use_parquet_trades {
            let iter = ParquetTradeIterator::new(&self.trades_path, self.suppress_warnings)?;
            TradeSource::Parquet(iter)
        } else {
            let trades_file = File::open(&self.trades_path)?;
            let trades_buf = std::io::BufReader::with_capacity(1024 * 1024, trades_file);
            let trades_rdr = ReaderBuilder::new().from_reader(trades_buf);
            let trades_iter = trades_rdr.into_deserialize::<RawTrade>();
            TradeSource::Csv(trades_iter.peekable())
        };

        // Setup Orderbook Source
        let use_parquet_ob = self.orderbook_path.is_dir()
            || self.orderbook_path.extension().and_then(|s| s.to_str()) == Some("parquet");

        let (orderbook_source, max_levels) = if use_parquet_ob {
            let parquet_iter = ParquetOrderbookIterator::new(&self.orderbook_path, self.suppress_warnings)?;
            let max = parquet_iter.max_levels;
            (OrderbookSource::Parquet(parquet_iter), max)
        } else {
            let orderbook_file = File::open(&self.orderbook_path)?;
            let orderbook_buf = std::io::BufReader::with_capacity(1024 * 1024, orderbook_file);
            let mut orderbook_rdr = ReaderBuilder::new().from_reader(orderbook_buf);

            let headers = orderbook_rdr.headers()?.clone();
            let mut max = 0;
            for field in headers.iter() {
                if field.starts_with("bid_price") {
                    max += 1;
                }
            }

            let orderbook_iter = orderbook_rdr.into_records();
            (OrderbookSource::Csv(orderbook_iter.peekable()), max)
        };

        Ok(MergedDataIterator {
            trade_source,
            orderbook_source,
            max_levels,
            next_trade: None,
            next_orderbook: None,
        })
    }

    /// Load all data events into memory for fast iteration, sorted by timestamp and deduplicated
    pub fn load_all_events(&self) -> Result<Vec<DataEvent>, Box<dyn Error>> {
        let mut events = self.load_events_raw()?;

        // 1. Sort by timestamp (stable sort to preserve relative order of same-timestamp events if needed)
        events.sort_by_key(|e| e.timestamp());

        // 2. Deduplicate trades
        // Only removes trades that are EXACTLY identical (timestamp, price, qty, side)
        // Does NOT remove different trades at the same timestamp.
        let mut unique_events = Vec::with_capacity(events.len());
        let mut seen_trades_at_ts: HashSet<(Decimal, Decimal, bool)> = HashSet::new();
        let mut current_ts = 0;

        for event in events {
            match event {
                DataEvent::Trade(ref t) => {
                    if t.timestamp != current_ts {
                        current_ts = t.timestamp;
                        seen_trades_at_ts.clear();
                    }

                    // Create key for deduplication: (price, quantity, side)
                    // Note: Decimal implements Hash and Eq
                    let key = (t.price, t.quantity, t.is_buyer_maker);

                    if seen_trades_at_ts.contains(&key) {
                        // Duplicate found - skip
                        continue;
                    }

                    seen_trades_at_ts.insert(key);
                    unique_events.push(event);
                }
                DataEvent::Orderbook(_) => {
                    // Always keep orderbook updates
                    // Reset trades seen for this timestamp?
                    // No, trades and orderbooks can be interleaved at the same millisecond.
                    // The set logic handles duplicates within the same timestamp correctly regardless of interleaving orderbooks.
                    // But if timestamp changes, we clear.
                    if event.timestamp() != current_ts {
                        current_ts = event.timestamp();
                        seen_trades_at_ts.clear();
                    }
                    unique_events.push(event);
                }
            }
        }

        Ok(unique_events)
    }

    /// Load events in their original arrival order without sorting or deduplication
    pub fn load_events_raw(&self) -> Result<Vec<DataEvent>, Box<dyn Error>> {
        let stream = self.stream()?;
        let mut events = Vec::with_capacity(100_000); // Reasonable initial guess
        for event in stream {
            events.push(event?);
        }
        Ok(events)
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
            OrderbookIterator::Csv { iter, max_levels } => match iter.next() {
                Some(Ok(record)) => match parse_orderbook(record, *max_levels) {
                    Ok(snapshot) => {
                        let ts = snapshot.timestamp;
                        Some(Ok((ts, snapshot)))
                    }
                    Err(e) => Some(Err(e)),
                },
                Some(Err(e)) => Some(Err(Box::new(e))),
                None => None,
            },
            OrderbookIterator::Parquet(iter) => match iter.next() {
                Some(Ok(snapshot)) => {
                    let ts = snapshot.timestamp;
                    Some(Ok((ts, snapshot)))
                }
                Some(Err(e)) => Some(Err(e)),
                None => None,
            },
        }
    }
}

enum OrderbookSource {
    Csv(Peekable<csv::StringRecordsIntoIter<std::io::BufReader<File>>>),
    Parquet(ParquetOrderbookIterator),
}

enum TradeSource {
    Csv(Peekable<csv::DeserializeRecordsIntoIter<std::io::BufReader<File>, RawTrade>>),
    Parquet(ParquetTradeIterator),
}

pub struct MergedDataIterator {
    trade_source: TradeSource,
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
            match &mut self.trade_source {
                TradeSource::Csv(iter) => {
                    match iter.next() {
                        Some(Ok(raw)) => match parse_trade(raw) {
                            Ok(trade) => self.next_trade = Some(trade),
                            Err(e) => return Some(Err(e)),
                        },
                        Some(Err(e)) => return Some(Err(Box::new(e))),
                        None => {} // End of trades
                    }
                }
                TradeSource::Parquet(iter) => match iter.next() {
                    Some(Ok(trade)) => self.next_trade = Some(trade),
                    Some(Err(e)) => return Some(Err(e)),
                    None => {}
                },
            }
        }

        // Ensure we have the next orderbook buffered if available
        if self.next_orderbook.is_none() {
            match &mut self.orderbook_source {
                OrderbookSource::Csv(iter) => {
                    match iter.next() {
                        Some(Ok(record)) => match parse_orderbook(record, self.max_levels) {
                            Ok(ob) => self.next_orderbook = Some(ob),
                            Err(e) => return Some(Err(e)),
                        },
                        Some(Err(e)) => return Some(Err(Box::new(e))),
                        None => {} // End of orderbooks
                    }
                }
                OrderbookSource::Parquet(iter) => {
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
            }
            (Some(_), None) => {
                let trade = self.next_trade.take().unwrap();
                Some(Ok(DataEvent::Trade(trade)))
            }
            (None, Some(_)) => {
                let ob = self.next_orderbook.take().unwrap();
                Some(Ok(DataEvent::Orderbook(ob)))
            }
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

fn parse_orderbook(
    record: StringRecord,
    max_levels: usize,
) -> Result<OrderbookSnapshot, Box<dyn Error>> {
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

fn validate_trade_schema(schema: &arrow::datatypes::Schema) -> Result<(), String> {
    let expected = [
        (
            "timestamp_ms",
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ),
        ("price", DataType::Float64),
        ("quantity", DataType::Float64),
        ("is_buyer_maker", DataType::Boolean),
    ];

    for (name, ty) in expected.iter() {
        let field = schema
            .field_with_name(name)
            .map_err(|_| format!("Missing {} column", name))?;
        if field.data_type() != ty {
            return Err(format!(
                "Invalid type for {}: expected {:?}, got {:?}",
                name,
                ty,
                field.data_type()
            ));
        }
    }

    Ok(())
}

fn validate_orderbook_schema(
    schema: &arrow::datatypes::Schema,
    max_levels: usize,
) -> Result<(), String> {
    if max_levels == 0 {
        return Err("Orderbook schema must include at least one level".to_string());
    }

    let base_fields = [
        (
            "timestamp_ms",
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ),
        ("market", DataType::Utf8),
        ("seq", DataType::Int64),
    ];

    for (name, ty) in base_fields.iter() {
        let field = schema
            .field_with_name(name)
            .map_err(|_| format!("Missing {} column", name))?;
        if field.data_type() != ty {
            return Err(format!(
                "Invalid type for {}: expected {:?}, got {:?}",
                name,
                ty,
                field.data_type()
            ));
        }
    }

    for level in 0..max_levels {
        let bid_price = format!("bid_price_{}", level);
        let bid_qty = format!("bid_qty_{}", level);
        let ask_price = format!("ask_price_{}", level);
        let ask_qty = format!("ask_qty_{}", level);

        for name in [&bid_price, &bid_qty, &ask_price, &ask_qty] {
            let field = schema
                .field_with_name(name)
                .map_err(|_| format!("Missing {} column", name))?;
            if field.data_type() != &DataType::Float64 {
                return Err(format!(
                    "Invalid type for {}: expected Float64, got {:?}",
                    name,
                    field.data_type()
                ));
            }
        }
    }

    Ok(())
}

/// Iterator over Parquet trade files
pub struct ParquetTradeIterator {
    files: Vec<PathBuf>,
    current_file_idx: usize,
    current_reader: Option<Box<dyn Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>>>>,
    current_batch: Option<RecordBatch>,
    current_row_idx: usize,
    warned_files: HashSet<PathBuf>,
    suppress_warnings: bool,
}

impl ParquetTradeIterator {
    pub fn new(path: &Path, suppress_warnings: bool) -> Result<Self, Box<dyn Error>> {
        let files = if path.is_dir() {
            let mut files: Vec<PathBuf> = fs::read_dir(path)?
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.path())
                .filter(|path| path.extension().and_then(|s| s.to_str()) == Some("parquet"))
                .collect();
            files.sort();
            files
        } else {
            vec![path.to_path_buf()]
        };

        if files.is_empty() {
            return Err("No Parquet trade files found".into());
        }

        let mut iter = Self {
            files,
            current_file_idx: 0,
            current_reader: None,
            current_batch: None,
            current_row_idx: 0,
            warned_files: HashSet::new(),
            suppress_warnings,
        };

        iter.advance_file()?;
        if iter.current_reader.is_none() {
            return Err("No valid Parquet trade files found".into());
        }
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
                Ok(file) => match ParquetRecordBatchReaderBuilder::try_new(file) {
                    Ok(builder) => {
                        let schema = builder.schema();
                        if let Err(e) = validate_trade_schema(schema.as_ref()) {
                            eprintln!(
                                "Warning: Skipping parquet trade file with invalid schema {:?}: {}",
                                file_path.file_name().unwrap_or_default(),
                                e
                            );
                            continue;
                        }

                        match builder.build() {
                            Ok(reader) => {
                                self.current_reader = Some(Box::new(reader));
                                self.current_batch = None;
                                self.current_row_idx = 0;
                                return Ok(true);
                            }
                            Err(e) => {
                                if !self.suppress_warnings && self.warned_files.insert(file_path.clone()) {
                                    eprintln!("Warning: Skipping corrupt parquet file (build failed) {:?}: {}", file_path, e);
                                }
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        if !self.suppress_warnings && self.warned_files.insert(file_path.clone()) {
                            eprintln!("Warning: Skipping corrupt parquet file (invalid footer/metadata) {:?}: {}", file_path, e);
                        }
                        continue;
                    }
                },
                Err(e) => {
                    if !self.suppress_warnings && self.warned_files.insert(file_path.clone()) {
                        eprintln!(
                            "Warning: Skipping unreadable parquet file {:?}: {}",
                            file_path, e
                        );
                    }
                    continue;
                }
            }
        }
    }

    fn read_trade_from_batch(
        &self,
        batch: &RecordBatch,
        row_idx: usize,
    ) -> Result<TradeEvent, Box<dyn Error>> {
        let timestamp_col = batch
            .column_by_name("timestamp_ms")
            .ok_or("Missing timestamp_ms column")?
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or("Invalid timestamp column type")?;

        let price_col = batch
            .column_by_name("price")
            .ok_or("Missing price column")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("Invalid price column type")?;

        let quantity_col = batch
            .column_by_name("quantity")
            .ok_or("Missing quantity column")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or("Invalid quantity column type")?;

        let maker_col = batch
            .column_by_name("is_buyer_maker")
            .ok_or("Missing is_buyer_maker column")?
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or("Invalid is_buyer_maker column type")?;

        let timestamp = timestamp_col.value(row_idx) as u64;
        let price = Decimal::from_f64(price_col.value(row_idx)).ok_or("Invalid price value")?;
        let quantity =
            Decimal::from_f64(quantity_col.value(row_idx)).ok_or("Invalid quantity value")?;
        let is_buyer_maker = maker_col.value(row_idx);

        Ok(TradeEvent {
            timestamp,
            price,
            quantity,
            is_buyer_maker,
        })
    }
}

impl Iterator for ParquetTradeIterator {
    type Item = Result<TradeEvent, Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref batch) = self.current_batch {
                if self.current_row_idx < batch.num_rows() {
                    let trade = self.read_trade_from_batch(batch, self.current_row_idx);
                    self.current_row_idx += 1;
                    return Some(trade);
                } else {
                    self.current_batch = None;
                    self.current_row_idx = 0;
                }
            }

            if let Some(ref mut reader) = self.current_reader {
                match reader.next() {
                    Some(Ok(batch)) => {
                        self.current_batch = Some(batch);
                        self.current_row_idx = 0;
                        continue;
                    }
                    Some(Err(e)) => return Some(Err(Box::new(e))),
                    None => {
                        self.current_reader = None;
                    }
                }
            }

            if self.current_reader.is_none() {
                match self.advance_file() {
                    Ok(true) => continue,
                    Ok(false) => return None,
                    Err(e) => return Some(Err(e)),
                }
            }
        }
    }
}

/// Iterator over Parquet orderbook files
pub struct ParquetOrderbookIterator {
    files: Vec<PathBuf>,
    current_file_idx: usize,
    current_reader: Option<Box<dyn Iterator<Item = Result<RecordBatch, arrow::error::ArrowError>>>>,
    current_batch: Option<RecordBatch>,
    current_row_idx: usize,
    pub max_levels: usize, // Made public to access from stream()
    warned_files: HashSet<PathBuf>,
    suppress_warnings: bool,
}

impl ParquetOrderbookIterator {
    pub fn new(path: &Path, suppress_warnings: bool) -> Result<Self, Box<dyn Error>> {
        // If path is a directory, collect all .parquet files
        let files = if path.is_dir() {
            let mut files: Vec<PathBuf> = fs::read_dir(path)?
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.path())
                .filter(|path| path.extension().and_then(|s| s.to_str()) == Some("parquet"))
                .collect();
            files.sort(); // Sort by filename for time ordering
            files
        } else {
            vec![path.to_path_buf()]
        };

        if files.is_empty() {
            return Err("No Parquet files found".into());
        }

        // Find the first valid file to determine max_levels
        let mut max_levels = 0;
        let mut first_valid_file_found = false;

        // Filter out invalid files from the start
        // This avoids issues where the first file is corrupt and crashes initialization
        let mut valid_files = Vec::new();
        let mut warned_files = HashSet::new();

        for file_path in files {
            match File::open(&file_path) {
                Ok(file) => match ParquetRecordBatchReaderBuilder::try_new(file) {
                    Ok(builder) => {
                        let schema = builder.schema();
                        let detected_levels = schema
                            .fields()
                            .iter()
                            .filter(|field| field.name().starts_with("bid_price_"))
                            .count();

                        if detected_levels == 0 {
                            eprintln!(
                                "Warning: Skipping parquet orderbook file with no levels {:?}",
                                file_path.file_name().unwrap()
                            );
                            continue;
                        }

                        if !first_valid_file_found {
                            max_levels = detected_levels;
                            first_valid_file_found = true;
                        } else if detected_levels != max_levels {
                            eprintln!(
                                    "Warning: Skipping parquet orderbook file with mismatched level count {:?} (expected {}, found {})",
                                    file_path.file_name().unwrap(),
                                    max_levels,
                                    detected_levels
                                );
                            continue;
                        }

                        if let Err(e) = validate_orderbook_schema(schema.as_ref(), max_levels) {
                            if !suppress_warnings && warned_files.insert(file_path.clone()) {
                                eprintln!(
                                    "Warning: Skipping parquet orderbook file with invalid schema {:?}: {}",
                                    file_path.file_name().unwrap(),
                                    e
                                );
                            }
                            continue;
                        }

                        valid_files.push(file_path);
                    }
                    Err(e) => {
                        if !suppress_warnings && warned_files.insert(file_path.clone()) {
                            eprintln!("Warning: Skipping corrupt/incomplete parquet file during init {:?}: {}", file_path.file_name().unwrap(), e);
                        }
                    }
                },
                Err(e) => {
                    if !suppress_warnings && warned_files.insert(file_path.clone()) {
                        eprintln!(
                            "Warning: Skipping unreadable parquet file during init {:?}: {}",
                            file_path.file_name().unwrap(),
                            e
                        );
                    }
                }
            }
        }

        if valid_files.is_empty() {
            return Err("No valid Parquet files found".into());
        }

        let mut iter = Self {
            files: valid_files,
            current_file_idx: 0,
            current_reader: None,
            current_batch: None,
            current_row_idx: 0,
            max_levels,
            warned_files,
            suppress_warnings,
        };

        // Initialize first file
        iter.advance_file()?;
        if iter.current_reader.is_none() {
            return Err("No valid Parquet orderbook files found".into());
        }

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
                Ok(file) => match ParquetRecordBatchReaderBuilder::try_new(file) {
                    Ok(builder) => {
                        let schema = builder.schema();
                        if let Err(e) = validate_orderbook_schema(schema.as_ref(), self.max_levels)
                        {
                            if !self.suppress_warnings && self.warned_files.insert(file_path.clone()) {
                                eprintln!(
                                    "Warning: Skipping parquet orderbook file with invalid schema {:?}: {}",
                                    file_path,
                                    e
                                );
                            }
                            continue;
                        }

                        match builder.build() {
                            Ok(reader) => {
                                self.current_reader = Some(Box::new(reader));
                                self.current_batch = None;
                                self.current_row_idx = 0;
                                return Ok(true);
                            }
                            Err(e) => {
                                if !self.suppress_warnings && self.warned_files.insert(file_path.clone()) {
                                    eprintln!("Warning: Skipping corrupt parquet file (build failed) {:?}: {}", file_path, e);
                                }
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        if !self.suppress_warnings && self.warned_files.insert(file_path.clone()) {
                            eprintln!("Warning: Skipping corrupt parquet file (invalid footer/metadata) {:?}: {}", file_path, e);
                        }
                        continue;
                    }
                },
                Err(e) => {
                    if !self.suppress_warnings && self.warned_files.insert(file_path.clone()) {
                        eprintln!(
                            "Warning: Skipping unreadable parquet file {:?}: {}",
                            file_path, e
                        );
                    }
                    continue;
                }
            }
        }
    }

    fn read_snapshot_from_batch(
        &self,
        batch: &RecordBatch,
        row_idx: usize,
    ) -> Result<OrderbookSnapshot, Box<dyn Error>> {
        // Extract timestamp
        let timestamp_col = batch
            .column(0)
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

            let bid_price_col = batch
                .column(bid_price_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("Invalid bid_price column")?;
            let bid_qty_col = batch
                .column(bid_qty_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("Invalid bid_qty column")?;
            let ask_price_col = batch
                .column(ask_price_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("Invalid ask_price column")?;
            let ask_qty_col = batch
                .column(ask_qty_idx)
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
