use crate::error::Result;
use crate::data_collector::{CollectorState, OrderbookState};
use crate::types::WsOrderBookMessage;
use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::cmp::Reverse;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

const MAX_ROWS_PER_FILE: usize = 100_000; // Close file and start new one at 100K rows
const FLUSH_BATCH_SIZE: usize = 100; // Write batch every 100 rows
const FLUSH_INTERVAL_SECS: u64 = 10; // Or write every 10 seconds

fn min_quantity_threshold() -> Decimal {
    Decimal::new(1, 9) // 1e-9
}

/// Flattened orderbook snapshot for batching
#[derive(Debug, Clone)]
struct FlattenedSnapshot {
    timestamp_ms: i64,
    market: String,
    seq: i64,
    bid_prices: Vec<Option<f64>>,
    bid_qtys: Vec<Option<f64>>,
    ask_prices: Vec<Option<f64>>,
    ask_qtys: Vec<Option<f64>>,
}

pub struct OrderbookParquetWriter {
    parts_dir: PathBuf,
    state: Arc<Mutex<CollectorState>>,
    last_seq: Arc<Mutex<Option<u64>>>,
    max_levels: usize,
    market: String,
    orderbook_state: Arc<Mutex<Option<OrderbookState>>>,

    // Small batch for periodic writes to same file
    current_batch: Arc<Mutex<Vec<FlattenedSnapshot>>>,
    
    // Current file writer (stays open until 100K rows)
    current_writer: Arc<Mutex<Option<ArrowWriter<File>>>>,
    current_file_rows: Arc<Mutex<usize>>,
    current_file_path: Arc<Mutex<Option<PathBuf>>>,
    
    last_flush: Arc<Mutex<Instant>>,
    part_counter: Arc<Mutex<usize>>,
}

impl OrderbookParquetWriter {
    pub fn new(data_dir: &Path, market: &str, max_levels: usize) -> Result<Self> {
        // Create data directory if it doesn't exist
        fs::create_dir_all(data_dir)?;

        // Create market-specific subdirectory
        let market_dir = data_dir.join(market.replace("-", "_").to_lowercase());
        fs::create_dir_all(&market_dir)?;

        // Create parts subdirectory for Parquet files
        let parts_dir = market_dir.join("orderbook_parts");
        fs::create_dir_all(&parts_dir)?;

        let state_path = market_dir.join("state.json");

        // Load or create state
        let state = if state_path.exists() {
            match CollectorState::load_from_file(&state_path) {
                Ok(s) => {
                    info!(
                        "Loaded existing state for {}: {} orderbook depth updates",
                        market, s.orderbook_updates_count
                    );
                    s
                }
                Err(e) => {
                    warn!(
                        "Failed to load state for {}: {}. Creating new state.",
                        market, e
                    );
                    CollectorState::new(market.to_string())
                }
            }
        } else {
            CollectorState::new(market.to_string())
        };

        let last_seq = Arc::new(Mutex::new(state.last_orderbook_seq));
        let state = Arc::new(Mutex::new(state));

        info!(
            "Initialized Parquet writer for {} (max_levels={}, flush_interval={}s)",
            market, max_levels, FLUSH_INTERVAL_SECS
        );

        Ok(Self {
            parts_dir,
            state,
            last_seq,
            max_levels,
            market: market.to_string(),
            orderbook_state: Arc::new(Mutex::new(None)),
            current_batch: Arc::new(Mutex::new(Vec::with_capacity(FLUSH_BATCH_SIZE))),
            current_writer: Arc::new(Mutex::new(None)),
            current_file_rows: Arc::new(Mutex::new(0)),
            current_file_path: Arc::new(Mutex::new(None)),
            last_flush: Arc::new(Mutex::new(Instant::now())),
            part_counter: Arc::new(Mutex::new(0)),
        })
    }

    pub async fn write_full_orderbook(&self, msg: &WsOrderBookMessage) -> Result<()> {
        // Scoped lock for deduplication and state checks
        let should_process = {
            let mut last_seq = self.last_seq.lock().await;
            let state = self.state.lock().await;

            // Log message type for debugging
            if last_seq.is_none() || (state.orderbook_updates_count % 1000 == 0) {
                debug!(
                    "Full orderbook message type: {} (seq: {}, levels: bid={}, ask={})",
                    msg.message_type,
                    msg.seq,
                    msg.data.b.len(),
                    msg.data.a.len()
                );
            }

            // Check if we've seen this sequence before
            if let Some(prev_seq) = *last_seq {
                if msg.seq <= prev_seq {
                    if msg.message_type == "SNAPSHOT" {
                        warn!(
                            "Received SNAPSHOT with lower seq ({} <= {}). Resetting sequence tracking.",
                            msg.seq, prev_seq
                        );
                        // Allow it to proceed
                    } else {
                        debug!("Skipping duplicate/old orderbook seq: {} <= {}", msg.seq, prev_seq);
                        return Ok(());
                    }
                }
            }

            // Check if timestamp is after last recorded
            if let Some(last_ts) = state.last_orderbook_timestamp {
                if msg.ts < last_ts {
                    debug!("Skipping out-of-order orderbook: {} < {}", msg.ts, last_ts);
                    return Ok(());
                }
            }

            true
        };

        if !should_process {
            return Ok(());
        }

        // Scoped lock for orderbook state update
        let flattened = {
            let mut ob_state = self.orderbook_state.lock().await;

            if ob_state.is_none() {
                *ob_state = Some(OrderbookState::new(self.market.clone()));
            }

            let orderbook = ob_state.as_mut().unwrap();
            orderbook.apply_update(msg);

            // Extract top N levels from sorted orderbook
            let bids: Vec<(Decimal, Decimal)> = orderbook
                .bids
                .iter()
                .filter(|(_, &q)| q > min_quantity_threshold())
                .take(self.max_levels)
                .map(|(Reverse(p), q)| (*p, *q))
                .collect();

            let asks: Vec<(Decimal, Decimal)> = orderbook
                .asks
                .iter()
                .filter(|(_, &q)| q > min_quantity_threshold())
                .take(self.max_levels)
                .map(|(p, q)| (*p, *q))
                .collect();

            // Skip if we don't have both bids and asks
            if bids.is_empty() || asks.is_empty() {
                debug!("Skipping orderbook update with missing bids or asks");
                let mut last_seq = self.last_seq.lock().await;
                let mut state = self.state.lock().await;
                *last_seq = Some(msg.seq);
                state.last_orderbook_seq = Some(msg.seq);
                state.last_orderbook_timestamp = Some(msg.ts);
                return Ok(());
            }

            // Create flattened snapshot
            let mut bid_prices = Vec::with_capacity(self.max_levels);
            let mut bid_qtys = Vec::with_capacity(self.max_levels);
            let mut ask_prices = Vec::with_capacity(self.max_levels);
            let mut ask_qtys = Vec::with_capacity(self.max_levels);

            for level in 0..self.max_levels {
                let (bid_price, bid_qty) = bids.get(level).copied()
                    .unwrap_or((Decimal::ZERO, Decimal::ZERO));
                let (ask_price, ask_qty) = asks.get(level).copied()
                    .unwrap_or((Decimal::ZERO, Decimal::ZERO));

                bid_prices.push(if bid_price.is_zero() { None } else { Some(bid_price.to_f64().unwrap_or(0.0)) });
                bid_qtys.push(if bid_qty.is_zero() { None } else { Some(bid_qty.to_f64().unwrap_or(0.0)) });
                ask_prices.push(if ask_price.is_zero() { None } else { Some(ask_price.to_f64().unwrap_or(0.0)) });
                ask_qtys.push(if ask_qty.is_zero() { None } else { Some(ask_qty.to_f64().unwrap_or(0.0)) });
            }

            FlattenedSnapshot {
                timestamp_ms: msg.ts as i64,
                market: msg.data.m.clone(),
                seq: msg.seq as i64,
                bid_prices,
                bid_qtys,
                ask_prices,
                ask_qtys,
            }
        };

        // Add to current batch
        let should_flush = {
            let mut batch = self.current_batch.lock().await;
            batch.push(flattened);
            batch.len() >= FLUSH_BATCH_SIZE
        };

        // Update state
        {
            let mut last_seq = self.last_seq.lock().await;
            let mut state = self.state.lock().await;

            *last_seq = Some(msg.seq);
            state.last_orderbook_seq = Some(msg.seq);
            state.last_orderbook_timestamp = Some(msg.ts);
            state.orderbook_updates_count += 1;

            // Save state every 100 updates
            if state.orderbook_updates_count % 100 == 0 {
                let state_path = self.parts_dir.parent().unwrap().join("state.json");
                if let Err(e) = state.save_to_file(&state_path) {
                   warn!("Failed to save state: {}", e);
                }
            }
        }

        // Flush batch to current file every FLUSH_BATCH_SIZE or FLUSH_INTERVAL_SECS
        let elapsed_secs = self.last_flush.lock().await.elapsed().as_secs();
        if should_flush || elapsed_secs >= FLUSH_INTERVAL_SECS {
            self.flush_current_batch().await?;
        }

        Ok(())
    }

    async fn flush_current_batch(&self) -> Result<()> {
        let batch = {
            let mut current_batch = self.current_batch.lock().await;
            if current_batch.is_empty() {
                return Ok(());
            }
            std::mem::replace(&mut *current_batch, Vec::with_capacity(FLUSH_BATCH_SIZE))
        };

        if batch.is_empty() {
            return Ok(());
        }

        let batch_size = batch.len();
        let schema = self.build_schema();

        let mut writer_lock = self.current_writer.lock().await;
        let mut file_rows_lock = self.current_file_rows.lock().await;
        let mut file_path_lock = self.current_file_path.lock().await;

        // If no writer OR current file would exceed 100K, close and start new file
        if writer_lock.is_none() || (*file_rows_lock + batch_size) > MAX_ROWS_PER_FILE {
            // Close current writer if exists
            if let Some(writer) = writer_lock.take() {
                writer.close()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to close writer: {}", e)))?;
                
                if let Some(path) = file_path_lock.take() {
                    info!(
                        "Closed parquet file {} with {} rows (reached {}K limit)",
                        path.display(),
                        *file_rows_lock,
                        MAX_ROWS_PER_FILE / 1000
                    );
                }
                *file_rows_lock = 0;
            }

            // Generate filename for new file
            let first_ts = batch[0].timestamp_ms;
            let first_seq = batch[0].seq;
            let part_num = {
                let mut counter = self.part_counter.lock().await;
                let num = *counter;
                *counter += 1;
                num
            };

            let filename = format!("part_{:013}_{:010}_{:06}.parquet", first_ts, first_seq, part_num);
            let file_path = self.parts_dir.join(&filename);

            // Create new file and writer
            let file = File::create(&file_path)?;
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();

            let new_writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props))
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to create writer: {}", e)))?;

            *writer_lock = Some(new_writer);
            *file_path_lock = Some(file_path.clone());
            *file_rows_lock = 0;

            info!("Created new parquet file: {} (will grow to {}K rows)", filename, MAX_ROWS_PER_FILE / 1000);
        }

        // Write batch to current open file
        let record_batch = self.build_record_batch(&batch, &schema)?;
        if let Some(writer) = writer_lock.as_mut() {
            writer.write(&record_batch)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to write batch: {}", e)))?;
            
            *file_rows_lock += batch_size;

            debug!(
                "Wrote {} rows to current file (total: {}/{} = {:.1}%)",
                batch_size,
                *file_rows_lock,
                MAX_ROWS_PER_FILE,
                (*file_rows_lock as f64 / MAX_ROWS_PER_FILE as f64) * 100.0
            );
        }

        // Update last flush time
        *self.last_flush.lock().await = Instant::now();

        Ok(())
    }

    fn build_schema(&self) -> Schema {
        let mut fields = vec![
            Field::new("timestamp_ms", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("market", DataType::Utf8, false),
            Field::new("seq", DataType::Int64, false),
        ];

        for i in 0..self.max_levels {
            fields.push(Field::new(format!("bid_price_{}", i), DataType::Float64, true));
            fields.push(Field::new(format!("bid_qty_{}", i), DataType::Float64, true));
            fields.push(Field::new(format!("ask_price_{}", i), DataType::Float64, true));
            fields.push(Field::new(format!("ask_qty_{}", i), DataType::Float64, true));
        }

        Schema::new(fields)
    }

    fn build_record_batch(&self, batch: &[FlattenedSnapshot], schema: &Schema) -> Result<RecordBatch> {

        // Build timestamp array
        let timestamps: Vec<i64> = batch.iter().map(|s| s.timestamp_ms).collect();
        let timestamp_array = Arc::new(TimestampMillisecondArray::from(timestamps)) as ArrayRef;

        // Build market array
        let markets: Vec<String> = batch.iter().map(|s| s.market.clone()).collect();
        let market_array = Arc::new(StringArray::from(markets)) as ArrayRef;

        // Build seq array
        let seqs: Vec<i64> = batch.iter().map(|s| s.seq).collect();
        let seq_array = Arc::new(Int64Array::from(seqs)) as ArrayRef;

        // Build price/qty arrays for each level
        let mut columns: Vec<ArrayRef> = vec![timestamp_array, market_array, seq_array];

        for level in 0..self.max_levels {
            // Bid price
            let bid_prices: Vec<Option<f64>> = batch.iter()
                .map(|s| s.bid_prices.get(level).copied().flatten())
                .collect();
            columns.push(Arc::new(Float64Array::from(bid_prices)) as ArrayRef);

            // Bid qty
            let bid_qtys: Vec<Option<f64>> = batch.iter()
                .map(|s| s.bid_qtys.get(level).copied().flatten())
                .collect();
            columns.push(Arc::new(Float64Array::from(bid_qtys)) as ArrayRef);

            // Ask price
            let ask_prices: Vec<Option<f64>> = batch.iter()
                .map(|s| s.ask_prices.get(level).copied().flatten())
                .collect();
            columns.push(Arc::new(Float64Array::from(ask_prices)) as ArrayRef);

            // Ask qty
            let ask_qtys: Vec<Option<f64>> = batch.iter()
                .map(|s| s.ask_qtys.get(level).copied().flatten())
                .collect();
            columns.push(Arc::new(Float64Array::from(ask_qtys)) as ArrayRef);
        }

        RecordBatch::try_new(Arc::new(schema.clone()), columns)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to create RecordBatch: {}", e)).into())
    }

    pub async fn get_stats(&self) -> (u64, Option<u64>, Option<u64>) {
        let state = self.state.lock().await;
        (
            state.orderbook_updates_count,
            state.last_orderbook_seq,
            state.last_orderbook_timestamp,
        )
    }

    pub async fn save_state(&self) -> Result<()> {
        // Flush current batch
        self.flush_current_batch().await?;

        // Close current writer if open
        let mut writer_lock = self.current_writer.lock().await;
        if let Some(writer) = writer_lock.take() {
            let file_rows = *self.current_file_rows.lock().await;
            writer.close()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to close writer: {}", e)))?;
            info!("Closed final parquet file with {} rows on shutdown", file_rows);
        }

        let state = self.state.lock().await;
        let state_path = self.parts_dir.parent().unwrap().join("state.json");
        state.save_to_file(&state_path)?;
        info!(
            "Saved state for {}: {} full orderbook depth updates",
            state.market, state.orderbook_updates_count
        );
        Ok(())
    }

    pub async fn get_best_bid_ask(&self) -> Option<(Decimal, Decimal)> {
        let ob_state = self.orderbook_state.lock().await;

        if let Some(orderbook) = ob_state.as_ref() {
            return orderbook.get_best_bid_ask();
        }

        None
    }
}
