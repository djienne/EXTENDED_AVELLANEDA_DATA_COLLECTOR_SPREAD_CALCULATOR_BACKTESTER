/// Migration utility to convert trades.csv to Parquet format
///
/// This tool reads existing trades.csv files and converts them to
/// partitioned Parquet files with ZSTD compression.
///
/// Usage:
///   cargo run --release --bin migrate_trades_to_parquet [market_dir]
///
/// If no market_dir is specified, all markets in data/ will be processed.

use csv::ReaderBuilder;
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;
use arrow::array::{ArrayRef, Float64Array, BooleanArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::Deserialize;

const ROWS_PER_PART: usize = 500_000; // Split into files of 500K rows (trades are smaller than orderbooks)

#[derive(Debug, Deserialize)]
struct RawTrade {
    timestamp_ms: u64,
    price: String,
    quantity: String,
    side: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    
    let market_dirs: Vec<std::path::PathBuf> = if args.len() < 2 {
        // No argument provided - process all market directories in data/
        println!("No market directory specified. Processing all markets in data/..\n");
        
        let data_dir = Path::new("data");
        if !data_dir.exists() {
            eprintln!("Data directory does not exist: {}", data_dir.display());
            std::process::exit(1);
        }
        
        let entries = fs::read_dir(data_dir)?;
        let mut dirs = Vec::new();
        
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                let csv_path = path.join("trades.csv");
                if csv_path.exists() {
                    dirs.push(path);
                }
            }
        }
        
        if dirs.is_empty() {
            eprintln!("No market directories with trades.csv found in data/");
            std::process::exit(1);
        }
        
        dirs.sort();
        dirs
    } else {
        // Argument provided - process single market directory
        let market_dir = Path::new(&args[1]);
        if !market_dir.exists() {
            eprintln!("Market directory does not exist: {}", market_dir.display());
            std::process::exit(1);
        }
        
        let csv_path = market_dir.join("trades.csv");
        if !csv_path.exists() {
            eprintln!("No trades.csv found in {}", market_dir.display());
            std::process::exit(1);
        }
        
        vec![market_dir.to_path_buf()]
    };
    
    println!("Processing {} market(s)\n", market_dirs.len());
    
    // Process each market directory
    for (idx, market_dir) in market_dirs.iter().enumerate() {
        println!("========================================");
        println!("Market {}/{}: {}", idx + 1, market_dirs.len(), market_dir.display());
        println!("========================================\n");
        
        if let Err(e) = migrate_market(market_dir) {
            eprintln!("Error migrating market {}: {}", market_dir.display(), e);
        }
        println!();
    }
    
    println!("All markets processed!");
    Ok(())
}

fn migrate_market(market_dir: &Path) -> Result<(), Box<dyn Error>> {
    let csv_path = market_dir.join("trades.csv");
    
    println!("Migrating {} to Parquet format...", csv_path.display());
    println!("Rows per partition: {}", ROWS_PER_PART);

    // Create parts directory (using trades_parts to differentiate from orderbook_parts if needed, or check if we want to replace trades.csv with a dir)
    // The previous implementation used `orderbook_parts`. I will use `trades_parts`.
    let parts_dir = market_dir.join("trades_parts");
    if parts_dir.exists() {
         // Clean up existing parts to avoid duplicates if re-running
         fs::remove_dir_all(&parts_dir)?;
    }
    fs::create_dir_all(&parts_dir)?;

    // Open CSV file
    let file = File::open(&csv_path)?;
    let buf = std::io::BufReader::with_capacity(1024 * 1024, file);
    let rdr = ReaderBuilder::new().from_reader(buf);

    // Build schema
    let schema = build_schema();

    // Process CSV in chunks
    let mut records = rdr.into_deserialize::<RawTrade>();
    let mut part_num = 0;
    let mut total_rows = 0;

    loop {
        let mut timestamps = Vec::with_capacity(ROWS_PER_PART);
        let mut prices = Vec::with_capacity(ROWS_PER_PART);
        let mut quantities = Vec::with_capacity(ROWS_PER_PART);
        let mut is_buyer_makers = Vec::with_capacity(ROWS_PER_PART);

        let mut batch_rows = 0;

        // Read up to ROWS_PER_PART rows
        while batch_rows < ROWS_PER_PART {
            match records.next() {
                Some(Ok(raw)) => {
                    let price: f64 = raw.price.parse().unwrap_or(0.0);
                    let quantity: f64 = raw.quantity.parse().unwrap_or(0.0);
                    let is_buyer_maker = raw.side.to_lowercase() == "sell";

                    timestamps.push(raw.timestamp_ms as i64);
                    prices.push(price);
                    quantities.push(quantity);
                    is_buyer_makers.push(is_buyer_maker);

                    batch_rows += 1;
                }
                Some(Err(e)) => return Err(Box::new(e)),
                None => break, // End of file
            }
        }

        if batch_rows == 0 {
            break; // No more data
        }

        // Create RecordBatch
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampMillisecondArray::from(timestamps)),
            Arc::new(Float64Array::from(prices)),
            Arc::new(Float64Array::from(quantities)),
            Arc::new(BooleanArray::from(is_buyer_makers)),
        ];

        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;

        // Write to Parquet file
        let first_ts = batch.column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0);
        
        // Use timestamp and part number for filename
        let filename = format!("part_{:013}_{:06}.parquet", first_ts, part_num);
        let file_path = parts_dir.join(&filename);

        let file = File::create(&file_path)?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3).unwrap()))
            .build();

        let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        total_rows += batch_rows;
        part_num += 1;

        println!(
            "Wrote part {} ({} rows, {} total): {}",
            part_num, batch_rows, total_rows, filename
        );
    }

    println!("Migration complete for {}!", market_dir.display());
    println!("Total rows migrated: {}", total_rows);
    println!("Total parts created: {}", part_num);
    println!("Output directory: {}", parts_dir.display());

    Ok(())
}

fn build_schema() -> Schema {
    Schema::new(vec![
        Field::new("timestamp_ms", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("price", DataType::Float64, false),
        Field::new("quantity", DataType::Float64, false),
        Field::new("is_buyer_maker", DataType::Boolean, false),
    ])
}
