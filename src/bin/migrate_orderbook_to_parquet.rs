/// Migration utility to convert orderbook_depth.csv to Parquet format
///
/// This tool reads existing orderbook_depth.csv files and converts them to
/// partitioned Parquet files with Snappy compression.
///
/// Usage:
///   cargo run --release --bin migrate_orderbook_to_parquet [market_dir]
///
/// If no market_dir is specified, all markets in data/ will be processed.
///
/// Examples:
///   # Process all markets
///   cargo run --release --bin migrate_orderbook_to_parquet
///
///   # Process a specific market
///   cargo run --release --bin migrate_orderbook_to_parquet -- data/eth_usd

use csv::ReaderBuilder;
use std::env;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs;
use std::sync::Arc;

const ROWS_PER_PART: usize = 100_000; // Split into files of 100K rows

fn main() -> Result<(), Box<dyn Error>> {
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    
    let market_dirs: Vec<std::path::PathBuf> = if args.len() < 2 {
        // No argument provided - process all market directories in data/
        println!("No market directory specified. Processing all markets in data/...\n");
        
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
                let csv_path = path.join("orderbook_depth.csv");
                if csv_path.exists() {
                    dirs.push(path);
                }
            }
        }
        
        if dirs.is_empty() {
            eprintln!("No market directories with orderbook_depth.csv found in data/");
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
        
        let csv_path = market_dir.join("orderbook_depth.csv");
        if !csv_path.exists() {
            eprintln!("No orderbook_depth.csv found in {}", market_dir.display());
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
        
        migrate_market(market_dir)?;
        println!();
    }
    
    println!("All markets migrated successfully!");
    Ok(())
}

fn migrate_market(market_dir: &Path) -> Result<(), Box<dyn Error>> {
    let csv_path = market_dir.join("orderbook_depth.csv");
    
    println!("Migrating {} to Parquet format...", csv_path.display());
    println!("Rows per partition: {}", ROWS_PER_PART);

    // Create parts directory
    let parts_dir = market_dir.join("orderbook_parts");
    fs::create_dir_all(&parts_dir)?;

    // Open CSV file
    let file = File::open(&csv_path)?;
    let buf = std::io::BufReader::with_capacity(1024 * 1024, file);
    let mut rdr = ReaderBuilder::new().from_reader(buf);

    // Parse headers to determine max_levels
    let headers = rdr.headers()?.clone();
    let mut max_levels = 0;
    for field in headers.iter() {
        if field.starts_with("bid_price") {
            max_levels += 1;
        }
    }

    println!("Detected {} orderbook levels", max_levels);

    // Build schema
    let schema = build_schema(max_levels);

    // Process CSV in chunks
    let mut records = rdr.records();
    let mut part_num = 0;
    let mut total_rows = 0;

    loop {
        let mut timestamps = Vec::new();
        let mut markets = Vec::new();
        let mut seqs = Vec::new();
        let mut bid_prices: Vec<Vec<Option<f64>>> = (0..max_levels).map(|_| Vec::new()).collect();
        let mut bid_qtys: Vec<Vec<Option<f64>>> = (0..max_levels).map(|_| Vec::new()).collect();
        let mut ask_prices: Vec<Vec<Option<f64>>> = (0..max_levels).map(|_| Vec::new()).collect();
        let mut ask_qtys: Vec<Vec<Option<f64>>> = (0..max_levels).map(|_| Vec::new()).collect();

        let mut batch_rows = 0;

        // Read up to ROWS_PER_PART rows
        while batch_rows < ROWS_PER_PART {
            match records.next() {
                Some(Ok(record)) => {
                    // Parse row
                    let timestamp: i64 = record[0].parse()?;
                    let market = record[2].to_string();
                    let seq: i64 = record[3].parse()?;

                    timestamps.push(timestamp);
                    markets.push(market);
                    seqs.push(seq);

                    // Parse bid/ask prices and quantities
                    for i in 0..max_levels {
                        let base_idx = 4 + (i * 4);
                        if base_idx + 3 >= record.len() {
                            bid_prices[i].push(None);
                            bid_qtys[i].push(None);
                            ask_prices[i].push(None);
                            ask_qtys[i].push(None);
                            continue;
                        }

                        let bid_price: f64 = record[base_idx].parse().unwrap_or(0.0);
                        let bid_qty: f64 = record[base_idx + 1].parse().unwrap_or(0.0);
                        let ask_price: f64 = record[base_idx + 2].parse().unwrap_or(0.0);
                        let ask_qty: f64 = record[base_idx + 3].parse().unwrap_or(0.0);

                        bid_prices[i].push(if bid_price > 0.0 { Some(bid_price) } else { None });
                        bid_qtys[i].push(if bid_qty > 0.0 { Some(bid_qty) } else { None });
                        ask_prices[i].push(if ask_price > 0.0 { Some(ask_price) } else { None });
                        ask_qtys[i].push(if ask_qty > 0.0 { Some(ask_qty) } else { None });
                    }

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
        let mut columns: Vec<ArrayRef> = Vec::new();
        columns.push(Arc::new(TimestampMillisecondArray::from(timestamps)) as ArrayRef);
        columns.push(Arc::new(StringArray::from(markets)) as ArrayRef);
        columns.push(Arc::new(Int64Array::from(seqs)) as ArrayRef);

        for i in 0..max_levels {
            columns.push(Arc::new(Float64Array::from(bid_prices[i].clone())) as ArrayRef);
            columns.push(Arc::new(Float64Array::from(bid_qtys[i].clone())) as ArrayRef);
            columns.push(Arc::new(Float64Array::from(ask_prices[i].clone())) as ArrayRef);
            columns.push(Arc::new(Float64Array::from(ask_qtys[i].clone())) as ArrayRef);
        }

        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;

        // Write to Parquet file
        let first_ts = batch.column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(0);
        let first_seq = batch.column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);

        let filename = format!("part_{:013}_{:010}_{:06}.parquet", first_ts, first_seq, part_num);
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

    println!("\nMigration complete!");
    println!("Total rows migrated: {}", total_rows);
    println!("Total parts created: {}", part_num);
    println!("Output directory: {}", parts_dir.display());

    Ok(())
}

fn build_schema(max_levels: usize) -> Schema {
    let mut fields = vec![
        Field::new("timestamp_ms", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("market", DataType::Utf8, false),
        Field::new("seq", DataType::Int64, false),
    ];

    for i in 0..max_levels {
        fields.push(Field::new(format!("bid_price_{}", i), DataType::Float64, true));
        fields.push(Field::new(format!("bid_qty_{}", i), DataType::Float64, true));
        fields.push(Field::new(format!("ask_price_{}", i), DataType::Float64, true));
        fields.push(Field::new(format!("ask_qty_{}", i), DataType::Float64, true));
    }

    Schema::new(fields)
}
