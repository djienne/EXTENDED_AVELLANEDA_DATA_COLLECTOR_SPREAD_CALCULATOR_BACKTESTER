/// Test script to compare Parquet compression algorithms
///
/// Reads actual orderbook CSV data and writes it with different compression
/// algorithms to compare file sizes and write performance.
///
/// Usage:
///   cargo run --release --bin test_compression

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use csv::ReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Parquet Compression Comparison ===\n");

    // Load actual CSV data
    let csv_path = Path::new("OLD/eth_usd/orderbook_depth.csv");

    if !csv_path.exists() {
        eprintln!("ERROR: CSV file not found at {}", csv_path.display());
        eprintln!("Please ensure the file exists or modify the csv_path variable.");
        return Err("CSV file not found".into());
    }

    println!("Reading CSV file: {}", csv_path.display());

    // Create test output directory
    let test_dir = Path::new("test_compression");
    fs::create_dir_all(test_dir)?;

    // Load data from CSV
    let (schema, record_batch, csv_size) = load_csv_data(csv_path)?;

    let num_rows = record_batch.num_rows();
    println!("Loaded {} rows from CSV", num_rows);
    println!("Original CSV size: {} bytes ({:.2} MB)\n", csv_size, csv_size as f64 / 1_000_000.0);

    println!("Testing compression algorithms...\n");

    // Test different compression algorithms
    let algorithms = vec![
        ("UNCOMPRESSED", Compression::UNCOMPRESSED),
        ("SNAPPY (current)", Compression::SNAPPY),
        ("GZIP(6)", Compression::GZIP(parquet::basic::GzipLevel::default())),
        ("GZIP(9)", Compression::GZIP(parquet::basic::GzipLevel::try_new(9)?)),
        ("ZSTD(1)", Compression::ZSTD(parquet::basic::ZstdLevel::try_new(1)?)),
        ("ZSTD(3)", Compression::ZSTD(parquet::basic::ZstdLevel::default())),
        ("ZSTD(10)", Compression::ZSTD(parquet::basic::ZstdLevel::try_new(10)?)),
        ("ZSTD(15)", Compression::ZSTD(parquet::basic::ZstdLevel::try_new(15)?)),
        ("ZSTD(22)", Compression::ZSTD(parquet::basic::ZstdLevel::try_new(22)?)),
        ("BROTLI(6)", Compression::BROTLI(parquet::basic::BrotliLevel::default())),
        ("BROTLI(11)", Compression::BROTLI(parquet::basic::BrotliLevel::try_new(11)?)),
        ("LZ4", Compression::LZ4),
        ("LZ4_RAW", Compression::LZ4_RAW),
    ];

    let mut results = Vec::new();

    for (name, compression) in algorithms {
        let filename = format!("test_{}.parquet", name.replace("(", "_").replace(")", "").replace(" ", "_"));
        let file_path = test_dir.join(&filename);

        let start = Instant::now();

        // Write with this compression
        let file = File::create(&file_path)?;
        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();

        let mut writer = ArrowWriter::try_new(file, Arc::new(schema.clone()), Some(props))?;
        writer.write(&record_batch)?;
        writer.close()?;

        let write_time = start.elapsed();
        let file_size = fs::metadata(&file_path)?.len();

        results.push((name, file_size, write_time));

        println!(
            "{:<20} | Size: {:>10} bytes ({:>6.2} MB) | Time: {:>6} ms",
            name,
            file_size,
            file_size as f64 / 1_000_000.0,
            write_time.as_millis()
        );
    }

    // Calculate compression ratios relative to UNCOMPRESSED
    let uncompressed_size = results[0].1 as f64;

    println!("\n=== Compression Ratios ===\n");
    println!("{:<20} | {:>10} | {:>10} | {:>10} | {:>10}", "Algorithm", "Size (MB)", "vs Uncomp", "vs CSV", "Time (ms)");
    println!("{}", "-".repeat(85));

    for (name, size, time) in &results {
        let ratio_uncomp = uncompressed_size / *size as f64;
        let ratio_csv = csv_size as f64 / *size as f64;
        let size_mb = *size as f64 / 1_000_000.0;
        println!(
            "{:<20} | {:>10.2} | {:>10.2}x | {:>10.2}x | {:>10}",
            name, size_mb, ratio_uncomp, ratio_csv, time.as_millis()
        );
    }

    // Show recommendations
    println!("\n=== Recommendations ===\n");
    println!("Current:  SNAPPY - Fast but moderate compression (~2-3x)");
    println!("Best:     ZSTD(3) - Excellent balance of speed and compression (~4-5x)");
    println!("Moderate: ZSTD(10) - Better compression, still reasonable speed (~5-6x)");
    println!("Maximum:  ZSTD(22) or BROTLI(11) - Highest compression, slowest (~6-8x)");
    println!("\nFor high-frequency data collection, ZSTD(3) or ZSTD(10) is recommended.");
    println!("ZSTD(22) is best for archival/long-term storage.");

    println!("\nTest files created in: {}", test_dir.display());
    println!("\nTo use ZSTD in your code, change:");
    println!("  .set_compression(Compression::SNAPPY)");
    println!("to:");
    println!("  .set_compression(Compression::ZSTD(parquet::basic::ZstdLevel::try_new(10).unwrap()))");

    Ok(())
}

fn load_csv_data(csv_path: &Path) -> Result<(Schema, RecordBatch, u64), Box<dyn std::error::Error>> {
    // Get CSV file size
    let csv_size = fs::metadata(csv_path)?.len();

    // Open CSV file
    let file = File::open(csv_path)?;
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

    let schema = Schema::new(fields);

    // Read all rows into memory
    let mut timestamps = Vec::new();
    let mut markets = Vec::new();
    let mut seqs = Vec::new();
    let mut bid_prices: Vec<Vec<Option<f64>>> = (0..max_levels).map(|_| Vec::new()).collect();
    let mut bid_qtys: Vec<Vec<Option<f64>>> = (0..max_levels).map(|_| Vec::new()).collect();
    let mut ask_prices: Vec<Vec<Option<f64>>> = (0..max_levels).map(|_| Vec::new()).collect();
    let mut ask_qtys: Vec<Vec<Option<f64>>> = (0..max_levels).map(|_| Vec::new()).collect();

    for result in rdr.records() {
        let record = result?;

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
    }

    // Build columns
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

    Ok((schema, batch, csv_size))
}
