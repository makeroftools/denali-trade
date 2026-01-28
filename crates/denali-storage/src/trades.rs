use anyhow::Result;
use arrow::array::{Float64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hypersdk::hypercore::types::Trade;
use num_traits::cast::ToPrimitive;
use parquet::arrow::async_writer::AsyncArrowWriter;
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::sync::mpsc::Receiver;

pub async fn write_trades_to_parquet(rx: Receiver<Trade>) -> Result<()> {
    const ROWS_PER_FILE: usize = 10000;
    const BATCH_SIZE: usize = 8192;
    const OUTPUT_DIR: &str = "data/trades";
    write_trades(rx, ROWS_PER_FILE, BATCH_SIZE, OUTPUT_DIR).await
}

async fn write_trades(
    mut rx: Receiver<Trade>,
    rows_per_file: usize,
    batch_size: usize,
    output_dir: &str,
) -> Result<()> {
    println!("Writer task started");
    fs::create_dir_all(output_dir).await?;
    println!("Output directory ensured: {}", output_dir);

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::UInt64, false),
        Field::new("coin", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("size", DataType::Float64, false),
        Field::new("side", DataType::Utf8, false),
    ]));

    let mut file_idx = 0u32;
    let mut rows_in_current_file = 0usize;
    let mut writer: Option<AsyncArrowWriter<File>> = None;

    let mut timestamps = Vec::with_capacity(batch_size);
    let mut coins = Vec::with_capacity(batch_size);
    let mut prices = Vec::with_capacity(batch_size);
    let mut sizes = Vec::with_capacity(batch_size);
    let mut sides = Vec::with_capacity(batch_size);

    let mut total_trades = 0usize;
    let mut expected_coin: Option<String> = None;

    while let Some(t) = rx.recv().await {
        if let Some(ref ec) = expected_coin {
            if ec != &t.coin {
                eprintln!("Mixed coins detected: expected {}, got {}", ec, t.coin);
                continue;
            }
        } else {
            expected_coin = Some(t.coin.clone());
        }

        timestamps.push(t.time);
        coins.push(t.coin.clone());
        prices.push(t.px.to_f64().expect("price → f64"));
        sizes.push(t.sz.to_f64().expect("size → f64"));
        sides.push(t.side.to_string());

        total_trades += 1;
        rows_in_current_file += 1;

        if total_trades % 100 == 0 {
            println!("");
            println!("Received trade #{} (price={:.2})", total_trades, t.px.to_f64().expect("price → f64"));
            println!("");
            println!("");
        }

        if timestamps.len() >= batch_size || rows_in_current_file >= rows_per_file {
            let batch_rows = timestamps.len();
            println!("Building batch of {} rows (file rows so far: {})", batch_rows, rows_in_current_file);

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(std::mem::take(&mut timestamps))),
                    Arc::new(StringArray::from(std::mem::take(&mut coins))),
                    Arc::new(Float64Array::from(std::mem::take(&mut prices))),
                    Arc::new(Float64Array::from(std::mem::take(&mut sizes))),
                    Arc::new(StringArray::from(std::mem::take(&mut sides))),
                ],
            )?;

            if writer.is_none() {
                let coin_lower = expected_coin.as_ref().unwrap().to_lowercase();
                let file_prefix = format!("trades_{}", coin_lower);
                let path = format!("{}/{}_part_{:04}.parquet", output_dir, file_prefix, file_idx);
                println!("Creating new Parquet file: {}", path);
                let file = File::create(&path).await?;
                writer = Some(AsyncArrowWriter::try_new(file, schema.clone(), None)?);
                file_idx += 1;
            }

            println!("Writing batch of {} rows...", batch_rows);
            writer.as_mut().unwrap().write(&batch).await?;
            println!("Batch written");

            if rows_in_current_file >= rows_per_file {
                println!("Closing file after {} rows", rows_in_current_file);
                if let Some(mut w) = writer.take() {
                    w.flush().await?;
                    w.close().await?;
                    tokio::time::sleep(std::time::Duration::from_secs(4)).await;
                    println!("File closed");
                }
                rows_in_current_file = 0;
            }
        }
    }

    // Final flush
    if !timestamps.is_empty() {
        let remaining = timestamps.len();
        println!("Final flush: {} remaining rows", remaining);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(timestamps)),
                Arc::new(StringArray::from(coins)),
                Arc::new(Float64Array::from(prices)),
                Arc::new(Float64Array::from(sizes)),
                Arc::new(StringArray::from(sides)),
            ],
        )?;

        if writer.is_none() {
            let coin_lower = expected_coin.as_ref().unwrap().to_lowercase();
            let file_prefix = format!("trades_{}", coin_lower);
            let path = format!("{}/{}_part_{:04}.parquet", output_dir, file_prefix, file_idx);
            println!("Creating final file: {}", path);
            let file = File::create(&path).await?;
            writer = Some(AsyncArrowWriter::try_new(file, schema.clone(), None)?);
        }

        println!("Writing final batch of {} rows", remaining);
        writer.as_mut().unwrap().write(&batch).await?;
        writer.as_mut().unwrap().flush().await?;
        println!("Final batch written");
    }

    if let Some(w) = writer {
        println!("Closing final writer");
        if let Err(e) = w.close().await {
            eprintln!("Final close failed: {}", e);
        } else {
            println!("All files closed");
        }
        // Give OS time to settle
        tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;
    }

    println!("Writer task finished – total trades processed: {}", total_trades);

    Ok(())
}