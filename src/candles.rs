// src/candles.rs

use anyhow::Result;
use arrow::array::{Float64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hypersdk::hypercore::types::Candle;
use num_traits::ToPrimitive;
use parquet::arrow::async_writer::AsyncArrowWriter;
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::sync::mpsc::Receiver;

pub async fn clean_candle(c: Candle) -> Candle {
    c  // simple: no-op for now
}

pub async fn should_persist(_c: &Candle) -> bool {
    true  // simple: always persist
}

pub async fn write_candles_to_parquet(rx: Receiver<Candle>) -> Result<()> {
    const ROWS_PER_FILE: usize = 10000;
    const BATCH_SIZE: usize = 8192;
    const OUTPUT_DIR: &str = "data/candles";
    write_candles(rx, ROWS_PER_FILE, BATCH_SIZE, OUTPUT_DIR).await
}

async fn write_candles(
    mut rx: Receiver<Candle>,
    rows_per_file: usize,
    batch_size: usize,
    output_dir: &str,
) -> Result<()> {
    println!("Writer task started");
    fs::create_dir_all(output_dir).await?;
    println!("Output directory ensured: {}", output_dir);

    let schema = Arc::new(Schema::new(vec![
        Field::new("open_time",  DataType::UInt64, false),
        Field::new("close_time", DataType::UInt64, false),
        Field::new("coin",       DataType::Utf8,   false),
        Field::new("interval",   DataType::Utf8,   false),
        Field::new("open",       DataType::Float64, false),
        Field::new("close",      DataType::Float64, false),
        Field::new("high",       DataType::Float64, false),
        Field::new("low",        DataType::Float64, false),
        Field::new("volume",     DataType::Float64, false),
        Field::new("num_trades", DataType::UInt64,  false),
    ]));

    let mut file_idx = 0u32;
    let mut rows_in_current_file = 0usize;
    let mut writer: Option<AsyncArrowWriter<File>> = None;

    let mut open_times   = Vec::with_capacity(batch_size);
    let mut close_times  = Vec::with_capacity(batch_size);
    let mut coins        = Vec::with_capacity(batch_size);
    let mut intervals    = Vec::with_capacity(batch_size);
    let mut opens        = Vec::with_capacity(batch_size);
    let mut closes       = Vec::with_capacity(batch_size);
    let mut highs        = Vec::with_capacity(batch_size);
    let mut lows         = Vec::with_capacity(batch_size);
    let mut volumes      = Vec::with_capacity(batch_size);
    let mut num_trades   = Vec::with_capacity(batch_size);

    let mut total_candles = 0usize;
    let mut expected_coin: Option<String> = None;
    let mut expected_interval: Option<String> = None;

    while let Some(c) = rx.recv().await {
        if let Some(ref ec) = expected_coin {
            if ec != &c.coin {
                eprintln!("Mixed coins detected: expected {}, got {}", ec, c.coin);
                continue;
            }
        } else {
            expected_coin = Some(c.coin.clone());
        }

        if let Some(ref ei) = expected_interval {
            if ei != &c.interval {
                eprintln!("Mixed intervals detected: expected {}, got {}", ei, c.interval);
                continue;
            }
        } else {
            expected_interval = Some(c.interval.clone());
        }

        open_times.push(c.open_time);
        close_times.push(c.close_time);
        coins.push(c.coin.clone());
        intervals.push(c.interval.clone());
        opens.push(c.open.to_f64().expect("open → f64"));
        closes.push(c.close.to_f64().expect("close → f64"));
        highs.push(c.high.to_f64().expect("high → f64"));
        lows.push(c.low.to_f64().expect("low → f64"));
        volumes.push(c.volume.to_f64().expect("volume → f64"));
        num_trades.push(c.num_trades);

        total_candles += 1;
        rows_in_current_file += 1;

        if total_candles % 100 == 0 {
            println!("");
            println!("Received candle #{} (open={:.2})", total_candles, c.open.to_f64().expect("open → f64"));
            println!("");
            println!("");
        }

        if open_times.len() >= batch_size || rows_in_current_file >= rows_per_file {
            let batch_rows = open_times.len();
            println!("Building batch of {} rows (file rows so far: {})", batch_rows, rows_in_current_file);

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(std::mem::take(&mut open_times))),
                    Arc::new(UInt64Array::from(std::mem::take(&mut close_times))),
                    Arc::new(StringArray::from(std::mem::take(&mut coins))),
                    Arc::new(StringArray::from(std::mem::take(&mut intervals))),
                    Arc::new(Float64Array::from(std::mem::take(&mut opens))),
                    Arc::new(Float64Array::from(std::mem::take(&mut closes))),
                    Arc::new(Float64Array::from(std::mem::take(&mut highs))),
                    Arc::new(Float64Array::from(std::mem::take(&mut lows))),
                    Arc::new(Float64Array::from(std::mem::take(&mut volumes))),
                    Arc::new(UInt64Array::from(std::mem::take(&mut num_trades))),
                ],
            )?;

            if writer.is_none() {
                let coin_lower = expected_coin.as_ref().unwrap().to_lowercase();
                let file_prefix = format!("candles_{}_{}", coin_lower, expected_interval.as_ref().unwrap());
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
    if !open_times.is_empty() {
        let remaining = open_times.len();
        println!("Final flush: {} remaining rows", remaining);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(open_times)),
                Arc::new(UInt64Array::from(close_times)),
                Arc::new(StringArray::from(coins)),
                Arc::new(StringArray::from(intervals)),
                Arc::new(Float64Array::from(opens)),
                Arc::new(Float64Array::from(closes)),
                Arc::new(Float64Array::from(highs)),
                Arc::new(Float64Array::from(lows)),
                Arc::new(Float64Array::from(volumes)),
                Arc::new(UInt64Array::from(num_trades)),
            ],
        )?;

        if writer.is_none() {
            let coin_lower = expected_coin.as_ref().unwrap().to_lowercase();
            let file_prefix = format!("candles_{}_{}", coin_lower, expected_interval.as_ref().unwrap());
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

    println!("Writer task finished – total candles processed: {}", total_candles);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hypersdk::hypercore::types::Candle;
    use rust_decimal::Decimal;
    use tokio::sync::mpsc;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_write_single_batch() -> anyhow::Result<()> {
        let temp_dir = tempdir()?;

        let output_path = temp_dir.path().to_path_buf();

        let (tx, rx) = mpsc::channel::<Candle>(10);

        let output_path_for_writer = output_path.to_str().unwrap().to_owned();

        let handle = tokio::spawn(async move {
            write_candles(
                rx,
                10,
                8192,
                &output_path_for_writer,
            )
            .await
        });

        for i in 0..7 {
            tx.send(Candle {
                open_time: 1700000000000 + i * 60000,
                close_time: 1700000060000 + i * 60000,
                coin: "BTC".to_string(),
                interval: "1m".to_string(),
                open: Decimal::from(50000 + i * 100),
                close: Decimal::from(50100 + i * 100),
                high: Decimal::from(50500 + i * 100),
                low: Decimal::from(49500 + i * 100),
                volume: Decimal::from(10 + i),
                num_trades: 50 + i as u64,
            })
            .await
            .unwrap();
        }
        drop(tx);

        handle.await??;

        let file_path = output_path.join("candles_btc_1m_part_0000.parquet");
        assert!(file_path.exists());

        Ok(())
    }
}