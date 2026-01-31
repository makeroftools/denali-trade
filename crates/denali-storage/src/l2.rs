use anyhow::Result;
use arrow::array::{BooleanArray, StringArray, UInt64Array, Decimal128Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hypersdk::hypercore::types::L2Book;
use parquet::arrow::async_writer::AsyncArrowWriter;
use rust_decimal::{Decimal, MathematicalOps};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::sync::mpsc::Receiver;
use num_traits::ToPrimitive;

pub async fn write_l2_to_parquet(rx: Receiver<L2Book>) -> Result<()> {
    const ROWS_PER_FILE: usize = 200;       // small for testing
    const BATCH_SIZE: usize = 150;          // small for testing
    const OUTPUT_DIR: &str = "data/l2";

    write_l2(rx, ROWS_PER_FILE, BATCH_SIZE, OUTPUT_DIR).await
}

async fn write_l2(
    mut rx: Receiver<L2Book>,
    rows_per_file: usize,
    batch_size: usize,
    output_dir: &str,
) -> Result<()> {
    println!("L2 writer started | batch={}, file_rows={}", batch_size, rows_per_file);
    fs::create_dir_all(output_dir).await?;
    println!("L2 output dir ready: {}", output_dir);

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::UInt64, false),
        Field::new("coin", DataType::Utf8, false),
        Field::new("is_snapshot", DataType::Boolean, false),
        Field::new("side", DataType::Utf8, false),
        Field::new("price", DataType::Decimal128(28, 8), false),
        Field::new("size", DataType::Decimal128(28, 8), false),
        Field::new("num_orders", DataType::UInt64, false),
    ]));

    let mut writers: HashMap<String, AsyncArrowWriter<File>> = HashMap::new();
    let mut file_indices: HashMap<String, u32> = HashMap::new();
    let mut rows_in_current_file: HashMap<String, usize> = HashMap::new();

    let mut timestamps: Vec<u64> = Vec::with_capacity(batch_size);
    let mut coins: Vec<String> = Vec::with_capacity(batch_size);
    let mut is_snapshots: Vec<bool> = Vec::with_capacity(batch_size);
    let mut sides: Vec<String> = Vec::with_capacity(batch_size);
    let mut prices: Vec<Decimal> = Vec::with_capacity(batch_size);
    let mut sizes: Vec<Decimal> = Vec::with_capacity(batch_size);
    let mut num_orders: Vec<u64> = Vec::with_capacity(batch_size);

    let mut total_books = 0usize;

    while let Some(b) = rx.recv().await {
        let coin_lower = b.coin.to_lowercase();

        let is_snap = b.is_snapshot();
        let levels = b.bids().len() + b.asks().len();

        if levels == 0 {
            println!("[L2] empty book for {} – skipping", b.coin);
            continue;
        }

        for level in b.bids() {
            timestamps.push(b.time);
            coins.push(b.coin.clone());
            is_snapshots.push(is_snap);
            sides.push("bid".to_string());
            prices.push(level.px);
            sizes.push(level.sz);
            num_orders.push(level.n as u64);
        }

        for level in b.asks() {
            timestamps.push(b.time);
            coins.push(b.coin.clone());
            is_snapshots.push(is_snap);
            sides.push("ask".to_string());
            prices.push(level.px);
            sizes.push(level.sz);
            num_orders.push(level.n as u64);
        }

        let coin_rows = rows_in_current_file.entry(coin_lower.clone()).or_insert(0);
        *coin_rows += levels;

        total_books += 1;

        if total_books % 10 == 0 {
            println!(
                "[L2 STATUS] book #{} | coin={} | levels added={} | buffer rows={} | coin rows={} | writers open={}",
                total_books,
                coin_lower,
                levels,
                timestamps.len(),
                *coin_rows,
                writers.len()
            );
        }

        let should_flush = timestamps.len() >= batch_size || *coin_rows >= rows_per_file;

        if should_flush {
            println!(
                "[L2 FLUSH] rows={} | coin={} | buffer_full={} | file_limit={} | open_writers={}",
                timestamps.len(),
                coin_lower,
                timestamps.len() >= batch_size,
                *coin_rows >= rows_per_file,
                writers.len()
            );

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt64Array::from(std::mem::take(&mut timestamps))),
                    Arc::new(StringArray::from(std::mem::take(&mut coins))),
                    Arc::new(BooleanArray::from(std::mem::take(&mut is_snapshots))),
                    Arc::new(StringArray::from(std::mem::take(&mut sides))),
                    Arc::new(Decimal128Array::from_iter_values(
                        std::mem::take(&mut prices).into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap())
                    )),
                    Arc::new(Decimal128Array::from_iter_values(
                        std::mem::take(&mut sizes).into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap())
                    )),
                    Arc::new(UInt64Array::from(std::mem::take(&mut num_orders))),
                ],
            )?;

            let writer = writers.entry(coin_lower.clone()).or_insert_with(|| {
                let idx = file_indices.entry(coin_lower.clone()).or_insert(0);
                let part = *idx;
                *idx += 1;

                let path = format!("{}/l2_{}_part_{:04}.parquet", output_dir, coin_lower, part);
                println!("→ L2 CREATING FILE: {}", path);

                let rt = tokio::runtime::Handle::current();
                let file = rt.block_on(File::create(&path)).expect("L2 file create failed");

                println!("→ L2 writer created for {}", coin_lower);
                AsyncArrowWriter::try_new(file, schema.clone(), None).expect("L2 writer init failed")
            });

            println!("→ L2 writing {} rows to {}", batch.num_rows(), coin_lower);
            writer.write(&batch).await.expect("L2 write failed");
            println!("→ L2 flushing {}", coin_lower);
            writer.flush().await.expect("L2 flush failed");
            println!("→ L2 flush OK");

            if *coin_rows >= rows_per_file {
                *coin_rows = 0;
            }
        }
    }

    println!("[L2 SHUTDOWN] remaining rows={}, open writers={}", timestamps.len(), writers.len());

    if !timestamps.is_empty() || !writers.is_empty() {
        println!("[L2 FINAL] {} remaining rows | {} writers", timestamps.len(), writers.len());

        let mut owned_writers = std::mem::take(&mut writers);

        if timestamps.is_empty() {
            println!("[L2 FINAL] No data left, closing open writers");
            for (coin, w) in owned_writers {
                println!("Closing empty L2 writer for {}", coin);
                let _ = w.close().await;
            }
        } else {
            let mut by_coin: HashMap<String, Vec<usize>> = HashMap::new();
            for (i, c) in coins.iter().enumerate() {
                by_coin.entry(c.clone()).or_default().push(i);
            }

            for (coin_lower, idxs) in by_coin {
                if idxs.is_empty() { continue; }

                println!("→ L2 final batch for {}: {} rows", coin_lower, idxs.len());

                let mut tss = Vec::new();
                let mut cs = Vec::new();
                let mut iss = Vec::new();
                let mut sds = Vec::new();
                let mut ps = Vec::new();
                let mut szs = Vec::new();
                let mut nos = Vec::new();

                for &i in &idxs {
                    tss.push(timestamps[i]);
                    cs.push(coins[i].clone());
                    iss.push(is_snapshots[i]);
                    sds.push(sides[i].clone());
                    ps.push(prices[i]);
                    szs.push(sizes[i]);
                    nos.push(num_orders[i]);
                }

                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt64Array::from(tss)),
                        Arc::new(StringArray::from(cs)),
                        Arc::new(BooleanArray::from(iss)),
                        Arc::new(StringArray::from(sds)),
                        Arc::new(Decimal128Array::from_iter_values(ps.into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap()))),
                        Arc::new(Decimal128Array::from_iter_values(szs.into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap()))),
                        Arc::new(UInt64Array::from(nos)),
                    ],
                )?;

                let mut writer = if let Some(w) = owned_writers.remove(&coin_lower) {
                    w
                } else {
                    println!("→ Creating new writer for final L2 batch {}", coin_lower);

                    let idx = file_indices.entry(coin_lower.clone()).or_insert(0);
                    let part = *idx;
                    *idx += 1;

                    let path = format!("{}/l2_{}_part_{:04}.parquet", output_dir, coin_lower, part);
                    println!("→ Creating final file: {}", path);

                    let rt = tokio::runtime::Handle::current();
                    let file = rt.block_on(File::create(&path)).expect("Final L2 file create failed");

                    AsyncArrowWriter::try_new(file, schema.clone(), None).expect("Final L2 writer failed")
                };

                writer.write(&batch).await.expect("L2 final write failed");
                writer.flush().await.expect("L2 final flush failed");
                println!("→ L2 final batch + flush OK for {}", coin_lower);

                match writer.close().await {
                    Ok(_) => println!("→ L2 writer closed OK for {}", coin_lower),
                    Err(e) => eprintln!("→ L2 close failed for {}: {}", coin_lower, e),
                }
            }
        }
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    println!("L2 writer finished – processed {} books", total_books);

    Ok(())
}