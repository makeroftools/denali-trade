use anyhow::Result;
use arrow::array::{BooleanArray, Float64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hypersdk::hypercore::types::Fill;
use num_traits::cast::ToPrimitive;
use parquet::arrow::async_writer::AsyncArrowWriter;
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::sync::mpsc::Receiver;

pub async fn write_user_fills_to_parquet(rx: Receiver<Fill>) -> Result<()> {
    const ROWS_PER_FILE: usize = 10000;
    const BATCH_SIZE: usize = 8192;
    const OUTPUT_DIR: &str = "data/user_fills";
    write_user_fills(rx, ROWS_PER_FILE, BATCH_SIZE, OUTPUT_DIR).await
}

async fn write_user_fills(
    mut rx: Receiver<Fill>,
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
        Field::new("start_position", DataType::Float64, false),
        Field::new("dir", DataType::Utf8, false),
        Field::new("closed_pnl", DataType::Float64, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("oid", DataType::UInt64, false),
        Field::new("crossed", DataType::Boolean, false),
        Field::new("fee", DataType::Float64, false),
        Field::new("tid", DataType::UInt64, false),
        Field::new("cloid", DataType::Utf8, true),
        Field::new("fee_token", DataType::Utf8, false),
        Field::new("liquidated_user", DataType::Utf8, true),
        Field::new("mark_px", DataType::Float64, true),
        Field::new("method", DataType::Utf8, true),
    ]));

    let mut file_idx = 0u32;
    let mut rows_in_current_file = 0usize;
    let mut writer: Option<AsyncArrowWriter<File>> = None;

    let mut timestamps: Vec<u64> = Vec::with_capacity(batch_size);
    let mut coins: Vec<String> = Vec::with_capacity(batch_size);
    let mut prices: Vec<f64> = Vec::with_capacity(batch_size);
    let mut sizes: Vec<f64> = Vec::with_capacity(batch_size);
    let mut sides: Vec<String> = Vec::with_capacity(batch_size);
    let mut start_positions: Vec<f64> = Vec::with_capacity(batch_size);
    let mut dirs: Vec<String> = Vec::with_capacity(batch_size);
    let mut closed_pnls: Vec<f64> = Vec::with_capacity(batch_size);
    let mut tx_hashes: Vec<String> = Vec::with_capacity(batch_size);
    let mut oids: Vec<u64> = Vec::with_capacity(batch_size);
    let mut crosseds: Vec<bool> = Vec::with_capacity(batch_size);
    let mut fees: Vec<f64> = Vec::with_capacity(batch_size);
    let mut tids: Vec<u64> = Vec::with_capacity(batch_size);
    let mut cloids: Vec<Option<String>> = Vec::with_capacity(batch_size);
    let mut fee_tokens: Vec<String> = Vec::with_capacity(batch_size);
    let mut liquidated_users: Vec<Option<String>> = Vec::with_capacity(batch_size);
    let mut mark_pxs: Vec<Option<f64>> = Vec::with_capacity(batch_size);
    let mut methods: Vec<Option<String>> = Vec::with_capacity(batch_size);

    let mut total_fills = 0usize;
    let mut expected_coin: Option<String> = None;

    while let Some(f) = rx.recv().await {
        if let Some(ref ec) = expected_coin {
            if ec != &f.coin {
                eprintln!("Mixed coins detected: expected {}, got {}", ec, f.coin);
                continue;
            }
        } else {
            expected_coin = Some(f.coin.clone());
        }

        timestamps.push(f.time);
        coins.push(f.coin.clone());
        prices.push(f.px.to_f64().expect("price → f64"));
        sizes.push(f.sz.to_f64().expect("size → f64"));
        sides.push(f.side.to_string());
        start_positions.push(f.start_position.to_f64().expect("start_position → f64"));
        dirs.push(f.dir.clone());
        closed_pnls.push(f.closed_pnl.to_f64().expect("closed_pnl → f64"));
        tx_hashes.push(f.hash.clone());
        oids.push(f.oid);
        crosseds.push(f.crossed);
        fees.push(f.fee.to_f64().expect("fee → f64"));
        tids.push(f.tid);
        cloids.push(f.cloid.map(|b| format!("0x{}", (0..16).map(|i| format!("{:02x}", b[i])).collect::<String>())));
        fee_tokens.push(f.fee_token.clone());
        liquidated_users.push(f.liquidation.as_ref().map(|l| l.liquidated_user.clone()));
        mark_pxs.push(f.liquidation.as_ref().map(|l| l.mark_px.to_f64().expect("mark_px → f64")));
        methods.push(f.liquidation.as_ref().map(|l| l.method.clone()));

        total_fills += 1;
        rows_in_current_file += 1;

        if total_fills % 100 == 0 {
            println!("");
            println!("Received fill #{} (price={:.2})", total_fills, f.px.to_f64().expect("price → f64"));
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
                    Arc::new(Float64Array::from(std::mem::take(&mut start_positions))),
                    Arc::new(StringArray::from(std::mem::take(&mut dirs))),
                    Arc::new(Float64Array::from(std::mem::take(&mut closed_pnls))),
                    Arc::new(StringArray::from(std::mem::take(&mut tx_hashes))),
                    Arc::new(UInt64Array::from(std::mem::take(&mut oids))),
                    Arc::new(BooleanArray::from(std::mem::take(&mut crosseds))),
                    Arc::new(Float64Array::from(std::mem::take(&mut fees))),
                    Arc::new(UInt64Array::from(std::mem::take(&mut tids))),
                    Arc::new(StringArray::from(std::mem::take(&mut cloids))),
                    Arc::new(StringArray::from(std::mem::take(&mut fee_tokens))),
                    Arc::new(StringArray::from(std::mem::take(&mut liquidated_users))),
                    Arc::new(Float64Array::from(std::mem::take(&mut mark_pxs))),
                    Arc::new(StringArray::from(std::mem::take(&mut methods))),
                ],
            )?;

            if writer.is_none() {
                let coin_lower = expected_coin.as_ref().unwrap().to_lowercase();
                let file_prefix = format!("user_fills_{}", coin_lower);
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
                Arc::new(Float64Array::from(start_positions)),
                Arc::new(StringArray::from(dirs)),
                Arc::new(Float64Array::from(closed_pnls)),
                Arc::new(StringArray::from(tx_hashes)),
                Arc::new(UInt64Array::from(oids)),
                Arc::new(BooleanArray::from(crosseds)),
                Arc::new(Float64Array::from(fees)),
                Arc::new(UInt64Array::from(tids)),
                Arc::new(StringArray::from(cloids)),
                Arc::new(StringArray::from(fee_tokens)),
                Arc::new(StringArray::from(liquidated_users)),
                Arc::new(Float64Array::from(mark_pxs)),
                Arc::new(StringArray::from(methods)),
            ],
            )?;

        if writer.is_none() {
            let coin_lower = expected_coin.as_ref().unwrap().to_lowercase();
            let file_prefix = format!("user_fills_{}", coin_lower);
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

    println!("Writer task finished – total fills processed: {}", total_fills);

    Ok(())
}