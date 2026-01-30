use anyhow::Result;
use arrow::array::{BooleanArray, StringArray, UInt64Array, Decimal128Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hypersdk::hypercore::types::Fill;
use num_traits::cast::ToPrimitive;
use parquet::arrow::async_writer::AsyncArrowWriter;
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::sync::mpsc::Receiver;
use rust_decimal::{MathematicalOps, self, Decimal};

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
        Field::new("price", DataType::Decimal128(28, 8), false),
        Field::new("size", DataType::Decimal128(28, 8), false),
        Field::new("side", DataType::Utf8, false),
        Field::new("start_position", DataType::Decimal128(28, 8), false),
        Field::new("dir", DataType::Utf8, false),
        Field::new("closed_pnl", DataType::Decimal128(28, 8), false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("oid", DataType::UInt64, false),
        Field::new("crossed", DataType::Boolean, false),
        Field::new("fee", DataType::Decimal128(28, 8), false),
        Field::new("tid", DataType::UInt64, false),
        Field::new("cloid", DataType::Utf8, true),
        Field::new("fee_token", DataType::Utf8, false),
        Field::new("liquidated_user", DataType::Utf8, true),
        Field::new("px", DataType::Decimal128(28, 8), false),
        Field::new("method", DataType::Utf8, true),
    ]));

    let mut file_idx = 0u32;
    let mut rows_in_current_file = 0usize;
    let mut writer: Option<AsyncArrowWriter<File>> = None;

    let mut timestamps: Vec<u64> = Vec::with_capacity(batch_size);
    let mut coins: Vec<String> = Vec::with_capacity(batch_size);
    let mut prices: Vec<rust_decimal::Decimal> = Vec::with_capacity(batch_size);
    let mut sizes: Vec<rust_decimal::Decimal> = Vec::with_capacity(batch_size);
    let mut sides: Vec<String> = Vec::with_capacity(batch_size);
    let mut start_positions: Vec<rust_decimal::Decimal> = Vec::with_capacity(batch_size);
    let mut dirs: Vec<String> = Vec::with_capacity(batch_size);
    let mut closed_pnls: Vec<rust_decimal::Decimal> = Vec::with_capacity(batch_size);
    let mut tx_hashes: Vec<String> = Vec::with_capacity(batch_size);
    let mut oids: Vec<u64> = Vec::with_capacity(batch_size);
    let mut crosseds: Vec<bool> = Vec::with_capacity(batch_size);
    let mut fees: Vec<rust_decimal::Decimal> = Vec::with_capacity(batch_size);
    let mut tids: Vec<u64> = Vec::with_capacity(batch_size);
    let mut cloids: Vec<Option<String>> = Vec::with_capacity(batch_size);
    let mut fee_tokens: Vec<String> = Vec::with_capacity(batch_size);
    let mut liquidated_users: Vec<Option<String>> = Vec::with_capacity(batch_size);
    let mut mark_pxs: Vec<rust_decimal::Decimal> = Vec::with_capacity(batch_size);    let mut methods: Vec<Option<String>> = Vec::with_capacity(batch_size);

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
        prices.push(f.px);
        sizes.push(f.sz);
        sides.push(f.side.to_string());
        start_positions.push(f.start_position);
        dirs.push(f.dir.clone());
        closed_pnls.push(f.closed_pnl);
        tx_hashes.push(f.hash.clone());
        oids.push(f.oid);
        crosseds.push(f.crossed);
        fees.push(f.fee);
        tids.push(f.tid);
        cloids.push(f.cloid.map(|b| format!("0x{}", (0..16).map(|i| format!("{:02x}", b[i])).collect::<String>())));
        fee_tokens.push(f.fee_token.clone());
        liquidated_users.push(f.liquidation.as_ref().map(|l| l.liquidated_user.clone()));
        mark_pxs.push(f.px);

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
                    Arc::new(Decimal128Array::from_iter_values(
                        std::mem::take(&mut prices).into_iter().map(|d| (d * rust_decimal::Decimal::TEN.powu(8)).to_i128().unwrap())
                    )),
                    Arc::new(Decimal128Array::from_iter_values(
                        std::mem::take(&mut sizes).into_iter().map(|d| (d * rust_decimal::Decimal::TEN.powu(8)).to_i128().unwrap())
                    )),
                    Arc::new(StringArray::from(std::mem::take(&mut sides))),
                    Arc::new(Decimal128Array::from_iter_values(
                        std::mem::take(&mut start_positions).into_iter().map(|d| (d * rust_decimal::Decimal::TEN.powu(8)).to_i128().unwrap())
                    )),
                    Arc::new(StringArray::from(std::mem::take(&mut dirs))),
                    Arc::new(Decimal128Array::from_iter_values(
                        std::mem::take(&mut closed_pnls).into_iter().map(|d| (d * rust_decimal::Decimal::TEN.powu(8)).to_i128().unwrap())
                    )),
                    Arc::new(StringArray::from(std::mem::take(&mut tx_hashes))),
                    Arc::new(UInt64Array::from(std::mem::take(&mut oids))),
                    Arc::new(BooleanArray::from(std::mem::take(&mut crosseds))),
                    Arc::new(Decimal128Array::from_iter_values(
                        std::mem::take(&mut fees).into_iter().map(|d| (d * rust_decimal::Decimal::TEN.powu(8)).to_i128().unwrap())
                    )),
                    Arc::new(UInt64Array::from(std::mem::take(&mut tids))),
                    Arc::new(StringArray::from(std::mem::take(&mut cloids))),
                    Arc::new(StringArray::from(std::mem::take(&mut fee_tokens))),
                    Arc::new(StringArray::from(std::mem::take(&mut liquidated_users))),
                    Arc::new(Decimal128Array::from_iter_values(
                        std::mem::take(&mut mark_pxs).into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap())
                    )),
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
                    // tokio::time::sleep(std::time::Duration::from_secs(4)).await;
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
                Arc::new(Decimal128Array::from_iter_values(
                            prices.into_iter().map(|d| (d * rust_decimal::Decimal::TEN.powu(8)).to_i128().unwrap())
                        )),                
                Arc::new(Decimal128Array::from_iter_values(
                            sizes.into_iter().map(|d| (d * rust_decimal::Decimal::TEN.powu(8)).to_i128().unwrap())
                        )),
                Arc::new(StringArray::from(sides)),
                Arc::new(Decimal128Array::from_iter_values(
                            start_positions.into_iter().map(|d| (d * rust_decimal::Decimal::TEN.powu(8)).to_i128().unwrap())
                        )),
                Arc::new(StringArray::from(dirs)),
                Arc::new(Decimal128Array::from_iter_values(
                            closed_pnls.into_iter().map(|d| (d * rust_decimal::Decimal::TEN.powu(8)).to_i128().unwrap())
                        )),
                Arc::new(StringArray::from(tx_hashes)),
                Arc::new(UInt64Array::from(oids)),
                Arc::new(BooleanArray::from(crosseds)),
                Arc::new(Decimal128Array::from_iter_values(
                            fees.into_iter().map(|d| (d * rust_decimal::Decimal::TEN.powu(8)).to_i128().unwrap())
                        )),
                Arc::new(UInt64Array::from(tids)),
                Arc::new(StringArray::from(cloids)),
                Arc::new(StringArray::from(fee_tokens)),
                Arc::new(StringArray::from(liquidated_users)),
                Arc::new(Decimal128Array::from_iter_values(
                            mark_pxs.into_iter().map(|d| (d * rust_decimal::Decimal::TEN.powu(8)).to_i128().unwrap())
                        )),
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
        // tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;
    }

    println!("Writer task finished – total fills processed: {}", total_fills);

    Ok(())
}