use anyhow::Result;
use arrow::array::{BooleanArray, StringArray, UInt64Array, Decimal128Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use hypersdk::hypercore::types::Fill;
use parquet::arrow::async_writer::AsyncArrowWriter;
use rust_decimal::{Decimal, MathematicalOps};
use rust_decimal::prelude::ToPrimitive;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio::sync::mpsc::Receiver;

pub async fn write_user_fills_to_parquet(rx: Receiver<Fill>) -> Result<()> {
    const ROWS_PER_FILE: usize = 100;
    // const BATCH_SIZE: usize = 8192;
    const BATCH_SIZE: usize = 100;
    const OUTPUT_DIR: &str = "data/user_fills";

    write_user_fills(rx, ROWS_PER_FILE, BATCH_SIZE, OUTPUT_DIR).await
}

async fn write_user_fills(
    mut rx: Receiver<Fill>,
    rows_per_file: usize,
    batch_size: usize,
    output_dir: &str,
) -> Result<()> {
    println!("User fills writer started | batch_size={}, rows_per_file={}", batch_size, rows_per_file);
    fs::create_dir_all(output_dir).await?;
    println!("Output directory ready: {}", output_dir);

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
        Field::new("mark_px", DataType::Decimal128(28, 8), false),
        Field::new("method", DataType::Utf8, true),
    ]));

    let mut writers: HashMap<String, AsyncArrowWriter<File>> = HashMap::new();
    let mut file_indices: HashMap<String, u32> = HashMap::new();
    let mut rows_in_current_file: HashMap<String, usize> = HashMap::new();

    let mut timestamps: Vec<u64> = Vec::with_capacity(batch_size);
    let mut coins: Vec<String> = Vec::with_capacity(batch_size);
    let mut prices: Vec<Decimal> = Vec::with_capacity(batch_size);
    let mut sizes: Vec<Decimal> = Vec::with_capacity(batch_size);
    let mut sides: Vec<String> = Vec::with_capacity(batch_size);
    let mut start_positions: Vec<Decimal> = Vec::with_capacity(batch_size);
    let mut dirs: Vec<String> = Vec::with_capacity(batch_size);
    let mut closed_pnls: Vec<Decimal> = Vec::with_capacity(batch_size);
    let mut tx_hashes: Vec<String> = Vec::with_capacity(batch_size);
    let mut oids: Vec<u64> = Vec::with_capacity(batch_size);
    let mut crosseds: Vec<bool> = Vec::with_capacity(batch_size);
    let mut fees: Vec<Decimal> = Vec::with_capacity(batch_size);
    let mut tids: Vec<u64> = Vec::with_capacity(batch_size);
    let mut cloids: Vec<String> = Vec::with_capacity(batch_size);
    let mut fee_tokens: Vec<String> = Vec::with_capacity(batch_size);
    let mut liquidated_users: Vec<String> = Vec::with_capacity(batch_size);
    let mut mark_pxs: Vec<Decimal> = Vec::with_capacity(batch_size);
    let mut methods: Vec<String> = Vec::with_capacity(batch_size);

    let mut total_fills: usize = 0;

    while let Some(f) = rx.recv().await {
        let coin_lower = f.coin.to_lowercase();

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
        cloids.push(f.cloid.map(|b| format!("0x{}", b)).unwrap_or_default());
        fee_tokens.push(f.fee_token.clone());
        liquidated_users.push(
            f.liquidation
                .as_ref()
                .map(|liq| liq.liquidated_user.clone())
                .unwrap_or_default(),
        );
        mark_pxs.push(
            f.liquidation
                .as_ref()
                .map(|liq| liq.mark_px)
                .unwrap_or(Decimal::ZERO),
        );
        methods.push(
            f.liquidation
                .as_ref()
                .map(|liq| liq.method.clone())
                .unwrap_or_default(),
        );

        let coin_rows = rows_in_current_file.entry(coin_lower.clone()).or_insert(0);
        *coin_rows += 1;
        total_fills += 1;

        if total_fills % 50 == 0 {  // more frequent status
            println!(
                "[STATUS] fill #{} | coin={} | buffer={}/{} | rows_this_coin={}/{} | total_writers={}",
                total_fills,
                coin_lower,
                timestamps.len(),
                batch_size,
                *coin_rows,
                rows_per_file,
                writers.len()
            );
        }

        let should_flush = timestamps.len() >= batch_size || *coin_rows >= rows_per_file;

        if should_flush {
            println!(
                "[FLUSH] rows={} | coin={} | buffer_full={} | file_limit={} | current_writers={}",
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
                    Arc::new(Decimal128Array::from_iter_values(std::mem::take(&mut prices).into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap()))),
                    Arc::new(Decimal128Array::from_iter_values(std::mem::take(&mut sizes).into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap()))),
                    Arc::new(StringArray::from(std::mem::take(&mut sides))),
                    Arc::new(Decimal128Array::from_iter_values(std::mem::take(&mut start_positions).into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap()))),
                    Arc::new(StringArray::from(std::mem::take(&mut dirs))),
                    Arc::new(Decimal128Array::from_iter_values(std::mem::take(&mut closed_pnls).into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap()))),
                    Arc::new(StringArray::from(std::mem::take(&mut tx_hashes))),
                    Arc::new(UInt64Array::from(std::mem::take(&mut oids))),
                    Arc::new(BooleanArray::from(std::mem::take(&mut crosseds))),
                    Arc::new(Decimal128Array::from_iter_values(std::mem::take(&mut fees).into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap()))),
                    Arc::new(UInt64Array::from(std::mem::take(&mut tids))),
                    Arc::new(StringArray::from(std::mem::take(&mut cloids))),
                    Arc::new(StringArray::from(std::mem::take(&mut fee_tokens))),
                    Arc::new(StringArray::from(std::mem::take(&mut liquidated_users))),
                    Arc::new(Decimal128Array::from_iter_values(std::mem::take(&mut mark_pxs).into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap()))),
                    Arc::new(StringArray::from(std::mem::take(&mut methods))),
                ],
            )?;

            let writer_entry = writers.entry(coin_lower.clone()).or_insert_with(|| {
                let idx = file_indices.entry(coin_lower.clone()).or_insert(0);
                let part = *idx;
                *idx += 1;

                let path = format!("{}/user_fills_{}_part_{:04}.parquet", output_dir, coin_lower, part);
                println!("→ CREATING FILE: {}", path);

                let rt = tokio::runtime::Handle::current();
                let file = rt.block_on(File::create(&path)).expect("File create failed");

                println!("→ Writer created for {}", coin_lower);
                AsyncArrowWriter::try_new(file, schema.clone(), None).expect("Writer init failed")
            });

            println!("→ Writing {} rows to {}", batch.num_rows(), coin_lower);
            writer_entry.write(&batch).await.expect("write failed");
            println!("→ Flushing {}", coin_lower);
            writer_entry.flush().await.expect("flush failed");
            println!("→ Flush OK for {}", coin_lower);

            if *coin_rows >= rows_per_file {
                *coin_rows = 0;
            }
        }
    }

    println!("[SHUTDOWN] Final cleanup | remaining rows={}, active writers={}", timestamps.len(), writers.len());

    if !timestamps.is_empty() || !writers.is_empty() {
        println!("[FINAL] {} remaining rows | {} open writers", timestamps.len(), writers.len());

        let mut owned_writers = std::mem::take(&mut writers);

        if timestamps.is_empty() {
            println!("[FINAL] No remaining data, closing existing writers");
            for (coin, w) in owned_writers {
                println!("Closing empty writer for {}", coin);
                let _ = w.close().await;
            }
        } else {
            println!("[FINAL] Building per-coin final batches");

            let mut by_coin: HashMap<String, Vec<usize>> = HashMap::new();
            for (i, c) in coins.iter().enumerate() {
                by_coin.entry(c.clone()).or_default().push(i);
            }

            for (coin_lower, idxs) in by_coin {
                if idxs.is_empty() { continue; }

                println!("→ Final batch for {}: {} rows", coin_lower, idxs.len());

                let mut tss = Vec::new(); let mut cs = Vec::new(); let mut ps = Vec::new(); let mut szs = Vec::new();
                let mut sds = Vec::new(); let mut sts = Vec::new(); let mut ds = Vec::new(); let mut cpns = Vec::new();
                let mut ths = Vec::new(); let mut os = Vec::new(); let mut crs = Vec::new(); let mut fs = Vec::new();
                let mut ts = Vec::new(); let mut cls = Vec::new(); let mut fts = Vec::new(); let mut lus = Vec::new();
                let mut mps = Vec::new(); let mut mds = Vec::new();

                for &i in &idxs {
                    tss.push(timestamps[i]);
                    cs.push(coins[i].clone());
                    ps.push(prices[i]);
                    szs.push(sizes[i]);
                    sds.push(sides[i].clone());
                    sts.push(start_positions[i]);
                    ds.push(dirs[i].clone());
                    cpns.push(closed_pnls[i]);
                    ths.push(tx_hashes[i].clone());
                    os.push(oids[i]);
                    crs.push(crosseds[i]);
                    fs.push(fees[i]);
                    ts.push(tids[i]);
                    cls.push(cloids[i].clone());
                    fts.push(fee_tokens[i].clone());
                    lus.push(liquidated_users[i].clone());
                    mps.push(mark_pxs[i]);
                    mds.push(methods[i].clone());
                }

                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt64Array::from(tss)),
                        Arc::new(StringArray::from(cs)),
                        Arc::new(Decimal128Array::from_iter_values(ps.into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap()))),
                        Arc::new(Decimal128Array::from_iter_values(szs.into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap()))),
                        Arc::new(StringArray::from(sds)),
                        Arc::new(Decimal128Array::from_iter_values(sts.into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap()))),
                        Arc::new(StringArray::from(ds)),
                        Arc::new(Decimal128Array::from_iter_values(cpns.into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap()))),
                        Arc::new(StringArray::from(ths)),
                        Arc::new(UInt64Array::from(os)),
                        Arc::new(BooleanArray::from(crs)),
                        Arc::new(Decimal128Array::from_iter_values(fs.into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap()))),
                        Arc::new(UInt64Array::from(ts)),
                        Arc::new(StringArray::from(cls)),
                        Arc::new(StringArray::from(fts)),
                        Arc::new(StringArray::from(lus)),
                        Arc::new(Decimal128Array::from_iter_values(mps.into_iter().map(|d| (d * Decimal::TEN.powu(8)).to_i128().unwrap()))),
                        Arc::new(StringArray::from(mds)),
                    ],
                )?;

                if let Some(mut w) = owned_writers.remove(&coin_lower) {
                    w.write(&batch).await.expect("final write");
                    w.flush().await.expect("final flush");
                    println!("→ Final batch + flush OK for {}", coin_lower);
                    let _ = w.close().await;
                    println!("→ Closed {}", coin_lower);
                } else {
                    println!("→ No writer for {} in final flush", coin_lower);
                }
            }
        }
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    println!("Writer task ended – total fills: {}", total_fills);

    Ok(())
}