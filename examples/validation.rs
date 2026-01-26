// examples/validation.rs

use anyhow::Result;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::path::Path;

fn validate_trades(file_path: &str) -> Result<()> {
    let file = File::open(file_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;

    let mut timestamps: Vec<u64> = Vec::new();
    let mut prices: Vec<f64> = Vec::new();
    let mut sizes: Vec<f64> = Vec::new();

    for batch in &batches {
        let ts_col = batch.column(0).as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap();
        let price_col = batch.column(2).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        let size_col = batch.column(3).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();

        for i in 0..batch.num_rows() {
            timestamps.push(ts_col.value(i));
            prices.push(price_col.value(i));
            sizes.push(size_col.value(i));
        }
    }

    let mut data: Vec<(u64, f64, f64)> = timestamps.into_iter().zip(prices).zip(sizes).map(|((t, p), s)| (t, p, s)).collect();
    data.sort_by_key(|&(t, _, _)| t);

    for (i, &(_, price, size)) in data.iter().enumerate() {
        if price <= 0.0 || size <= 0.0 {
            return Err(anyhow::anyhow!("Invalid value at row {}: price={} size={}", i, price, size));
        }
    }
    Ok(())
}

fn validate_candles(file_path: &str) -> Result<()> {
    let file = File::open(file_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;

    let mut open_times: Vec<u64> = Vec::new();
    let mut close_times: Vec<u64> = Vec::new();
    let mut opens: Vec<f64> = Vec::new();
    let mut highs: Vec<f64> = Vec::new();
    let mut lows: Vec<f64> = Vec::new();
    let mut closes: Vec<f64> = Vec::new();
    let mut volumes: Vec<f64> = Vec::new();

    for batch in &batches {
        let open_ts_col = batch.column(0).as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap();
        let close_ts_col = batch.column(1).as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap();
        let open_col = batch.column(4).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        let high_col = batch.column(6).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        let low_col = batch.column(7).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        let close_col = batch.column(5).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        let volume_col = batch.column(8).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();

        for i in 0..batch.num_rows() {
            open_times.push(open_ts_col.value(i));
            close_times.push(close_ts_col.value(i));
            opens.push(open_col.value(i));
            highs.push(high_col.value(i));
            lows.push(low_col.value(i));
            closes.push(close_col.value(i));
            volumes.push(volume_col.value(i));
        }
    }

    let mut data: Vec<(u64, u64, f64, f64, f64, f64, f64)> = open_times.into_iter().zip(close_times).zip(opens).zip(highs).zip(lows).zip(closes).zip(volumes)
        .map(|((((((ot, ct), o), h), l), c), v)| (ot, ct, o, h, l, c, v)).collect();
    data.sort_by_key(|&(ot, _, _, _, _, _, _)| ot);

    for (i, &(open_ts, close_ts, open, high, low, close, volume)) in data.iter().enumerate() {
        if open <= 0.0 || close <= 0.0 || high <= 0.0 || low <= 0.0 || volume < 0.0 {
            return Err(anyhow::anyhow!("Invalid value at row {}", i));
        }
        if low > high || open < low || open > high || close < low || close > high {
            return Err(anyhow::anyhow!("Invalid OHLC at row {}", i));
        }
        if open_ts >= close_ts {
            return Err(anyhow::anyhow!("Invalid times at row {}", i));
        }
    }
    Ok(())
}

fn validate_l2(file_path: &str) -> Result<()> {
    let file = File::open(file_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>()?;

    let mut timestamps: Vec<u64> = Vec::new();
    let mut prices: Vec<f64> = Vec::new();
    let mut sizes: Vec<f64> = Vec::new();
    let mut num_orders: Vec<u64> = Vec::new();

    for batch in &batches {
        let ts_col = batch.column(0).as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap();
        let price_col = batch.column(4).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        let size_col = batch.column(5).as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
        let num_orders_col = batch.column(6).as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap();

        for i in 0..batch.num_rows() {
            timestamps.push(ts_col.value(i));
            prices.push(price_col.value(i));
            sizes.push(size_col.value(i));
            num_orders.push(num_orders_col.value(i));
        }
    }

    let mut data: Vec<(u64, f64, f64, u64)> = timestamps.into_iter().zip(prices).zip(sizes).zip(num_orders).map(|(((t, p), s), n)| (t, p, s, n)).collect();
    data.sort_by_key(|&(t, _, _, _)| t);

    for (i, &(_, price, size, num)) in data.iter().enumerate() {
        if price <= 0.0 || size <= 0.0 || num == 0 {
            return Err(anyhow::anyhow!("Invalid value at row {}", i));
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let paths = [
        ("data/trades/trades_btc_part_0000.parquet", validate_trades as fn(&str) -> Result<()>),
        ("data/candles/candles_btc_1m_part_0000.parquet", validate_candles as fn(&str) -> Result<()>),
        ("data/l2/l2book_btc_part_0000.parquet", validate_l2 as fn(&str) -> Result<()>),
    ];

    for (path_str, validator) in paths {
        let path = Path::new(path_str);
        if path.exists() {
            validator(path_str)?;
            println!("Validated: {}", path_str);
        } else {
            println!("Skipped missing: {}", path_str);
        }
    }
    Ok(())
}