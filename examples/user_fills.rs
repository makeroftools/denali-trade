// examples/print_user_fills.rs
use arrow::util::pretty::print_batches;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

fn main() -> anyhow::Result<()> {
    let file = File::open("data/user_fills/user_fills_btc_part_0000.parquet")?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let batches: Vec<_> = reader.collect::<Result<_, _>>()?;
    print_batches(&batches)?;
    Ok(())
}