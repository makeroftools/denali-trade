// src/main.rs
use anyhow::Result;

// mod parquet_reader;

fn main() -> Result<()> {
    println!("Reading all Parquet files in 'candles_data'...\n");

    // parquet_reader::read_all_in_directory("candles_data")?;
    parquet_reader::list_parquet_files("/home/makeroftools/github/denali-trade/candles_data")?;

    let dir = "/home/makeroftools/github/denali-trade/candles_data";
    println!("Trying to read: {}", dir);
    println!("Current working dir: {:?}", std::env::current_dir().unwrap_or_default());
    
    let files = parquet_reader::list_parquet_files(dir)?;
    println!("Found {} files", files.len());
    for f in &files {
        println!(" - {}", f.display());
    }
    Ok(())
}
