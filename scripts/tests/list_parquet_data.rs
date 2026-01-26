// src/main.rs
use anyhow::Result;
// mod parquet_reader;
use parquet_reader:
use std::fs;
use std::path::Path;

fn main() -> Result<()> {
    let dir = "candles_data";
    let path = Path::new(dir);

    if !path.is_dir() {
        println!("Directory '{}' not found", dir);
        return Ok(());
    }

    let mut entries: Vec<_> = fs::read_dir(path)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
        .map(|e| e.path())
        .collect();

    entries.sort(); // optional: sort by filename

    for entry in entries {
        println!("\nFile: {}", entry.display());
        read_and_print_parquet(&entry)?;
    }

    Ok(())
}
