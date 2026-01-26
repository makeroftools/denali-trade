// examples/demo.rs
// Demonstrates using the pipeline for trade processing.

use denali_trade::pipeline::{Builder, SendMode};
use denali_trade::trades::{clean_trade, should_persist};
use hypersdk::hypercore::types::Trade;
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (pipeline, rx_final, _tasks) = Builder::new()
            .input_cap(10)
            .process_cap(5)
            .output_cap(5)
            .process_workers(1)
            .filter_workers(1)
            .process_send_mode(SendMode::Blocking)
            .filter_send_mode(SendMode::Blocking)
            .preserve_order(true)
            .log_fn(|msg| println!("[PIPE] {}", msg))
            .build::<Trade, anyhow::Error, _, _, _, _>(
                |t: Trade| async move { Ok(clean_trade(t).await) },
                |t: &Trade| { let t = t.clone(); async move { Ok(should_persist(&t).await) } },
            );

        // Send dummy trade
        let dummy_trade = Trade {
            time: 1700000000000,
            coin: "BTC".to_string(),
            px: Decimal::from(50000),
            sz: Decimal::from(1),
            side: hypersdk::hypercore::types::Side::Buy,
        };
        pipeline.tx_input.send_async(dummy_trade).await.unwrap();

        drop(pipeline.tx_input);

        // Receive processed
        while let Ok(res) = rx_final.recv_async().await {
            match res {
                Ok(trade) => println!("Processed trade: {:?}", trade),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
    });
}