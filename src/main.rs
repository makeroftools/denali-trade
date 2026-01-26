// main.rs
mod pipeline;
mod trades;
mod candles;
mod l2;
mod user_fills;

use anyhow::Result;
use futures::StreamExt;
use hypersdk::hypercore::{self, types::{Trade, Candle, Incoming, Subscription, L2Book, Fill}};
use hypersdk::Address;
use pipeline::{Builder, SendMode};
use std::collections::HashMap;
use tokio::signal;
use tokio::sync::mpsc;
use trades::{clean_trade, should_persist as should_persist_trade, write_trades_to_parquet};
use candles::{clean_candle, should_persist as should_persist_candle, write_candles_to_parquet};
use l2::{clean_l2book, should_persist as should_persist_l2, write_l2_to_parquet};
use user_fills::{clean_fill, should_persist as should_persist_fill, write_user_fills_to_parquet};

#[tokio::main]
async fn main() -> Result<()> {
    let coins: Vec<String> = vec!["BTC".into(), "ETH".into(), "SOL".into()];
    let interval = "1m".to_string();
    let user_address = "0x0c3391b8cC5CA37b4a8f1E01432dAF1a933394cF".to_string(); // Replace with actual user address
    let user: Address = user_address.parse().expect("Invalid user address");

    let mut ws = hypercore::mainnet_ws();

    for coin in &coins {
        ws.subscribe(Subscription::Trades { coin: coin.clone() });
        ws.subscribe(Subscription::Candle { coin: coin.clone(), interval: interval.clone() });
        ws.subscribe(Subscription::L2Book { coin: coin.clone() });
    }
    ws.subscribe(Subscription::UserFills { user });

    // Trades setups
    let mut trades_pipelines: HashMap<String, pipeline::Pipeline<Trade>> = HashMap::new();
    let mut trades_tasks: HashMap<String, Vec<tokio::task::JoinHandle<()>>> = HashMap::new();
    let mut trades_error_handles: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut trades_writer_handles: HashMap<String, tokio::task::JoinHandle<Result<()>>> = HashMap::new();
    let mut trades_tx_writers: HashMap<String, mpsc::Sender<Trade>> = HashMap::new();

    for coin in &coins {
        let coin_upper = coin.to_uppercase();
        let log_coin_upper = coin_upper.clone();
        let err_coin_upper = coin_upper.clone();

        let (pipeline, rx_final, tasks) = Builder::new()
            .input_cap(4096)
            .process_cap(2048)
            .output_cap(512)
            .process_workers(2)
            .filter_workers(1)
            .process_send_mode(SendMode::Blocking)
            .filter_send_mode(SendMode::TryDrop)
            .log_fn(move |msg| println!("[PIPE_TRADES_{}] {}", log_coin_upper, msg))
            .build::<Trade, anyhow::Error, _, _, _, _>(
                |t: Trade| async move { Ok(clean_trade(t).await) },
                |t: &Trade| { let t = t.clone(); async move { Ok(should_persist_trade(&t).await) } },
            );

        let (tx_writer, rx_writer) = mpsc::channel::<Trade>(8192);
        let tx_writer_clone = tx_writer.clone();

        let writer_handle = tokio::spawn(write_trades_to_parquet(rx_writer));
        let error_handle = tokio::spawn(async move {
            while let Ok(res) = rx_final.recv_async().await {
                match res {
                    Ok(trade) => { let _ = tx_writer_clone.send(trade).await; }
                    Err(e) => eprintln!("[TRADES_{}] Pipeline error: {}", err_coin_upper, e),
                }
            }
        });

        trades_pipelines.insert(coin.clone(), pipeline);
        trades_tasks.insert(coin.clone(), tasks);
        trades_error_handles.insert(coin.clone(), error_handle);
        trades_writer_handles.insert(coin.clone(), writer_handle);
        trades_tx_writers.insert(coin.clone(), tx_writer);
    }

    // Candles setups
    let mut candles_pipelines: HashMap<String, pipeline::Pipeline<Candle>> = HashMap::new();
    let mut candles_tasks: HashMap<String, Vec<tokio::task::JoinHandle<()>>> = HashMap::new();
    let mut candles_error_handles: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut candles_writer_handles: HashMap<String, tokio::task::JoinHandle<Result<()>>> = HashMap::new();
    let mut candles_tx_writers: HashMap<String, mpsc::Sender<Candle>> = HashMap::new();

    for coin in &coins {
        let coin_upper = coin.to_uppercase();
        let log_coin_upper = coin_upper.clone();
        let err_coin_upper = coin_upper.clone();

        let (pipeline, rx_final, tasks) = Builder::new()
            .input_cap(2048)
            .process_cap(1024)
            .output_cap(512)
            .process_workers(2)
            .filter_workers(1)
            .process_send_mode(SendMode::Blocking)
            .filter_send_mode(SendMode::TryDrop)
            .log_fn(move |msg| println!("[PIPE_CANDLES_{}] {}", log_coin_upper, msg))
            .build::<Candle, anyhow::Error, _, _, _, _>(
                |c: Candle| async move { Ok(clean_candle(c).await) },
                |c: &Candle| { let c = c.clone(); async move { Ok(should_persist_candle(&c).await) } },
            );

        let (tx_writer, rx_writer) = mpsc::channel::<Candle>(8192);
        let tx_writer_clone = tx_writer.clone();

        let writer_handle = tokio::spawn(write_candles_to_parquet(rx_writer));
        let error_handle = tokio::spawn(async move {
            while let Ok(res) = rx_final.recv_async().await {
                match res {
                    Ok(candle) => { let _ = tx_writer_clone.send(candle).await; }
                    Err(e) => eprintln!("[CANDLES_{}] Pipeline error: {}", err_coin_upper, e),
                }
            }
        });

        candles_pipelines.insert(coin.clone(), pipeline);
        candles_tasks.insert(coin.clone(), tasks);
        candles_error_handles.insert(coin.clone(), error_handle);
        candles_writer_handles.insert(coin.clone(), writer_handle);
        candles_tx_writers.insert(coin.clone(), tx_writer);
    }

    // L2 setups
    let mut l2_pipelines: HashMap<String, pipeline::Pipeline<L2Book>> = HashMap::new();
    let mut l2_tasks: HashMap<String, Vec<tokio::task::JoinHandle<()>>> = HashMap::new();
    let mut l2_error_handles: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut l2_writer_handles: HashMap<String, tokio::task::JoinHandle<Result<()>>> = HashMap::new();
    let mut l2_tx_writers: HashMap<String, mpsc::Sender<L2Book>> = HashMap::new();

    for coin in &coins {
        let coin_upper = coin.to_uppercase();
        let log_coin_upper = coin_upper.clone();
        let err_coin_upper = coin_upper.clone();

        let (pipeline, rx_final, tasks) = Builder::new()
            .input_cap(2048)
            .process_cap(1024)
            .output_cap(512)
            .process_workers(2)
            .filter_workers(1)
            .process_send_mode(SendMode::Blocking)
            .filter_send_mode(SendMode::TryDrop)
            .log_fn(move |msg| println!("[PIPE_L2_{}] {}", log_coin_upper, msg))
            .build::<L2Book, anyhow::Error, _, _, _, _>(
                |b: L2Book| async move { Ok(clean_l2book(b).await) },
                |b: &L2Book| { let b = b.clone(); async move { Ok(should_persist_l2(&b).await) } },
            );

        let (tx_writer, rx_writer) = mpsc::channel::<L2Book>(8192);
        let tx_writer_clone = tx_writer.clone();

        let writer_handle = tokio::spawn(write_l2_to_parquet(rx_writer));
        let error_handle = tokio::spawn(async move {
            while let Ok(res) = rx_final.recv_async().await {
                match res {
                    Ok(book) => { let _ = tx_writer_clone.send(book).await; }
                    Err(e) => eprintln!("[L2_{}] Pipeline error: {}", err_coin_upper, e),
                }
            }
        });

        l2_pipelines.insert(coin.clone(), pipeline);
        l2_tasks.insert(coin.clone(), tasks);
        l2_error_handles.insert(coin.clone(), error_handle);
        l2_writer_handles.insert(coin.clone(), writer_handle);
        l2_tx_writers.insert(coin.clone(), tx_writer);
    }

    // User Fills setups (single for the user)
    let user_upper = user_address.to_uppercase();
    let log_user_upper = user_upper.clone();
    let err_user_upper = user_upper.clone();

    let (user_fills_pipeline, user_fills_rx_final, user_fills_tasks) = Builder::new()
        .input_cap(2048)
        .process_cap(1024)
        .output_cap(512)
        .process_workers(2)
        .filter_workers(1)
        .process_send_mode(SendMode::Blocking)
        .filter_send_mode(SendMode::TryDrop)
        .log_fn(move |msg| println!("[PIPE_USER_FILLS_{}] {}", log_user_upper, msg))
        .build::<Fill, anyhow::Error, _, _, _, _>(
            |f: Fill| async move { Ok(clean_fill(f).await) },
            |f: &Fill| { let f = f.clone(); async move { Ok(should_persist_fill(&f).await) } },
        );

    let (user_fills_tx_writer, user_fills_rx_writer) = mpsc::channel::<Fill>(8192);
    let user_fills_tx_writer_clone = user_fills_tx_writer.clone();

    let user_fills_writer_handle = tokio::spawn(write_user_fills_to_parquet(user_fills_rx_writer));
    let user_fills_error_handle = tokio::spawn(async move {
        while let Ok(res) = user_fills_rx_final.recv_async().await {
            match res {
                Ok(fill) => { let _ = user_fills_tx_writer_clone.send(fill).await; }
                Err(e) => eprintln!("[USER_FILLS_{}] Pipeline error: {}", err_user_upper, e),
            }
        }
    });

    let mut i = 0;
    loop {
        tokio::select! {
            Some(msg) = ws.next() => {
                i += 1;
                match msg {
                    Incoming::Trades(trades) => {
                        if !trades.is_empty() {
                            let coin = trades[0].coin.clone();
                            if let Some(pipeline) = trades_pipelines.get(&coin) {
                                for trade in trades {
                                    let _ = pipeline.tx_input.send_async(trade).await;
                                }
                            }
                        }
                    }
                    Incoming::Candle(candle) => {
                        let coin = candle.coin.clone();
                        if let Some(pipeline) = candles_pipelines.get(&coin) {
                            let _ = pipeline.tx_input.send_async(candle).await;
                        }
                    }
                    Incoming::L2Book(book) => {
                        let coin = book.coin.clone();
                        if let Some(pipeline) = l2_pipelines.get(&coin) {
                            let _ = pipeline.tx_input.send_async(book).await;
                        }
                    }
                    Incoming::UserFills { fills, .. } => {
                        if !fills.is_empty() {
                            for fill in fills {
                                let _ = user_fills_pipeline.tx_input.send_async(fill).await;
                            }
                        }
                    }
                    _ => {}
                }
                if i % 100 == 0 {
                    println!("Processed {} messages", i);
                }
            }
            _ = signal::ctrl_c() => {
                println!("\nCtrl+C → shutting down");
                for coin in &coins {
                    trades_pipelines.remove(coin);
                    candles_pipelines.remove(coin);
                    l2_pipelines.remove(coin);
                }
                break;
            }
        }
    }

    for coin in &coins {
        trades_tx_writers.remove(coin);
        candles_tx_writers.remove(coin);
        l2_tx_writers.remove(coin);
    }

    for tasks in trades_tasks.into_values() {
        for task in tasks {
            let _ = task.await;
        }
    }
    for tasks in candles_tasks.into_values() {
        for task in tasks {
            let _ = task.await;
        }
    }
    for tasks in l2_tasks.into_values() {
        for task in tasks {
            let _ = task.await;
        }
    }
    for task in user_fills_tasks {
        let _ = task.await;
    }

    for handle in trades_error_handles.into_values() {
        let _ = handle.await;
    }
    for handle in candles_error_handles.into_values() {
        let _ = handle.await;
    }
    for handle in l2_error_handles.into_values() {
        let _ = handle.await;
    }
    let _ = user_fills_error_handle.await;

    for handle in trades_writer_handles.into_values() {
        let _ = handle.await;
    }
    for handle in candles_writer_handles.into_values() {
        let _ = handle.await;
    }
    for handle in l2_writer_handles.into_values() {
        let _ = handle.await;
    }
    let _ = user_fills_writer_handle.await;

    println!("Done");
    Ok(())
}