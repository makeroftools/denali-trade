use tokio::sync::mpsc;
use futures::StreamExt;
// ... keep your existing imports

#[tokio::main]
async fn main() -> Result<()> {
    let (tx_raw, rx_raw)     = mpsc::channel(4096);     // websocket → raw candles
    let (tx_clean, rx_clean) = mpsc::channel(2048);     // cleaning/normalization
    let (tx_agg,   rx_agg)   = mpsc::channel(1024);     // aggregation / filtering

    // Stage 1: websocket → raw sender
    let ws_handle = tokio::spawn(async move {
        let mut ws = hypercore::mainnet_ws();
        ws.subscribe(Subscription::Candle { coin: "BTC".into(), interval: "1m".into() });

        let mut i = 0;
        loop {
            tokio::select! {
                Some(Incoming::Candle(candle)) = ws.next() => {
                    let _ = tx_raw.send(candle).await;
                    i += 1;
                    if i % 100 == 0 { println!("Received {}", i); }
                }
                _ = signal::ctrl_c() => break,
                else => break,
            }
        }
        drop(tx_raw);
    });

    // Stage 2: clean/normalize/filter → send to next
    let clean_handle = tokio::spawn(async move {
        while let Some(candle) = rx_raw.recv().await {
            let cleaned = clean_candle(candle);           // your logic
            let _ = tx_clean.send(cleaned).await;
        }
        drop(tx_clean);
    });

    // Stage 3: aggregate / enrich / decide what to persist
    let agg_handle = tokio::spawn(async move {
        while let Some(candle) = rx_clean.recv().await {
            if should_persist(&candle) {                  // your logic
                let _ = tx_agg.send(candle).await;
            }
        }
        drop(tx_agg);
    });

    // Stage 4: parquet writer (your existing logic)
    let writer_handle = tokio::spawn(write_candles_to_parquet(rx_agg));

    // Wait for graceful shutdown
    let _ = ws_handle.await;
    let _ = clean_handle.await;
    let _ = agg_handle.await;
    let _ = writer_handle.await;

    println!("Done");
    Ok(())
}

fn clean_candle(c: Candle) -> Candle { /* ... */ c }
fn should_persist(c: &Candle) -> bool { /* ... */ true }