# Pipeline – Advanced Async Processing & Filtering (Rust + Tokio + Flume)

Lightweight, configurable, fault-tolerant pipeline for streaming item processing.

## Features

- Error propagation (`Result<T, E>`)
- Configurable channel capacities (0 = unbounded)
- Multiple workers per stage
- Send modes: `Blocking` (wait) or `TryDrop` (drop on full + log)
- Optional logging hook
- Cancellation / graceful shutdown support (via returned task handles)
- Order preservation option (panics if >1 worker per stage)
- Builder pattern

## Usage

```rust
use pipeline::{Builder, SendMode};

let (pipeline, mut rx_final, tasks) = Builder::new()
    .input_cap(8192)
    .process_workers(4)
    .filter_workers(2)
    .process_send_mode(SendMode::TryDrop)
    .preserve_order(false)
    .log_fn(|msg| println!("[PIPE] {}", msg))
    .build(
        |candle: Candle| async move {
            Ok(clean_candle(candle))                    // → Result<T, E>
        },
        |candle: &Candle| async move {
            Ok(candle.volume > Decimal::ZERO)           // → Result<bool, E>
        },
    );

// Feed items (non-blocking if bounded & full → blocks or drops depending on mode)
pipeline.tx_input.send_async(candle).await.ok();

// Consume results
while let Ok(result) = rx_final.recv_async().await {
    match result {
        Ok(item)  => persist(item).await,
        Err(e)    => log::error!("Pipeline error: {}", e),
    }
}

// Shutdown cleanly
drop(pipeline.tx_input);           // stop feeding
drop(rx_final);                    // optional: stop reading

// Wait for workers to finish (or .abort() them)
for task in tasks {
    let _ = task.await;
}

## Builder Options

Method,Default,Description
.input_cap(n),4096,0 = unbounded
.process_cap(n),2048,After processing
.output_cap(n),512,Final output
.process_workers(n),1,Parallel processing tasks
.filter_workers(n),1,Parallel filtering tasks
.process_send_mode,Blocking,Blocking / TryDrop
.filter_send_mode,Blocking,Blocking / TryDrop
.preserve_order,false,true → panics if >1 worker per stage
.log_fn(fn),None,Logs stage events (optional)

## Notes

- T: Send + 'static, E: Send + 'static
- Functions must be Send + Sync + 'static
- Use flume channels → mpmc, async-friendly
- Drop tx_input to signal end → workers exit when queues drain
- rx_final returns Result<T, E> (errors from process or filter)
- For clean shutdown: drop sender → drain receiver → await tasks
- TryDrop + log is useful for lossy high-throughput pipelines

Simple, robust, production-ready async pipeline.