
# Crypto Dashboard (Plotters + Leptos)
A real-time cryptocurrency candlestick dashboard built with Rust, Leptos (client-side rendering), and Plotters for Wasm.
Description
This project displays six cryptocurrency trading pair charts (BTC/USD, ETH/USD, SOL/USD, XRP/USD, ADA/USD, DOGE/USD) using HTML canvases rendered with Plotters-canvas.
Currently, charts are blank (empty data vectors). The architecture is designed for easy integration of real-time websocket data (e.g., via hypersdk and tokio::mpsc).
Features

Leptos CSR app mounted via Trunk
Responsive grid layout with individual chart cards
Plotters-based candlestick rendering (green up, red down bars)
Signals per symbol for independent updates
Effect redraws chart whenever data signal changes
No external JS charting libraries — pure Rust/Wasm

Project Structure

src/main.rs — Leptos App and Chart component
src/candlestick.rs — Candle struct + Plotters drawing logic
src/data.rs — Placeholder for future websocket/data fetching
Cargo.toml — Dependencies (Leptos, Plotters, wasm-bindgen, etc.)
index.html — Basic HTML shell

Setup & Running
Requirements:

Rust toolchain (stable)
Trunk (cargo install trunk)

Commands:
Bashtrunk serve          # Development server (hot reload)
trunk build          # Production build (dist folder)
Open http://127.0.0.1:8080 for the dashboard.
Architecture Notes

Each chart uses a RwSignal<Vec<Candle>>
create_effect tracks data changes and redraws immediately
Canvas cast uses unchecked_ref + clone for safe repeated access
Title cloned separately for view and chart caption

Adding Real Data (Future)

Uncomment/add hypersdk and required tokio features in Cargo.toml
Implement in src/data.rs:
Spawn tokio tasks
Use mpsc channels or direct signal updates
Parse websocket messages → push Candle structs

In App, call spawn functions with each symbol's signal

Example skeleton:
Rust// In App
data::spawn_feed(btc_data, "BTC-USD");

// In data.rs
pub fn spawn_feed(signal: RwSignal<Vec<Candle>>, symbol: &str) {
    leptos::spawn_local(async move {
        // hypersdk websocket connection + message handling
        // signal.update(|d| d.push(new_candle));
    });
}
Dependencies

leptos 0.6 (CSR)
plotters 0.3 + plotters-canvas
wasm-bindgen
console_error_panic_hook
wee_alloc (Wasm allocator)

Known Issues / TODO

Charts blank until data added
No zoom/pan (Plotters-canvas limitation)
No hover tooltip/crosshair
No timeframe selection
Responsive canvas sizing (fixed 500x300)

License
MIT (or as per your preference)
Built by Peter Alexander (@makeroftools) with assistance from Grok.
