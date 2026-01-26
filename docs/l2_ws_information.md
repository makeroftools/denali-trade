# Rust Client Module for L2 WebSocket Subscription

The hypersdk library provides a complete WebSocket client implementation for subscribing to L2 order book data on Hyperliquid. The main components are the `WebSocket` connection type, `Subscription` enum for specifying channels, and `Incoming` enum for handling received messages.

## Quick Start

```rust
use hypersdk::hypercore::{self, types::*};
use futures::StreamExt;

let mut ws = hypercore::mainnet_ws();

// Subscribe to L2 order book
ws.subscribe(Subscription::L2Book { coin: "BTC".into() });

// Process messages
while let Some(msg) = ws.next().await {
    match msg {
        Incoming::L2Book(book) => {
            println!("Book update for {}: {} bids, {} asks", 
                book.coin, book.bids().len(), book.asks().len());
        }
        _ => {}
    }
}
```

## Core Components

### WebSocket Connection

The `Connection` struct in `src/hypercore/ws.rs` provides automatic reconnection and subscription management [1](#0-0) . Create connections using convenience functions:

- `hypercore::mainnet_ws()` - Mainnet WebSocket [2](#0-1) 
- `hypercore::testnet_ws()` - Testnet WebSocket [3](#0-2) 

### Subscription Types

Subscribe to L2 book data using `Subscription::L2Book` [4](#0-3) :

```rust
Subscription::L2Book { coin: "BTC".into() }
```

### L2Book Data Structure

Received L2 book messages contain [5](#0-4) :
- `coin`: Market symbol
- `time`: Timestamp in milliseconds  
- `snapshot`: True for full snapshot, false/None for delta
- `levels`: Array of [bids, asks] with price levels

Access helper methods [6](#0-5) :
- `book.bids()` - Get bid levels (sorted high to low)
- `book.asks()` - Get ask levels (sorted low to high)
- `book.best_bid()` - Get best bid level
- `book.best_ask()` - Get best ask level
- `book.mid()` - Calculate mid price
- `book.spread()` - Calculate spread

## Advanced Features

### Automatic Reconnection

The WebSocket client automatically reconnects on failure with exponential backoff [7](#0-6)  and re-subscribes to all active channels [8](#0-7) .

### Subscription Management

Use `subscribe()` and `unsubscribe()` methods to manage channels [9](#0-8) . Subscriptions persist across reconnections.

### Message Processing

The WebSocket implements `futures::Stream` yielding `Incoming` messages [10](#0-9) . Match on message types to handle different data streams.

## Complete Example

See the WebSocket subscription example in README.md [11](#0-10)  for a complete working example of L2 book subscription with message processing.

## Notes

- The WebSocket uses the yawc library for zero-copy performance [12](#0-11) 
- All prices use `rust_decimal::Decimal` for high precision [13](#0-12) 
- Connection includes automatic ping/pong heartbeat every 5 seconds [14](#0-13) 

Wiki pages you might want to explore:
- [hypersdk Library (infinitefield/hypersdk)](/wiki/infinitefield/hypersdk#2)

### Citations

**File:** src/hypercore/ws.rs (L175-196)
```rust
pub struct Connection {
    rx: UnboundedReceiver<Incoming>,
    // TODO: oneshot??
    tx: UnboundedSender<SubChannelData>,
}

impl Connection {
    /// Creates a new WebSocket connection to the specified URL.
    ///
    /// The connection starts immediately and runs in the background,
    /// automatically reconnecting on failures.
    ///
    /// # Example
    ///
    /// Create a new WebSocket connection:
    /// `WebSocket::new(hypercore::mainnet_websocket_url())`
    pub fn new(url: Url) -> Self {
        let (tx, rx) = unbounded_channel();
        let (stx, srx) = unbounded_channel();
        tokio::spawn(connection(url, tx, srx));
        Self { rx, tx: stx }
    }
```

**File:** src/hypercore/ws.rs (L208-223)
```rust
    pub fn subscribe(&self, subscription: Subscription) {
        let _ = self.tx.send((true, subscription));
    }

    /// Unsubscribes from a WebSocket channel.
    ///
    /// Stops receiving updates for this subscription. Does nothing if you're
    /// not currently subscribed to this channel.
    ///
    /// # Example
    ///
    /// Unsubscribe from a channel:
    /// `ws.unsubscribe(Subscription::Trades { coin: "BTC".into() })`
    pub fn unsubscribe(&self, subscription: Subscription) {
        let _ = self.tx.send((false, subscription));
    }
```

**File:** src/hypercore/ws.rs (L238-245)
```rust
impl futures::Stream for Connection {
    type Item = Incoming;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.rx.poll_recv(cx)
    }
}
```

**File:** src/hypercore/ws.rs (L252-299)
```rust
    let mut subs: HashSet<Subscription> = HashSet::new();
    let mut reconnect_attempts = 0u32;
    const MAX_RECONNECT_DELAY_MS: u64 = 5_000; // 5 seconds max
    const INITIAL_RECONNECT_DELAY_MS: u64 = 500;

    loop {
        let mut stream = match timeout(Duration::from_secs(10), Stream::connect(url.clone())).await
        {
            Ok(ok) => match ok {
                Ok(ok) => {
                    log::info!("Connected to {url}");
                    reconnect_attempts = 0; // Reset on successful connection
                    ok
                }
                Err(err) => {
                    log::error!("Unable to connect to {url}: {err:?}");

                    // Exponential backoff: 500ms, 1s, 2s, 4s, 5s (capped)
                    let delay_ms = (INITIAL_RECONNECT_DELAY_MS * (1u64 << reconnect_attempts))
                        .min(MAX_RECONNECT_DELAY_MS);
                    reconnect_attempts = reconnect_attempts.saturating_add(1);

                    log::info!(
                        "Reconnecting in {}ms (attempt {})",
                        delay_ms,
                        reconnect_attempts
                    );
                    sleep(Duration::from_millis(delay_ms)).await;
                    continue;
                }
            },
            Err(err) => {
                log::error!("Connection timeout to {url}: {err:?}");

                let delay_ms = (INITIAL_RECONNECT_DELAY_MS * (1u64 << reconnect_attempts))
                    .min(MAX_RECONNECT_DELAY_MS);
                reconnect_attempts = reconnect_attempts.saturating_add(1);

                log::info!(
                    "Reconnecting in {}ms (attempt {})",
                    delay_ms,
                    reconnect_attempts
                );
                sleep(Duration::from_millis(delay_ms)).await;

                continue;
            }
        };
```

**File:** src/hypercore/ws.rs (L301-310)
```rust
        // Re-subscribe to all active subscriptions after reconnection
        if !subs.is_empty() {
            log::info!("Re-subscribing to {} channels", subs.len());
            for sub in subs.iter() {
                log::debug!("Re-subscribing to {sub}");
                if let Err(err) = stream.subscribe(sub.clone()).await {
                    log::error!("Failed to re-subscribe to {sub}: {err:?}");
                }
            }
        }
```

**File:** src/hypercore/ws.rs (L312-317)
```rust
        let mut ping = interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = ping.tick() => {
                    let _ = stream.ping().await;
                }
```

**File:** src/hypercore/mod.rs (L461-463)
```rust
pub fn mainnet_ws() -> WebSocket {
    WebSocket::new(mainnet_websocket_url())
}
```

**File:** src/hypercore/mod.rs (L497-511)
```rust
/// Creates a testnet WebSocket connection for HyperCore.
///
/// This is a convenience function that creates a WebSocket connection to the testnet API.
///
/// # Example
///
/// ```
/// use hypersdk::hypercore;
/// use futures::StreamExt;
///
/// # async fn example() {
/// let mut ws = hypercore::testnet_ws();
/// // Subscribe to market data
/// # }
/// ```
```

**File:** src/hypercore/types/mod.rs (L234-236)
```rust
    /// Order book snapshots and updates
    #[display("l2Book({coin})")]
    L2Book { coin: String },
```

**File:** src/hypercore/types/mod.rs (L715-725)
```rust
pub struct L2Book {
    /// Market symbol
    pub coin: String,
    /// Timestamp in milliseconds
    pub time: u64,
    /// True if snapshot, false/None if delta
    #[serde(default)]
    pub snapshot: Option<bool>,
    /// [bids, asks]
    pub levels: [Vec<BookLevel>; 2],
}
```

**File:** src/hypercore/types/mod.rs (L727-773)
```rust
impl L2Book {
    /// Returns true if this is a full snapshot (not a delta update).
    #[must_use]
    pub fn is_snapshot(&self) -> bool {
        self.snapshot.unwrap_or(false)
    }

    /// Returns the bid levels (sorted from highest to lowest).
    #[must_use]
    pub fn bids(&self) -> &[BookLevel] {
        &self.levels[0]
    }

    /// Returns the ask levels (sorted from lowest to highest).
    #[must_use]
    pub fn asks(&self) -> &[BookLevel] {
        &self.levels[1]
    }

    /// Returns the best bid level, if available.
    #[must_use]
    pub fn best_bid(&self) -> Option<&BookLevel> {
        self.bids().first()
    }

    /// Returns the best ask level, if available.
    #[must_use]
    pub fn best_ask(&self) -> Option<&BookLevel> {
        self.asks().first()
    }

    /// Returns the mid price (average of best bid and ask), if both are available.
    #[must_use]
    pub fn mid(&self) -> Option<Decimal> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        Some((bid.px + ask.px) / rust_decimal::Decimal::TWO)
    }

    /// Returns the spread (best ask - best bid), if both are available.
    #[must_use]
    pub fn spread(&self) -> Option<Decimal> {
        let bid = self.best_bid()?;
        let ask = self.best_ask()?;
        Some(ask.px - bid.px)
    }
}
```

**File:** README.md (L37-42)
```markdown
**[rust_decimal](https://docs.rs/rust_decimal)** - High-precision decimals

- Primary choice for financial calculations requiring precision
- Converts WebSocket string payloads to high-precision decimal numbers
- Can be easily converted to other fixed-point number types
- Note: Some specialized EVM types may require alternative approaches
```

**File:** README.md (L44-48)
```markdown
**[yawc](https://docs.rs/yawc)** - WebSocket implementation

- Zero-copy WebSocket protocol implementation
- Supports per-message deflate compression
- Optimized for performance-critical applications
```

**File:** README.md (L166-197)
```markdown
### HyperCore - WebSocket Subscriptions

```rust
use hypersdk::hypercore::{self, types::*};
use futures::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut ws = hypercore::mainnet_ws();

    // Subscribe to market data
    ws.subscribe(Subscription::Trades { coin: "BTC".into() });
    ws.subscribe(Subscription::L2Book { coin: "ETH".into() });

    // Process incoming messages
    while let Some(msg) = ws.next().await {
        match msg {
            Incoming::Trades(trades) => {
                for trade in trades {
                    println!("{} @ {} size {}", trade.side, trade.px, trade.sz);
                }
            }
            Incoming::L2Book(book) => {
                println!("Order book update for {}", book.coin);
            }
            _ => {}
        }
    }

    Ok(())
}
```
```
