# DeepWiki Q&A with Code Context for Repository: infinitefield/hypersdk
## Q1
give me all the websocket endpoint subscriptions available
### Answer
The hypersdk provides WebSocket subscriptions for real-time market data and user events through the `Subscription` enum in `src/hypercore/types/mod.rs` hypersdk:225-252 .

## Available WebSocket Subscriptions

### Market Data Subscriptions

| Subscription | Incoming Message | Description |
|--------------|------------------|-------------|
| `Bbo { coin }` | `Incoming::Bbo` | Best bid and offer updates for a specific coin |
| `Trades { coin }` | `Incoming::Trades` | Real-time trade feed for a specific coin |
| `L2Book { coin }` | `Incoming::L2Book` | Order book snapshots and updates for a specific coin |
| `Candle { coin, interval }` | `Incoming::Candle` | Candlestick (OHLCV) data with specified interval |
| `AllMids { dex }` | `Incoming::AllMids` | Mid prices for all markets, optionally filtered by DEX |

### User-Specific Subscriptions

| Subscription | Incoming Message | Description |
|--------------|------------------|-------------|
| `OrderUpdates { user }` | `Incoming::OrderUpdates` | Order status changes for a specific user address |
| `UserFills { user }` | `Incoming::UserFills` | Trade fill events for a specific user address |

## Usage Example

```rust
use hypersdk::hypercore::{self, types::*};
use futures::StreamExt;

let mut ws = hypercore::mainnet_ws();

// Market data subscriptions
ws.subscribe(Subscription::Trades { coin: "BTC".into() });
ws.subscribe(Subscription::L2Book { coin: "ETH".into() });
ws.subscribe(Subscription::Candle {
    coin: "BTC".into(),
    interval: "15m".into()
});

// User-specific subscriptions
let user: Address = "0x...".parse().unwrap();
ws.subscribe(Subscription::OrderUpdates { user });
ws.subscribe(Subscription::UserFills { user });
```

The WebSocket client handles automatic reconnection and re-subscription, ensuring your subscriptions persist across connection failures hypersdk:151-196 .

## Notes

- All subscriptions use the `Subscription` enum defined in `src/hypercore/types/mod.rs`
- The WebSocket connection is created using `hypercore::mainnet_ws()` or `hypercore::testnet_ws()` hypersdk:461-462 
- Incoming messages are handled through the `Incoming` enum which corresponds to each subscription type hypersdk:300-327 
- The WebSocket client implements automatic reconnection with exponential backoff hypersdk:254-295 

Wiki pages you might want to explore:
- [hypersdk Library (infinitefield/hypersdk)](/wiki/infinitefield/hypersdk#2)
### Citations
**File:** src/hypercore/types/mod.rs (L225-252)
```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize, derive_more::Display)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Subscription {
    /// Best bid and offer updates
    #[display("bbo({coin})")]
    Bbo { coin: String },
    /// Real-time trade feed
    #[display("trades({coin})")]
    Trades { coin: String },
    /// Order book snapshots and updates
    #[display("l2Book({coin})")]
    L2Book { coin: String },
    /// Real-time candlestick updates
    #[display("candle({coin}@{interval})")]
    Candle { coin: String, interval: String },
    /// Mid prices for all markets
    #[display("allMids({dex:?})")]
    AllMids {
        #[serde(skip_serializing_if = "Option::is_none")]
        dex: Option<String>,
    },
    /// Order status updates for user
    #[display("orderUpdates({user})")]
    OrderUpdates { user: Address },
    /// Fill events for user
    #[display("userFills({user})")]
    UserFills { user: Address },
}
```
**File:** src/hypercore/types/mod.rs (L300-327)
```rust
#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
#[serde(tag = "channel", content = "data")]
pub enum Incoming {
    /// Confirmation of subscription/unsubscription
    SubscriptionResponse(Outgoing),
    /// Best bid and offer update
    Bbo(Bbo),
    /// Order book snapshot or delta
    L2Book(L2Book),
    /// Candlestick update
    Candle(Candle),
    /// Mid prices for all markets
    AllMids {
        dex: Option<String>,
        mids: HashMap<String, Decimal>,
    },
    /// Trade events for a market
    Trades(Vec<Trade>),
    /// Order status changes for a user
    OrderUpdates(Vec<OrderUpdate>),
    /// Fill events for a user
    UserFills { user: Address, fills: Vec<Fill> },
    /// Server heartbeat ping
    Ping,
    /// Server heartbeat pong
    Pong,
}
```
**File:** src/hypercore/ws.rs (L151-196)
```rust
/// Persistent WebSocket connection with automatic reconnection.
///
/// This connection automatically handles:
/// - Reconnection on connection failure
/// - Re-subscription after reconnection
/// - Periodic ping/pong to keep the connection alive
///
/// The connection implements `futures::Stream`, yielding [`Incoming`] messages.
///
/// # Example
///
/// ```no_run
/// use hypersdk::hypercore::{self, types::*};
/// use futures::StreamExt;
///
/// # async fn example() {
/// let mut ws = hypercore::mainnet_ws();
/// ws.subscribe(Subscription::Trades { coin: "BTC".into() });
///
/// while let Some(msg) = ws.next().await {
///     // Handle messages
/// }
/// # }
/// ```
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
**File:** src/hypercore/ws.rs (L254-295)
```rust
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
```
**File:** src/hypercore/mod.rs (L461-462)
```rust
pub fn mainnet_ws() -> WebSocket {
    WebSocket::new(mainnet_websocket_url())
```