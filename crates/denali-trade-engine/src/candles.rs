use hypersdk::hypercore::types::Candle;

pub async fn clean_candle(c: Candle) -> Candle {
    c  // simple: no-op for now
}

pub async fn should_persist_candle(_c: &Candle) -> bool {
    true  // simple: always persist
}