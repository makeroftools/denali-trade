use hypersdk::hypercore::types::Trade;

pub async fn clean_trade(t: Trade) -> Trade {
    t
}

pub async fn should_persist_trade(_t: &Trade) -> bool {
    true
}