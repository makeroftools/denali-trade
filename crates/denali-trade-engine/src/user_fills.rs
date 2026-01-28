use hypersdk::hypercore::types::Fill;

pub async fn clean_fill(f: Fill) -> Fill {
    f
}

pub async fn should_persist_fill(_f: &Fill) -> bool {
    true
}