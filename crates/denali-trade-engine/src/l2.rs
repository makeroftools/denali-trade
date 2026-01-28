use hypersdk::hypercore::types::L2Book;

pub async fn clean_l2book(b: L2Book) -> L2Book {
    b
}

pub async fn should_persist_l2(_b: &L2Book) -> bool {
    true
}