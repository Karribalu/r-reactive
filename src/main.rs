#![feature(reentrant_lock)]

use crate::reactive_store::{ReactiveStore, StoreValue};
use std::time::Duration;

mod dash;
mod reactive_store;

#[tokio::main]
async fn main() {
    let store = ReactiveStore::new();
    let mut sub = store.subscribe();

    // Insert 100000 items with same ttl and see if the reactive store can handle it

    for i in 0..100000 {
        store.set_with_ttl(
            &format!("key{}", i),
            StoreValue::Text(format!("value{}", i)),
            Duration::from_secs(1),
        );
    }
    // Wait for the items to expire
    tokio::time::sleep(Duration::from_secs(2)).await;
    // Check if the items are expired
    for i in 0..100000 {
        assert_eq!(store.get(&format!("key{}", i)), None);
    }
    // Check if the subscriber received the expired messages
    for i in 0..100000 {
        let (k, v) = sub.recv().await.unwrap();
        assert_eq!(k, format!("key{}", i));
        assert_eq!(v, StoreValue::Text("EXPIRED".into()));
    }
}
