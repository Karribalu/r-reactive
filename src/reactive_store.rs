use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;
#[derive(Debug, Clone, PartialEq)]
pub enum StoreValue {
    Map(HashMap<String, StoreValue>),
    List(Vec<StoreValue>),
    Set(HashSet<String>),
    Counter(i64),
    Text(String),
}

#[derive(Debug, Clone)]
pub struct ReactiveStore {
    data: Arc<RwLock<HashMap<String, StoreValue>>>,
    tx: broadcast::Sender<(String, StoreValue)>,
}

impl ReactiveStore {
    pub fn new() -> Self {
        ReactiveStore {
            data: Arc::new(RwLock::new(HashMap::new())),
            tx: broadcast::channel(100).0,
        }
    }

    pub fn set(&self, key: &str, value: StoreValue) {
        {
            let mut data = self.data.write().unwrap();
            data.insert(key.to_string(), value.clone());
        }

        // Notify subscribers about the change
        let _ = self.tx.send((key.to_string(), value));
    }

    pub fn get(&self, key: &str) -> Option<StoreValue> {
        let data = self.data.read().unwrap();
        data.get(key).cloned()
    }

    pub fn remove(&self, key: &str) {
        let mut data = self.data.write().unwrap();
        data.remove(key);
    }

    pub fn set_with_ttl(&self, key: &str, value: StoreValue, ttl: std::time::Duration) {
        self.set(key, value.clone());

        let data = self.data.clone();
        let tx = self.tx.clone();
        let key = key.to_string();
    }

    pub fn subscribe(&self) -> broadcast::Receiver<(String, StoreValue)> {
        self.tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_reactive_store() {
        let store = ReactiveStore::new();

        store.set("key1", StoreValue::Text("value1".to_string()));
        assert_eq!(
            store.get("key1"),
            Some(StoreValue::Text("value1".to_string()))
        );

        store.set("key2", StoreValue::Counter(42));
        assert_eq!(store.get("key2"), Some(StoreValue::Counter(42)));

        store.remove("key1");
        assert_eq!(store.get("key1"), None);
    }

    #[tokio::test]
    async fn test_reactive_store_subscribe() {
        let store = ReactiveStore::new();
        let mut rx = store.subscribe();

        store.set("key1", StoreValue::Text("value1".to_string()));
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.0, "key1");
        assert_eq!(msg.1, StoreValue::Text("value1".to_string()));

        store.set("key2", StoreValue::Counter(42));
        let msg = rx.recv().await.unwrap();
        assert_eq!(msg.0, "key2");
        assert_eq!(msg.1, StoreValue::Counter(42));
    }

    #[tokio::test]
    async fn test_reactive_store_ttl() {
        let store = ReactiveStore::new();
        let mut sub = store.subscribe();

        store.set_with_ttl(
            "temp",
            StoreValue::Text("value".into()),
            Duration::from_secs(1),
        );

        let (k1, v1) = sub.recv().await.unwrap();
        assert_eq!(k1, "temp");
        assert_eq!(v1, StoreValue::Text("value".into()));

        let (k2, v2) = sub.recv().await.unwrap();
        assert_eq!(k2, "temp");
        assert_eq!(v2, StoreValue::Text("EXPIRED".into()));

        assert_eq!(store.get("temp"), None);
    }
}
