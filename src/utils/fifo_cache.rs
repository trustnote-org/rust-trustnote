extern crate indexmap;

use may::sync::RwLock;
use std::hash::Hash;

pub struct FifoCache<K, V> {
    inner: RwLock<indexmap::IndexMap<K, V>>,
}

impl<K: Eq + Hash, V: Clone> FifoCache<K, V> {
    pub fn with_capacity(capacity: usize) -> FifoCache<K, V> {
        FifoCache {
            inner: RwLock::new(indexmap::IndexMap::with_capacity(capacity)),
        }
    }

    pub fn get(&self, k: K) -> Option<V> {
        let g = self.inner.read().unwrap();
        g.get(&k).map(|v| v.clone())
    }

    pub fn insert(&self, k: K, v: V) -> Option<V> {
        self.inner.write().unwrap().insert(k, v)
    }

    pub fn remove(&self, k: &K) -> Option<V> {
        self.inner.write().unwrap().remove(k)
    }
}
