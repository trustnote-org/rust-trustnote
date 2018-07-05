extern crate indexmap;

use may::sync::RwLock;
use std::hash::Hash;

pub struct FifoCache<K, V> {
    inner: RwLock<indexmap::IndexMap<K, V>>,
    size: usize,
}

impl<K: Eq + Hash, V: Clone> FifoCache<K, V> {
    pub fn with_capacity(capacity: usize) -> FifoCache<K, V> {
        FifoCache {
            inner: RwLock::new(indexmap::IndexMap::with_capacity(capacity)),
            size: capacity,
        }
    }

    #[inline]
    pub fn get(&self, k: &K) -> Option<V> {
        self.inner.read().unwrap().get(k).map(|v| v.clone())
    }

    #[inline]
    pub fn insert(&self, k: K, v: V) -> Option<V> {
        let map = self.inner.write().unwrap();
        while self.size - 1 < map.len() {
            map.pop();
        }
        map.insert(k, v)
    }

    #[inline]
    pub fn remove(&self, k: &K) -> Option<V> {
        self.inner.write().unwrap().remove(k)
    }
}
