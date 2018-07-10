extern crate indexmap;

use may::sync::RwLock;
use std::hash::Hash;

pub struct FifoCache<K, V> {
    inner: RwLock<indexmap::IndexMap<K, V>>,
    capacity: usize,
}

impl<K: Eq + Hash, V: Clone> FifoCache<K, V> {
    pub fn with_capacity(capacity: usize) -> FifoCache<K, V> {
        FifoCache {
            inner: RwLock::new(indexmap::IndexMap::with_capacity(capacity)),
            capacity,
        }
    }

    #[inline]
    pub fn get(&self, k: &K) -> Option<V> {
        self.inner.read().unwrap().get(k).cloned()
    }

    #[inline]
    pub fn insert(&self, k: K, v: V) -> Option<V> {
        let mut map = self.inner.write().unwrap();
        while self.capacity - 1 < map.len() {
            map.pop();
        }
        map.insert(k, v)
    }

    #[inline]
    pub fn remove(&self, k: &K) -> Option<V> {
        self.inner.write().unwrap().remove(k)
    }
}
