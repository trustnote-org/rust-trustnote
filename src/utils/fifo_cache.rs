extern crate fifo_cache;

use may::sync::RwLock;
use std::hash::Hash;
use std::sync::Arc;

#[derive(Debug)]
pub struct FifoCache<K: Eq + Hash, V> {
    inner: RwLock<fifo_cache::FifoCache<K, V>>,
}

// pub struct FifoCache<K: Eq + Hash, V>(RwLock<fifo_cache::FifoCache<K, V>>);

unsafe impl<K: Send + Sync + Eq + Hash, V> Send for FifoCache<K, V> {}
unsafe impl<K: Send + Sync + Eq + Hash, V> Sync for FifoCache<K, V> {}

impl<K: Eq + Hash, V> FifoCache<K, V> {
    pub fn with_capacity(capacity: usize) -> FifoCache<K, V> {
        unimplemented!()
    }

    pub fn get(&self, k: &K) -> Option<Arc<V>> {
        unimplemented!()
    }

    pub fn insert(&mut self, k: K, v: V) -> Option<Arc<V>> {
        unimplemented!()
    }
}
