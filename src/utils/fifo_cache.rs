extern crate fifo_cache;

use may::sync::RwLock;
use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::rc::Rc;
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
        FifoCache {
            inner: RwLock::new(fifo_cache::FifoCache::new(capacity)),
        }
    }

    pub fn get(&self, k: &K) -> Option<Arc<V>> {
        if let Some(_) = Some(self.inner.is_poisoned()) {
            return self.inner.read().unwrap().get(k);
        }
        None
    }

    pub fn insert(&mut self, k: K, v: V) -> Option<Arc<V>> {
        if let Some(_) = Some(self.inner.is_poisoned()) {
            return self.inner.write().unwrap().insert(k, v);
        }
        None
    }

    /*    pub fn remove<Q: ?Sized>(&mut self, k: &K) -> Option<Arc<V>> {
        if let Some(_) = Some(self.inner.is_poisoned()) {
            return self.inner.write().unwrap().remove(&k);
        }
        None
    } */
}
