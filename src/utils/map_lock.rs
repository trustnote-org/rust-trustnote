use may::sync::{Blocker, Mutex};

use std::collections::HashSet;
use std::collections::LinkedList;
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::sync::Arc;

struct Task<T> {
    keys: Vec<T>,
    blocker: Arc<Blocker>,
}

struct Inner<T> {
    // curent keys in the
    keys: HashSet<T>,
    // queue for blockers
    tasks: LinkedList<Task<T>>,
}

impl<T: Clone + Hash + Eq> Inner<T> {
    // detect if key is in use
    fn keys_is_locked(&self, keys: &[T]) -> bool {
        for key in keys {
            if self.keys.contains(key) {
                return true;
            }
        }
        false
    }

    fn keys_unlock(&mut self, keys: &[T]) {
        self.keys.retain(|key| !keys.contains(key));
    }

    fn keys_lock(&mut self, keys: &[T]) {
        for k in keys {
            let ret = self.keys.insert(k.clone());
            assert_eq!(ret, true);
        }
    }

    // find out next suitable blocker for wakeup
    fn task_dequeue(&mut self) -> Option<Task<T>> {
        let mut idx = 0;
        let mut is_found = false;
        for task in &self.tasks {
            // find the first task that is ready to go
            if !self.keys_is_locked(&task.keys) {
                is_found = true;
                break;
            }
            idx += 1;
        }

        if !is_found {
            return None;
        }

        // remove the task in the queue
        let mut list = self.tasks.split_off(idx);
        let task = list.pop_front().unwrap();
        self.tasks.append(&mut list);

        Some(task)
    }

    // put the entry into the waiting queue
    fn task_enqueue(&mut self, entry: Task<T>) {
        self.tasks.push_back(entry);
    }
}

// protect the struct by a RwLock
pub struct MapLock<T>(Mutex<Inner<T>>);

impl<T: Hash + Eq> Debug for MapLock<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MapLock{{ ... }}")
    }
}

impl<T: Clone + Hash + Eq> Default for MapLock<T> {
    fn default() -> Self {
        MapLock::new()
    }
}

impl<T: Clone + Hash + Eq> MapLock<T> {
    pub fn new() -> Self {
        MapLock(Mutex::new(Inner {
            keys: HashSet::new(),
            tasks: LinkedList::new(),
        }))
    }

    // return how many tasks waiting on the MapLock
    pub fn get_waiter_num(&self) -> usize {
        let g = self.0.lock().unwrap();
        g.tasks.len()
    }

    // used internally
    fn release_keys(&self, keys: &[T]) {
        let mut g = self.0.lock().unwrap();
        // remove keys from the map lock
        g.keys_unlock(keys);

        // release all the tasks that is qualified
        while let Some(task) = g.task_dequeue() {
            g.keys_lock(&task.keys);
            task.blocker.unpark();
        }
    }

    // we must keep the strict order for fairness
    pub fn try_lock(&self, keys: Vec<T>) -> Option<MapLockGuard<T>> {
        let mut g = self.0.lock().unwrap();

        // first check if there are other pending task is ok for wakeup
        // we don't need to do this because:
        // 1. the mutex already have a task queue for fairness
        // 2. the drop of guard should wakeup all the tasks
        // while let Some(task) = g.task_dequeue() {
        //     g.keys_lock(&task.keys);
        //     task.blocker.unpark();
        // }

        // check our keys at last
        if g.keys_is_locked(&keys) {
            return None;
        }

        // ok, the keys are not in use
        // mark the keys as in use
        g.keys_lock(&keys);

        // return the guard
        Some(MapLockGuard { owner: self, keys })
    }

    pub fn lock(&self, keys: Vec<T>) -> MapLockGuard<T> {
        use may::coroutine::{self, ParkError};

        let mut g = self.0.lock().unwrap();

        // first check if there are other pending task is ok for wakeup
        // we don't need to do this because:
        // 1. the mutex already have a task queue for fairness
        // 2. the drop of guard should wakeup all the tasks
        // while let Some(task) = g.task_dequeue() {
        //     g.keys_lock(&task.keys);
        //     task.blocker.unpark();
        // }

        if !g.keys_is_locked(&keys) {
            // ok, the keys are not in use
            // mark the keys as in use
            g.keys_lock(&keys);
            return MapLockGuard { owner: self, keys };
        }

        // put the blocker in the waiting queue
        let blocker = Blocker::current();
        let task = Task {
            keys: keys.clone(),
            blocker: blocker.clone(),
        };
        g.task_enqueue(task);
        drop(g);

        // wait until unparked
        match blocker.park(None) {
            Ok(_) => {}
            Err(ParkError::Timeout) => unreachable!(),
            Err(ParkError::Canceled) => {
                coroutine::trigger_cancel_panic();
            }
        }

        // we comeback, it's safe to say that our keys are locked
        MapLockGuard { owner: self, keys }
    }
}

#[derive(Debug)]
pub struct MapLockGuard<'a, T: Clone + Hash + Eq + 'a> {
    owner: &'a MapLock<T>,
    keys: Vec<T>,
}

impl<'a, T: Clone + Hash + Eq> Drop for MapLockGuard<'a, T> {
    fn drop(&mut self) {
        // remove the entry
        self.owner.release_keys(&self.keys);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_lock() {
        let lock = Arc::new(MapLock::new());
        let g = lock.lock(vec!["test"]);
        let g1 = lock.try_lock(vec!["test", "test1"]);
        assert_eq!(g1.is_some(), false);
        drop(g);
        let g2 = lock.try_lock(vec!["test", "test1"]);
        assert_eq!(g2.is_some(), true);

        let lock_1 = lock.clone();
        let j = go!(move || {
            let _g = lock_1.lock(vec!["test"]);
            println!("comeback in coroutine");
        });

        drop(g2);
        j.join().unwrap();
    }

    #[test]
    fn test_map_try_lock() {
        let lock = MapLock::new();
        let g = lock.try_lock(vec!["test"]);
        assert_eq!(g.is_some(), true);
        let g1 = lock.try_lock(vec!["test"]);
        assert_eq!(g1.is_some(), false);
        let g2 = lock.try_lock(vec!["test1"]);
        assert_eq!(g2.is_some(), true);
        drop(g);
        let g1 = lock.try_lock(vec!["test"]);
        assert_eq!(g1.is_some(), true);
    }

    #[test]
    fn test_map_unlock() {
        let lock = Arc::new(MapLock::new());
        let g = lock.lock(vec!["test1", "test2"]);

        let lock_1 = lock.clone();
        let j1 = go!(move || {
            let _g = lock_1.lock(vec!["test1"]);
            println!("comeback in coroutine1");
        });

        let lock_2 = lock.clone();
        let j2 = go!(move || {
            let _g = lock_2.lock(vec!["test2"]);
            println!("comeback in coroutine2");
        });

        drop(g); // this will release both coroutine
        j1.join().unwrap();
        j2.join().unwrap();
    }
}
