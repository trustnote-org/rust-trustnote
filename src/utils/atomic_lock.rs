use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Default)]
pub struct AtomicLock {
    is_locked: AtomicBool,
}

#[derive(Debug)]
pub struct AtomicLockGuard<'a> {
    owner: &'a AtomicLock,
}

impl<'a> Drop for AtomicLockGuard<'a> {
    fn drop(&mut self) {
        self.owner.is_locked.store(false, Ordering::Release);
    }
}

impl AtomicLock {
    pub fn new() -> Self {
        AtomicLock {
            is_locked: AtomicBool::new(false),
        }
    }

    pub fn try_lock(&self) -> Option<AtomicLockGuard> {
        if self
            .is_locked
            .compare_and_swap(false, true, Ordering::SeqCst)
        {
            return None;
        }

        Some(AtomicLockGuard { owner: self })
    }

    pub fn is_locked(&self) -> bool {
        self.is_locked.load(Ordering::Acquire)
    }
}

#[test]
fn test_atomic_lock() {
    let lock = AtomicLock::new();
    assert_eq!(lock.is_locked(), false);
    let g = lock.try_lock().unwrap();
    assert_eq!(lock.is_locked(), true);
    drop(g);
    assert_eq!(lock.is_locked(), false);
}
