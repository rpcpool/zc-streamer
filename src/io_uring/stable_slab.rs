use std::pin::Pin;

///
/// Stable slab never grows past its capacity, guaranteeing data to be pinned in memory.
///
pub struct StableSlab<T> {
    slab: slab::Slab<T>,
    fixed_cap: usize,
}

impl<T> StableSlab<T> {
    pub fn with_capacity(fixed_cap: usize) -> Self {
        let slab = slab::Slab::with_capacity(fixed_cap);
        StableSlab { slab, fixed_cap }
    }

    pub fn insert(&mut self, value: T) -> usize {
        if self.slab.len() == self.fixed_cap {
            panic!("not enough space in the slab");
        }
        self.slab.insert(value)
    }

    pub fn remove(&mut self, key: usize) -> T {
        self.slab.remove(key)
    }

    pub fn get(&self, key: usize) -> Option<Pin<&T>> {
        self.slab.get(key).map(|v| unsafe { Pin::new_unchecked(v) })
    }
}
