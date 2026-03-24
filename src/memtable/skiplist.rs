use super::arena_allocator::Arena;
use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const MAX_HEIGHT: usize = 12;

#[repr(C)]
struct Node<K, V> {
    key: K,
    value: V,
    height: usize,
}

impl<K, V> Node<K, V> {
    #[inline]
    fn next_array_offset() -> usize {
        let base_size = std::mem::size_of::<Self>();
        let align = std::mem::align_of::<AtomicPtr<Node<K, V>>>();
        (base_size + align - 1) & !(align - 1)
    }

    fn new(key: K, value: V, height: usize, arena: &Arena) -> *mut Self {
        let offset = Self::next_array_offset();
        let size = offset + height * std::mem::size_of::<AtomicPtr<Node<K, V>>>();

        // Allocate raw bytes from Arena dynamically sized to our generated max height
        let ptr = arena.allocate(size) as *mut Self;

        unsafe {
            // Write the base struct
            ptr::write(ptr, Self { key, value, height });

            // Initialize the dynamic array of AtomicPtrs tail
            let next_array = (ptr as *mut u8).add(offset) as *mut AtomicPtr<Node<K, V>>;
            for i in 0..height {
                ptr::write(next_array.add(i), AtomicPtr::new(ptr::null_mut()));
            }
        }

        ptr
    }

    #[inline]
    fn next_ptr(&self, level: usize) -> &AtomicPtr<Node<K, V>> {
        debug_assert!(level < self.height);
        unsafe {
            let offset = Self::next_array_offset();
            let base = (self as *const _ as *const u8).add(offset);
            &*(base as *const AtomicPtr<Node<K, V>>).add(level)
        }
    }
}

/// A Probabilistic Lock-Free Concurrent SkipList.
/// Supports multiple concurrent readers and a synchronized single-writer
/// (or multiple writers if wrapped in a Mutex, providing lock-free reads regardless).
pub struct SkipList<K, V> {
    arena: Arena,
    head: *mut Node<K, V>,
    max_height: AtomicUsize,
    len: AtomicUsize,
}

// SkipList is perfectly safe to share across threads as long as K and V are Send/Sync.
unsafe impl<K: Send + Sync, V: Send + Sync> Send for SkipList<K, V> {}
unsafe impl<K: Send + Sync, V: Send + Sync> Sync for SkipList<K, V> {}

impl<K: Ord + Default, V: Default> SkipList<K, V> {
    pub fn new() -> Self {
        let arena = Arena::new();
        let head = Node::new(K::default(), V::default(), MAX_HEIGHT, &arena);
        Self {
            arena,
            head,
            max_height: AtomicUsize::new(1),
            len: AtomicUsize::new(0),
        }
    }

    /// Access to the internal arena to check memory usage
    #[allow(dead_code)]
    pub fn memory_usage(&self) -> usize {
        self.arena.memory_usage()
    }

    /// Thread-local pseudo-random number generator for calculating node heights (P=0.25).
    fn random_height() -> usize {
        use std::time::{SystemTime, UNIX_EPOCH};
        thread_local! {
            static SEED: std::cell::Cell<u64> = std::cell::Cell::new(
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
            );
        }
        SEED.with(|seed_cell| {
            let mut x = seed_cell.get();
            if x == 0 {
                x = 1; // Prevent xorshift getting stuck at 0
            }
            let mut height = 1;
            while height < MAX_HEIGHT {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                if (x % 4) == 0 {
                    height += 1;
                } else {
                    break;
                }
            }
            seed_cell.set(x);
            height
        })
    }

    /// Core traversal logic. Returns the first node whose key is >= search_key.
    /// Optionally records the trajectory path in `prev` for insert operations.
    fn find_greater_or_equal(
        &self,
        key: &K,
        mut prev: Option<&mut [*mut Node<K, V>; MAX_HEIGHT]>,
    ) -> *mut Node<K, V> {
        let mut current = self.head;
        let mut level = self.max_height.load(Ordering::Relaxed) - 1;

        loop {
            // Acquire ensures we see a fully initialized node if we observe its pointer
            let next = unsafe { (*current).next_ptr(level).load(Ordering::Acquire) };

            if !next.is_null() && unsafe { (*next).key < *key } {
                // Keep moving forward on the same level
                current = next;
            } else {
                // We've gone as far as we can on this level.
                if let Some(ref mut p) = prev {
                    p[level] = current;
                }

                if level == 0 {
                    return next; // Return the first node >= key (or null)
                } else {
                    level -= 1; // Drop down to a finer-grained level
                }
            }
        }
    }

    /// Inserts a new key-value pair, or overwrites the value in-place if the key already exists.
    pub fn insert(&self, key: K, value: V) {
        let mut prev = [ptr::null_mut(); MAX_HEIGHT];
        let next = self.find_greater_or_equal(&key, Some(&mut prev));

        // INFO: If the key already exists, overwrite its value in-place.
        // Since the MemTable has a single logical writer (the StorageEngine write lock),
        // this ptr::write is safe — no concurrent mutations of the same node can occur.
        if !next.is_null() && unsafe { (*next).key == key } {
            unsafe { ptr::write(&mut (*next).value, value) };
            // INFO: Do NOT increment len — we updated an existing slot, not added a new one.
            return;
        }

        let height = Self::random_height();
        let mut current_max = self.max_height.load(Ordering::Relaxed);

        // If the new node is taller than the SkipList's current max height, update max_height
        while height > current_max {
            let next_max = current_max + 1;
            // The previously uninitialized prev pointers at these new levels must point to the Head.
            prev[current_max] = self.head;

            // Atomically update the max height so readers can start using this level
            match self.max_height.compare_exchange_weak(
                current_max,
                next_max,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    current_max = next_max;
                }
                Err(new_val) => {
                    current_max = new_val;
                }
            }
        }

        let new_node = Node::new(key, value, height, &self.arena);

        // 1. Initialize the new node's next pointers FIRST.
        // This ensures readers jumping onto this node mid-flight will see valid `next` paths.
        for i in 0..height {
            let next_node = unsafe { (*prev[i]).next_ptr(i).load(Ordering::Relaxed) };
            unsafe { (*new_node).next_ptr(i).store(next_node, Ordering::Relaxed) };
        }

        // 2. Link the previous nodes to the new node, from BOTTOM to TOP.
        // Release ordering guarantees all writes to new_node are visible to readers
        // who acquire these next pointers.
        for i in 0..height {
            unsafe {
                (*prev[i]).next_ptr(i).store(new_node, Ordering::Release);
            }
        }

        self.len.fetch_add(1, Ordering::Relaxed);
    }

    /// Retrieves a reference to the value if the key exists.
    pub fn get(&self, key: &K) -> Option<&V> {
        let node = self.find_greater_or_equal(key, None);
        if !node.is_null() && unsafe { (*node).key == *key } {
            unsafe { Some(&(*node).value) }
        } else {
            None
        }
    }

    /// Returns the number of elements in the `SkipList`.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Iterator over the elements sequentially.
    pub fn iter(&self) -> SkipListIterator<'_, K, V> {
        SkipListIterator {
            current: unsafe { (*self.head).next_ptr(0).load(Ordering::Acquire) },
            _phantom: PhantomData,
        }
    }
}

impl<K, V> Drop for SkipList<K, V> {
    fn drop(&mut self) {
        let mut current = self.head;
        while !current.is_null() {
            let next = unsafe { (*current).next_ptr(0).load(Ordering::Relaxed) };
            unsafe {
                // Drop the contents (like Vec or String) of the node to prevent leaks.
                // The actual Node memory is completely managed by the Arena and will be
                // freed in bulk when the Arena itself is dropped immediately after this.
                ptr::drop_in_place(current);
            }
            current = next;
        }
    }
}

pub struct SkipListIterator<'a, K, V> {
    current: *mut Node<K, V>,
    _phantom: PhantomData<&'a Node<K, V>>,
}

impl<'a, K: 'a, V: 'a> Iterator for SkipListIterator<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_null() {
            return None;
        }

        // Unsafe block here requires caution: the returned references are tied to the
        // lifetime of the Node. Because nodes are never deleted (until SkipList is dropped),
        // we can safely yield references as long as readers don't outlive the SkipList.
        // We cheat slightly on lifetimes here by using 'a.
        let node = unsafe { &*self.current };
        self.current = node.next_ptr(0).load(Ordering::Acquire);

        Some((&node.key, &node.value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_skip_list_empty() {
        let list: SkipList<String, String> = SkipList::new();
        assert_eq!(list.get(&"apple".to_string()), None);
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn test_skip_list_insert_and_get() {
        let list = SkipList::new();

        list.insert(b"apple".to_vec(), b"red".to_vec());
        list.insert(b"banana".to_vec(), b"yellow".to_vec());
        list.insert(b"grape".to_vec(), b"purple".to_vec());

        assert_eq!(list.get(&b"apple".to_vec()), Some(&b"red".to_vec()));
        assert_eq!(list.get(&b"banana".to_vec()), Some(&b"yellow".to_vec()));
        assert_eq!(list.get(&b"grape".to_vec()), Some(&b"purple".to_vec()));

        assert_eq!(list.get(&b"missing".to_vec()), None);
        assert_eq!(list.len(), 3);
    }

    #[test]
    fn test_skip_list_iterator() {
        let list = SkipList::new();

        // Insert out of order
        list.insert(3, "three");
        list.insert(1, "one");
        list.insert(4, "four");
        list.insert(2, "two");

        let mut iter = list.iter();

        assert_eq!(iter.next(), Some((&1, &"one")));
        assert_eq!(iter.next(), Some((&2, &"two")));
        assert_eq!(iter.next(), Some((&3, &"three")));
        assert_eq!(iter.next(), Some((&4, &"four")));
        assert_eq!(iter.next(), None);
    }
}
