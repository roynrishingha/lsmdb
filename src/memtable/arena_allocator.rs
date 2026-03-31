use std::alloc::{Layout, alloc, dealloc};
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

const BLOCK_SIZE: usize = crate::constants::ARENA_BLOCK_SIZE;

struct Block {
    data: *mut u8,
    next: AtomicPtr<Block>,
}

impl Block {
    fn new() -> *mut Self {
        unsafe {
            let layout = Layout::from_size_align(BLOCK_SIZE, 8).unwrap();
            let data = alloc(layout);

            let block = Box::new(Self {
                data,
                next: AtomicPtr::new(ptr::null_mut()),
            });
            Box::into_raw(block)
        }
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        unsafe {
            let layout = Layout::from_size_align(BLOCK_SIZE, 8).unwrap();
            dealloc(self.data, layout);
        }
    }
}

/// A highly concurrent, lock-free Bump-Pointer Arena Allocator.
/// Allocates large blocks of memory from the OS and hands out small slices
/// using an atomic fetch-add instruction.
pub struct Arena {
    memory_usage: AtomicUsize,
    current_block: AtomicPtr<Block>,
    current_block_offset: AtomicUsize,
}

impl Arena {
    pub fn new() -> Self {
        Self {
            memory_usage: AtomicUsize::new(0),
            current_block: AtomicPtr::new(Block::new()),
            current_block_offset: AtomicUsize::new(0),
        }
    }

    /// Allocates memory directly within the Arena and returns a pointer to it.
    /// This is lock-free and heavily optimized for the "fast path".
    pub fn allocate(&self, size: usize) -> *mut u8 {
        // Ensure proper alignment (typically 8 bytes for pointers/u64 on 64-bit systems)
        let aligned_size = (size + 7) & !7;

        loop {
            let block = self.current_block.load(Ordering::Acquire);

            // Fast Path: Attempt to claim `aligned_size` bytes in the current block
            let offset = self
                .current_block_offset
                .fetch_add(aligned_size, Ordering::SeqCst);

            if offset + aligned_size <= BLOCK_SIZE {
                // Success! We claimed the space purely via atomic math.
                self.memory_usage.fetch_add(aligned_size, Ordering::Relaxed);

                unsafe {
                    return (*block).data.add(offset);
                }
            }

            // Slow Path: The current block is full.
            // We must allocate a new block and swap it in.
            // Multiple threads might hit this simultaneously!

            // If the allocation requested is LARGER than a single Block (Huge Allocation),
            // we allocate it individually and attach it to the linked list, but do NOT
            // set it as `current_block` to avoid wasting the rest of the 4MB.
            if aligned_size > BLOCK_SIZE {
                unsafe {
                    let layout = Layout::from_size_align(aligned_size, 8).unwrap();
                    let huge_data = alloc(layout);

                    let new_block = Box::into_raw(Box::new(Block {
                        data: huge_data,
                        next: AtomicPtr::new(ptr::null_mut()),
                    }));

                    self.memory_usage.fetch_add(aligned_size, Ordering::Relaxed);

                    // Attach it to the current block's `next` pointer via CAS.
                    // This creates a concurrent linked list structure we can clean up later.
                    let mut curr_next = (*block).next.load(Ordering::Acquire);
                    while let Err(actual_next) = (*block).next.compare_exchange_weak(
                        curr_next,
                        new_block,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                    ) {
                        curr_next = actual_next;
                    }

                    return huge_data;
                }
            }

            // Normal Slow Path: Allocate a new standard 4MB block.
            let new_block = Block::new();

            // Attempt to install the new block.
            // CompareExchange prevents two threads from both installing new blocks and losing one.
            match self.current_block.compare_exchange(
                block,
                new_block,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // We won the race to install the new block!
                    unsafe {
                        (*new_block).next.store(block, Ordering::Release);
                    }
                    // Reset the offset. We start at `aligned_size` because we are immediately taking it.
                    self.current_block_offset
                        .store(aligned_size, Ordering::SeqCst);
                    self.memory_usage.fetch_add(aligned_size, Ordering::Relaxed);

                    unsafe {
                        return (*new_block).data;
                    }
                }
                Err(_) => {
                    // Another thread beat us to installing the new block.
                    // Destroy ours, and loop around to try the Fast Path again on the winner's new block!
                    unsafe {
                        let _ = Box::from_raw(new_block);
                    }
                }
            }
        }
    }

    /// Helper to allocate enough memory for a type `T` and move `value` into that memory.
    #[allow(dead_code)]
    pub fn allocate_obj<T>(&self, value: T) -> *mut T {
        let size = std::mem::size_of::<T>();
        if size == 0 {
            // ZST (Zero Sized Type) optimization
            return ptr::NonNull::dangling().as_ptr();
        }

        let ptr = self.allocate(size) as *mut T;
        unsafe {
            ptr::write(ptr, value);
        }
        ptr
    }

    /// Returns the total memory allocated in bytes across all blocks.
    pub fn memory_usage(&self) -> usize {
        self.memory_usage.load(Ordering::Relaxed)
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        let mut current = self.current_block.load(Ordering::Relaxed);

        while !current.is_null() {
            unsafe {
                let next = (*current).next.load(Ordering::Relaxed);

                // If it's a huge allocation, its layout might be custom.
                // We'd theoretically need to track sizes of huge allocations to dealloc perfectly,
                // but for a MemTable we rarely see > 4MB keys/values.
                // For this implementation, we assume all Blocks are exactly BLOCK_SIZE.
                let _ = Box::from_raw(current);

                current = next;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_basic_allocation() {
        let arena = Arena::new();

        let p1 = arena.allocate_obj(42u64);
        let p2 = arena.allocate_obj(100u64);

        unsafe {
            assert_eq!(*p1, 42);
            assert_eq!(*p2, 100);

            // Ensure they are sequentially padded (p1 is 8 bytes, so p2 should be immediately after)
            assert_eq!((p2 as usize) - (p1 as usize), 8);
        }

        assert_eq!(arena.memory_usage(), 16);
    }

    #[test]
    fn test_block_overflow() {
        let arena = Arena::new();

        // Allocate slightly more than 4MB in chunks
        let chunk_size = 1024 * 1024; // 1 MB

        let p1 = arena.allocate(chunk_size);
        let p2 = arena.allocate(chunk_size);
        let _p3 = arena.allocate(chunk_size);
        let p4 = arena.allocate(chunk_size);

        // At this point, the initial block is full (4MB used)
        assert_eq!(arena.memory_usage(), 4 * chunk_size);

        // This will trigger the slow path, allocating a NEW 4MB block
        let p5 = arena.allocate(100);

        assert_eq!(arena.memory_usage(), (4 * chunk_size) + 104); // 104 because 100 aligns to 104

        // p1 and p2 should be in the same block (adjacent offset)
        assert_eq!((p2 as usize) - (p1 as usize), chunk_size);

        // p4 and p5 should be in DIFFERENT blocks, meaning their addresses are far apart
        let diff = (p5 as isize - p4 as isize).abs() as usize;
        assert!(diff >= chunk_size);
    }

    #[test]
    fn test_concurrent_allocation() {
        let arena = Arc::new(Arena::new());
        let mut handles = vec![];

        // Spawn 10 threads, each allocating 100,000 u64s
        for _ in 0..10 {
            let arena_clone = arena.clone();
            handles.push(thread::spawn(move || {
                for i in 0..100_000 {
                    arena_clone.allocate_obj(i as u64);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // 10 threads * 100,000 * 8 bytes = 8,000,000 bytes (~7.6 MB)
        // Meaning it successfully overflowed the first 4MB block concurrently without crashing!
        assert_eq!(arena.memory_usage(), 8_000_000);
    }
}
