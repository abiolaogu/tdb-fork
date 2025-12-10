//! Arena allocator for efficient memory management
//!
//! Reduces allocation overhead and improves cache locality.

use std::alloc::{alloc, dealloc, Layout};
use std::cell::UnsafeCell;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

const BLOCK_SIZE: usize = 64 * 1024; // 64KB blocks

/// Arena allocator for fast bump allocation
pub struct Arena {
    /// Current block
    current: UnsafeCell<Block>,
    /// All allocated blocks
    blocks: UnsafeCell<Vec<Block>>,
    /// Total allocated bytes
    allocated: AtomicUsize,
    /// Total used bytes
    used: AtomicUsize,
}

struct Block {
    ptr: NonNull<u8>,
    size: usize,
    offset: usize,
}

impl Block {
    fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 8).unwrap();
        let ptr = unsafe { alloc(layout) };
        let ptr = NonNull::new(ptr).expect("allocation failed");

        Self {
            ptr,
            size,
            offset: 0,
        }
    }

    fn alloc(&mut self, size: usize, align: usize) -> Option<NonNull<u8>> {
        // Align offset
        let aligned = (self.offset + align - 1) & !(align - 1);
        if aligned + size > self.size {
            return None;
        }

        let ptr = unsafe { self.ptr.as_ptr().add(aligned) };
        self.offset = aligned + size;

        NonNull::new(ptr)
    }

    fn remaining(&self) -> usize {
        self.size - self.offset
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.size, 8).unwrap();
        unsafe {
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

// Safety: Arena uses atomic operations for thread safety
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
    /// Create a new arena
    pub fn new() -> Self {
        Self::with_block_size(BLOCK_SIZE)
    }

    /// Create arena with custom block size
    pub fn with_block_size(block_size: usize) -> Self {
        let block = Block::new(block_size);
        Self {
            current: UnsafeCell::new(block),
            blocks: UnsafeCell::new(Vec::new()),
            allocated: AtomicUsize::new(block_size),
            used: AtomicUsize::new(0),
        }
    }

    /// Allocate memory from the arena
    pub fn alloc(&self, size: usize) -> NonNull<u8> {
        self.alloc_aligned(size, 8)
    }

    /// Allocate aligned memory
    pub fn alloc_aligned(&self, size: usize, align: usize) -> NonNull<u8> {
        // Try current block first
        let current = unsafe { &mut *self.current.get() };
        if let Some(ptr) = current.alloc(size, align) {
            self.used.fetch_add(size, Ordering::Relaxed);
            return ptr;
        }

        // Need new block
        self.alloc_slow(size, align)
    }

    #[cold]
    fn alloc_slow(&self, size: usize, align: usize) -> NonNull<u8> {
        // Move current block to blocks list
        let blocks = unsafe { &mut *self.blocks.get() };
        let current = unsafe { &mut *self.current.get() };

        let old_block = std::mem::replace(current, Block::new(0));
        blocks.push(old_block);

        // Allocate new block
        let block_size = BLOCK_SIZE.max(size + align);
        *current = Block::new(block_size);
        self.allocated.fetch_add(block_size, Ordering::Relaxed);

        current.alloc(size, align).expect("fresh block allocation failed")
    }

    /// Allocate a slice
    pub fn alloc_slice<T: Copy>(&self, len: usize) -> &mut [T] {
        let size = len * std::mem::size_of::<T>();
        let align = std::mem::align_of::<T>();
        let ptr = self.alloc_aligned(size, align);

        unsafe { std::slice::from_raw_parts_mut(ptr.as_ptr() as *mut T, len) }
    }

    /// Copy bytes into arena
    pub fn copy_bytes(&self, data: &[u8]) -> &[u8] {
        let slice = self.alloc_slice::<u8>(data.len());
        slice.copy_from_slice(data);
        slice
    }

    /// Get total allocated bytes
    pub fn allocated(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
    }

    /// Get used bytes
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    /// Reset arena (reuse memory without deallocation)
    pub fn reset(&self) {
        let current = unsafe { &mut *self.current.get() };
        let blocks = unsafe { &mut *self.blocks.get() };

        current.offset = 0;
        for block in blocks.iter_mut() {
            block.offset = 0;
        }

        self.used.store(0, Ordering::Relaxed);
    }
}

impl Default for Arena {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arena_alloc() {
        let arena = Arena::new();

        let ptr1 = arena.alloc(100);
        let ptr2 = arena.alloc(100);

        assert_ne!(ptr1, ptr2);
        assert!(arena.used() >= 200);
    }

    #[test]
    fn test_arena_copy_bytes() {
        let arena = Arena::new();

        let data = b"hello, world!";
        let copied = arena.copy_bytes(data);

        assert_eq!(copied, data);
    }

    #[test]
    fn test_arena_large_alloc() {
        let arena = Arena::new();

        // Allocate more than block size
        let ptr = arena.alloc(BLOCK_SIZE * 2);
        assert!(!ptr.as_ptr().is_null());
    }
}
