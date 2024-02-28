#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if self.estimated_size_with(key.raw_ref(), value) > self.block_size && !self.is_empty() {
            return false;
        }

        if self.offsets.is_empty() {
            self.first_key = key.to_key_vec();
        }

        self.offsets.push(self.data.len() as u16);

        self.append_data(key.raw_ref());
        self.append_data(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}

impl BlockBuilder {
    fn append_data(&mut self, b: &[u8]) {
        let n = (b.len() as u16).to_le_bytes();
        self.data.extend_from_slice(&n);
        self.data.extend_from_slice(b);
    }

    fn estimated_size_with(&self, k: &[u8], v: &[u8]) -> usize {
        2 + self.offsets.len() * 2 + self.data.len() + (2 + k.len()) + (2 + v.len())
    }
}
