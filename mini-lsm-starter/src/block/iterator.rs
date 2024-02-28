#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the value range from the block
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        if block.offsets.is_empty() {
            return Self::new(block);
        }

        let (k, value_range) = decode_key_and_value_range(&block.data, 0);

        let first_key = KeyVec::from_vec(k.to_vec());
        let key = first_key.clone();

        Self {
            block,
            key,
            value_range,
            idx: 0,
            first_key,
        }
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut out = Self::new(block);

        out.seek_to_key(key);

        out
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        let r = self.value_range.0..self.value_range.1;
        &self.block.data[r]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let k = self.first_key.clone();
        self.seek_to_key(k.as_key_slice())
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        let o = match self.block.offsets.get(self.idx) {
            Some(&v) => v as usize,
            None => {
                self.key.clear();
                return;
            }
        };

        let (key, value_range) = decode_key_and_value_range(&self.block.data, o);

        self.key.clear();
        self.key.append(key);

        self.value_range = value_range;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let idx = find_key(
            &self.block.data,
            &self.block.offsets,
            0,
            self.block.offsets.len(),
            key.raw_ref(),
        );

        let (first_key, value_range) = match self.block.offsets.get(idx) {
            Some(&o) => {
                let (k, value_range) = decode_key_and_value_range(&self.block.data, o as usize);
                (KeyVec::from_vec(k.to_vec()), value_range)
            }
            None => (KeyVec::new(), (0, 0)),
        };

        self.key = first_key.clone();
        self.value_range = value_range;
        self.idx = idx;
        self.first_key = first_key;
    }
}

fn decode_key(data: &[u8]) -> &[u8] {
    let n = u16::from_le_bytes([data[0], data[1]]) as usize;
    &data[2..(n + 2)]
}

fn decode_key_and_value_range(data: &[u8], offset: usize) -> (&[u8], (usize, usize)) {
    let data = &data[offset..];

    let klen = u16::from_le_bytes([data[0], data[1]]) as usize;
    let k = &data[2..(klen + 2)];

    let data = &data[(2 + k.len())..];
    let vlen = u16::from_le_bytes([data[0], data[1]]) as usize;
    let v_start = offset + 2 + klen + 2;
    let v_end = v_start + vlen;

    (k, (v_start, v_end))
}

fn find_key(data: &[u8], offsets: &[u16], l: usize, r: usize, target: &[u8]) -> usize {
    use std::cmp::Ordering;

    if l >= r {
        return r;
    }

    let m = (l + r) / 2;
    let o = offsets[m] as usize;

    let k = decode_key(&data[o..]);
    match k.cmp(target) {
        Ordering::Less => find_key(data, offsets, m + 1, r, target),
        Ordering::Equal => m,
        Ordering::Greater => find_key(data, offsets, l, m, target),
    }
}
