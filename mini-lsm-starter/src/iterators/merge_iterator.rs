#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

/// (memtable 下标，迭代器)
struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    #[allow(clippy::non_canonical_partial_ord_impl)]
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(&other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters: BinaryHeap<_> = iters
            .into_iter()
            .enumerate()
            .filter(|v| v.1.is_valid())
            .map(|(idx, iter)| HeapWrapper(idx, iter))
            .collect();

        let current = iters.pop();

        Self { iters, current }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        match &self.current {
            Some(v) => v.1.is_valid(),
            None => false,
        }
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Err(anyhow::anyhow!("eof"));
        }

        let last_key = match self.current.take() {
            Some(mut v) if v.1.is_valid() => {
                let k = v.1.key().raw_ref().to_vec();

                v.1.next()?;
                if v.1.is_valid() {
                    self.iters.push(v);
                }

                k
            }
            _ => vec![],
        };

        while let Some(mut v) = self.iters.pop() {
            let k = v.1.key();
            if k.raw_ref() > last_key.as_slice() {
                self.current = Some(v);
                break;
            }

            v.1.next()?;
            if v.1.is_valid() {
                self.iters.push(v);
            }
        }

        Ok(())
    }
}
