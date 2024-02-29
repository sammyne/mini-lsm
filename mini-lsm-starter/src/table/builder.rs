#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};

use super::{BlockMeta, FileObject, SsTable};
use crate::block::BlockBuilder;
use crate::key::KeySlice;
use crate::lsm_storage::BlockCache;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        let builder = BlockBuilder::new(block_size);

        let meta = BlockMeta::default();

        Self {
            builder,
            first_key: vec![],
            last_key: vec![],
            data: vec![],
            meta: vec![meta],
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.raw_ref().to_vec();
            self.last_key = self.first_key.clone();
        }

        if !self.builder.add(key, value) {
            self.spawn_block();
            let _ = self.builder.add(key, value);
        }

        let meta = self.meta.last_mut().unwrap();

        // 更新 block 的键范围
        if key.raw_ref() < meta.first_key.raw_ref() {
            meta.first_key = key.raw_ref().to_vec().into();
        }
        if key.raw_ref() > meta.last_key.raw_ref() {
            meta.last_key = key.raw_ref().to_vec().into();
        }

        // 更新 SST 的键范围
        if key.raw_ref() < self.first_key.as_slice() {
            self.first_key = key.raw_ref().to_vec();
        }
        if key.raw_ref() > self.last_key.as_slice() {
            self.last_key = key.raw_ref().to_vec();
        }
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if self.first_key.is_empty() {
            return Err(anyhow::anyhow!("empty SST"));
        }

        self.spawn_block();
        let _ = self.meta.pop();

        let Self {
            first_key,
            last_key,
            mut data,
            meta,
            block_size,
            ..
        } = self;

        let block_meta_offset = data.len();

        BlockMeta::encode_block_meta(&meta, &mut data);

        data.extend_from_slice(block_meta_offset.to_le_bytes().as_slice());

        let file = FileObject::create(path.as_ref(), data).with_context(|| "write to file")?;

        let out = SsTable {
            file,
            block_meta: meta,
            block_meta_offset,
            id,
            block_cache,
            first_key: first_key.into(),
            last_key: last_key.into(),
            bloom: None,
            max_ts: 0,
        };

        Ok(out)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}

impl SsTableBuilder {
    fn spawn_block(&mut self) {
        let encoded_block =
            std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size))
                .build()
                .encode();

        // meta for the new block
        let meta = BlockMeta {
            offset: self.data.len(),
            ..Default::default()
        };

        self.data.extend_from_slice(encoded_block.as_ref());
        self.meta.push(meta);
    }
}
