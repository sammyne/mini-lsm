#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        // todo: 检查 2 字节记录 #(entries) 是否足够
        let cap = self.data.len() + self.offsets.len() * 2 + 2;
        let mut out = Vec::with_capacity(cap);

        out.extend_from_slice(&self.data);
        for v in &self.offsets {
            out.extend_from_slice(&v.to_le_bytes());
        }

        out.extend_from_slice(&(self.offsets.len() as u16).to_le_bytes());

        Bytes::from(out)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let ell = data.len();

        let n = u16::from_le_bytes([data[ell - 2], data[ell - 1]]);

        let ell = ell - 2;
        let (data, offsets) = data[..ell].split_at(ell - 2 * (n as usize));

        let data = data.to_vec();
        let offsets: Vec<u16> = offsets
            .chunks_exact(2)
            .map(|v| u16::from_le_bytes([v[0], v[1]]))
            .collect();

        Self { data, offsets }
    }
}
