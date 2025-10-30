// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, bail};
pub use builder::SsTableBuilder;
use bytes::Buf;
pub use iterator::SsTableIterator;

use crate::block::{Block, SIZEOF_U32};
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;
use bytes::BufMut;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

// -----------------------------------------------------------
// |          Meta Section         |          Extra          |
// -----------------------------------------------------------
// |            metadata           | meta block offset (u32) |
// -----------------------------------------------------------
//   |
//   |
//   |---> | number of  meta block (u32) | metadata for block#1 | ... | metadata for block #N | checksum for metadata u32 | meta block offset u32 |
//                                              |
//                                              |
//                                              |
//                                              |-> | offset for data block | first_key_len| first_key | last_key_len | last_key |
impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let original_len = buf.len();
        buf.put_u32(block_meta.len() as u32);

        // TODO(xingyu): improve this by acquiring buffer pool with a calculated size;
        for meta_data in block_meta.iter() {
            let first_key_len = meta_data.first_key.len();
            let last_key_len = meta_data.last_key.len();
            buf.put_u32(meta_data.offset as u32);
            buf.put_u16(first_key_len as u16);
            buf.put(meta_data.first_key.raw_ref());
            buf.put_u16(last_key_len as u16);
            buf.put(meta_data.last_key.raw_ref());
        }

        // WARN: we shouldn't include the first u32 since it's for number of block_meta
        let checksum = crc32fast::hash(&buf[original_len + SIZEOF_U32..]);
        buf.put_u32(checksum);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: &[u8]) -> Result<Vec<BlockMeta>> {
        let mut meta_data_blocks = Vec::new();

        let num_block_meta = buf.get_u32();
        let raw_block_meta = &buf[..buf.remaining() - SIZEOF_U32];

        for _ in 0..num_block_meta {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            let last_key_len = buf.get_u16() as usize;
            let last_key = buf.copy_to_bytes(last_key_len);
            meta_data_blocks.push(BlockMeta {
                offset: offset,
                first_key: KeyBytes::from_bytes(first_key),
                last_key: KeyBytes::from_bytes(last_key),
            });
        }

        let checksum = buf.get_u32();
        if checksum != crc32fast::hash(raw_block_meta) {
            bail!("checksum doesn't match!");
        }

        Ok(meta_data_blocks)
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        // read file and decode block_meta data
        let file_len = file.size();
        let raw_bloom_offset = file.read(file_len - SIZEOF_U32 as u64, SIZEOF_U32 as u64)?;
        let bloom_offset = (&raw_bloom_offset[..]).get_u32();
        let raw_bloom = file.read(
            bloom_offset as u64,
            file_len - SIZEOF_U32 as u64 - bloom_offset as u64,
        )?;
        let bloom = Bloom::decode(&raw_bloom)?;

        let raw_meta_offset =
            file.read(bloom_offset as u64 - SIZEOF_U32 as u64, SIZEOF_U32 as u64)?;
        let meta_offset = (&raw_meta_offset[..]).get_u32();
        // here is the len, it's not the end.
        let raw_meta = file.read(
            meta_offset as u64,
            bloom_offset as u64 - SIZEOF_U32 as u64 - meta_offset as u64,
        )?;
        let block_meta = BlockMeta::decode_block_meta(&raw_meta[..])?;
        Ok(SsTable {
            id: id,
            file: file,
            block_meta_offset: meta_offset as usize,
            first_key: block_meta.first().unwrap().first_key.clone(),
            last_key: block_meta.last().unwrap().last_key.clone(),
            block_meta: block_meta,
            block_cache: block_cache,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self.block_meta[block_idx].offset;
        let offset_end = self
            .block_meta
            .get(block_idx + 1)
            .map_or(self.block_meta_offset, |x| x.offset);
        let raw_block = self
            .file
            .read(offset as u64, (offset_end - offset - SIZEOF_U32) as u64)?;
        let raw_checksum = self
            .file
            .read((offset_end - SIZEOF_U32) as u64, SIZEOF_U32 as u64)?;
        let checksum = (&raw_checksum[..]).get_u32();

        if crc32fast::hash(&raw_block) != checksum {
            bail!("checksum doesn't match!");
        }

        let block = Block::decode(&raw_block[..]);
        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        // need to handle if block_cache was None
        if let Some(block_cache) = &self.block_cache {
            let cache_key = (self.id, block_idx);
            let block = block_cache
                .clone()
                .try_get_with(cache_key, || self.read_block(block_idx));
            assert!(block.is_ok());
            Ok(block.unwrap())
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        // binary search on block_meta
        let mut left = 0 as usize;
        let mut right = self.block_meta.len() - 1;

        // find the first key which key > target and we subtract by 1,
        // so we make sure target might be exist from there.
        while left + 1 < right {
            let mid = left + (right - left) / 2;
            if self.block_meta[mid].first_key.as_key_slice() > key {
                right = mid;
            } else {
                left = mid;
            }
        }
        if self.block_meta[left].first_key.as_key_slice() > key {
            // use saturating_sub to prevent "attempt to subtract with overflow"
            return left.saturating_sub(1);
        }
        if self.block_meta[right].first_key.as_key_slice() > key {
            return right.saturating_sub(1);
        }
        return right;

        // self.block_meta
        //     .partition_point(|meta| meta.first_key.as_key_slice() <= key)
        //     .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
