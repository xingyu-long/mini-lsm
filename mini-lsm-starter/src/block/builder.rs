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

use crate::{
    block::SIZEOF_U16,
    key::{KeySlice, KeyVec},
};
use bytes::BufMut;

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

// return index which means all things before index are same.
fn compute_overlap(first_key: KeySlice, key: KeySlice) -> usize {
    let first_key = first_key.raw_ref();
    let key = key.raw_ref();
    let mut i = 0;
    loop {
        if i >= first_key.len() || i >= key.len() {
            break;
        }
        if first_key[i] != key[i] {
            break;
        }
        i += 1
    }
    i
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size: block_size,
            first_key: KeyVec::new(),
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // |             Data Section             |              Offset Section             |      Extra      |
    // ----------------------------------------------------------------------------------------------------
    // | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
    // ----------------------------------------------------------------------------------------------------
    fn estimated_size(&self) -> usize {
        self.data.len() + self.offsets.len() * SIZEOF_U16 + SIZEOF_U16
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let total_size = self.estimated_size() + key.len() + value.len() + 3 * SIZEOF_U16; /* key_len + value_len + offset */

        // for the first calculation this is inaccurate
        // since we don't have data and we shouldn't add SIZEOF_U16 for num_of_elements field
        if total_size >= self.block_size && !self.is_empty() {
            return false;
        }

        self.offsets.push(self.data.len() as u16);

        // add key and value
        let overlap = compute_overlap(self.first_key.as_key_slice(), key);
        // key_overlap_len (u16) | rest_key_len (u16) | key (rest_key_len)
        // example:
        // first_key = mini-something, above keys are 5|3|LSM
        // mini-LSM
        self.data.put_u16(overlap as u16);
        self.data.put_u16((key.len() - overlap) as u16);
        self.data.put(&key.raw_ref()[overlap..]);

        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        // why moved it here?
        // for the first key, our expected overlap would be 0
        // so we should set this first_key at later to avoid issues.
        // check how Block::get_first_key() was implemented.
        if self.first_key.is_empty() {
            // record the first_key
            self.first_key = key.to_key_vec();
        }

        return true;
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        return self.data.is_empty();
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty!");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
