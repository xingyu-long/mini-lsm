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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use bytes::Buf;

use crate::{
    block::SIZEOF_U16,
    key::{KeySlice, KeyVec},
};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
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
        let mut iter = Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        };
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        };
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    // go to entry by index (i.e., offset)
    fn seek_to(&mut self, index: usize) {
        if index >= self.block.offsets.len() {
            // already reached the end of the block, clear everything
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        let offset = self.block.offsets[index] as usize;
        self.seek_to_offset(offset);
        // update the index instead of offset!!!
        self.idx = index;
    }

    // update the key and value accroding to the data entry
    fn seek_to_offset(&mut self, offset: usize) {
        let key_len = (&self.block.data[offset..offset + SIZEOF_U16]).get_u16() as usize;
        let key_start_index = offset + SIZEOF_U16;
        let key =
            KeySlice::from_slice(&self.block.data[key_start_index..key_start_index + key_len]);
        let value_len = (&self.block.data
            [key_start_index + key_len..key_start_index + key_len + SIZEOF_U16])
            .get_u16() as usize;
        let value_start_index = key_start_index + key_len + SIZEOF_U16;

        self.key = key.to_key_vec();
        self.value_range = (value_start_index, value_start_index + value_len);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // use binary search to speed up since it's sorted.
        let mut left = 0;
        let mut right = self.block.offsets.len();

        while left + 1 < right {
            let mid = left + (right - left) / 2;
            self.seek_to(mid);
            if self.is_valid() {
                if self.key() >= key {
                    right = mid;
                } else {
                    left = mid;
                }
            }
        }
        // try the left first
        self.seek_to(left);
        if self.is_valid() && self.key() >= key {
            return;
        }
        self.seek_to(right);
        if self.is_valid() && self.key() >= key {
            return;
        }

        // let mut index = 0;
        // TOO SLOW!!!
        // while self.key.is_empty() || self.key().cmp(&key) == std::cmp::Ordering::Less {
        //     self.seek_to(index);
        //     index += 1;
        // }
    }
}
