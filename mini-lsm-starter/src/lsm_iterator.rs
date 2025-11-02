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

use std::ops::Bound;

use anyhow::{Result, bail};
use bytes::Bytes;

use crate::{
    iterators::{
        StorageIterator, concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
    prev_key: Vec<u8>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound: end_bound,
            prev_key: Vec::new(),
        };
        // skip DELETED values
        // for the case, we had deletions at the beginning
        // and didn't even trigger the next() to call move_to_non_delete.
        // please refer 2nd test within test_task4_integration.
        // iter.move_to_non_delete()?;

        iter.move_to_non_delete_and_skip_same_key()?;
        Ok(iter)
    }

    fn next_inner(&mut self) -> Result<()> {
        self.inner.next()?;
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }
        match &self.end_bound {
            Bound::Included(value) => self.is_valid = self.inner.key().key_ref() <= value.as_ref(),
            Bound::Excluded(value) => self.is_valid = self.inner.key().key_ref() < value.as_ref(),
            Bound::Unbounded => {}
        }
        Ok(())
    }

    fn move_to_non_delete_and_skip_same_key(&mut self) -> Result<()> {
        /*
        ---------------
        memtable:
        1(t8) -> 233333
        3(t9) -> 233333
        ---------------
        imm_memtable:
        1(t4) -> None
        2(t5) -> None
        3(t6) -> 2333
        4(t7) -> 23333
        ---------------
        1(t1) -> 233
        2(t2) -> 2333
        3(t3) -> 23333


        from heap order (key: ascending, t: descending):
        1(t8) -> 233333
        1(t4) -> None
        1(t1) -> 233
        2(t5) -> None
        2(t2) -> 2333
        3(t9) -> 233333
        3(t6) -> 2333
        3(t3) -> 23333
        4(t7) -> 23333

        */
        loop {
            while self.inner.is_valid() && self.inner.key().key_ref() == self.prev_key {
                self.next_inner()?;
            }
            if !self.inner.is_valid() {
                break;
            }
            self.prev_key.clear();
            self.prev_key.extend(self.inner.key().key_ref());
            if !self.inner.value().is_empty() {
                break;
            }
        }

        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        self.move_to_non_delete_and_skip_same_key()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if !self.is_valid() {
            panic!("invalid iterator")
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("invalid iterator")
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("this iteator is tainted")
        }
        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
