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

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::{Ok, Result};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    pub(crate) map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Create a bound of `KeyBytes` from a bound of `&[u8]` and `u64`.
pub(crate) fn map_key_bound_plus_ts<'a>(
    lower: Bound<&'a [u8]>,
    upper: Bound<&'a [u8]>,
    ts: u64,
) -> (Bound<KeySlice<'a>>, Bound<KeySlice<'a>>) {
    /*

    See https://github.com/skyzh/mini-lsm/pull/140

    Keys are ordered by (key_bytes, ts) where ts is descending (higher ts first).

        "key1"@100 -> "value1_v100"
        "key1"@50  -> "value1_v50"
        "key1"@10  -> "value1_v10"
        "key2"@200 -> "value2_v200"
        "key2"@100 -> "value2_v100"
        "key3"@50  -> "value3_v50"

        Example 1: Lower bound with Bound::Included("key1") and ts = 75 => Bound::Included(KeySlice::from_slice("key1", 75))
            so we'd like to everything after key1@75, => "key1"@50, "key1"@10, "key2"@200, "key2"@100, "key3"@50

        Example 2: Lower bound with Bound::Excluded("key1") and any ts => Bound::Excluded(KeySlice::from_slice("key1", TS_RANGE_END)) => Bound::Excluded("key1"@0)
            so we'd like to exclude all key1, anything after the MIN KEY of key1 => "key2"@200, "key2"@100, "key3"@50

        Example 3: Upper bound with Bound::Included("key2") (MVCC scan) => Bound::Included(KeySlice::from_slice(x, TS_RANGE_END)) => Bound::Included("key2"@0)
            so we'd like to have until all key2, so the possible end of key2 would be key2@0

        Example 4: Upper bound with Bound::Excluded("key2") => Bound::Excluded(KeySlice::from_slice(x, TS_RANGE_BEGIN)) => Bound::Excluded("key2@MAX")
            so we'd like to exclude all keys, so the possible start of key2 would be key2@MAX

        NOTE: ts is only for lower_bound

    */
    (
        match lower {
            Bound::Included(x) => Bound::Included(KeySlice::from_slice(x, ts)),
            Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice(x, TS_RANGE_END)),
            Bound::Unbounded => Bound::Unbounded,
        },
        match upper {
            Bound::Included(x) => Bound::Included(KeySlice::from_slice(x, TS_RANGE_END)),
            Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice(x, TS_RANGE_BEGIN)),
            Bound::Unbounded => Bound::Unbounded,
        },
    )
}

/// Create a bound of `KeyBytes` from a bound of `KeySlice`.
pub(crate) fn map_key_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        Bound::Excluded(x) => Bound::Excluded(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(x.key_ref()),
            x.ts(),
        )),
        // Bound::Included(x) => Bound::Included(x.to_key_vec().into_key_bytes()),
        // Bound::Excluded(x) => Bound::Excluded(x.to_key_vec().into_key_bytes()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(_id: usize) -> Self {
        Self {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        let path = _path.as_ref();
        let wal = Wal::create(path)?;

        Ok(Self {
            map: Arc::new(SkipMap::new()),
            wal: Some(wal),
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        let path = _path.as_ref();
        let skiplist = SkipMap::new();

        let wal = Wal::recover(path, &skiplist)?;
        Ok(Self {
            map: Arc::new(skiplist),
            wal: Some(wal),
            id: _id,
            // TODO(xingyu): probably update this?
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::from_slice(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        // This function is only used in week 1 tests, so during the week 3 key-ts refactor, you do
        // not need to consider the bound exclude/include logic. Simply provide `DEFAULT_TS` as the
        // timestamp for the key-ts pair.
        self.scan(
            lower.map(|x| KeySlice::from_slice(x, TS_DEFAULT)),
            upper.map(|x| KeySlice::from_slice(x, TS_DEFAULT)),
        )
    }

    /// Get a value by key.
    pub fn get(&self, _key: KeySlice) -> Option<Bytes> {
        // use clone to have new owned data instead of reference
        // Some(self.map.get(_key).unwrap().value().clone())
        //
        // TODO(xingyu): ? ensure Slice won't free this memory.
        let key_bytes = Bytes::from_static(unsafe { std::mem::transmute(_key.key_ref()) });
        self.map
            .get(&KeyBytes::from_bytes_with_ts(key_bytes, _key.ts()))
            .map(|e| e.value().clone())
        // self.map
        //     .get(&_key.to_key_vec().into_key_bytes())
        //     .map(|e| e.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, _key: KeySlice, _value: &[u8]) -> Result<()> {
        self.put_batch(&[(_key, _value)])
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.put_batch(_data)?;
        }

        let mut size = 0;
        for (key, value) in _data {
            size += key.raw_len() + value.len();
            self.map.insert(
                key.to_key_vec().into_key_bytes(),
                Bytes::copy_from_slice(value),
            );
        }

        self.approximate_size
            .fetch_add(size, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, _lower: Bound<KeySlice>, _upper: Bound<KeySlice>) -> MemTableIterator {
        let lower = map_key_bound(_lower);
        let upper = map_key_bound(_upper);
        let mut iter = MemTableIteratorBuilder {
            // this is under shared Arc, we need to clone this to be fully owned by iter.
            map: self.map.clone(),
            iter_builder: |map| map.range((lower, upper)),
            item: (KeyBytes::new(), Bytes::new()),
        }
        .build();

        let entry = iter.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        iter.with_mut(|x| *x.item = entry);
        return iter;
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, _builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            _builder.add(
                KeySlice::from_slice(entry.key().key_ref(), entry.key().ts()),
                entry.value(),
            );
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl MemTableIterator {
    fn entry_to_item(entry: Option<Entry<'_, KeyBytes, Bytes>>) -> (KeyBytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            // 0 allocations if the entry is None
            .unwrap_or_else(|| (KeyBytes::new(), Bytes::from_static(&[])))
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| MemTableIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}
