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

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Result, bail};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
    mem_table::map_bound,
    mvcc::CommittedTxnData,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("already committed!");
        }
        // add hash(key) into the read set
        if let Some(write_read_set) = &self.key_hashes {
            let mut guard = write_read_set.lock();
            guard.1.insert(farmhash::hash32(key));
        }
        // check the local_storage first
        if let Some(entry) = self.local_storage.get(key) {
            if entry.value().is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(entry.value().clone()));
            }
        }

        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("already committed!");
        }
        // get the TxnLocalIterator from local_storage
        let lower_bytes = map_bound(lower);
        let upper_bytes = map_bound(upper);
        let mut local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((lower_bytes, upper_bytes)),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        let entry = local_iter.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        local_iter.with_mut(|x| *x.item = entry);

        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(
                local_iter,
                self.inner.scan_with_ts(lower, upper, self.read_ts)?,
            )?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("already committed!");
        }
        // add hash(key) into the write set
        if let Some(write_read_set) = &self.key_hashes {
            let mut guard = write_read_set.lock();
            guard.0.insert(farmhash::hash32(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("already committed!");
        }
        // add hash(key) into the write set
        if let Some(write_read_set) = &self.key_hashes {
            let mut guard = write_read_set.lock();
            guard.0.insert(farmhash::hash32(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::from_static(b""));
    }

    pub fn commit(&self) -> Result<()> {
        self.committed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .expect("cannot operate on committed txn!");

        let _commit_lock = self.inner.mvcc().commit_lock.lock();
        let serializable;

        if let Some(guard) = &self.key_hashes {
            let guard = guard.lock();
            let (write_set, read_set) = &*guard;
            println!(
                "commit txn: write_set: {:?}, read_set: {:?}",
                write_set, read_set
            );
            if !write_set.is_empty() {
                let committed_txns = self.inner.mvcc().committed_txns.lock();
                for (_, txn_data) in committed_txns.range((self.read_ts + 1)..) {
                    for key_hash in read_set {
                        // check if the read set of current txn overlaps with committed txns' write set
                        // this is to prevent write skew
                        if txn_data.key_hashes.contains(key_hash) {
                            bail!("serializable check failed");
                        }
                    }
                }
            }
            serializable = true;
        } else {
            serializable = false;
        }

        if serializable {
            // critical section
            let mut records = Vec::new();
            for entry in self.local_storage.iter() {
                if entry.value().is_empty() {
                    records.push(WriteBatchRecord::Del(entry.key().clone()));
                } else {
                    records.push(WriteBatchRecord::Put(
                        entry.key().clone(),
                        entry.value().clone(),
                    ));
                }
            }
            let ts = self.inner.write_batch_inner(&records)?;

            // add this txn into committed_txns
            let mut committed_txns = self.inner.mvcc().committed_txns.lock();
            let mut key_hashes = self.key_hashes.as_ref().unwrap().lock();
            let (write_set, _) = &mut *key_hashes;

            let old_data = committed_txns.insert(
                ts,
                CommittedTxnData {
                    // use write_set for committed data and reset this as empty too.
                    key_hashes: std::mem::take(write_set),
                    read_ts: self.read_ts,
                    commit_ts: ts,
                },
            );

            // ensure this is no same committed ts entry
            assert!(old_data.is_none());

            // remove txns below the watermark
            let watermark = self.inner.mvcc().watermark();
            while let Some(entry) = committed_txns.first_entry() {
                if *entry.key() < watermark {
                    entry.remove();
                } else {
                    break;
                }
            }
        }
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // remove the reader from watermark
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_ref()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = Self { txn, iter: iter };
        iter.move_to_non_delete()?;
        if iter.is_valid() {
            iter.add_to_read_set()?;
        }

        Ok(iter)
    }

    fn move_to_non_delete(&mut self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }

    fn add_to_read_set(&mut self) -> Result<()> {
        // record the valid entry to read set for scan.
        if let Some(guard) = &self.txn.key_hashes {
            let mut guard = guard.lock();
            guard.1.insert(farmhash::hash32(self.iter.key().as_ref()));
        }

        Ok(())
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.move_to_non_delete()?;
        if self.is_valid() {
            self.add_to_read_set()?;
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
