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

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{KeySlice, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{MemTable, map_bound, map_bound_plus_ts};
use crate::mvcc::LsmMvccInner;
use crate::mvcc::txn::{Transaction, TxnIterator};
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

fn range_overlap(
    user_lower: Bound<&[u8]>,
    user_upper: Bound<&[u8]>,
    table_lower: &[u8],
    table_upper: &[u8],
) -> bool {
    // [table_lower, table_upper] [user_lower]
    match user_lower {
        Bound::Included(key) if key > table_upper => {
            return false;
        }
        Bound::Excluded(key) if key >= table_upper => {
            return false;
        }
        _ => {}
    }

    // [user_lower, user_upper] [table_lower]
    match user_upper {
        Bound::Included(key) if table_lower > key => {
            return false;
        }
        Bound::Excluded(key) if table_lower >= key => {
            return false;
        }
        _ => {}
    }
    true
}

fn key_within(user_key: &[u8], table_lower: &[u8], table_upper: &[u8]) -> bool {
    user_key >= table_lower && user_key <= table_upper
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;

        // notify these two threads to stop.
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();

        // wait until current fhreads to stop
        let mut compact_thread = self.compaction_thread.lock();
        if let Some(compact_thread) = compact_thread.take() {
            compact_thread.join().unwrap();
        }

        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread.join().unwrap();
        }

        if !self.inner.options.enable_wal {
            // flush memtables to imm_memtables
            // TODO(xingyu): why we don't need the state_lock???
            if !self.inner.state.read().memtable.is_empty() {
                self.inner
                    .freeze_memtable_with_memtable(Arc::new(MemTable::create(
                        self.inner.next_sst_id(),
                    )))?;
            }

            // flush imm_memtables to disk (i.e., sstable)
            while {
                let snapshot = self.inner.state.read();
                !snapshot.imm_memtables.is_empty()
            } {
                self.inner.force_flush_next_imm_memtable()?;
            }
            self.inner.sync_dir()?;
        } else {
            // with wal enabled, we don't need to flush memtables and wait for the next compaction
            // to complete in the future, and the data won't be lost since WAL ensures data
            // persistency.
            self.inner.sync()?;
            self.inner.sync_dir()?;
        }
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let manifest;
        let block_cache = Arc::new(BlockCache::new(1 << 20));
        let mut state = LsmStorageState::create(&options);
        let mut next_sst_id = 1;

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        if !path.exists() {
            std::fs::create_dir(path)?;
        }

        // recover from manifest file
        let manifest_file = path.join("MANIFEST");
        if !manifest_file.exists() {
            manifest = Manifest::create(manifest_file)?;
            // also check wal option and init wal based memtable if needed
            if options.enable_wal {
                state.memtable = Arc::new(MemTable::create_with_wal(
                    state.memtable.id(),
                    Self::path_of_wal_static(path, state.memtable.id()),
                )?);
            }
            // Q: why do we need this?
            // A: this would help us to record for unfrozen memtable (i.e., not yet freeze to
            // imm_memtables) and also record the memtable with id = 0.
            manifest.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;
        } else {
            let (m, records) = Manifest::recover(manifest_file)?;
            // this memtables means memtable and imm_memtables;
            let mut memtables = BTreeSet::new();
            for record in records {
                match record {
                    // before match
                    ManifestRecord::Flush(sst_id) => {
                        // this sst_id has been flushed to SST and no longer be part of memtables
                        assert!(memtables.remove(&sst_id));
                        // this Flush means from imm_memtables to l0_sstables
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, sst_id);
                        } else {
                            state.levels.insert(0, (sst_id, vec![sst_id]));
                        }
                        next_sst_id = next_sst_id.max(sst_id);
                    }
                    ManifestRecord::Compaction(task, output) => {
                        // this call would modify l0_sstables and levels accordingly
                        let (new_state, _) = compaction_controller
                            .apply_compaction_result(&state, &task, &output, true);
                        state = new_state;
                        next_sst_id =
                            next_sst_id.max(output.iter().max().copied().unwrap_or_default());
                    }
                    ManifestRecord::NewMemtable(memtable_id) => {
                        next_sst_id = next_sst_id.max(memtable_id);
                        // record all memtables
                        memtables.insert(memtable_id);
                    }
                }
            }

            let mut sst_count = 0;
            // recover SST, specifically for sstables (HashMap)
            // iterate l0_sstables and levels to construct the HashMap
            for sst_id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().map(|(_, files)| files).flatten())
            {
                let sst_id = *sst_id;
                let sst = SsTable::open(
                    sst_id,
                    Some(block_cache.clone()),
                    FileObject::open(&Self::path_of_sst_static(path, sst_id))?,
                )?;

                state.sstables.insert(sst_id, Arc::new(sst));
                sst_count += 1;
                next_sst_id = next_sst_id.max(sst_id);
            }
            println!("{} SSTs opened", sst_count);

            next_sst_id += 1;

            // memtable also use sst_id
            if options.enable_wal {
                let mut wal_count = 0;

                // if enable_wal is true, we should recover it from the correspoding wal file.
                for id in memtables.iter() {
                    let memtable =
                        MemTable::recover_from_wal(*id, Self::path_of_wal_static(path, *id))?;
                    if !memtable.is_empty() {
                        state.imm_memtables.insert(0, Arc::new(memtable));
                        wal_count += 1;
                    }
                }
                println!("{} WALs recovered", wal_count);

                state.memtable = Arc::new(MemTable::create_with_wal(
                    next_sst_id,
                    Self::path_of_wal_static(path, next_sst_id),
                )?);
            } else {
                state.memtable = Arc::new(MemTable::create(next_sst_id));
            }

            // do one more record for the current memtable in manifest
            m.add_record_when_init(ManifestRecord::NewMemtable(state.memtable.id()))?;

            next_sst_id += 1;

            manifest = m;
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(LsmMvccInner::new(0)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    pub fn get(self: &Arc<Self>, _key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
        txn.get(_key)
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub(crate) fn get_with_ts(&self, _key: &[u8], read_ts: u64) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // ts-based memtable/imm_memtables retrieve
        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);

        memtable_iters.push(Box::new(snapshot.memtable.scan(
            Bound::Included(KeySlice::from_slice(_key, TS_RANGE_BEGIN)),
            Bound::Included(KeySlice::from_slice(_key, TS_RANGE_END)),
        )));

        for imm_table in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(imm_table.scan(
                Bound::Included(KeySlice::from_slice(_key, TS_RANGE_BEGIN)),
                Bound::Included(KeySlice::from_slice(_key, TS_RANGE_END)),
            )));
        }

        let mem_merge_iter = MergeIterator::create(memtable_iters);
        // a convenient function to check if key might be in SST.
        let is_valid_table = |_key: &[u8], sstable: &SsTable| -> bool {
            if key_within(
                _key,
                sstable.first_key().key_ref(),
                sstable.last_key().key_ref(),
            ) {
                if let Some(bloom) = sstable.bloom.as_ref() {
                    if bloom.may_contain(farmhash::fingerprint32(_key)) {
                        return true;
                    }
                } else {
                    // in case, we don't have bloom, we just move forward
                    return true;
                }
            }
            false
        };

        let mut l0_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        // check the sstables
        for sst_id in snapshot.l0_sstables.iter() {
            let sstable = snapshot.sstables[sst_id].clone();
            if is_valid_table(_key, &sstable) {
                l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_key(
                    sstable,
                    KeySlice::from_slice(_key, TS_RANGE_BEGIN),
                )?));
            }
        }

        // expand this read from L1(i.e., levels[0]) to all levels
        let mut iters_after_l0 = Vec::new();
        for (_, sst_ids) in snapshot.levels.iter() {
            let mut ssts_to_concat = Vec::with_capacity(sst_ids.len());
            for sst_id in sst_ids {
                let sstable = &snapshot.sstables[sst_id];
                if is_valid_table(_key, sstable) {
                    ssts_to_concat.push(sstable.clone());
                }
            }

            iters_after_l0.push(Box::new(SstConcatIterator::create_and_seek_to_key(
                ssts_to_concat,
                KeySlice::from_slice(_key, TS_RANGE_BEGIN),
            )?));
        }

        let merge_iter_after_l0 = MergeIterator::create(iters_after_l0);

        let iter = LsmIterator::new(
            TwoMergeIterator::create(
                TwoMergeIterator::create(mem_merge_iter, MergeIterator::create(l0_iters))?,
                merge_iter_after_l0,
            )?,
            Bound::Unbounded,
            read_ts,
        )?;

        // the iter will skip empty value and always return the valid key, even if the key
        // may doesn't match with _key, but we can have the condition check and ensure the value
        // it returns, otherwise, we just return None for this _key. (that's why we did
        // move_to_non_delete() in lsm_iterator)
        //
        // we don't need to walk through all items since we only care if the first key is _key
        if iter.is_valid() && iter.key() == _key && !iter.value().is_empty() {
            return Ok(Some(Bytes::copy_from_slice(iter.value())));
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let _state_lock = self.mvcc().write_lock.lock();

        let ts = self.mvcc().latest_commit_ts() + 1;
        for record in _batch {
            match record {
                WriteBatchRecord::Put(key, value) => {
                    let key = key.as_ref();
                    let value = value.as_ref();
                    assert!(!key.is_empty());
                    assert!(!value.is_empty());

                    let size;
                    {
                        let snapshot = self.state.read();
                        snapshot
                            .memtable
                            .put(KeySlice::from_slice(key, ts), value)?;
                        // check if we need to force_freeze_memtable
                        size = snapshot.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                }
                WriteBatchRecord::Del(key) => {
                    let key = key.as_ref();
                    assert!(!key.is_empty());

                    let size;
                    {
                        let snapshot = self.state.read();
                        snapshot.memtable.put(KeySlice::from_slice(key, ts), b"")?;
                        // check if we need to force_freeze_memtable
                        size = snapshot.memtable.approximate_size();
                    }
                    self.try_freeze(size)?;
                }
            }
        }
        self.mvcc().update_commit_ts(ts);
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(self: &Arc<Self>, _key: &[u8], _value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(_key, _value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(self: &Arc<Self>, _key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(_key)])
    }

    fn try_freeze(&self, estimated_size: usize) -> Result<()> {
        if estimated_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            // read it again from the CURRENT state
            let guard = self.state.read();

            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                // need to drop guard here explicitly
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable_id = self.next_sst_id();
        let memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(
                memtable_id,
                self.path_of_wal(memtable_id),
            )?)
        } else {
            Arc::new(MemTable::create(memtable_id))
        };

        self.freeze_memtable_with_memtable(memtable)?;

        // also record NewMemtable behavior into manifest
        self.manifest.as_ref().unwrap().add_record(
            state_lock_observer,
            ManifestRecord::NewMemtable(memtable_id),
        )?;

        // TODO(xingyu): why do we need sync here?
        self.sync_dir()?;
        Ok(())
    }

    pub fn freeze_memtable_with_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        let old_memtable;
        {
            // acquire the write lock
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();

            // 1) old_memtable: snapshot.memtable
            // 2) it also moves new memtable to snapshot.memtable
            old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);

            // this shoud be similar with above replace call
            // old_memtable = snapshot.memtable.clone();
            // snapshot.memtable = memtable;

            snapshot.imm_memtables.insert(0, old_memtable);

            // TODO(xingyu): do we need to reset approximate_size???
            // update the snapshot
            *guard = Arc::new(snapshot)
        }
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();

        let flush_memtable;

        {
            let guard = self.state.read();
            flush_memtable = guard
                .imm_memtables
                .last()
                .expect("no imm_memtable!")
                .clone();
        }

        // genereate sstables
        let mut builder = SsTableBuilder::new(self.options.block_size);
        flush_memtable.flush(&mut builder)?;
        let sst_id = flush_memtable.id();
        let sstable = Arc::new(builder.build(sst_id, None, self.path_of_sst(sst_id))?);

        // update internal state, i.e., l0_sstables, sstables and also remove the
        // imm_memtables.last()
        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let mem = snapshot.imm_memtables.pop().unwrap();
            assert_eq!(mem.id(), sst_id);
            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }
            println!("flushed {}.sst with size={}", sst_id, sstable.table_size());
            snapshot.sstables.insert(sst_id, sstable);

            // update guard
            *guard = Arc::new(snapshot);
        }

        self.sync_dir()?;
        // record the flush behavior into Manifest file.
        self.manifest
            .as_ref()
            .unwrap()
            .add_record_when_init(ManifestRecord::Flush(sst_id))?;
        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        // no-op
        Ok(self.mvcc().new_txn(self.clone(), self.options.serializable))
    }

    pub fn scan<'a>(
        self: &'a Arc<Self>,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<TxnIterator> {
        let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);

        txn.scan(_lower, _upper)
    }

    /// Create an iterator over a range of keys.
    pub(crate) fn scan_with_ts(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let mut mem_iters = Vec::new();

        let mem_iter = snapshot.memtable.scan(
            map_bound_plus_ts(_lower, TS_RANGE_BEGIN),
            map_bound_plus_ts(_upper, TS_RANGE_END),
        );

        mem_iters.push(Box::new(mem_iter));

        for table in snapshot.imm_memtables.iter() {
            mem_iters.push(Box::new(table.scan(
                map_bound_plus_ts(_lower, TS_RANGE_BEGIN),
                map_bound_plus_ts(_upper, TS_RANGE_END),
            )));
        }

        let mut l0_sst_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for sst_id in snapshot.l0_sstables.iter() {
            let sstable = snapshot.sstables[sst_id].clone();
            // rule out these impossible ranges
            if range_overlap(
                _lower,
                _upper,
                sstable.first_key().key_ref(),
                sstable.last_key().key_ref(),
            ) {
                // ensure the _lower for the sstables;
                let iter = match _lower {
                    Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                        sstable,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?,
                    Bound::Excluded(key) => {
                        let mut temp_iter = SsTableIterator::create_and_seek_to_key(
                            sstable,
                            KeySlice::from_slice(key, TS_RANGE_BEGIN),
                        )?;

                        if temp_iter.is_valid() && temp_iter.key().key_ref() == key {
                            temp_iter.next()?;
                        }
                        temp_iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sstable)?,
                };
                l0_sst_iters.push(Box::new(iter));
            }
        }

        let mut iters_after_l0 = Vec::new();
        for (_, sst_ids) in snapshot.levels.iter() {
            let mut ssts_to_concat = Vec::with_capacity(sst_ids.len());
            for sst_id in sst_ids {
                let sstable = &snapshot.sstables[sst_id];
                if range_overlap(
                    _lower,
                    _upper,
                    sstable.first_key().key_ref(),
                    sstable.last_key().key_ref(),
                ) {
                    ssts_to_concat.push(sstable.clone());
                }
            }
            let iter = match _lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    ssts_to_concat,
                    KeySlice::from_slice(key, TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(key) => {
                    let mut temp_iter = SstConcatIterator::create_and_seek_to_key(
                        ssts_to_concat,
                        KeySlice::from_slice(key, TS_RANGE_BEGIN),
                    )?;
                    if temp_iter.is_valid() && temp_iter.key().key_ref() == key {
                        temp_iter.next()?;
                    }
                    temp_iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(ssts_to_concat)?,
            };
            iters_after_l0.push(Box::new(iter));
        }

        let merge_iter_after_l0 = MergeIterator::create(iters_after_l0);

        // Note from week2_day1: task3
        // construct a two merge iterator that merges memtables and L0 SSTs,
        // and another merge iterator that merges that iterator with the L1 concat iterator.
        // A: TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>
        // B: SstableConcatIterator<L1_sstables>
        // TwoMergeIterator<A, B>

        let memtable_iter = MergeIterator::create(mem_iters);
        let l0_iter = MergeIterator::create(l0_sst_iters);

        let iter = TwoMergeIterator::create(
            TwoMergeIterator::create(memtable_iter, l0_iter)?,
            merge_iter_after_l0,
        )?;

        Ok(FusedIterator::new(LsmIterator::new(
            iter,
            map_bound(_upper),
            read_ts,
        )?))
    }
}
