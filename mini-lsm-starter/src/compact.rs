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

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        is_lower_level_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut new_ssts = Vec::new();

        // also need to handle builder
        let mut builder = None;
        let mut last_key = Vec::<u8>::new();

        let watermark = self.mvcc().watermark();
        let mut first_key_below_watermark = false;

        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }

            let is_same_key = iter.key().key_ref() == &last_key;
            let builder_inner = builder.as_mut().unwrap();

            // Prior to MVCC: if it's in bottom level, we can ignore the empty values
            // otherwise, we should keep the value even if it's empty
            // since the other lower levels might have some invalid values.
            //
            // with MVCC: we would need to keep it since another user may have it with older ts

            /*
               a@4=del <- above watermark
               a@3=3   <- latest version below or equal to watermark
               a@2=2   <- can be removed, no one will read it
               a@1=1   <- can be removed, no one will read it
               b@1=1   <- latest version below or equal to watermark
               c@4=4   <- above watermark
               d@3=del <- can be removed if compacting to bottom-most level
               d@2=2   <- can be removed
            */


            // TODO(xingyu): this is smart!!! need to revisit here later
            if !is_same_key {
                first_key_below_watermark = true;
            }

            // handle empty value at bottom level.
            if is_lower_level_bottom_level
                && !is_same_key
                && iter.key().ts() <= watermark
                && iter.value().is_empty()
            {
                last_key.clear();
                last_key.extend(iter.key().key_ref());
                iter.next()?;
                first_key_below_watermark = false;
                continue;
            }

            if is_same_key && iter.key().ts() <= watermark {
                // we deal the case for the first key is less or equal to watermark
                if !first_key_below_watermark {
                    iter.next()?;
                    continue;
                }
                first_key_below_watermark = false;
            }

            builder_inner.add(iter.key(), iter.value());

            // Q: Do I need to do control how many ssts we should have here?
            // A: we use self.options.target_sst_size
            // with MVCC: we'd like to put same key in same file even if the size is greater than
            // target_sst_size
            if builder_inner.estimated_size() >= self.options.target_sst_size && !is_same_key {
                // Q: how to get the id?
                // A: next_sst_id()
                let sst_id = self.next_sst_id();
                // WARNING: this will take the builder and leave it with None
                let builder = builder.take().unwrap();
                let sst = Arc::new(builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?);
                new_ssts.push(sst);
            }

            // update the prev_key;
            if !is_same_key {
                last_key.clear();
                last_key.extend(iter.key().key_ref());
            }

            iter.next()?;
        }

        if let Some(builder) = builder {
            let sst_id = self.next_sst_id();
            let sst = Arc::new(builder.build(sst_id, None, self.path_of_sst(sst_id))?);
            new_ssts.push(sst);
        }
        Ok(new_ssts)
    }

    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                for id in l0_sstables.iter() {
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables[id].clone(),
                    )?));
                }
                let mut l1_ssts_to_concat = Vec::with_capacity(l1_sstables.len());
                for id in l1_sstables.iter() {
                    l1_ssts_to_concat.push(snapshot.sstables[id].clone());
                }
                let iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_ssts_to_concat)?,
                )?;

                self.compact_generate_sst_from_iter(iter, _task.compact_to_bottom_level())
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                ..
            }) => match upper_level {
                Some(_) => {
                    let mut upper_ssts = Vec::with_capacity(upper_level_sst_ids.len());
                    for sst_id in upper_level_sst_ids.iter() {
                        upper_ssts.push(snapshot.sstables[sst_id].clone());
                    }
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;

                    let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                    for sst_id in lower_level_sst_ids.iter() {
                        lower_ssts.push(snapshot.sstables[sst_id].clone());
                    }
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;

                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    self.compact_generate_sst_from_iter(iter, _task.compact_to_bottom_level())
                }
                None => {
                    // use MergeIterator for L0 since it's not sorted
                    let mut upper_iters = Vec::with_capacity(upper_level_sst_ids.len());
                    for sst_id in upper_level_sst_ids.iter() {
                        upper_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables[sst_id].clone(),
                        )?));
                    }

                    let upper_iter = MergeIterator::create(upper_iters);

                    let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                    for sst_id in lower_level_sst_ids.iter() {
                        lower_ssts.push(snapshot.sstables[sst_id].clone());
                    }
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;

                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                    self.compact_generate_sst_from_iter(iter, _task.compact_to_bottom_level())
                }
            },
            CompactionTask::Tiered(TieredCompactionTask {
                tiers,
                bottom_tier_included,
            }) => {
                let mut iters = Vec::with_capacity(tiers.len());
                for (tier_id, sst_ids) in tiers {
                    let mut ssts_to_concat = Vec::with_capacity(sst_ids.len());
                    for sst_id in sst_ids {
                        ssts_to_concat.push(snapshot.sstables[sst_id].clone());
                    }
                    iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(
                        ssts_to_concat,
                    )?));
                }
                let iter = MergeIterator::create(iters);
                self.compact_generate_sst_from_iter(iter, *bottom_tier_included)
            }
            _ => {
                unimplemented!()
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let l0_sstables;
        let l1_sstables;
        {
            let snapshot = self.state.read();
            l0_sstables = snapshot.l0_sstables.clone();
            l1_sstables = snapshot.levels[0].1.clone();
        };
        let compaction_task = &CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        println!("force full compaction: {:?}", compaction_task);

        let new_ssts = self.compact(compaction_task)?;
        // grab the state lock and update it
        {
            let state_lock = self.state_lock.lock();
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();

            for id in snapshot.l0_sstables.iter() {
                snapshot.sstables.remove(id);
            }
            snapshot.l0_sstables.clear();

            for id in snapshot.levels[0].1.iter() {
                snapshot.sstables.remove(id);
            }
            snapshot.levels[0].1.clear();
            for new_sst in new_ssts.iter() {
                snapshot.levels[0].1.push(new_sst.sst_id());
                snapshot.sstables.insert(new_sst.sst_id(), new_sst.clone());
            }
            *guard = Arc::new(snapshot);
        }

        // also remove all old files
        for sst_id in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst_id))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        // 1. trigger compaction with task
        // 2. call controller.apply_compaction_result to update interal states: l0_sstables, levels
        // 3. update snapshot sstables and related info
        // 4. remove all old files
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        if task.is_none() {
            return Ok(());
        }
        let task = task.unwrap();
        self.dump_structure();
        println!("Running compaction task: {:?}", task);

        let new_ssts = self.compact(&task)?;
        let files_added = new_ssts.len();

        // this will be used in apply_compaction_result(...)
        let output = new_ssts.iter().map(|x| x.sst_id()).collect::<Vec<_>>();
        let ssts_to_remove = {
            //  grab the state_lock since we will update snapshot interal state;
            let _state_lock = self.state_lock.lock();

            let (mut new_snapshot, to_be_removed) = self
                .compaction_controller
                // WARN: we need to grab lock here!!!
                // otherwise, we would see things like this
                // flushed 0.sst with size=1069497
                // flushed 1.sst with size=1069586
                // ...
                // flushed 2.sst with size=1069518
                // ...
                // flushed 2.sst with size=1069518
                // ...
                // flushed 2.sst with size=1069518
                // compaction finished: 2 files removed, 2 files added, output=[17, 19]
                // flushed 2.sst with size=1069518 <------ this was repeated mutltiple times, which
                // is incorrect!!!
                // flushed 3.sst with size=1070533
                // Also check the comments at SimpleLeveledCompactionController::apply_compaction_result(...)
                .apply_compaction_result(&self.state.read(), &task, &output, false);
            let mut ssts_to_remove = Vec::with_capacity(to_be_removed.len());
            for file_to_remove in to_be_removed.iter() {
                let result = new_snapshot.sstables.remove(file_to_remove);
                assert!(result.is_some());
                ssts_to_remove.push(result.unwrap());
            }

            let mut new_sst_ids = Vec::new();
            for file_to_add in new_ssts.iter() {
                new_sst_ids.push(file_to_add.sst_id());
                let result = new_snapshot
                    .sstables
                    .insert(file_to_add.sst_id(), file_to_add.clone());
                // ensure this is the new key
                assert!(result.is_none());
            }

            let mut guard = self.state.write();
            *guard = Arc::new(new_snapshot);
            drop(guard);

            self.sync_dir()?;
            // record the compaction task & results into Manifest file.
            self.manifest
                .as_ref()
                .unwrap()
                .add_record_when_init(ManifestRecord::Compaction(task, new_sst_ids))?;

            ssts_to_remove
        };

        println!(
            "compaction finished: {} files removed, {} files added, output={:?}",
            ssts_to_remove.len(),
            files_added,
            output,
        );
        for file_to_remove in ssts_to_remove.iter() {
            std::fs::remove_file(self.path_of_sst(file_to_remove.sst_id()))?;
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let total_memtables;
        {
            let guard = self.state.read();
            total_memtables = guard.imm_memtables.len() + 1;
        }

        if total_memtables >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
