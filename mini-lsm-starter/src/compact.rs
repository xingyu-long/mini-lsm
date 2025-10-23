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
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
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
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let mut new_ssts = Vec::new();
        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let snapshot = {
                    let guard = self.state.read();
                    Arc::clone(&guard)
                };
                let mut iters = Vec::new();
                for id in l0_sstables.iter() {
                    iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables[id].clone(),
                    )?));
                }
                for id in l1_sstables.iter() {
                    iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables[id].clone(),
                    )?));
                }
                let mut iter = MergeIterator::create(iters);

                // also need to handle builder
                let mut builder = None;

                while iter.is_valid() {
                    if builder.is_none() {
                        builder = Some(SsTableBuilder::new(self.options.block_size));
                    }

                    let builder_inner = builder.as_mut().unwrap();

                    if !iter.value().is_empty() {
                        builder_inner.add(iter.key(), iter.value());
                    }

                    // Q: Do I need to do control how many ssts we should have here?
                    // A: we use self.options.target_sst_size
                    if builder_inner.estimated_size() >= self.options.target_sst_size {
                        // Q: how to get the id?
                        // A: next_sst_id()
                        let sst_id = self.next_sst_id();
                        // WARNING: this will take the builder and leave it with None
                        let builder = builder.take().unwrap();
                        let sst =
                            Arc::new(builder.build(sst_id, None, self.path_of_sst(sst_id))?);
                        new_ssts.push(sst);
                    }
                    iter.next()?;
                }

                if let Some(builder) = builder {
                    let sst_id = self.next_sst_id();
                    let sst = Arc::new(builder.build(sst_id, None, self.path_of_sst(sst_id))?);
                    new_ssts.push(sst);
                }
            }
            _ => {}
        }
        Ok(new_ssts)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let l0_sstables;
        let l1_sstables;
        {
            let snapshot = self.state.read();
            l0_sstables = snapshot.l0_sstables.clone();
            l1_sstables = snapshot.levels[0].1.clone();
        };
        let new_ssts = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        })?;

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

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
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
