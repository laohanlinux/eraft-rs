// Copyright 2015 The etcd Authors
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

use crate::node::is_empty_snapshot;
use crate::raft::NO_LIMIT;
use crate::raftpb::raft::{Entry, Snapshot};
use crate::storage::Storage;
use crate::storage::StorageError;
use crate::unstable::Unstable;
use crate::util::limit_size;
use std::fmt::{Display, Formatter};
use thiserror::Error;

#[derive(Error, Clone, Debug, PartialEq)]
pub enum RaftLogError {
    #[error("first letter must be lowercase but was {:?}", (.0))]
    FromStorage(StorageError),
}

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
pub struct RaftLog<T: Storage> {
    // storage contains all stable entries since the last snapshot
    pub(crate) storage: T,

    // unstable contains all unstable entries and snapshot
    // they will be saved into storage
    pub(crate) unstable: Unstable,

    // committed is the highest log position that is known to be in
    // stable storage on a quorum of nodes
    pub(crate) committed: u64,

    // applied is the highest log position that the application has
    // been instructed to apply to its state machine.
    // Invariant: applied <= committed
    pub(crate) applied: u64,

    // log entries with index <= stabled are persisted to storage.
    // It is used to record the logs that are not persisted by storage yet.
    // Everytime handling `Ready`, the unstable logs will be included.
    stabled: u64,

    // max_next_ents_size is the maximum number aggregate byte size of the messages
    // returned from calls to nextEnts
    max_next_ents_size: u64,
}

impl<T: Storage> RaftLog<T> {
    // newLog returns log using the given storage. It recovers the log
    // to the state that it just commits and applies the latest snapshot.
    pub fn new(storage: T) -> Self {
        Self::new_log_with_size(storage, NO_LIMIT)
    }

    pub fn new_log_with_size(storage: T, max_next_ents_size: u64) -> Self {
        let mut log = Self {
            storage,
            unstable: Default::default(),
            committed: 0,
            applied: 0,
            stabled: 0,
            max_next_ents_size,
        };
        let first_index = log.storage.first_index().unwrap();
        let last_index = log.storage.last_index().unwrap();
        // TODO
        log.unstable.offset = last_index + 1;
        log.committed = first_index - 1;
        log.applied = first_index - 1;
        info!("init raft_log=> {}", log.to_string());
        log
    }

    // maybe_append returns `None` if the entries cannot be appended. Otherwise,
    // it returns `Some(last index of new entries)`
    // NOTICE: ents[0].index = index + 1, ents[0].term <= log_term if ents not empty, the message come from leader node
    pub(crate) fn maybe_append(
        &mut self,
        index: u64,
        log_term: u64,
        committed: u64,
        ents: &[Entry],
    ) -> Option<u64> {
        if self.match_term(index, log_term) {
            let last_new_i = index + ents.len() as u64;
            match self.find_conflict(ents) {
                0 => {}
                ci if ci <= self.committed => {
                    panic!(
                        "entry {} conflict with committed entry [committed({})]",
                        ci, self.committed
                    );
                }
                ci => {
                    let offset = index + 1;
                    self.append(&ents[(ci - offset) as usize..]);
                }
            }
            //? Why not panic if committed > last_new_i
            self.commit_to(committed.min(last_new_i));
            Some(last_new_i)
        } else {
            None
        }
    }

    pub(crate) fn append(&mut self, ents: &[Entry]) -> u64 {
        if ents.is_empty() {
            return self.last_index();
        }
        let after = ents[0].get_Index() - 1;
        if after < self.committed {
            panic!(
                "after({}) is out of range [committed({})]",
                after, self.committed
            );
        }
        self.unstable.truncate_and_append(ents);
        self.last_index()
    }

    // find_conflict finds the index of the conflict.
    // It returns the first pair of conflicting entries between the existing
    // entries and the given entries, if there are any.
    // If there is no conflicting entries, and the existing entries contains
    // all the given entries, zero will be returned.
    // If there is no conflicting entries, but the given entries contains new
    // entries, the index of the first new entry will be returned.
    // An entry is considered to be conflicting if it has the same index but
    // a different term.
    // The first entry MUST be have an index equal to the argument `from`.
    // The index of the given entries MUST be continuously increasing.
    pub(crate) fn find_conflict(&self, ents: &[Entry]) -> u64 {
        for ent in ents {
            if !self.match_term(ent.get_Index(), ent.get_Term()) {
                // info!("not match at index {}", ent.get_Index());
                if ent.get_Index() <= self.last_index() {
                    let exist_term = self.term(ent.get_Index()).map_or(0, |t| t);
                    info!(
                        "found conflict at index {} [existing term: {}, conflicting term: {}]",
                        ent.get_Index(),
                        exist_term,
                        ent.get_Term()
                    );
                }
                return ent.get_Index();
            }
        }
        0
    }

    /// unstable_entries returns all the unstable entries
    pub fn unstable_entries(&self) -> &[Entry] {
        return &self.unstable.entries;
    }

    /// next_ents returns all the available entries for execution.
    /// If applied is smaller than the index of snapshot, it returns all committed
    /// entries after the index of snapshot.
    pub fn next_ents(&self) -> Vec<Entry> {
        let off = self.first_index().max(self.applied + 1);
        if self.committed + 1 > off {
            self.slice(off, self.committed + 1, self.max_next_ents_size)
                .map_err(|err| panic!("unexpected error when getting unapplied entries ({})", err))
                .unwrap()
        } else {
            vec![]
        }
    }

    // has_next_entries returns if there is any available entries for execution. This
    // is a fast check without heavy raftLog.slice() in raftLog.next_ents().
    pub(crate) fn has_next_entries(&self) -> bool {
        self.committed + 1 > self.first_index().max(self.applied + 1)
    }

    // returns if there is pending snapshot waiting for applying
    pub(crate) fn has_pending_snapshot(&self) -> bool {
        self.unstable.snapshot.is_some()
            && !is_empty_snapshot(&self.unstable.snapshot.as_ref().unwrap())
    }

    pub(crate) fn snapshot(&self) -> Result<Snapshot, RaftLogError> {
        if let Some(snapshot) = &self.unstable.snapshot {
            return Ok(snapshot.clone());
        }
        self.storage
            .snapshot()
            .map_err(|err| RaftLogError::FromStorage(err))
    }

    pub fn first_index(&self) -> u64 {
        if let Some(i) = self.unstable.maybe_first_index() {
            return i;
        }
        self.storage.first_index().unwrap()
    }

    // LastIndex returns the last index of the log entries
    pub fn last_index(&self) -> u64 {
        if let Some(index) = self.unstable.maybe_last_index() {
            index
        } else {
            // TODO(bdarnell)
            self.storage
                .last_index()
                .map_err(|err| unimplemented!("{}", err))
                .unwrap()
        }
    }

    pub(crate) fn commit_to(&mut self, to_commit: u64) {
        // never decrease commit
        if self.committed < to_commit {
            if self.last_index() < to_commit {
                panic!(
                    "to_commit({}) is out of range [last_index({})]. Was the raft log corrupted, truncated, or lost?",
                    to_commit,
                    self.last_index()
                );
            }
            self.committed = to_commit;
        }
    }

    pub(crate) fn applied_to(&mut self, i: u64) {
        if i == 0 {
            return;
        }
        if self.committed < i || i < self.applied {
            panic!(
                "applied({}) is out of range [prev_applied({}), committed({})]",
                i, self.applied, self.committed
            );
        }
        self.applied = i;
    }

    pub(crate) fn stable_to(&mut self, i: u64, t: u64) {
        self.unstable.stable_to(i, t);
    }

    pub(crate) fn stable_snap_to(&mut self, i: u64) {
        self.unstable.stable_snap_to(i)
    }

    pub(crate) fn last_term(&self) -> u64 {
        match self.term(self.last_index()) {
            Ok(t) => t,
            Err(err) => panic!("unexpected error when getting the last term ({})", err),
        }
    }

    // Term return the term of the entry in the given index.
    pub fn term(&self, i: u64) -> Result<u64, RaftLogError> {
        // the valid from range is [index of dummy entry, last index]
        let dummy_index = self.first_index() - 1;
        if i < dummy_index || i > self.last_index() {
            // TODO: return an error instead?
            return Ok(0);
        }
        if let Some(t) = self.unstable.maybe_term(i) {
            return Ok(t);
        }

        self.storage.term(i).or_else(|e| match e {
            StorageError::Compacted | StorageError::Unavailable => Ok(0),
            _ => panic!("unexpected error: {:?}", e),
        })
    }

    pub(crate) fn entries(&self, i: u64, max_size: u64) -> Result<Vec<Entry>, RaftLogError> {
        if i > self.last_index() {
            return Ok(vec![]);
        }
        self.slice(i, self.last_index() + 1, max_size)
    }

    pub(crate) fn all_entries(&self) -> Vec<Entry> {
        match self.entries(self.first_index(), NO_LIMIT) {
            Ok(entries) => entries,
            Err(RaftLogError::FromStorage(StorageError::Compacted)) => self.all_entries(), // try again if there was a racing compact
            Err(err) => panic!("{}", err), // TODO (xiangli): handle error?
        }
    }

    // is_up_to_date determines if the given (last_index, term) log is more up_to_date
    // by comparing the index and term of the last entries in the existing logs.
    // If the logs have last entries with different terms, then the log with the
    // later term is more up-to-date. If the logs end with the same term, then
    // whichever log has the larger last_index is more up-to-date. If the logs are
    // the same, the given log is up-to-date
    pub(crate) fn is_up_to_date(&self, lasti: u64, term: u64) -> bool {
        term > self.last_term() || (term == self.last_term() && lasti >= self.last_index())
    }

    pub(crate) fn match_term(&self, i: u64, term: u64) -> bool {
        self.term(i).map(|t| t == term).map_err(|_| false).unwrap()
    }

    pub(crate) fn maybe_commit(&mut self, max_index: u64, term: u64) -> bool {
        if max_index > self.committed && self.term(max_index).map_or(false, |t| t == term) {
            self.commit_to(max_index);
            return true;
        }
        false
    }

    pub(crate) fn restore(&mut self, s: Snapshot) {
        info!(
            "log [{:?}] starts to restore snapshot [index:{}, term: {}]",
            &self.to_string(),
            s.get_metadata().get_index(),
            s.get_metadata().get_term()
        );
        self.committed = s.get_metadata().get_index();
        self.unstable.restore(s);
    }

    // [lo, hi)
    pub(crate) fn slice(
        &self,
        lo: u64,
        hi: u64,
        max_size: u64,
    ) -> Result<Vec<Entry>, RaftLogError> {
        self.must_check_out_of_bounds(lo, hi)
            .map(|_| Vec::<Entry>::new())?;
        if lo == hi {
            return Ok(vec![]);
        }
        let mut ents = Vec::new();
        if lo < self.unstable.offset {
            match self
                .storage
                .entries(lo, hi.min(self.unstable.offset), max_size)
            {
                Ok(entries) => {
                    // check if ents has reached the size limitation
                    if (entries.len() as u64) < hi.min(self.unstable.offset) - lo {
                        return Ok(entries);
                    }
                    ents = entries;
                }
                Err(StorageError::Compacted) => {
                    return Ok(vec![]);
                }
                Err(StorageError::Unavailable) => unimplemented!(
                    "entries[{}:{}] is unavailable from storage",
                    lo,
                    hi.min(self.unstable.offset)
                ),
                Err(err) => unimplemented!("{}", err),
            }
        }
        if hi > self.unstable.offset {
            let mut unstable = self.unstable.slice(lo.max(self.unstable.offset), hi);
            ents.extend_from_slice(&unstable);
        }

        Ok(limit_size(ents, max_size))
    }

    // l.first_index <= lo <= hi <= l.first_index + l.entries.len()
    fn must_check_out_of_bounds(&self, lo: u64, hi: u64) -> Result<(), RaftLogError> {
        if lo > hi {
            panic!("invalid slice {} > {}", lo, hi);
        }
        let fi = self.first_index();
        if lo < fi {
            return Err(RaftLogError::FromStorage(StorageError::Compacted));
        }
        let length = self.last_index() + 1 - fi;
        if hi > fi + length {
            // It is not same to etcd
            panic!(
                "slice[{}:{}] out of bound [{}:{}]",
                lo,
                hi,
                fi,
                self.last_index()
            );
        }
        Ok(())
    }

    pub fn zero_term_on_err_compacted(&self, index: u64) -> u64 {
        self.term(index).map_or_else(
            |err| {
                if err == RaftLogError::FromStorage(StorageError::Compacted) {
                    0
                } else {
                    panic!("unexpected error ({})", err)
                }
            },
            |term| term,
        )
    }

    fn to_string(&self) -> String {
        format!(
            "last_index={}, term={}, committed={}, applied={}, unstable.offset={}, len(unstable.entries)={}",
            self.last_index(),
            self.last_term(),
            self.committed,
            self.applied,
            self.unstable.offset,
            self.unstable.entries.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::mock::{
        new_empty_entry_set, new_entry, new_entry_set, new_log, new_log_with_storage, new_snapshot,
    };
    use crate::raft::NO_LIMIT;
    use crate::storage::{MemoryStorage, SafeMemStorage, Storage, StorageError};

    use crate::raft_log::{RaftLog, RaftLogError};
    use crate::raftpb::raft::{Entry, SnapshotMetadata};
    use protobuf::Message;
    use std::io::Write;
    use std::panic::{self, AssertUnwindSafe};

    fn init() {
        let mut env = env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "trace");
        env_logger::Builder::from_env(env)
            .format(|buf, record| {
                writeln!(
                    buf,
                    "{} {} [{}:{}], {}",
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                    record.level(),
                    record.module_path().unwrap_or("<unnamed>"),
                    record.line().unwrap(),
                    &record.args()
                )
            })
            .try_init();
    }

    #[test]
    fn it_find_conflict() {
        init();
        let previous_ents = new_entry_set(vec![(1, 1), (2, 2), (3, 3)]);
        // (&[Entry], w_conflict)
        let tests = &[
            // no conflict, empty ent
            (new_entry_set(vec![]), 0),
            // no conflict
            (new_entry_set(vec![(1, 1), (2, 2), (3, 3)]), 0),
            (new_entry_set(vec![(2, 2), (3, 3)]), 0),
            (new_entry_set(vec![(3, 3)]), 0),
            // no conflict, but has new entries
            (
                new_entry_set(vec![(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)]),
                4,
            ),
            (new_entry_set(vec![(2, 2), (3, 3), (4, 4), (5, 4)]), 4),
            (new_entry_set(vec![(3, 3), (4, 4), (5, 5)]), 4),
            (new_entry_set(vec![(4, 4), (5, 5)]), 4),
            // conflicts with existing entries
            (new_entry_set(vec![(1, 4), (2, 4)]), 1),
            (new_entry_set(vec![(2, 1), (3, 4), (4, 4)]), 2),
            (new_entry_set(vec![(3, 1), (4, 2), (5, 4), (6, 4)]), 3),
        ];
        for (entries, w_conflict) in tests.to_vec() {
            let mut raft_log = new_log();
            assert_eq!(3, raft_log.append(&previous_ents));
            let g_conflict = raft_log.find_conflict(&entries);
            assert_eq!(g_conflict, w_conflict);
        }
    }

    #[test]
    fn it_is_up_to_date() {
        init();
        let previous_ents = new_entry_set(vec![(1, 1), (2, 2), (3, 3)]);
        let mut raft_log = new_log();
        raft_log.append(&previous_ents);
        // (last_index, term, w_up_to_date)
        let tests = vec![
            // greater term, ignore, last_index
            (raft_log.last_index() - 1, 4, true),
            (raft_log.last_index(), 4, true),
            (raft_log.last_index() + 1, 4, true),
            // smaller term, ignore last_index
            (raft_log.last_index() - 1, 2, false),
            (raft_log.last_index(), 2, false),
            (raft_log.last_index() + 1, 2, false),
            // equal term, equal or larger last_index wins
            (raft_log.last_index() - 1, 3, false),
            (raft_log.last_index(), 3, true),
            (raft_log.last_index() + 1, 3, true),
        ];

        for (last_index, term, w_up_to_date) in tests {
            assert_eq!(raft_log.is_up_to_date(last_index, term), w_up_to_date);
        }
    }

    #[test]
    fn it_append() {
        init();
        let previous_ents = new_entry_set(vec![(1, 1), (2, 2)]);
        //([]Entry, w_index, w_ents, w_unstable)
        let tests = vec![
            // (new_entry_set(vec![]), 2, new_entry_set(vec![(1, 1), (2, 2)]), 3),
            // (new_entry_set(vec![(3, 2)]), 3, new_entry_set(vec![(1, 1), (2, 2), (3, 2)]), 3),
            // conflicts with index 1
            (
                new_entry_set(vec![(1, 2)]),
                1,
                new_entry_set(vec![(1, 2)]),
                1,
            ),
            // conflicts with index 2
            (
                new_entry_set(vec![(2, 3), (3, 3)]),
                3,
                new_entry_set(vec![(1, 1), (2, 3), (3, 3)]),
                2,
            ),
        ];

        for (entries, w_index, w_ents, w_unstable) in tests {
            let mut storage = MemoryStorage::new();
            assert!(storage.append(previous_ents.clone()).is_ok());
            let mut raft_log = new_log_with_storage(storage);
            let mut index = raft_log.append(&entries);
            // print!("{:?}", raft_log.unstable.entries);
            assert_eq!(index, w_index);
            let g = raft_log.entries(1, NO_LIMIT);
            assert!(g.is_ok());
            assert_eq!(g.unwrap(), w_ents);
            assert_eq!(raft_log.unstable.offset, w_unstable);
        }
    }

    // it_log_maybe_append ensures:
    // If the given (index, term) matches with the existing log:
    //  1. If an existing entry conflict with a new one (same index
    //  but different terms), delete the existing entry and all that
    //  follow it
    //  2. Append any new entries not already in the log
    //  If the given (index, term) does not match that with the existing log:
    //  return false
    #[test]
    fn it_log_maybe_append() {
        init();
        let previous_ents = new_entry_set(vec![(1, 1), (2, 2), (3, 3)]);
        let last_index = 3;
        let last_term = 3;
        let commit = 1;

        // (log_term, index, committed, entries, w_last_i, w_append, w_commit, w_panic)
        let tests = vec![
            // not match: term is different
            (
                last_term - 1,
                last_index,
                last_index,
                new_entry_set(vec![(1, 4)]),
                0,
                false,
                commit,
                false,
            ),
            // not match: index out of bound
            (
                last_term,
                last_index + 1,
                last_index,
                new_entry_set(vec![(last_index + 2, 4)]),
                0,
                false,
                commit,
                false,
            ),
            // match with the last existing entry
            (
                last_term,
                last_index,
                last_index,
                new_empty_entry_set(),
                last_index,
                true,
                last_index,
                false,
            ),
            (
                last_term,
                last_index,
                last_index + 1,
                new_empty_entry_set(),
                last_index,
                true,
                last_index,
                false,
            ), // do not increase commit higher than last_new_i
            (
                last_term,
                last_index,
                last_index - 1,
                new_empty_entry_set(),
                last_index,
                true,
                last_index - 1,
                false,
            ), // commit up to the commit in the message
            (
                last_term,
                last_index,
                0,
                new_empty_entry_set(),
                last_index,
                true,
                commit,
                false,
            ), // commit do not decrease
            (
                0,
                0,
                last_index,
                new_empty_entry_set(),
                0,
                true,
                commit,
                false,
            ), // commit do not decrease
            (
                last_term,
                last_index,
                last_index,
                new_entry_set(vec![(last_index + 1, 4)]),
                last_index + 1,
                true,
                last_index,
                false,
            ),
            (
                last_term,
                last_index,
                last_index + 1,
                new_entry_set(vec![(last_index + 1, 4)]),
                last_index + 1,
                true,
                last_index + 1,
                false,
            ),
            (
                last_term,
                last_index,
                last_index + 2,
                new_entry_set(vec![(last_index + 1, 4)]),
                last_index + 1,
                true,
                last_index + 1,
                false,
            ), // do not increase commit higher than last_new_i
            (
                last_term,
                last_index,
                last_index + 2,
                new_entry_set(vec![(last_index + 1, 4), (last_index + 2, 4)]),
                last_index + 2,
                true,
                last_index + 2,
                false,
            ),
            // match with the entry in the middle
            (
                last_term - 1,
                last_index - 1,
                last_index,
                new_entry_set(vec![(last_index, 4)]),
                last_index,
                true,
                last_index,
                false,
            ),
            (
                last_term - 2,
                last_index - 2,
                last_index,
                new_entry_set(vec![(last_index - 1, 4)]),
                last_index - 1,
                true,
                last_index - 1,
                false,
            ),
            (
                last_term - 3,
                last_index - 3,
                last_index,
                new_entry_set(vec![(last_index - 2, 4)]),
                last_index - 2,
                true,
                last_index - 2,
                true,
            ), // conflict with existing committed entry
            (
                last_term - 2,
                last_index - 2,
                last_index,
                new_entry_set(vec![(last_index - 1, 4), (last_index, 4)]),
                last_index,
                true,
                last_index,
                false,
            ),
        ];

        for (log_term, index, committed, entries, w_last_i, w_append, w_commit, w_panic) in tests {
            let mut raft_log = new_log();
            raft_log.append(&previous_ents);
            raft_log.committed = commit;
            let catch: Result<Option<u64>, _> = panic::catch_unwind(AssertUnwindSafe(|| {
                raft_log.maybe_append(index, log_term, committed, &entries)
            }));
            assert_eq!(catch.is_err(), w_panic);
            if catch.is_err() {
                continue;
            }

            match catch.as_ref().unwrap() {
                Some(g_last_i) => assert_eq!(*g_last_i, w_last_i),
                None => assert!(!w_append),
            }

            assert_eq!(raft_log.committed, w_commit);
            if catch.unwrap().is_some() && !entries.is_empty() {
                let g_ents = raft_log.slice(
                    raft_log.last_index() - entries.len() as u64 + 1,
                    raft_log.last_index() + 1,
                    NO_LIMIT,
                );
                assert!(g_ents.is_ok());
                assert_eq!(entries, g_ents.unwrap());
            }
        }
    }

    // TestCompactionSideEffects ensure that all the log related functionality works correctly after
    // a compaction
    #[test]
    fn it_compaction_side_effects() {
        init();
        let mut i = 0;
        // Populate the log with 1000 entries; 750 in stable storage and 250 in unstable.
        let last_index = 1000;
        let unstable_index = 750;
        let last_term = last_index;
        let mut storage = SafeMemStorage::default();
        for ux in 1..=unstable_index {
            assert!(storage.wl().append(new_entry_set(vec![(ux, ux)])).is_ok());
            assert_eq!(storage.rl().first_index().unwrap(), 1);
            assert_eq!(storage.rl().last_index().unwrap(), ux);
        }
        // assert_eq!(storage.rl().entries(0, unstable_index+1, NO_LIMIT).as_ref().unwrap().len(), unstable_index as usize);
        let mut raft_log = new_log_with_storage(storage.clone());
        println!(
            "last_index {}, last_term {}, committed {}",
            raft_log.last_index(),
            raft_log.last_term(),
            raft_log.committed
        );
        for i in unstable_index..last_index {
            let last_index = raft_log.append(new_entry_set(vec![(i + 1, i + 1)]).as_slice());
            assert_eq!(last_index, i + 1);
        }
        assert!(raft_log.maybe_commit(last_index, last_term));
        raft_log.applied_to(raft_log.committed);

        let offset = 500;
        storage.wl().compact(offset);
        assert_eq!(raft_log.last_index(), last_index);

        for i in offset..=raft_log.last_index() {
            let term = raft_log.term(i);
            assert!(term.is_ok());
            assert_eq!(i, term.unwrap());
        }

        for j in offset..=raft_log.last_index() {
            assert!(raft_log.match_term(j, j));
        }

        let unstable_ents = raft_log.unstable_entries();
        assert_eq!(250, unstable_ents.len());
        assert_eq!(751, unstable_ents[0].get_Index());

        let prev = raft_log.last_index();
        raft_log.append(
            new_entry_set(vec![(raft_log.last_index() + 1, raft_log.last_index() + 1)]).as_slice(),
        );
        assert_eq!(prev + 1, raft_log.last_index());

        let ents = raft_log.entries(raft_log.last_index(), NO_LIMIT);
        assert!(ents.is_ok());
        assert_eq!(ents.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn it_has_next_ents() {
        init();
        let snap = new_snapshot(3, 1);
        let ents = new_entry_set(vec![(4, 1), (5, 1), (6, 1)]);
        // (applied, has_next)
        let tests = vec![(0, true), (3, true), (4, true), (5, false)];

        for (applied, has_next) in tests {
            let mut storage = SafeMemStorage::new();
            storage.wl().apply_snapshot(snap.clone());
            let mut raft_log = new_log_with_storage(storage);
            raft_log.append(ents.as_slice());
            assert!(raft_log.maybe_commit(5, 1));
            raft_log.applied_to(applied);
            info!(
                "first_index {}, committed {}, applied {}",
                raft_log.first_index(),
                raft_log.committed,
                raft_log.applied
            );
            let actual_has_next = raft_log.has_next_entries();
            assert_eq!(has_next, actual_has_next);
        }
    }

    // unstable_ents ensure unstable_ents returns the unstable part of the
    // entries correctly.
    #[test]
    fn it_unstable_ents() {
        init();
        let previous_ents = new_entry_set(vec![(1, 1), (2, 2)]);
        // (unstable, w_ents)
        let tests = vec![(3, vec![]), (1, previous_ents.clone())];

        for (unstable, w_ents) in tests {
            // append stable entries to storage
            let storage = SafeMemStorage::new();
            storage.wl().append(
                previous_ents[..(unstable - 1)]
                    .iter()
                    .map(|entry| entry.clone())
                    .collect(),
            );

            // append unstable entries to raft_log
            let mut raft_log = new_log_with_storage(storage);
            raft_log.append(&previous_ents[(unstable - 1)..]);

            let ents: Vec<Entry> = raft_log
                .unstable_entries()
                .iter()
                .map(|entry| entry.clone())
                .collect();
            if !ents.is_empty() {
                let spot = ents.len() - 1;
                raft_log.stable_to(ents[spot].get_Index(), ents[spot].get_Term());
            }
            assert_eq!(ents.clone(), w_ents.as_slice());

            let w = previous_ents[previous_ents.len() - 1].get_Index() + 1;
            assert_eq!(raft_log.unstable.offset, w);
        }
    }

    #[test]
    fn it_commit_to() {
        init();
        let previous_ents = new_entry_set(vec![(1, 1), (2, 2), (3, 3)]);
        let commit = 2;
        // (commit, w_commit, w_panic)
        let tests = vec![
            (3, 3, false),
            (1, 2, false), // never decrease
            (4, 0, true),  // commit out of range -> panic
        ];

        for (_commit, w_commit, w_panic) in tests {
            let mut raft_log = new_log();
            raft_log.append(previous_ents.as_slice());
            raft_log.committed = commit;
            let catch = panic::catch_unwind(AssertUnwindSafe(|| {
                raft_log.commit_to(_commit);
            }));
            if catch.is_err() {
                assert!(w_panic);
                continue;
            }
            assert_eq!(raft_log.committed, w_commit);
        }
    }

    #[test]
    fn it_stable_to() {
        // (stable_i, stable_t, w_unstable)
        let tests = vec![
            (1, 1, 2),
            (2, 2, 3),
            (2, 1, 1), // bad term
            (3, 1, 1), // bad index
        ];
        for (stable_i, stable_t, w_unstable) in tests {
            let mut raft_log = new_log();
            raft_log.append(new_entry_set(vec![(1, 1), (2, 2)]).as_slice());
            raft_log.stable_to(stable_i, stable_t);
            assert_eq!(raft_log.unstable.offset, w_unstable);
        }
    }

    #[test]
    fn it_stable_to_with_snap() {
        let (snap_i, snap_t) = (5, 2);
        // (stable_i, stable_t, new_ents, w_unstable)
        let tests = vec![
            (snap_i + 1, snap_t, new_empty_entry_set(), snap_i + 1),
            (snap_i, snap_t, new_empty_entry_set(), snap_i + 1),
            (snap_i - 1, snap_t, new_empty_entry_set(), snap_i + 1),
            (snap_i + 1, snap_t + 1, new_empty_entry_set(), snap_i + 1),
            (snap_i, snap_t + 1, new_empty_entry_set(), snap_i + 1),
            (snap_i - 1, snap_t + 1, new_empty_entry_set(), snap_i + 1),
            (
                snap_i + 1,
                snap_t,
                new_entry_set(vec![(snap_i + 1, snap_t)]),
                snap_i + 2,
            ),
            (
                snap_i,
                snap_t,
                new_entry_set(vec![(snap_i + 1, snap_t)]),
                snap_i + 1,
            ),
            (
                snap_i - 1,
                snap_t,
                new_entry_set(vec![(snap_i + 1, snap_t)]),
                snap_i + 1,
            ),
            (
                snap_i + 1,
                snap_t + 1,
                new_entry_set(vec![(snap_i + 1, snap_t)]),
                snap_i + 1,
            ),
            (
                snap_i,
                snap_t + 1,
                new_entry_set(vec![(snap_i + 1, snap_t)]),
                snap_i + 1,
            ),
            (
                snap_i - 1,
                snap_t + 1,
                new_entry_set(vec![(snap_i + 1, snap_t)]),
                snap_i + 1,
            ),
        ];

        for (stable_i, stable_t, new_ents, w_unstable) in tests {
            let s = SafeMemStorage::new();
            s.wl().apply_snapshot(new_snapshot(snap_i, snap_t));
            let mut raft_log = new_log_with_storage(s);
            raft_log.append(new_ents.as_slice());
            raft_log.stable_to(stable_i, stable_t);
            assert_eq!(raft_log.unstable.offset, w_unstable);
        }
    }

    // it_compaction ensure that number of log entries is correct after compactions
    #[test]
    fn it_compaction() {
        init();
        // (last_index, compact, w_left, w_allow)
        let tests = vec![
            // out of upper bound
            // (1000, vec![1001], vec![u64::MAX], false),
            // (1000, vec![300, 500, 800, 900], vec![700, 500, 200, 100], true),
            // out of lower bound
            (1000, vec![300, 299], vec![700, u64::MAX], false),
        ];
        for (last_index, compact, w_left, w_allow) in tests {
            let storage = SafeMemStorage::new();
            for i in 1..=last_index {
                storage.wl().append(new_entry_set(vec![(i, 0)]));
            }
            let mut raft_log = new_log_with_storage(storage.clone());
            raft_log.maybe_commit(last_index, 0);
            raft_log.applied_to(raft_log.committed);

            for (i, compact) in compact.iter().enumerate() {
                let catch =
                    panic::catch_unwind(AssertUnwindSafe(|| storage.wl().compact(*compact)));
                if catch.is_err() || catch.unwrap().is_err() {
                    assert!(!w_allow);
                    continue;
                }
                assert_eq!(raft_log.all_entries().len() as u64, w_left[i]);
            }
        }
    }

    #[test]
    fn it_log_restore() {
        let index = 1000;
        let term = 1000;
        let snap = new_snapshot(index, term);
        let storage = SafeMemStorage::new();
        storage.wl().apply_snapshot(snap.clone());
        let raft_log = new_log_with_storage(storage);
        assert!(raft_log.all_entries().is_empty());
        assert_eq!(raft_log.first_index(), index + 1);
        assert_eq!(raft_log.committed, index);
        assert_eq!(raft_log.unstable.offset, index + 1);
        assert_eq!(raft_log.term(index).map_err(|_| 0).unwrap(), term);
    }

    #[test]
    fn it_is_out_of_bounds() {
        init();
        let offset = 100;
        let num = 100;
        let storage = SafeMemStorage::new();
        storage.wl().apply_snapshot(new_snapshot(offset, 0));
        let mut raft_log = new_log_with_storage(storage);
        (1..=num).for_each(|i| {
            raft_log.append(new_entry_set(vec![(i + offset, 0)]).as_slice());
        });
        let first = offset + 1;
        // (lo, hi, w_panic, w_errCompacted)
        let tests = vec![
            (first - 2, first + 1, false, true),
            (first - 1, first + 1, false, true),
            (first, first, false, false),
            (first + num / 2, first + num / 2, false, false),
            (first + num - 1, first + num - 1, false, false),
            (first + num, first + num, false, false),
            (first + num, first + num + 1, true, false),
            (first + num + 1, first + num + 1, true, false),
        ];

        for (lo, hi, w_panic, w_err_compacted) in tests {
            let catch = panic::catch_unwind(AssertUnwindSafe(|| {
                raft_log.must_check_out_of_bounds(lo, hi)
            }));
            if catch.is_err() {
                assert!(w_panic);
                continue;
            }
            if let Err(err) = catch.unwrap() {
                assert_eq!(err, RaftLogError::FromStorage(StorageError::Compacted));
            }
        }
    }

    #[test]
    fn it_term() {
        let offset = 100;
        let num = 100;
        let storage = SafeMemStorage::new();
        storage.wl().apply_snapshot(new_snapshot(offset, 1));
        let mut raft_log = new_log_with_storage(storage);
        for i in 1..num {
            raft_log.append(new_entry_set(vec![(offset + i, i)]).as_slice());
        }

        // (index, w)
        let tests = vec![
            (offset - 1, 0),
            (offset, 1),
            (offset + num / 2, num / 2),
            (offset + num - 1, num - 1),
            (offset + num, 0),
        ];
        for (index, w) in tests {
            let term = raft_log.term(index).map_err(|_| 0).unwrap();
            assert_eq!(term, w);
        }
    }

    #[test]
    fn it_term_with_unstable_snapshot() {
        let storage_snap_i = 100;
        let unstable_snap_i = storage_snap_i + 5;
        let storage = SafeMemStorage::new();
        storage.wl().apply_snapshot(new_snapshot(storage_snap_i, 1));
        let mut raft_log = new_log_with_storage(storage);
        raft_log.restore(new_snapshot(unstable_snap_i, 1));

        // (index, w)
        let tests = vec![
            // cannot get term from storage
            (storage_snap_i, 0),
            // cannot get term from gap between storage ents and unstable snapshot
            (storage_snap_i + 1, 0),
            (unstable_snap_i - 1, 0),
            // get term from unstable snapshot index
            (unstable_snap_i, 1),
        ];

        for (index, w) in tests {
            let term = raft_log.term(index).unwrap();
            assert_eq!(term, w);
        }
    }

    #[test]
    fn it_slice() {
        init();
        let offset = 100;
        let num = 100;
        let last = offset + num;
        let half = offset + num / 2;
        let half_e = new_entry(half, half);

        let storage = SafeMemStorage::new();
        storage.wl().apply_snapshot(new_snapshot(offset, 0));
        for i in 1..num / 2 {
            storage
                .wl()
                .append(new_entry_set(vec![(offset + i, offset + i)]));
        }
        let mut raft_log = new_log_with_storage(storage.clone());
        for i in num / 2..num {
            raft_log.append(new_entry_set(vec![(offset + i, offset + i)]).as_slice());
        }
        // (from, to, limit, w, w_panic)
        let tests = vec![
            // test no limit
            (
                offset - 1,
                offset + 1,
                NO_LIMIT,
                new_empty_entry_set(),
                false,
            ),
            (offset, offset + 1, NO_LIMIT, new_empty_entry_set(), false),
            (
                half - 1,
                half + 1,
                NO_LIMIT,
                new_entry_set(vec![(half - 1, half - 1), (half, half)]),
                false,
            ),
            (
                half,
                half + 1,
                NO_LIMIT,
                new_entry_set(vec![(half, half)]),
                false,
            ),
            (
                last - 1,
                last,
                NO_LIMIT,
                new_entry_set(vec![(last - 1, last - 1)]),
                false,
            ),
            (last, last + 1, NO_LIMIT, new_empty_entry_set(), true),
            // test limit
            (
                half - 1,
                half + 1,
                0,
                new_entry_set(vec![(half - 1, half - 1)]),
                false,
            ),
            (
                half - 1,
                half + 1,
                half_e.compute_size() as u64 + 1,
                new_entry_set(vec![(half - 1, half - 1)]),
                false,
            ),
            (
                half - 2,
                half + 1,
                half_e.compute_size() as u64 + 1,
                new_entry_set(vec![(half - 2, half - 2)]),
                false,
            ),
            (
                half - 1,
                half + 1,
                half_e.compute_size() as u64 * 2,
                new_entry_set(vec![(half - 1, half - 1), (half, half)]),
                false,
            ),
            (
                half - 1,
                half + 2,
                half_e.compute_size() as u64 * 3,
                new_entry_set(vec![
                    (half - 1, half - 1),
                    (half, half),
                    (half + 1, half + 1),
                ]),
                false,
            ),
            (
                half,
                half + 2,
                half_e.compute_size() as u64,
                new_entry_set(vec![(half, half)]),
                false,
            ),
            (
                half,
                half + 2,
                half_e.compute_size() as u64 * 2,
                new_entry_set(vec![(half, half), (half + 1, half + 1)]),
                false,
            ),
        ];

        for (from, to, limit, w, w_panic) in tests {
            let catch = panic::catch_unwind(AssertUnwindSafe(|| raft_log.slice(from, to, limit)));
            if catch.is_err() {
                assert!(w_panic);
                continue;
            }
            assert!(!w_panic);
            let slice_ret: Result<Vec<Entry>, RaftLogError> = catch.unwrap();
            if from <= offset {
                let err = slice_ret.unwrap_err();
                assert_eq!(err, RaftLogError::FromStorage(StorageError::Compacted));
            }
        }
    }
}
