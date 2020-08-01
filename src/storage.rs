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
use crate::raftpb::raft::{ConfState, HardState, Entry, Snapshot};
use crate::util::limit_size;
use thiserror::Error;
use protobuf::Message;
use bytes::{BytesMut, BufMut, Buf, Bytes};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Error, Debug, PartialEq)]
pub enum StorageError {
    // ErrCompacted is returned by Storage.Entries/Compact when a requested
    // index is unavailable because it predates the last snapshot.
    #[error("requested index is unavailable due to comaction")]
    Compacted,
    // ErrSnapOutOfDate is returned by Storage.create_snapshot when a requested
    // index is older than the existing snapshot.
    #[error("requested index is older than the existing snapshot")]
    SnapshotOfDate,
    // ErrUnavailable is returned by Storage interface when the requested log entries
    // are unavailable.
    #[error("requested entry at index is unavailable")]
    Unavailable,
    // ErrSnapshotTemporarilyUnavailable is returned by the Storage interface when the required
    // snapshot is temporarily unavailable.
    #[error("snapshot is temporarily unavailable")]
    SnapshotTemporarilyUnavailable,
}

pub trait Storage {
    // TODO(tbg): split this into two interfaces, LogStorage and StateStorage

    // InitialState returns the saved HardState and ConfState information.
    fn initial_state(&self) -> Result<(HardState, ConfState), StorageError>;
    // Entries returns a slice of log entries in the range [lo, hi).
    // MaxSize limits the total size of the log entries returned, but
    // Entries returns at least one entry if any.
    // NOTE: entries zero is not use
    fn entries(&self, lo: u64, hi: u64, limit: u64) -> Result<Vec<Entry>, StorageError>;
    // Term returns the term of entry i, which must be in the range
    // [FirstIndex()-1, LastIndex()]. The term of the entry before
    // FirstIndex is retained for matching purposes even though the
    // rest of that entry may not be available
    fn term(&self, i: u64) -> Result<u64, StorageError>;
    // LastIndex returns the index of the last entry in the log.
    fn last_index(&self) -> Result<u64, StorageError>;
    // FirstIndex returns the index of the first log entry in that is
    // possibly available via Entries (older entries have been incorporated
    // into the latest Snapshot; if storage only contains the dummy entry the
    // first log entry is not available).
    fn first_index(&self) -> Result<u64, StorageError>;
    // Snapshot returns the most recent snapshot.
    // If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
    // so raft state machine could know that Storage needs some time to prepare
    // snapshot and call Snapshot later.
    fn snapshot(&self) -> Result<Snapshot, StorageError>;
}

// Memory implements the Storage interface backed by an
// in-memory array.
pub struct MemoryStorage {
    // Protects access to all fields. Most methods of MemoryStorage are
    // run on the raft goroutine, but Append() is run on an application
    // goroutine.
    hard_state: HardState,
    snapshot: Snapshot,
    // ents[i] has raft log position i+snapshot.Metadata.Index
    ents: Vec<Entry>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        MemoryStorage {
            hard_state: Default::default(),
            snapshot: Default::default(),
            // When starting from scratch populate the list with a dummy entry at term zero.
            ents: vec![Entry::default()],
        }
    }

    pub fn new_with_entries(entries: Vec<Entry>) -> Self {
        MemoryStorage {
            hard_state: Default::default(),
            snapshot: Default::default(),
            ents: entries,
        }
    }

    // set_hard_state saves the current hard state.
    pub fn set_hard_state(&mut self, st: HardState) -> Result<(), StorageError> {
        self.hard_state = st;
        Ok(())
    }

    // ApplySnapshot overwrites the contents of this Storage object with
    // those of the given snapshot.
    pub fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<(), StorageError> {
        // handle check for old snapshot being applied
        let index = self.snapshot.get_metadata().get_index();
        let snapshot_index = snapshot.get_metadata().get_index();
        if index >= snapshot_index {
            return Err(StorageError::SnapshotOfDate);
        }
        let mut new_entry = Entry::new();
        new_entry.set_Term(snapshot.get_metadata().get_term());
        new_entry.set_Index(snapshot_index);
        self.snapshot = snapshot;
        self.ents = vec![new_entry];
        Ok(())
    }

    // create_snapshot makes a snapshot which can be retrieved with Snapshot() and
    // can be used to reconstruct the state at that point.
    // If any configuration changes have been made since the last compaction,
    // the result of the last ApplyConfigChange must be passed in.
    pub fn create_snapshot(&mut self, i: u64, cs: Option<ConfState>, data: Bytes) -> Result<Snapshot, StorageError> {
        if i <= self.snapshot.get_metadata().get_index() {
            return Err(StorageError::SnapshotOfDate);
        }
        let offset = self.ents.first().unwrap().get_Index();
        if i > self.last_index().unwrap() {
            unimplemented!("snapshot {} is out of bound last_index({})", i, self.last_index().unwrap());
        }
        self.snapshot.mut_metadata().set_index(i);
        self.snapshot.mut_metadata().set_term(self.ents[(i - offset) as usize].get_Term());
        if cs.is_some() {
            // TODO: what is it
            self.snapshot.mut_metadata().set_conf_state(cs.unwrap());
        }
        self.snapshot.set_data(data);
        Ok(self.snapshot.clone())
    }

    // Compact discards all log entries prior to compactIndex.
    // It is the application's responsibility to not attempt to compact an index
    // greater than raftLog.applied.
    pub fn compact(&mut self, compact_index: u64) -> Result<(), StorageError> {
        let offset = self.ents[0].get_Index();
        if compact_index <= offset {
            return Err(StorageError::Compacted);
        }
        if compact_index > self.last_index().unwrap() {
            unimplemented!("compact {} is out of bound last_index({})", compact_index, self.last_index().unwrap())
        }
        let i = (compact_index - offset) as usize;
        self.ents.drain(..i);
        Ok(())
    }

    // Append the new entries to the storage
    // TODO (xiangli): ensure that entries are continuous and
    // entries[0].Index > self.entries[0].Index
    pub fn append(&mut self, mut entries: Vec<Entry>) -> Result<(), StorageError> {
        if entries.is_empty() {
            return Ok(());
        }
        let first = self.first_index()?;
        let last = entries.last().unwrap().get_Index();

        // shortcut if there is no new entry
        if last < first {
            return Ok(());
        }
        // truncate compacted entries
        if first > entries[0].get_Index() {
            entries.drain(..(first - entries[0].get_Index()) as usize);
        }

        let offset = entries[0].get_Index() - self.ents[0].get_Index();
        if offset < self.ents.len() as u64 {
            self.ents.drain((offset as usize)..);
            self.ents.extend_from_slice(&entries);
        } else if offset == self.ents.len() as u64 {
            self.ents.extend_from_slice(&entries);
        } else {
            unimplemented!("missing log entry [last: {}, append at: {}]", self.last_index()?, entries[0].get_Index());
        }
        Ok(())
    }
}

impl Storage for MemoryStorage {
    // initial_state implements the Storage interface.
    fn initial_state(&self) -> Result<(HardState, ConfState), StorageError> {
        Ok((self.hard_state.clone(), self.snapshot.get_metadata().get_conf_state().clone()))
    }
    // TODO: optimized
    // entries implements the Storage interface.
    fn entries(&self, lo: u64, hi: u64, limit: u64) -> Result<Vec<Entry>, StorageError> {
        let offset = self.ents[0].get_Index();
        if lo <= offset {
            return Err(StorageError::Compacted);
        }
        if hi > self.ents.last().unwrap().get_Index() + 1 {
            unimplemented!("entries's hi({}) is out of bound last_index({})", lo, hi);
        }
        // only contains dummy entries
        if self.ents.len() == 1 {
            return Err(StorageError::Compacted);
        }

        let start = (lo - offset) as usize;
        let end = (hi - offset) as usize;

        let ents: Vec<Entry> = self.ents[start..end].iter().map(|item| item.clone()).collect();
        if self.ents.len() == 1 && !ents.is_empty() {
            return Err(StorageError::Unavailable);
        }
        Ok(limit_size(ents, limit))
    }
    // term implements the Storage interface.
    fn term(&self, i: u64) -> Result<u64, StorageError> {
        let offset = self.ents[0].get_Index();
        if i < offset {
            return Err(StorageError::Compacted);
        }
        if (i - offset) as usize >= self.ents.len() {
            return Err(StorageError::Unavailable);
        }
        Ok(self.ents[(i - offset) as usize].get_Term())
    }

    fn last_index(&self) -> Result<u64, StorageError> {
        Ok(self.ents[0].get_Index() + self.ents.len() as u64 - 1)
    }

    // not include dummy_index
    fn first_index(&self) -> Result<u64, StorageError> {
        Ok(self.ents[0].get_Index() + 1)
    }

    fn snapshot(&self) -> Result<Snapshot, StorageError> {
        Ok(self.snapshot.clone())
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        MemoryStorage::new()
    }
}

#[derive(Clone, Default)]
pub struct SafeMemStorage {
    storage: Arc<RwLock<MemoryStorage>>,
}

impl SafeMemStorage {
    pub fn new() -> Self {
        SafeMemStorage::default()
    }

    pub fn rl(&self) -> RwLockReadGuard<'_, MemoryStorage> {
        self.storage.read().unwrap()
    }

    pub fn wl(&self) -> RwLockWriteGuard<'_, MemoryStorage> {
        self.storage.write().unwrap()
    }
}

impl Storage for SafeMemStorage {
    fn initial_state(&self) -> Result<(HardState, ConfState), StorageError> {
        self.rl().initial_state()
    }

    fn entries(&self, lo: u64, hi: u64, limit: u64) -> Result<Vec<Entry>, StorageError> {
        self.rl().entries(lo, hi, limit)
    }

    fn term(&self, i: u64) -> Result<u64, StorageError> {
        self.rl().term(i)
    }

    fn last_index(&self) -> Result<u64, StorageError> {
        self.rl().last_index()
    }

    fn first_index(&self) -> Result<u64, StorageError> {
        self.rl().first_index()
    }

    fn snapshot(&self) -> Result<Snapshot, StorageError> {
        self.rl().snapshot()
    }
}

#[cfg(test)]
mod tests {
    use crate::raftpb::raft::{Entry, ConfChange, ConfState, Snapshot};
    use crate::storage::{StorageError, MemoryStorage, Storage};
    use protobuf::Message;
    use bytes::Bytes;

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn storage_term() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        //(i, w_err, w_term, w_panic)
        let tests = vec![
            (2, Err(StorageError::Compacted), 0, false),
            (3, Ok(()), 3, false),
            (4, Ok(()), 4, false),
            (5, Ok(()), 5, false),
            (6, Err(StorageError::Unavailable), 0, false),
        ];
        for (i, w_err, w_term, w_panic) in tests {
            let mut s = MemoryStorage::new_with_entries(ents.clone());
            match s.term(i) {
                Ok(term) => {}
                Err(e)  if e == w_err.unwrap_err() => {}
                Err(_) => unimplemented!()
            }
        }
    }

    #[test]
    fn storage_entries() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)];
        struct Arg {
            lo: u64,
            hi: u64,
            max_size: u64,
            w_err: Result<(), StorageError>,
            w_entries: Vec<Entry>,
        }
        let tests = vec![Arg {
            lo: 2,
            hi: 6,
            max_size: u64::MAX,
            w_err: Err(StorageError::Compacted),
            w_entries: vec![],
        }, Arg {
            lo: 3,
            hi: 4,
            max_size: u64::MAX,
            w_err: Err(StorageError::Compacted),
            w_entries: vec![],
        }, Arg {
            lo: 4,
            hi: 5,
            max_size: u64::MAX,
            w_err: Ok(()),
            w_entries: vec![new_entry(4, 4)],
        }, Arg {
            lo: 4,
            hi: 6,
            max_size: u64::MAX,
            w_err: Ok(()),
            w_entries: vec![new_entry(4, 4), new_entry(5, 5)],
        }, Arg {
            lo: 4,
            hi: 7,
            max_size: u64::MAX,
            w_err: Ok(()),
            w_entries: vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)],
        }, Arg {
            lo: 4,
            hi: 7,
            max_size: 0,
            w_err: Ok(()),
            w_entries: vec![new_entry(4, 4)],
        }, Arg {
            lo: 4,
            hi: 7,
            max_size: (ents[1].compute_size() + ents[2].compute_size()) as u64,
            w_err: Ok(()),
            w_entries: vec![new_entry(4, 4), new_entry(5, 5)],
        }, Arg {
            lo: 4,
            hi: 7,
            max_size: (ents[1].compute_size() + ents[2].compute_size()) as u64 + ents[3].compute_size() as u64 / 2,
            w_err: Ok(()),
            w_entries: vec![new_entry(4, 4), new_entry(5, 5)],
        }, Arg {
            lo: 4,
            hi: 7,
            max_size: (ents[1].compute_size() + ents[2].compute_size()) as u64 + ents[3].compute_size() as u64 - 1,
            w_err: Ok(()),
            w_entries: vec![new_entry(4, 4), new_entry(5, 5)],
        }, Arg {
            lo: 4,
            hi: 7,
            max_size: (ents[1].compute_size() + ents[2].compute_size() + ents[3].compute_size()) as u64,
            w_err: Ok(()),
            w_entries: vec![new_entry(4, 4), new_entry(5, 5), new_entry(6, 6)],
        }];
        for (i, tt) in tests.iter().enumerate() {
            let mut s = MemoryStorage::new_with_entries(ents.clone());
            match s.entries(tt.lo, tt.hi, tt.max_size) {
                Ok(entries) => {
                    assert_eq!(entries, tt.w_entries);
                }
                Err(ref e)  if e == tt.w_err.as_ref().unwrap_err() => {}
                Err(_) => unimplemented!()
            }
        }
    }

    #[test]
    fn storage_last_index() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut s = MemoryStorage::new_with_entries(ents);
        assert_eq!(s.last_index(), Ok(5));
        s.append(vec![new_entry(6, 6)]);
        assert_eq!(s.last_index(), Ok(6));
    }

    #[test]
    fn storage_first_index() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut s = MemoryStorage::new_with_entries(ents);
        assert_eq!(s.first_index(), Ok(4));
        s.compact(4);
        assert_eq!(s.first_index(), Ok(5));
        println!("{:?}", s.last_index());
    }

    #[test]
    fn storage_compact() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        struct Arg {
            i: u64,
            w_err: Result<(), StorageError>,
            w_index: u64,
            w_term: u64,
            w_len: usize,
        }
        let tests = vec![Arg {
            i: 2,
            w_err: Err(StorageError::Compacted),
            w_index: 3,
            w_term: 3,
            w_len: 3,
        }, Arg {
            i: 3,
            w_err: Err(StorageError::Compacted),
            w_index: 3,
            w_term: 3,
            w_len: 3,
        }, Arg {
            i: 4,
            w_err: Ok(()),
            w_index: 4,
            w_term: 4,
            w_len: 2,
        }, Arg {
            i: 5,
            w_err: Ok(()),
            w_index: 5,
            w_term: 5,
            w_len: 1,
        }];
        for (i, tt) in tests.iter().enumerate() {
            let mut s = MemoryStorage::new_with_entries(ents.clone());
            match s.compact(tt.i) {
                Ok(()) => {
                    assert_eq!(s.ents[0].get_Index(), tt.w_index);
                    assert_eq!(s.ents[0].get_Term(), tt.w_term);
                    assert_eq!(s.ents.len(), tt.w_len);
                }
                Err(ref e)  if e == tt.w_err.as_ref().unwrap_err() => {}
                Err(_) => unimplemented!()
            }
        }
    }

    #[test]
    fn storage_create_snapshot() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        let mut cs = ConfState::new();
        cs.set_voters(vec![1, 2, 3]);
        struct Arg {
            i: u64,
            w_err: Result<(), StorageError>,
            w_snap: Snapshot,
        }

        let data = Bytes::from("data");
        let tests = vec![Arg {
            i: 4,
            w_err: Ok(()),
            w_snap: new_snapshot(data.clone(), 4, 4, cs.clone()),
        }, Arg {
            i: 5,
            w_err: Ok(()),
            w_snap: new_snapshot(data.clone(), 5, 5, cs.clone()),
        }];

        for (i, tt) in tests.iter().enumerate() {
            let mut s = MemoryStorage::new_with_entries(ents.clone());
            match s.create_snapshot(tt.i, Some(cs.clone()), data.clone()) {
                Ok(snapshot) => {
                    assert_eq!(snapshot, tt.w_snap);
                }
                Err(ref e)  if e == tt.w_err.as_ref().unwrap_err() => {}
                Err(_) => unimplemented!()
            }
        }
    }

    #[test]
    fn storage_append() {
        let ents = vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)];
        struct Arg {
            entries: Vec<Entry>,
            w_err: Result<(), StorageError>,
            w_entries: Vec<Entry>,
        }
        let tests = vec![Arg {
            entries: vec![new_entry(1, 1), new_entry(2, 2)],
            w_err: Ok(()),
            w_entries: vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
        }, Arg {
            entries: vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
            w_err: Ok(()),
            w_entries: vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5)],
        }, Arg {
            entries: vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
            w_err: Ok(()),
            w_entries: vec![new_entry(3, 3), new_entry(4, 6), new_entry(5, 6)],
        }, Arg {
            entries: vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
            w_err: Ok(()),
            w_entries: vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
        }, Arg {
            // truncate incoming entries, truncate the existing entries and append
            entries: vec![new_entry(2, 3), new_entry(3, 3), new_entry(4, 5)],
            w_err: Ok(()),
            w_entries: vec![new_entry(3, 3), new_entry(4, 5)],
        }, Arg {
            // truncate the existing entries and append
            entries: vec![new_entry(4, 5)],
            w_err: Ok(()),
            w_entries: vec![new_entry(3, 3), new_entry(4, 5)],
        }, Arg {
            // direct append
            entries: vec![new_entry(6, 5)],
            w_err: Ok(()),
            w_entries: vec![new_entry(3, 3), new_entry(4, 4), new_entry(5, 5), new_entry(6, 5)],
        }];
        for (i, tt) in tests.iter().enumerate() {
            let mut s = MemoryStorage::new_with_entries(ents.clone());
            println!("{}", i);
            match s.append(tt.entries.clone()) {
                Ok(()) => {
                    assert_eq!(tt.w_entries, s.ents);
                }
                Err(ref e)  if e == tt.w_err.as_ref().unwrap_err() => {}
                Err(_) => unimplemented!()
            }
        }
    }

    #[test]
    fn storage_apply_snapshot() {
        let mut cs = ConfState::new();
        cs.set_voters(vec![1, 2, 3]);
        let data = Bytes::from("data");
        let tests = vec![new_snapshot(data.clone(), 4, 4, cs.clone()), new_snapshot(data.clone(), 3, 3, cs.clone())];
        let mut s = MemoryStorage::new();

        // Apply Snapshot successful
        let i = 0;
        let tt = &tests[i];
        assert!(s.apply_snapshot(tt.clone()).is_ok());

        // Apply Snapshot fails due to SnapOutDate
        let i = 1;
        let tt = &tests[i];
        assert_eq!(s.apply_snapshot(tt.clone()), Err(StorageError::SnapshotOfDate));
    }

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut entry = Entry::new();
        entry.set_Index(index);
        entry.set_Term(term);
        entry
    }

    fn new_snapshot(data: Bytes, index: u64, term: u64, cs: ConfState) -> Snapshot {
        let mut snapshot = Snapshot::new();
        snapshot.set_data(data);
        snapshot.mut_metadata().set_index(index);
        snapshot.mut_metadata().set_term(term);
        snapshot.mut_metadata().set_conf_state(cs);
        snapshot
    }
}
