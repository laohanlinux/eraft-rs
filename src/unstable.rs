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

use crate::raftpb::raft::{Entry, Snapshot};

// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
#[derive(Default, Clone)]
pub(crate) struct Unstable {
    // the incoming unstable snapshot, if any.
    pub(crate) snapshot: Option<Snapshot>,
    // all entries that have not been yet been written to storage.
    pub(crate) entries: Vec<Entry>,
    // the first index of `entries` first entry
    pub(crate) offset: u64,
}

impl Unstable {
    // Returns the index of the first possible entry in entries if it has a snapshot.
    pub(crate) fn maybe_first_index(&self) -> Option<u64> {
        self.snapshot
            .as_ref()
            .map(|snapshot| snapshot.get_metadata().get_index() + 1)
    }

    // Returns last index if it has at least one unstable entry or snapshot
    pub(crate) fn maybe_last_index(&self) -> Option<u64> {
        if !self.entries.is_empty() {
            return Some(self.offset + self.entries.len() as u64 - 1);
        }
        self.snapshot
            .as_ref()
            .map(|snapshot| snapshot.get_metadata().get_index())
    }

    // Returns the term of the entry at index i, if there is any.
    pub(crate) fn maybe_term(&self, i: u64) -> Option<u64> {
        if i < self.offset {
            if let Some(snapshot) = self.snapshot.as_ref() {
                if snapshot.get_metadata().get_index() == i {
                    return Some(snapshot.get_metadata().get_term());
                }
            }
            return None;
        }
        match self.maybe_last_index() {
            Some(index) => {
                if i > index {
                    None
                } else {
                    Some(self.entries[(i - self.offset) as usize].Term)
                }
            }
            None => None,
        }
    }

    // If self.entries had written to storage then clears these entries by stable_to
    pub(crate) fn stable_to(&mut self, i: u64, t: u64) {
        if let Some(gt) = self.maybe_term(i) {
            // if i < offset, term is matched with the snapshot
            // only update the unstable entries if term is matched with
            // an unstable entry
            if gt == t && i >= self.offset {
                let start = i + 1 - self.offset;
                // TODO: Optz entries memory 
                self.entries.drain(..start as usize);
                self.offset = i + 1;
            }
        }
    }

   
    // As same to stable_to, if self.snapshot had written to storage then reset snapshot
    pub(crate) fn stable_snap_to(&mut self, i: u64) {
        if let Some(ref snapshot) = self.snapshot {
            if snapshot.get_metadata().get_index() == i {
                self.snapshot = None;
            }
        }
    }

    pub(crate) fn restore(&mut self, s: Snapshot) {
        self.offset = s.get_metadata().get_index() + 1;
        self.entries.clear();
        self.snapshot = Some(s);
    }

    pub(crate) fn truncate_and_append(&mut self, ents: &[Entry]) {
        match ents[0].get_Index() {
            after if after == self.offset + self.entries.len() as u64 => {
                // after is the next index in the self.entries
                // directly append
                self.entries.extend_from_slice(ents);
            }
            after if after <= self.offset => {
                info!("replace the unstable entries from index {}", after);
                // The log is being truncated to before our current offset
                // portion, so set the offset and replace the entries
                self.offset = after;
                self.entries.clear();
                self.entries.extend_from_slice(ents);
            }
            after => {
                // truncate to after and copy to self.entries
                // then append
                info!("truncate the unstable entries before index {}", after);
                self.entries.truncate((after - self.offset) as usize);
                self.entries.extend_from_slice(&ents);
            }
        }
    }

    pub(crate) fn slice(&self, lo: u64, hi: u64) -> Vec<Entry> {
        self.must_check_out_of_bounds(lo, hi);
        self.entries[(lo - self.offset) as usize..(hi - self.offset) as usize].to_vec()
    }

    // self.offset <= lo <= hi <= self.offset + self.entries.len()
    fn must_check_out_of_bounds(&self, lo: u64, hi: u64) {
        if lo > hi {
            panic!("invalid unstable.slice {} > {}", lo, hi);
        }
        let upper = self.offset + self.entries.len() as u64;
        if lo < self.offset || hi > upper {
            panic!(
                "unstable.slice[{}, {}] out of bound [{}, {}]",
                lo, hi, self.offset, upper
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::raftpb::raft::{Entry, Snapshot};
    use crate::unstable::Unstable;

    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn it_unstable_maybe_first_index() {
        // (entries, offset, snapshot, w_ok, w_index)
        let tests = vec![
            // no snapshot
            (vec![new_entry(5, 1)], 0, None, false, 0),
            (vec![], 0, None, false, 0),
            // has snapshot
            (vec![new_entry(5, 1)], 5, Some(new_snapshot(4, 1)), true, 5),
            (vec![], 5, Some(new_snapshot(4, 1)), true, 5),
        ];
        for (i, (entries, offset, snapshot, w_ok, w_index)) in tests.iter().enumerate() {
            let mut u = Unstable {
                snapshot: snapshot.clone(),
                entries: entries.clone(),
                offset: *offset,
            };
            match u.maybe_first_index() {
                Some(i) => {
                    assert_eq!(i, *w_index);
                }
                None => assert!(!*w_ok),
            }
        }
    }

    #[test]
    fn it_maybe_last_index() {
        // (entries, offset, snapshot, w_ok, w_index)
        let tests = vec![
            // last in entries
            (vec![new_entry(5, 1)], 5, None, true, 5),
            (vec![new_entry(5, 1)], 5, Some(new_snapshot(4, 1)), true, 5),
            // last in snapshot
            (vec![], 5, Some(new_snapshot(4, 1)), true, 4),
            // empty unstable
            (vec![], 0, None, false, 0),
        ];
        for (i, (entries, offset, snapshot, w_ok, w_index)) in tests.iter().enumerate() {
            let u = Unstable {
                snapshot: snapshot.clone(),
                entries: entries.clone(),
                offset: *offset,
            };
            match u.maybe_last_index() {
                Some(i) => {
                    assert_eq!(i, *w_index);
                }
                None => assert!(!*w_ok),
            }
        }
    }

    #[test]
    fn it_unstable_maybe_term() {
        // (entries, offset, snapshot, index, w_ok, w_term)
        let tests = vec![
            // term from entries
            (vec![new_entry(5, 1)], 5, None, 5, true, 1),
            (vec![new_entry(5, 1)], 5, None, 6, false, 0),
            (
                vec![new_entry(5, 1)],
                5,
                Some(new_snapshot(4, 1)),
                5,
                true,
                1,
            ),
            (
                vec![new_entry(5, 1)],
                5,
                Some(new_snapshot(4, 1)),
                6,
                false,
                0,
            ),
            // term from snapshot
            (
                vec![new_entry(5, 1)],
                5,
                Some(new_snapshot(4, 1)),
                4,
                true,
                1,
            ),
            (
                vec![new_entry(5, 1)],
                5,
                Some(new_snapshot(4, 1)),
                3,
                false,
                0,
            ),
            (vec![], 5, Some(new_snapshot(4, 1)), 5, false, 0),
            (vec![], 5, Some(new_snapshot(4, 1)), 4, true, 1),
            (vec![], 0, None, 5, false, 0),
        ];
        for (i, (entries, offset, snapshot, index, w_ok, w_term)) in tests.iter().enumerate() {
            let u = Unstable {
                snapshot: snapshot.clone(),
                entries: entries.clone(),
                offset: *offset,
            };
            match u.maybe_term(*index) {
                Some(i) => assert_eq!(i, *w_term),
                None => assert!(!*w_ok),
            }
        }
    }

    #[test]
    fn it_unstable_restore() {
        let mut u = Unstable {
            snapshot: Some(new_snapshot(4, 1)),
            entries: vec![new_entry(5, 1)],
            offset: 5,
        };
        let s = new_snapshot(6, 2);
        u.restore(s.clone());
        assert_eq!(u.offset, s.get_metadata().get_index() + 1);
        assert!(u.entries.is_empty());
        assert_eq!(u.snapshot.unwrap(), s);
    }

    #[test]
    fn it_unstable_stable_to() {
        // (entries, offset, snapshot, index, term, w_offset, w_len)
        let tests = vec![
            (vec![], 0, None, 5, 1, 0, 0),
            (vec![new_entry(5, 1)], 5, None, 5, 1, 6, 0), // stable to the first entry
            (new_batch_entry(vec![(5, 1), (6, 1)]), 5, None, 5, 1, 6, 1), // stable to the first entry
            (vec![new_entry(6, 2)], 6, None, 6, 1, 6, 1), // stable to the first entry and term mismatch
            (vec![new_entry(5, 1)], 5, None, 4, 1, 5, 1), // stable to old entry
            (vec![new_entry(5, 1)], 5, None, 4, 2, 5, 1), // stable to old entry
            // with snapshot
            (
                vec![new_entry(5, 1)],
                5,
                Some(new_snapshot(4, 1)),
                5,
                1,
                6,
                0,
            ), // stable to the first entry
            (
                new_batch_entry(vec![(5, 1), (6, 1)]),
                5,
                Some(new_snapshot(4, 1)),
                5,
                1,
                6,
                1,
            ), // stable to the first entry
            (
                vec![new_entry(6, 2)],
                6,
                Some(new_snapshot(5, 1)),
                6,
                1,
                6,
                1,
            ), // stable to the first entry and term mismatch
            (
                vec![new_entry(5, 1)],
                5,
                Some(new_snapshot(4, 1)),
                4,
                1,
                5,
                1,
            ), // stable to snapshot
            (
                vec![new_entry(5, 2)],
                5,
                Some(new_snapshot(4, 2)),
                4,
                1,
                5,
                1,
            ), // stable to old entry
        ];
        for (i, (entries, offset, snapshot, index, term, w_offset, w_len)) in
        tests.iter().enumerate()
        {
            let mut u = Unstable {
                snapshot: snapshot.clone(),
                entries: entries.clone(),
                offset: *offset,
            };
            u.stable_to(*index, *term);
            assert_eq!(u.offset, *w_offset);
            assert_eq!(u.entries.len(), *w_len);
        }
    }

    #[test]
    fn it_unstable_stable_truncate_and_append() {
        // (entries, offset, snapshot, to_append, w_offset, w_entries)
        let tests: Vec<(_, _, Option<Snapshot>, _, _, _)> = vec![
            // append to the end
            (
                vec![new_entry(5, 1)],
                5,
                None,
                new_batch_entry(vec![(6, 1), (7, 1)]),
                5,
                new_batch_entry(vec![(5, 1), (6, 1), (7, 1)]),
            ),
            // replace the unstable entries
            (
                vec![new_entry(5, 1)],
                5,
                None,
                new_batch_entry(vec![(5, 2), (6, 2)]),
                5,
                new_batch_entry(vec![(5, 2), (6, 2)]),
            ),
            (
                vec![new_entry(5, 1)],
                5,
                None,
                new_batch_entry(vec![(4, 2), (5, 2), (6, 2)]),
                4,
                new_batch_entry(vec![(4, 2), (5, 2), (6, 2)]),
            ),
            // truncate the existing entries and append
            (
                new_batch_entry(vec![(5, 1), (6, 1), (7, 1)]),
                5,
                None,
                new_batch_entry(vec![(6, 2)]),
                5,
                new_batch_entry(vec![(5, 1), (6, 2)]),
            ),
            (
                new_batch_entry(vec![(5, 1), (6, 1), (7, 1)]),
                5,
                None,
                new_batch_entry(vec![(7, 2), (8, 2)]),
                5,
                new_batch_entry(vec![(5, 1), (6, 1), (7, 2), (8, 2)]),
            ),
        ];

        for (entries, offset, snapshot, to_append, w_offset, w_entries) in tests {
            let mut u = Unstable {
                snapshot,
                entries,
                offset,
            };
            u.truncate_and_append(to_append.as_slice());
            assert_eq!(u.offset, w_offset);
            assert_eq!(u.entries, w_entries);
        }
    }

    fn new_entry(index: u64, term: u64) -> Entry {
        let mut entry = Entry::new();
        entry.set_Term(term);
        entry.set_Index(index);
        entry
    }

    fn new_batch_entry(batch: Vec<(u64, u64)>) -> Vec<Entry> {
        batch
            .iter()
            .map(|(index, term)| new_entry(*index, *term))
            .collect()
    }

    fn new_snapshot(index: u64, term: u64) -> Snapshot {
        let mut snapshot = Snapshot::new();
        snapshot.mut_metadata().set_index(index);
        snapshot.mut_metadata().set_term(term);
        snapshot
    }
}
