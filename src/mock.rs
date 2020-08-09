use crate::raft::{Config, ReadOnlyOption, NO_LIMIT};
use crate::raft_log::RaftLog;
use crate::raftpb::raft::{Entry, Snapshot};
use crate::rawnode::SafeRawNode;
use crate::storage::{MemoryStorage, SafeMemStorage, Storage};

pub fn new_entry(index: u64, term: u64) -> Entry {
    let mut entry = Entry::new();
    entry.set_Index(index);
    entry.set_Term(term);
    entry
}

pub fn new_entry_set(set: Vec<(u64, u64)>) -> Vec<Entry> {
    set.iter()
        .map(|(index, term)| new_entry(*index, *term))
        .collect()
}

pub fn new_empty_entry_set() -> Vec<Entry> {
    Vec::new()
}

pub fn new_snapshot(index: u64, term: u64) -> Snapshot {
    let mut snapshot = Snapshot::new();
    snapshot.mut_metadata().set_index(index);
    snapshot.mut_metadata().set_term(term);
    snapshot
}

pub fn new_memory() -> SafeMemStorage {
    let storage = SafeMemStorage::new();
    storage
}

pub fn new_log() -> RaftLog<SafeMemStorage> {
    RaftLog::new(new_memory())
}

pub fn new_log_with_storage<T: Storage + Clone>(storage: T) -> RaftLog<T> {
    RaftLog::new(storage)
}

pub fn new_test_raw_node(
    id: u64,
    peers: Vec<u64>,
    election_tick: u64,
    heartbeat_tick: u64,
    s: SafeMemStorage,
) -> SafeRawNode<SafeMemStorage> {
    SafeRawNode::new2(new_test_conf(id, peers, election_tick, heartbeat_tick), s)
}

pub fn new_test_conf(id: u64, peers: Vec<u64>, election_tick: u64, heartbeat_tick: u64) -> Config {
    Config {
        id,
        peers,
        learners: vec![],
        election_tick,
        heartbeat_tick,
        applied: 0,
        max_size_per_msg: NO_LIMIT,
        max_committed_size_per_ready: 0,
        max_uncommitted_entries_size: 0,
        max_inflight_msgs: 1 << 8,
        check_quorum: false,
        pre_vote: false,
        read_only_option: ReadOnlyOption::ReadOnlySafe,
        disable_proposal_forwarding: false,
    }
}
