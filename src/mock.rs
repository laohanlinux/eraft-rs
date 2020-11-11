use crate::raft::{Config, ReadOnlyOption, NO_LIMIT, Raft};
use crate::raft_log::RaftLog;
use crate::raftpb::raft::{Entry, Snapshot, Message};
use crate::rawnode::{SafeRawNode, RawCoreNode};
use crate::storage::{MemoryStorage, SafeMemStorage, Storage};
use bytes::Bytes;
use protobuf::RepeatedField;

pub fn read_message<S: Storage>(raft: &mut Raft<S>) -> Vec<Message> {
    let msg = raft.msgs.clone();
    raft.msgs.clear();
    msg
}

pub struct MocksEnts(Entry);

impl Into<Entry> for MocksEnts {
    fn into(self) -> Entry {
        self.0
    }
}

impl Into<RepeatedField<Entry>> for MocksEnts {
    fn into(self) -> RepeatedField<Entry> {
        RepeatedField::from_vec(vec![self.0])
    }
}

impl From<&str> for MocksEnts {
    fn from(buf: &str) -> Self {
        let v = Vec::from(buf);
        let mut entry = Entry::new();
        entry.set_Data(Bytes::from(v));
        MocksEnts(entry)
    }
}

pub struct MockEntry(Entry);

impl MockEntry {
    pub fn set_data(mut self, buf: Vec<u8>) -> MockEntry {
        self.0.set_Data(Bytes::from(buf));
        self
    }

    pub fn set_index(mut self, index: u64) -> MockEntry {
        self.0.set_Index(index);
        self
    }
}

impl Into<Entry> for MockEntry {
    fn into(self) -> Entry {
        self.0
    }
}

impl From<Vec<u8>> for MockEntry {
    fn from(v: Vec<u8>) -> Self {
        let mut entry = Entry::new();
        entry.set_Data(Bytes::from(v));
        MockEntry(entry)
    }
}

impl From<&str> for MockEntry {
    fn from(buf: &str) -> Self {
        let v = Vec::from(buf);
        MockEntry::from(buf)
    }
}

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

pub fn new_test_core_node(id: u64, peers: Vec<u64>, election_tick: u64, heartbeat_tick: u64, s: SafeMemStorage) -> RawCoreNode<SafeMemStorage> {
    RawCoreNode::new(new_test_conf(id, peers, election_tick, heartbeat_tick), s)
}

pub fn new_test_inner_node(id: u64, peers: Vec<u64>, election_tick: u64, heartbeat_tick: u64, s: SafeMemStorage) -> Raft<SafeMemStorage> {
    Raft::new(new_test_conf(id, peers, election_tick, heartbeat_tick), s)
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
        max_inflight_msgs: 1 << 3,
        check_quorum: false,
        pre_vote: false,
        read_only_option: ReadOnlyOption::ReadOnlySafe,
        disable_proposal_forwarding: false,
    }
}
