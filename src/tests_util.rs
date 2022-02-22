use env_logger::Env;
use std::io::Write;

#[cfg(any(test))]
pub(crate) fn try_init_log() {
    // env_logger::try_init_from_env(Env::new().default_filter_or("info"));
    let mut env = env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "trace");
    env_logger::Builder::from_env(env)
        .format(|buf, record| {
            writeln!(
                buf,
                "{} {} [{}:{}], {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                record.level(),
                record.file().unwrap_or("<unnamed>"),
                record.line().unwrap(),
                &record.args()
            )
        })
        .try_init();
}


#[cfg(any(test))]
pub(crate) mod mock {
    use std::collections::HashMap;
    use bytes::Bytes;
    use protobuf::RepeatedField;
    use crate::raft::{Config, NO_LIMIT, Raft, ReadOnlyOption};
    use crate::raft_log::RaftLog;
    use crate::raftpb::raft::{Entry, Message, MessageType, Snapshot};
    use crate::rawnode::{RawCoreNode, SafeRawNode};
    use crate::storage::{SafeMemStorage, Storage};

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
            let data = Vec::from(buf);
            let mut entry = Entry::new();
            entry.set_Data(Bytes::from(data));
            MockEntry(entry)
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

    pub fn new_entry_set2(set: Vec<(u64, u64, &str)>) -> Vec<Entry> {
        set.iter().map(|(index, term, data)| {
            let mut entry = new_entry(*index, *term);
            let data = Vec::from(*data);
            entry.set_Data(Bytes::from(data));
            entry
        }).collect()
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

    pub fn new_test_core_node(
        id: u64,
        peers: Vec<u64>,
        election_tick: u64,
        heartbeat_tick: u64,
        s: SafeMemStorage,
    ) -> RawCoreNode<SafeMemStorage> {
        RawCoreNode::new(new_test_conf(id, peers, election_tick, heartbeat_tick), s)
    }

    pub fn new_test_inner_node(
        id: u64,
        peers: Vec<u64>,
        election_tick: u64,
        heartbeat_tick: u64,
        s: SafeMemStorage,
    ) -> Raft<SafeMemStorage> {
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


    // Returns the appliable entries and updates the applied index
    fn next_ents(mut raft: Raft<SafeMemStorage>, s: &mut SafeMemStorage) -> Vec<Entry> {
        // transfer all unstable entries to "stable" storage.
        s.wl().append(raft.raft_log.unstable_entries().to_vec());
        raft.raft_log.stable_to(raft.raft_log.last_index(), raft.raft_log.last_term());

        let ents = raft.raft_log.next_ents();
        raft.raft_log.applied_to(raft.raft_log.committed);
        return ents;
    }

    fn must_append_entry<S>(raft: &mut Raft<S>, mut ents: Vec<Entry>) where S: Storage {
        assert!(raft.append_entry(&mut ents), "entry unexpectedly dropped");
    }

    trait StateMachine {
        fn step(&mut self, m: Message) -> Result<(), String>;
        fn read_message(&mut self) -> Vec<Message>;
    }

    struct NetWork<M: StateMachine> {
        peers: HashMap<u64, M>,
        storage: HashMap<u64, SafeMemStorage>,
        dropm: HashMap<ConnEm, SafeMemStorage>,
        ignorem: HashMap<MessageType, bool>,
        // `msg_hook` is called for each message sent. It may inspect the
        // message and return true to send it for false to drop it
        msg_hook: Box<dyn Fn(Message) -> bool>,
    }

    impl<M: StateMachine> NetWork<M> {
        pub fn send(&mut self, msgs: Vec<Message>) {
            unimplemented!("unimplemented")
        }

        pub fn drop(&mut self, from: u64, to: u64, perc: f64) {
            unimplemented!("unimplemented")
        }

        pub fn cut(&mut self, one: u64, other: u64) {
            unimplemented!("unimplemented")
        }

        pub fn isolated(&mut self, id: u64) {
            unimplemented!("unimplemented")
        }

        pub fn ignore(&mut self, t: MessageType) {
            unimplemented!("unimplemented")
        }

        pub fn recover(&mut self) {
            self.dropm.clear();
            self.ignorem.clear();
        }

        pub fn filter(&mut self, msgs: Vec<Message>) -> Vec<Message> {
            unimplemented!("unimplemented")
        }
    }

    #[derive(Debug, Clone)]
    struct ConnEm {
        from: u64,
        to: u64,
    }

    #[derive(Debug, Clone)]
    struct BlackHole {}

    impl StateMachine for BlackHole {
        fn step(&mut self, m: Message) -> Result<(), String> {
            Ok(())
        }

        fn read_message(&mut self) -> Vec<Message> {
            vec![]
        }
    }


    pub fn ids_by_size(size: u64) -> Vec<u64> {
        (1..=size).collect::<Vec<_>>()
    }
}