use crate::raftpb::raft::{Message, Entry, MessageType};
use crate::raft::Raft;
use crate::storage::{SafeMemStorage, Storage};
use nom::lib::std::collections::HashMap;

// // Returns the appliable entries and updates the applied index
// fn next_ents(mut raft: Raft<SafeMemStorage>, s: &mut SafeMemStorage) -> Vec<Entry> {
//     // transfer all unstable entries to "stable" storage.
//     s.wl().append(raft.raft_log.unstable_entries().to_vec());
//     raft.raft_log.stable_to(raft.raft_log.last_index(), raft.raft_log.last_term());
//
//     let ents = raft.raft_log.next_ents();
//     raft.raft_log.applied_to(raft.raft_log.committed);
//     return ents;
// }
//
// fn must_append_entry<S>(raft: &mut Raft<S>, mut ents: Vec<Entry>) where S: Storage {
//     assert!(raft.append_entry(&mut ents), "entry unexpectedly dropped");
// }
//
// trait StateMachine {
//     fn step(&mut self, m: Message) -> Result<(), String>;
//     fn read_message(&mut self) -> Vec<Message>;
// }
//
// struct NetWork<M: StateMachine> {
//     peers: HashMap<u64, M>,
//     storage: HashMap<u64, SafeMemStorage>,
//     dropm: HashMap<ConnEm, SafeMemStorage>,
//     ignorem: HashMap<MessageType, bool>,
//     // `msg_hook` is called for each message sent. It may inspect the
//     // message and return true to send it for false to drop it
//     msg_hook: Box<Fn(Message) -> bool>,
// }
//
// impl<M: StateMachine> NetWork<M> {
//     pub fn send(&mut self, msgs: Vec<Message>) {
//         unimplemented!("unimplemented")
//     }
//
//     pub fn drop(&mut self, from: u64, to: u64, perc: f64) {
//         unimplemented!("unimplemented")
//     }
//
//     pub fn cut(&mut self, one: u64, other: u64) {
//         unimplemented!("unimplemented")
//     }
//
//     pub fn isolated(&mut self, id: u64) {
//         unimplemented!("unimplemented")
//     }
//
//     pub fn ignore(&mut self, t: MessageType) {
//         unimplemented!("unimplemented")
//     }
//
//     pub fn recover(&mut self) {
//         self.dropm.clear();
//         self.ignorem.clear();
//     }
//
//     pub fn filter(&mut self, msgs: Vec<Message>) -> Vec<Message> {
//         unimplemented!("unimplemented")
//     }
//
// }
//
// #[derive(Debug, Clone)]
// struct ConnEm {
//     from: u64,
//     to: u64,
// }
//
// #[derive(Debug, Clone)]
// struct BlackHole {}
//
// impl StateMachine for BlackHole {
//     fn step(&mut self, m: Message) -> Result<(), String> {
//         Ok(())
//     }
//
//     fn read_message(&mut self) -> Vec<Message> {
//         vec![]
//     }
// }