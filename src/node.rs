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

use crate::conf_change::conf_change::Changer;
use crate::conf_change::restore::restore;
use crate::raft_log::RaftLog;
use crate::raft::{self, Config, Raft, RaftError, StateType, NONE};
use crate::raftpb::raft::MessageType::{MsgHup, MsgProp, MsgReadIndex, MsgSnapStatus, MsgTransferLeader, MsgUnreachable};
use crate::raftpb::raft::{ConfChange, ConfChangeV2, ConfState, Entry, HardState, Message, MessageType, Snapshot};
use crate::raftpb::{equivalent, ConfChangeI};
use crate::rawnode::RawNode;
use crate::read_only::{ReadOnly, ReadState};
use crate::status::Status;
use crate::storage::{SafeMemStorage, Storage};
use crate::tracker::ProgressTracker;
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryFutureExt;
use protobuf::{ProtobufEnum, RepeatedField};
use std::error::Error;
use tokio::select;
use crate::util::is_local_message;
use async_channel::{self, unbounded, Receiver, Sender};
use futures::future::err;
use std::sync::Arc;
use tokio::task;

type SafeDynError = Box<Error + Send + Sync + 'static>;
type SafeResult<T: Send + Sync> = Result<T, Box<Error + Send + Sync + 'static>>;

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum SnapshotStatus {
    Finish,
    Failure,
}

impl Default for SnapshotStatus {
    fn default() -> Self {
        SnapshotStatus::Finish
    }
}

/// SoftState provides state that is usefull for logging and debugging.
/// The state is volatile and does not need to be persisted to the WAL.
#[derive(Default, Copy, Clone, Eq, Debug)]
pub struct SoftState {
    pub lead: u64,
    // must be atomic operations to access; keep 64-bit aligned.
    pub raft_state: StateType,
}

impl PartialEq for SoftState {
    fn eq(&self, other: &Self) -> bool {
        self.lead == other.lead && self.raft_state == other.raft_state
    }
}

/// encapsulates the entries and messages that are ready to read,
/// be saved to stable storage, committed or sent to other peers.
/// All fields in Ready are read-only.
#[derive(Default)]
pub struct Ready {
    /// The current volatile state of a Node.
    /// `SoftState` will be nil if there is no update.
    /// It is not required to consume or store `SoftState`.
    pub soft_state: Option<SoftState>,

    /// The current state of a Node of be saved to stable storage *BEFORE*
    /// `Message` are sent.
    /// `HardState` will be equal to empty state if there is no update.
    pub hard_state: HardState,

    /// `ReadStates` can be used for node to serve linearizable read requests locally
    /// when its applied index is greater than the index in `ReadState`.
    /// Note that the `read_state` will be returned when raft receives `msg_read_index`.
    /// The returned is only valid for the request that required to read.
    pub read_states: Vec<ReadState>,

    /// Entries specified entries to be saved to stable storage *BEFORE*
    /// `Messages` are sent.
    pub entries: Vec<Entry>,

    /// `Snapshot` specifies the snapshot entries to be committed to a
    pub snapshot: Snapshot,

    /// `committed_entries` specifies entries to be committed to a
    /// store/state-machine. These have previously been committed to stable
    /// store.    
    pub committed_entries: Vec<Entry>,

    /// `Message` specifies outbound messages to be sent AFTER Entries are
    /// committed to stable storage.
    /// If it contains a `MsgSnap` message, the application MUST report back to raft
    /// when the snapshot has been received or has failed by calling `ReportSnapshot`.
    pub messages: Vec<Message>,

    /// `must_sync` indicates whether the `HardState` and `Entries` must be synchronously
    /// written to disk or if an asynchronous write is permissible.
    pub must_sync: bool,
}

impl Ready {
    pub fn new<S: Storage + Clone>(raft: &Raft<S>, prev_soft_st: Option<SoftState>, prev_hard_st: HardState) -> Ready {
        let mut ready = Ready {
            soft_state: prev_soft_st.clone(),
            hard_state: prev_hard_st.clone(),
            read_states: vec![],
            entries: raft.raft_log.unstable_entries().to_vec(),
            snapshot: Default::default(),
            committed_entries: raft.raft_log.next_ents(),
            messages: raft.msgs.clone(),
            must_sync: false,
        };
        if prev_soft_st.is_some() && raft.soft_state() != prev_soft_st.unwrap() {
            ready.soft_state = Some(raft.soft_state());
        }
        if prev_hard_st != raft.hard_state() {
            ready.hard_state = raft.hard_state();
        }
        if raft.raft_log.unstable.snapshot.is_some() {
            ready.snapshot = raft.raft_log.unstable.snapshot.as_ref().unwrap().clone();
        }
        if !raft.read_states.is_empty() {
            ready.read_states = raft.read_states.clone();
        }
        must_sync(raft.hard_state(), prev_hard_st, ready.entries.len());
        ready
    }

    pub(crate) fn contains_update(&self) -> bool {
        self.soft_state.is_some()
            || is_empty_hard_state(&self.hard_state)
            || !is_empty_snapshot(&self.snapshot)
            || !self.entries.is_empty()
            || !self.committed_entries.is_empty()
            || !self.messages.is_empty()
            || !self.read_states.is_empty()
    }

    // extracts from the `Ready` the highest index the client has
    // applied (once the Ready is confirmed via advance). If no information is
    // contained in the Ready, returns zero.
    pub(crate) fn appliedCursor(&self) -> u64 {
        self.committed_entries
            .last()
            .map(|entry| entry.get_Index())
            .or_else(|| Some(self.snapshot.get_metadata().get_index()))
            .unwrap()
    }
}

/// represents a node in a raft cluster.
#[async_trait]
pub trait Node: Sync {
    /// Increments the interval logical clock for the `Node` by a single tick. Election
    /// timeouts and heartbeat timeouts are in units of ticks.
    async fn tick(&mut self);

    /// Causes the `Node` to transition to candidate state and start campaign to become leader.
    async fn campaign(&self) -> SafeResult<()>;

    /// proposes that data be appended to the log. Note that proposals can be lost without
    /// notice, therefore it is user's job to ensure proposal retries.
    fn propose(&self, data: &[u8]) -> SafeResult<()>;

    /// Proposes a configuration change. Like any proposal, the
    /// configuration change may be dropped with or without an error being
    /// returned. In particular, configuration changes are dropped unless the
    /// leader has certainty that there is no prior unapplied configuration
    /// change in its log.
    ///
    /// The method accepts either a pb.ConfChange (deprecated) or pb.ConfChangeV2
    /// message. The latter allows arbitrary configuration changes via joint
    /// consensus, notably including replacing a voter. Passing a ConfChangeV2
    /// message is only allowed if all Nodes participating in the cluster run a
    /// version of this library aware of the V2 API. See pb.ConfChangeV2 for
    /// usage details and semantics.
    fn propose_conf_change(&self, cc: impl ConfChangeI) -> SafeResult<()>;

    /// Step advances the state machine using the given message. ctx.Err() will be returned, if any.
    async fn step(&self, msg: Message) -> SafeResult<()>;

    /// Ready returns a channel that returns the current point-in-time state.
    /// Users of the Node must call Advance after retrieving the state returned by Ready.
    ///
    /// NOTE: No committed entries from the next Ready may be applied until all committed entries
    /// and snapshots from the previous one have finished.
    fn ready(&self) -> Receiver<Ready>;

    /// Advance notifies the Node that the application has saved progress up to the last Ready.
    /// It prepares the node to return the next available Ready.
    ///
    /// The application should generally call Advance after it applies the entries in last Ready.
    ///
    /// However, as an optimization, the application may call Advance while it is applying the
    /// commands. For example. when the last Ready contains a snapshot, the application might take
    /// a long time to apply the snapshot data. To continue receiving Ready without blocking raft
    /// progress, it can call Advance before finishing applying the last ready.
    async fn advance(&self);

    /// ApplyConfChange applies a config change (previously passed to
    /// ProposeConfChange) to the node. This must be called whenever a config
    /// change is observed in Ready.CommittedEntries, except when the app decides
    /// to reject the configuration change (i.e. treats it as a noop instead), in
    /// which case it must not be called.
    ///
    /// Returns an opaque non-nil ConfState protobuf which must be recorded in
    /// snapshots.
    async fn apply_conf_change(&self, cc: ConfChange) -> Option<ConfState>;

    /// TransferLeadership attempts to transfer leadership to the given transferee.
    async fn transfer_leader_ship(&self, lead: u64, transferee: u64);

    /// ReadIndex request a read state. The read state will be set in the ready.
    /// Read state has a read index. Once the application advances further than the read
    /// index, any linearizable read requests issued before the read request can be
    /// processed safely. The read state will have the same rctx attached.
    async fn read_index(&self, rctx: Vec<u8>) -> SafeResult<()>;

    /// Status returns the current status of the raft state machine.
    async fn status(&self) -> Receiver<Status>;

    /// reports the given node is not reachable for the last send.
    async fn report_unreachable(&self, id: u64);

    /// reports the status of the sent snapshot. The id is the raft `ID` of the follower
    /// who is meant to receive the snapshot, and the status is `SnapshotFinish` or `SnapshotFailure`.
    /// Calling `ReportSnapshot` with `SnapshotFinish` is a no-op. But, any failure in applying a
    /// snapshot (for e.g., while streaming it from leader to follower), should be reported to the
    /// leader with SnapshotFailure. When leader sends a snapshot to a follower, it pauses any raft
    /// log probes until the follower can apply the snapshot and advance its state. If the follower
    /// can't do that, for e.g., due to a crash, it could end up in a limbo, never getting any
    /// updates from the leader. Therefore, it is crucial that the application ensures that any
    /// failure in snapshot sending is caught and reported back to the leader; so it can resume raft
    /// log probing in the follower.
    async fn report_snapshot(&self, id: u64, status: SnapshotStatus);

    /// performs any necessary termination of the `Node`.
    async fn stop(&self);
}

#[inline]
fn is_hard_state_equal(a: &HardState, b: &HardState) -> bool {
    a.get_term() == b.get_term() && a.get_vote() == b.get_vote() && a.get_commit() == b.get_commit()
}

// returns true if the given HardState is empty.
pub fn is_empty_hard_state(st: &HardState) -> bool {
    let empty = HardState::new();
    is_hard_state_equal(&empty, st)
}

// returns true if the given snapshot is empty.
pub(crate) fn is_empty_snapshot(sp: &Snapshot) -> bool {
    sp.get_metadata().get_index() == 0
}

pub(crate) struct MsgWithResult {
    m: Option<Message>,
    result: InnerChan<SafeResult<()>>,
}

impl Default for MsgWithResult {
    fn default() -> Self {
        MsgWithResult {
            m: None,
            result: InnerChan::default(),
        }
    }
}

impl MsgWithResult {
    pub fn new() -> Self {
        MsgWithResult {
            m: None,
            result: InnerChan::new(),
        }
    }

    pub fn new_with_channel(tx: Sender<SafeResult<()>>, rx: Receiver<SafeResult<()>>) -> Self {
        MsgWithResult {
            m: None,
            result: InnerChan::new_with_channel(tx, rx),
        }
    }
}

#[derive(Clone)]
pub struct InnerChan<T> {
    rx: Option<Receiver<T>>,
    tx: Option<Sender<T>>,
}

struct InnerChan2<T> {
    tx: Option<async_channel::Sender<T>>,
    rx: Option<async_channel::Receiver<T>>,
}

impl<T> Default for InnerChan<T> {
    fn default() -> Self {
        let (tx, rx) = unbounded();
        InnerChan { tx: Some(tx), rx: Some(rx) }
    }
}

impl<T> InnerChan<T> {
    pub fn new() -> InnerChan<T> {
        let (tx, rx) = unbounded();
        InnerChan { tx: Some(tx), rx: Some(rx) }
    }
    pub fn new_with_channel(tx: Sender<T>, rx: Receiver<T>) -> InnerChan<T> {
        InnerChan { tx: Some(tx), rx: Some(rx) }
    }

    pub fn tx(&self) -> Sender<T> {
        self.tx.as_ref().unwrap().clone()
    }
    pub fn tx_ref(&self) -> &Sender<T> {
        self.tx.as_ref().unwrap()
    }
    pub fn rx(&self) -> Receiver<T> {
        self.rx.as_ref().unwrap().clone()
    }
    pub fn rx_ref(&self) -> &Receiver<T> {
        self.rx.as_ref().unwrap()
    }
}

pub struct Peer {
    id: u64,
    context: Vec<u8>,
}

pub(crate) struct InnerNode<S: Storage + Send + Sync + Clone> {
    pub(crate) propo_c: InnerChan<MsgWithResult>,
    pub(crate) recv_c: InnerChan<Message>,
    conf_c: InnerChan<ConfChangeV2>,
    conf_state_c: InnerChan<ConfState>,
    ready_c: InnerChan<Ready>,
    advance: InnerChan<()>,
    tick_c: InnerChan<()>,
    done: InnerChan<()>,
    stop: InnerChan<()>,
    status: InnerChan<Sender<Status>>,
    raw_node: Option<RawNode<S>>,
}

impl<S: Storage + Send + Sync + Clone> InnerNode<S> {
    pub fn new_node() -> InnerNode<S> {
        InnerNode {
            propo_c: InnerChan::default(),
            recv_c: InnerChan::default(),
            conf_c: InnerChan::default(),
            conf_state_c: InnerChan::default(),
            ready_c: InnerChan::default(),
            advance: InnerChan::default(),
            tick_c: InnerChan::default(),
            done: InnerChan::default(),
            stop: InnerChan::default(),
            status: InnerChan::default(),
            raw_node: None,
        }
    }

    async fn run(&self) {
        let mut prop_rx: Option<Receiver<MsgWithResult>> = Some(self.propo_c.rx());
        let mut readyc: Option<Sender<Ready>> = None;
        let mut advance: Option<Receiver<()>> = None;
        let mut rd: Option<Ready> = None;
        let raw_node = self.raw_node.as_ref().unwrap();
        let mut lead = NONE;
        // while true {
        //     if advance.is_some() {
        //         readyc = None;
        //     } else if raw_node.has_ready() {
        //         // Populate a Ready. Note that this Ready is not guaranteed to
        //         // actually be handled. We will arm readyc, but there's no guarantee
        //         // that we will actually send on it. It's possible that we will
        //         // service another channel instead, loop around, and then populate
        //         // the Ready again. We could instead force the previous Ready to be
        //         // handled first, but it's generally good to emit larger Readys plus
        //         // it simplifies testing (by emitting less frequently and more
        //         // predictably).
        //         rd = Some(raw_node.ready_without_accept());
        //         readyc = Some(self.ready_c.tx());
        //     }
        //     if lead != raw_node.raft.lead {
        //         if raw_node.raft.has_leader() {
        //             if lead == NONE {
        //                 info!(
        //                     "raft.node: {:#} elected leader {:#} at term {}",
        //                     raw_node.raft.id, raw_node.raft.lead, raw_node.raft.term
        //                 );
        //                 // propo_c = Some(self.propo_c.rx());
        //             }
        //         } else {
        //             info!(
        //                 "raft.node: {:#x} lost leader {:#x} at term {}",
        //                 raw_node.raft.id, lead, raw_node.raft.term
        //             );
        //             // propc.take();
        //         }
        //         lead = raw_node.raft.lead;
        //     }
        //
        //     let prop_rx = prop_rx.as_ref().unwrap();
        //     let recv_rx = self.recv_c.rx();
        //     let conf_rx = self.conf_c.rx();
        //     let tick_rx = self.tick_c.rx();
        //     let state_rx = self.status.rx();
        //     let stop_rx = self.stop.rx();
        //     let advance_rx = advance.
        //     // TODO: maybe buffer the config propose if there exists one (the way
        //     // described in raft dissertation)
        //     // Currently it is dropped in Step silently.
        //     // select! {
        //     //     pm = prop_rx.recv() => {}
        //     //     m = recv_rx.recv() => {}
        //     //     cc = conf_rx.recv() => {}
        //     //     _ = tick_rx.recv() => {}
        //     //     _ = rd.recv() => {}
        //     //     _ = advance
        //     // }
        //     break;
        // }
    }

    async fn do_step(&self, m: Message) -> SafeResult<()> {
        self.step_wait_option(m, false).await
    }

    async fn step_wait(&self, m: Message) -> SafeResult<()> {
        self.step_wait_option(m, true).await
    }

    async fn step_wait_option(&self, m: Message, wait: bool) -> SafeResult<()> {
        if m.get_field_type() != MsgProp {
            select! {
                _ = self.recv_c.tx.as_ref().unwrap().send(m.clone()) => return Ok(()),
                _ = self.done.rx.as_ref().unwrap().recv() => {
                    return Err(Box::new(RaftError::Stopped));
                }
            }
        }
        let ch = self.propo_c.tx();
        let mut pm = MsgWithResult::new();
        pm.m = Some(m);
        if wait {
            pm.result = InnerChan::new();
        }
        let rx = pm.result.rx();

        select! {
            _ = ch.send(pm) => {
                if !wait {
                    return Ok(());
                }
            }
            _ = self.done.rx.as_ref().unwrap().recv() => {
                return Err(Box::new(RaftError::Stopped));
            }
        }

        // wait result
        select! {
            res = rx.recv() => return res.unwrap(),
            _ = self.done.rx_ref().recv() => return Err(Box::new(RaftError::Stopped))
        }
        Ok(())
    }
}

#[async_trait]
impl<S: Storage + Send + Sync + Clone> Node for InnerNode<S> {
    async fn tick(&mut self) {
        let mut tick = self.tick_c.tx.as_mut().unwrap();
        let mut done = self.done.rx.as_mut().unwrap();
        let id = self.raw_node.as_ref().unwrap().raft.id;
        select! {
             _ = tick.send(()) => {}
             _ = done.recv() => {}
             else => {warn!("{:#x} A tick missed to fire. Node blocks too long", id)}
        }
    }

    async fn campaign(&self) -> SafeResult<()> {
        let mut msg = Message::new();
        msg.set_field_type(MsgHup);
        self.step(msg).await
    }

    fn propose(&self, data: &[u8]) -> SafeResult<()> {
        let mut msg = Message::new();
        msg.set_field_type(MsgProp);
        let mut entry = Entry::new();
        entry.set_Data(Bytes::from(data.to_vec()));
        msg.set_entries(RepeatedField::from(vec![entry]));
        smol::block_on(async { self.step(msg).await })
    }

    fn propose_conf_change(&self, cc: impl ConfChangeI) -> SafeResult<()> {
        let mut entry = cc.to_entry();
        let mut msg = Message::new();
        msg.set_field_type(MsgProp);
        msg.set_entries(RepeatedField::from(vec![entry]));
        smol::block_on(async { self.step(msg).await })
    }

    async fn step(&self, m: Message) -> SafeResult<()> {
        // ignore unexpected local messages receiving over network
        if is_local_message(m.get_field_type()) {
            return Ok(());
        }
        self.do_step(m).await
    }

    fn ready(&self) -> Receiver<Ready> {
        self.ready_c.rx()
    }

    async fn advance(&self) {
        let advance = self.advance.tx_ref();
        let done = self.done.rx_ref();
        select! {
            _ = advance.send(()) => {},
            _ = done.recv() => {}
        }
    }

    async fn apply_conf_change(&self, cc: ConfChange) -> Option<ConfState> {
        let cc_v2 = cc.as_v2();
        let conf_tx = self.conf_c.tx();
        let done = self.done.rx_ref();
        select! {
            _ = conf_tx.send(cc_v2) => {}
            _ = done.recv() => {}
        }
        let conf_state = self.conf_state_c.rx();
        select! {
            res = conf_state.recv() => Some(res.unwrap()),
            _ = done.recv() => None
        }
    }

    async fn transfer_leader_ship(&self, lead: u64, transferee: u64) {
        let recvc = self.recv_c.tx();
        let done = self.done.rx();
        let mut msg = Message::new();
        msg.set_field_type(MsgTransferLeader);
        msg.set_from(transferee);
        msg.set_to(lead);
        select! {
            _ = recvc.send(msg) => {} // manually set 'from' and 'to', so that leader can voluntarily transfers its leadership
            _ = done.recv() => {}
        }
    }

    async fn read_index(&self, rctx: Vec<u8>) -> SafeResult<()> {
        let mut msg = Message::new();
        msg.set_field_type(MsgReadIndex);
        let mut entry = Entry::default();
        entry.set_Data(Bytes::from(rctx));
        msg.set_entries(RepeatedField::from(vec![entry]));
        self.step(msg).await
    }

    async fn status(&self) -> Receiver<Status> {
        let done = self.done.rx_ref();
        let status = self.status.tx_ref();
        let ch: InnerChan<Status> = InnerChan::new();
        let (tx, rx) = (ch.tx(), ch.rx());
        select! {
            _ = status.send(tx) => {},
            _ = done.recv() => {}
        }
        rx
    }

    async fn report_unreachable(&self, id: u64) {
        let recv = self.recv_c.tx();
        let done = self.done.rx();
        let mut msg = Message::new();
        msg.set_field_type(MsgUnreachable);
        msg.set_from(id);
        select! {
            _ = recv.send(msg) => {},
            _ = done.recv() => {}
        }
    }

    async fn report_snapshot(&self, id: u64, status: SnapshotStatus) {
        let rejected = status == SnapshotStatus::Failure;
        let mut msg = Message::new();
        msg.set_field_type(MsgSnapStatus);
        msg.set_from(id);
        msg.set_reject(rejected);
        let recv = self.recv_c.tx();
        let done = self.done.rx();
        select! {
            _ = recv.send(msg) => {}
            _ = done.recv() => {}
        }
    }

    async fn stop(&self) {
        let done = self.done.rx_ref();
        let stop = self.stop.tx();
        select! {
            _ = stop.send(()) => {},
            _ = done.recv() => {}
        }
        // Block until the stop has been acknowledged by run()
        done.recv().await;
    }
}

// MustSync returns true if the hard state and count of Raft entries indicate
// that a synchronous write to persistent storage is required.
pub fn must_sync(st: HardState, pre_st: HardState, ents_num: usize) -> bool {
    // Persistent state on all servers:
    // (Updated on stable storage before responding to RPCs)
    // currentTerm
    // votedFor
    // log entries[]
    ents_num != 0 || st.get_vote() != pre_st.get_vote() || st.get_term() != pre_st.get_term()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{InnerChan, InnerNode, Node};
    use crate::raftpb::raft::MessageType::MsgProp;
    use crate::raftpb::raft::{Message, MessageType};
    use crate::storage::SafeMemStorage;
    use crate::util::is_local_message;
    use protobuf::ProtobufEnum;

    #[test]
    fn t_node_step() {
        flexi_logger::Logger::with_env().start();
        smol::block_on(async {
            for msgn in 0..MessageType::MsgPreVoteResp.value() {
                let mut node: InnerNode<SafeMemStorage> = InnerNode::new_node();
                node.propo_c = InnerChan::new();
                node.recv_c = InnerChan::new();
                let msgt = MessageType::from_i32(msgn).unwrap();
                let mut msg = Message::new();
                msg.set_field_type(msgt);
                node.step(msg).await;
                // Proposal goes to proc chan. Others go to recvc chan.
                if msgt == MsgProp {
                    let proposal_rx = node.propo_c.rx();
                    assert!(proposal_rx.try_recv().is_ok(), "{}: cannot receive {:?} on propc chan", msgn, msgt);
                } else {
                    let recv = node.recv_c.rx();
                    if is_local_message(msgt) {
                        assert!(recv.try_recv().is_err(), "{}: step should ignore {:?}", msgn, msgt);
                    } else {
                        assert!(recv.try_recv().is_ok(), "{}: cannot receive {:?} on recvc chan", msgn, msgt);
                    }
                }
            }
        });
    }
}
