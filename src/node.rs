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
use crate::raft::{self, Config, Raft, RaftError, StateType, NONE};
use crate::raft_log::RaftLog;
use crate::raftpb::raft::MessageType::{
    MsgHup, MsgProp, MsgReadIndex, MsgSnapStatus, MsgTransferLeader, MsgUnreachable,
};
use crate::raftpb::raft::{
    ConfChange, ConfChangeV2, ConfState, Entry, HardState, Message, MessageType, Snapshot,
};
use crate::raftpb::{equivalent, ConfChangeI};
use crate::rawnode::{RawCoreNode, SafeRawNode};
use crate::read_only::{ReadOnly, ReadState};
use crate::status::Status;
use crate::storage::{SafeMemStorage, Storage};
use crate::tracker::ProgressTracker;
use crate::util::{is_local_message, is_response_message};
use async_channel::{self, bounded, unbounded, Receiver, Sender};
use async_io::Timer;
use bytes::Bytes;
use env_logger::init_from_env;
use futures::future::err;
use futures::TryFutureExt;
use protobuf::{ProtobufEnum, RepeatedField};
use std::default::Default;
use std::error::Error;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::select;
use tokio::task;
use tokio::time::Duration;

pub type SafeResult<T: Send + Sync + Clone> = Result<T, RaftError>;

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
#[derive(Default, Clone, Debug)]
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
    pub fn new<S: Storage, T: Into<Option<SoftState>>>(
        raft: &Raft<S>,
        prev_soft_st: T,
        prev_hard_st: HardState,
    ) -> Ready {
        let prev_soft_st = prev_soft_st.into();
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
    pub(crate) fn applied_cursor(&self) -> u64 {
        self.committed_entries
            .last()
            .map(|entry| entry.get_Index())
            .or_else(|| Some(self.snapshot.get_metadata().get_index()))
            .unwrap()
    }
}

use async_trait::async_trait;
use crate::async_ch::{Channel, MsgWithResult};

/// represents a node in a raft cluster.
#[async_trait]
pub trait Node {
    /// Increments the interval logical clock for the `Node` by a single tick. Election
    /// timeouts and heartbeat timeouts are in units of ticks.
    async fn tick(&self);

    /// Causes the `Node` to transition to candidate state and start campaign to become leader.
    async fn campaign(&self) -> SafeResult<()>;

    /// proposes that data be appended to the log. Note that proposals can be lost without
    /// notice, therefore it is user's job to ensure proposal retries.
    async fn propose(&self, data: &[u8]) -> SafeResult<()>;

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
    async fn propose_conf_change(&self, cc: impl ConfChangeI) -> SafeResult<()>;

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
    /// index, any linearize read requests issued before the read request can be
    /// processed safely. The read state will have the same rctx attached.
    async fn read_index(&self, rctx: Vec<u8>) -> SafeResult<()>;

    /// Status returns the current status of the raft state machine.
    async fn status(&self) -> Status;

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

/// returns true if the given HardState is empty.
pub fn is_empty_hard_state(st: &HardState) -> bool {
    let empty = HardState::new();
    is_hard_state_equal(&empty, st)
}

/// returns true if the given snapshot is empty.
pub fn is_empty_snapshot(sp: &Snapshot) -> bool {
    sp.get_metadata().get_index() == 0
}

#[derive(Clone)]
pub struct InnerChan<T> {
    rx: Option<Receiver<T>>,
    tx: Option<Sender<T>>,
}

impl<T> Default for InnerChan<T> {
    fn default() -> Self {
        let (tx, rx) = unbounded();
        InnerChan {
            tx: Some(tx),
            rx: Some(rx),
        }
    }
}

impl<T> InnerChan<T> {
    pub fn new() -> InnerChan<T> {
        let (tx, rx) = unbounded();
        InnerChan {
            tx: Some(tx),
            rx: Some(rx),
        }
    }

    pub fn new_with_cap(n: usize) -> InnerChan<T> {
        let (tx, rx) = bounded(n);
        InnerChan {
            tx: Some(tx),
            rx: Some(rx),
        }
    }

    pub fn new_with_channel(tx: Sender<T>, rx: Receiver<T>) -> InnerChan<T> {
        InnerChan {
            tx: Some(tx),
            rx: Some(rx),
        }
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

    pub async fn try_send(&self, msg: T) -> Result<(), async_channel::SendError<T>> {
        if let Some(tx) = &self.tx {
            return tx.send(msg).await;
        }
        Ok(())
    }
}

pub struct Peer {
    pub id: u64,
    pub context: Vec<u8>,
}

pub(crate) async fn start_node<S: Storage + Send + Sync + Clone + 'static>(
    c: Config,
    peers: Vec<Peer>,
    storage: S,
) -> InnerNode<S> {
    assert!(!peers.is_empty(), "no peers given; use RestartNode instead");
    let mut raw_node = RawCoreNode::new(c, storage);
    raw_node.boot_strap(peers);

    let mut node = InnerNode::new(SafeRawNode::new(raw_node));
    let mut node1 = node.clone();
    tokio::spawn(async move {
        node1.run().await;
    });
    node
}

#[derive(Clone)]
pub(crate) struct InnerNode<S: Storage> {
    pub(crate) prop_c: Channel<MsgWithResult>,
    pub(crate) recv_c: Channel<Message>,
    conf_c: InnerChan<ConfChangeV2>,
    conf_state_c: InnerChan<ConfState>,
    ready_c: InnerChan<Ready>,
    advance: InnerChan<()>,
    tick_c: InnerChan<()>,
    done: Channel<()>,
    stop: InnerChan<()>,
    status: InnerChan<Sender<Status>>,
    raw_node: SafeRawNode<S>,
}

impl<S: Storage + Send + Sync + 'static> InnerNode<S> {
    fn new(raw_node: SafeRawNode<S>) -> Self {
        InnerNode {
            prop_c: Channel::new(1),
            recv_c: Channel::new(1),
            conf_c: InnerChan::default(),
            conf_state_c: InnerChan::default(),
            ready_c: InnerChan::default(),
            advance: InnerChan::default(),
            tick_c: InnerChan::default(),
            done: Channel::new(1),
            stop: InnerChan::default(),
            status: InnerChan::default(),
            raw_node,
        }
    }

    async fn run(&mut self) {
        let mut wait_advance = false;
        let mut ready = Ready::default();
        let mut first = true;
        loop {
            {
                let mut has_ready = false;
                {
                    if !wait_advance && self.rl_raw_node().has_ready() && !first {
                        ready = self.rl_raw_node().ready_without_accept();
                        has_ready = true;
                    }
                }
                if has_ready {
                    self.ready_c.tx_ref().send(ready.clone()).await.unwrap();
                    self.wl_raw_node().accept_ready(&ready);
                    wait_advance = true;
                }
                // Shit
                first = false;
            }

            select! {
                    conf = self.conf_c.rx_ref().recv() => {
                        let cc: ConfChangeV2 = conf.unwrap();
                        // If the node was removed, block incoming proposals. Note that we
                        // only do this if the node was in the config before. Nodes may be
                        // a member of the group without knowing this (when they're catching
                        // up on the log and don't have the latest config) and we don't want
                        // to block the proposal channel in that case.
                        //
                        // NB: propc is reset when the leader changes, which, if we learn
                        // about it, sort of implies that we got readded, maybe? This isn't
                        // very sound and likely has bugs.
                        let mut cs = ConfState::new();
                        {
                             let mut raw_node = self.wl_raw_node();
                             let ok_before = raw_node.raft.prs.progress.contains_key(&raw_node.raft.id);
                              cs = raw_node.apply_conf_change(Box::new(cc));
                             let (ok_after, id) = (raw_node.raft.prs.progress.contains_key(&raw_node.raft.id), raw_node.raft.id);
                             if ok_before && !ok_after {
                                   let _id = raw_node.raft.id;
                                   let found = cs.get_voters().iter().any(|id| *id == _id) || cs.get_voters_outgoing().iter().any(|id| *id == _id);
                                   if !found {
                                       warn!("Current node({:#x}) isn't voter", id);
                                   }
                             }
                        }

                        select! {
                            _ = self.conf_state_c.tx_ref().send(cs) => {}
                            _ = self.done.recv() => {}
                        }
                    }
                    pm = self.prop_c.recv() => {
                        let mut pm: MsgWithResult = pm.unwrap();
                        if !self.is_voter() {
                            pm.notify(Err(RaftError::NotIsVoter)).await;
                        }else if !self.rl_raw_node().raft.has_leader() {
                            pm.notify(Err(RaftError::NoLeader)).await;
                        }else {
                              let mut msg: Message = pm.get_msg().unwrap().clone();
                              msg.set_from(self.rl_raw_node().raft.id);
                              let res = self.wl_raw_node().step(msg);
                              pm.notify_and_close(res).await;
                        }
                    }
                    msg = self.recv_c.recv() => {
                        let msg: Message = msg.unwrap();
                        // filter out response message from unknown From.
                        let mut raw_node = self.wl_raw_node();
                        let is_pr = raw_node.raft.prs.progress.contains_key(&msg.get_from());
                        if is_pr || !is_response_message(msg.get_field_type()) {
                           raw_node.raft.step(msg);
                        }
                    }
                    _ = self.tick_c.rx_ref().recv() => {
                        self.tick().await;
                    }
                    _ = self.advance.rx_ref().recv() => {
                        {
                            self.wl_raw_node().advance(&ready);
                            wait_advance = false;
                        }
                    }
                    status = self.status.rx_ref().recv() => {
                        let _status = self.get_status();
                        status.unwrap().send(_status).await;
                    }
                    _ = self.stop.rx_ref().recv() => {
                        if let Some(tx) = self.done.take_tx() {
                            tx.send(()).await;
                        }
                        return;
                    }
            }
        }
    }

    async fn do_step(&self, m: Message) {
        self.step_wait_option(m, false).await;
    }

    async fn step_wait(&self, m: Message) -> SafeResult<()> {
        self.step_wait_option(m, true).await
    }

    async fn step_wait_option(&self, m: Message, wait: bool) -> SafeResult<()> {
        if m.get_field_type() != MsgProp {
            select! {
                _ = self.recv_c.send(m.clone()) => return Ok(()),
                _ = self.done.recv() => {
                    return Err(RaftError::Stopped);
                }
            }
        }

        let ch = self.prop_c.tx();
        let mut notify = Channel::new(1);
        let mut props_msg = if !wait {
            MsgWithResult::new_with_msg(m.clone())
        } else {
            MsgWithResult::new_with_channel(notify.tx(), m)
        };
        select! {
            _ = ch.send(props_msg) => {
                if !wait {
                    return Ok(());
                }
            }
            _ = self.done.recv() => {
                return Err(RaftError::Stopped);
            }
        }
        // wait result
        select! {
            res = notify.recv() => return res.unwrap(),
            _ = self.done.recv() => return Err(RaftError::Stopped)
        }
    }

    pub fn get_status(&self) -> Status {
        Status::from(&self.raw_node.rl().raft)
    }

    pub fn rl_raw_node_fn<F>(&self, mut f: F)
        where
            F: FnMut(RwLockReadGuard<'_, RawCoreNode<S>>),
    {
        let rl = self.rl_raw_node();
        f(rl)
    }

    pub fn wl_raw_node_fn<F>(&self, mut f: F)
        where
            F: FnMut(RwLockWriteGuard<'_, RawCoreNode<S>>),
    {
        let wl = self.wl_raw_node();
        f(wl)
    }

    pub fn rl_raw_node(&self) -> RwLockReadGuard<'_, RawCoreNode<S>> {
        self.raw_node.rl()
    }

    pub fn wl_raw_node(&self) -> RwLockWriteGuard<'_, RawCoreNode<S>> {
        self.raw_node.wl()
    }

    fn is_voter(&self) -> bool {
        let raw_node = self.rl_raw_node();
        let _id = raw_node.raft.id;
        let cs = raw_node.raft.prs.config_state();
        cs.get_voters().iter().any(|id| *id == _id)
            || cs.get_voters_outgoing().iter().any(|id| *id == _id)
    }

    fn is_voter_with_conf_state(&self, cs: &ConfState) -> bool {
        let raw_node = self.rl_raw_node();
        let _id = raw_node.raft.id;
        cs.get_voters().iter().any(|id| *id == _id)
            || cs.get_voters_outgoing().iter().any(|id| *id == _id)
    }
}

#[async_trait]
impl<S: Storage + Send + Sync + 'static> Node for InnerNode<S> {
    async fn tick(&self) {
        self.raw_node.wl().tick()
    }

    async fn campaign(&self) -> SafeResult<()> {
        let mut msg = Message::new();
        msg.set_field_type(MsgHup);
        self.do_step(msg).await;
        Ok(())
    }

    async fn propose(&self, data: &[u8]) -> SafeResult<()> {
        let msg = Message {
            field_type: MsgProp,
            entries: RepeatedField::from(vec![Entry {
                Data: Bytes::from(data.to_vec()),
                ..Default::default()
            }]),
            ..Default::default()
        };
        self.step_wait(msg).await
    }

    async fn propose_conf_change(&self, cc: impl ConfChangeI) -> SafeResult<()> {
        let mut entry = cc.to_entry();
        let mut msg = Message::new();
        msg.set_field_type(MsgProp);
        msg.set_entries(RepeatedField::from(vec![entry]));
        self.step(msg).await
    }

    async fn step(&self, m: Message) -> SafeResult<()> {
        //    ignore unexpected local messages receiving over network
        if is_local_message(m.get_field_type()) {
            return Ok(());
        }
        self.do_step(m).await;
        Ok(())
    }

    fn ready(&self) -> Receiver<Ready> {
        self.ready_c.rx()
    }

    async fn advance(&self) {
        let advance = self.advance.tx();
        select! {
            _ = advance.send(()) => {
                 info!("execute advance");
            },
            _ = self.done.recv() => {}
        }
    }

    async fn apply_conf_change(&self, cc: ConfChange) -> Option<ConfState> {
        let cc_v2 = cc.as_v2();
        let conf_tx = self.conf_c.tx();
        select! {
                _ = conf_tx.send(cc_v2) => {}
                _ = self.done.recv() => {}
            }
        let conf_state = self.conf_state_c.rx();
        select! {
                res = conf_state.recv() => Some(res.unwrap()),
                _ = self.done.recv() => None
            }
    }

    async fn transfer_leader_ship(&self, lead: u64, transferee: u64) {
        let recvc = self.recv_c.tx();
        let mut msg = Message::new();
        msg.set_field_type(MsgTransferLeader);
        msg.set_from(transferee);
        msg.set_to(lead);
        select! {
                _ = recvc.send(msg) => {} // manually set 'from' and 'to', so that leader can voluntarily transfers its leadership
                _ = self.done.recv() => {}
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

    async fn status(&self) -> Status {
        let status = self.status.tx();
        let ch: InnerChan<Status> = InnerChan::new();
        let (tx, rx) = (ch.tx(), ch.rx());
        select! {
                _ = status.send(tx) => {},
                _ = self.done.recv() => {}
            }
        rx.recv().await.unwrap()
    }

    async fn report_unreachable(&self, id: u64) {
        let recv = self.recv_c.tx();
        let mut msg = Message::new();
        msg.set_field_type(MsgUnreachable);
        msg.set_from(id);
        select! {
                _ = recv.send(msg) => {},
                _ = self.done.recv() => {}
            }
    }

    async fn report_snapshot(&self, id: u64, status: SnapshotStatus) {
        let recv = self.recv_c.tx();
        let rejected = status == SnapshotStatus::Failure;
        let mut msg = Message::new();
        msg.field_type = MsgSnapStatus;
        msg.from = id;
        msg.reject = rejected;
        select! {
           _ = recv.send(msg) => {}
           _ = self.done.recv() => {}
        }
    }

    async fn stop(&self) {
        let stop = self.stop.tx();
        select! {
                _ = stop.send(()) => {},
                _ = self.done.recv() => return
            }
        // Block until the stop has been acknowledged by run()
        self.done.recv().await;
    }
}

/// MustSync returns true if the hard state and count of Raft entries indicate
/// that a synchronous write to persistent storage is required.
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
    use crate::tests_util;
    use std::io;
    use std::io::Write;
    use super::*;
    use crate::tests_util::mock::new_test_raw_node;
    use crate::node::{InnerChan, InnerNode, Node};
    use crate::raft::{ReadOnlyOption, NO_LIMIT};
    use crate::raftpb::raft::MessageType::{MsgPreVoteResp, MsgProp, MsgVote};
    use crate::raftpb::raft::{Message, MessageType};
    use crate::storage::SafeMemStorage;
    use crate::util::is_local_message;
    use lazy_static::lazy_static;
    use nom::error::append_error;
    use protobuf::ProtobufEnum;
    use std::sync::{Arc, Mutex};
    use env_logger::Env;
    use tokio::time::{Duration, Instant, sleep};
    use crate::tests_util::try_init_log;
    lazy_static! {
        /// This is an example for using doc comment attributes
        static ref msgs: Arc<Mutex<Vec<Message>>> = Arc::new(Mutex::new(vec ! []));
    }

    #[test]
    fn t_drop() {
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let mut ch: InnerChan<usize> = InnerChan::new();
            let rx = ch.rx.take();
            drop(rx);
            let tx = ch.tx();
            let res = tx.send(19).await;
            assert!(res.is_err());
        });
    }

    // ensures that node.step sends msgProp to propc chan
    // and other kinds of messages to recvc chan.
    #[tokio::test]
    async fn t_node_step() {
        try_init_log();
        for msgn in 0..MsgPreVoteResp.value() {
            let mut node: InnerNode<SafeMemStorage> =
                InnerNode::new(new_test_raw_node(1, vec![1], 20, 10, SafeMemStorage::new()));
            node.prop_c = Channel::new(1);
            node.recv_c = Channel::new(1);
            let msgt = MessageType::from_i32(msgn).unwrap();
            let mut msg = Message::new();
            msg.set_field_type(msgt);

            let ok = node.step(msg.clone()).await;
            assert!(ok.is_ok(), "{:?}", ok.unwrap_err());
            // Proposal goes to proc chan. Others go to recvc chan.
            if msgt == MsgProp {
                let proposal_rx = node.prop_c.recv().await;
                assert!(
                    proposal_rx.is_ok(),
                    "{}: cannot receive {:?} on propc chan",
                    msgn,
                    msgt
                );
            } else {
                if is_local_message(msgt) {
                    assert!(
                        node.recv_c.try_recv().await.is_err(),
                        "{}: step should ignore {:?}",
                        msgn,
                        msgt
                    );
                } else {
                    assert!(
                        node.recv_c.try_recv().await.is_ok(),
                        "{}: cannot receive {:?} on recvc chan",
                        msgn,
                        msgt
                    );
                }
            }
        }
    }

    // TODO
    // cancal and stop should unblock step()
    #[test]
    fn t_node_step_unblock() {}

    fn append_step(raft: &mut Raft<SafeMemStorage>, m: Message) -> Result<(), RaftError> {
        msgs.lock().unwrap().push(m);
        Ok(())
    }

    // ensure that node.Propose sends the given proposal to the underlying raft.
    #[tokio::test]
    async fn t_node_process() {
        tests_util::try_init_log();
        {
            msgs.lock().unwrap().clear();
        }

        let s = SafeMemStorage::new();
        let raw_node = new_test_raw_node(1, vec![1], 10, 1, s.clone());
        let mut node = InnerNode::<SafeMemStorage>::new(raw_node);
        let mut node1 = node.clone();
        tokio::spawn(async move { node1.run().await });
        let ok = node.campaign().await;
        assert!(ok.is_ok(), "{:?}", ok.unwrap_err());
        loop {
            let rd = node.ready().recv().await.unwrap();
            info!("get a ready_rx, wait it: {:?}", rd);
            s.wl().append(rd.entries.clone());
            // change the step function to append_step until this raft becomes leader.
            if rd.soft_state.as_ref().unwrap().lead == node.rl_raw_node().raft.id {
                node.advance().await;
                break;
            }
            node.advance().await;
        }

        assert!(node.propose("somedata".as_bytes()).await.is_ok());
        node.stop().await;
        info!("mail-box: {:?}", node.raw_node.rl().raft.msgs);
    }

    #[test]
    fn t_node_read_index() {
        try_init_log();
        tokio::runtime::Runtime::new().unwrap().block_on(async move {
            let s = SafeMemStorage::new();
            let raw_node = new_test_raw_node(1, vec![1], 10, 1, s.clone());
            let mut node = InnerNode::<SafeMemStorage>::new(raw_node);
            let wrs = vec![ReadState {
                index: 1,
                request_ctx: "somedata".as_bytes().to_vec(),
            }];
            {
                node.wl_raw_node().raft.read_states = wrs.clone();
            }
            let mut node1 = node.clone();

            tokio::spawn(async move { node1.run().await });
            let ok = node.campaign().await;
            assert!(ok.is_ok(), "{:?}", ok.unwrap_err());
            loop {
                info!("t111ry again");
                let ready = node.ready_c.rx();
                let ready = ready.recv().await.unwrap();
                {
                    let mut raw_node = node.wl_raw_node();
                    let expect = ready.read_states.clone();
                    assert_eq!(expect, wrs);
                    s.wl().append(ready.entries);
                    if ready.soft_state.as_ref().unwrap().lead == raw_node.raft.id {
                        node.advance();
                        break;
                    }
                }

                node.advance();
            }

            let w_request = "somedata2".as_bytes().to_vec();
            node.read_index(w_request.clone());
            node.stop();
        });
    }
}
