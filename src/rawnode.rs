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
use crate::node::{is_empty_hard_state, is_empty_snapshot, Peer, Ready, SnapshotStatus, SoftState};
use crate::raft::{Config, Raft, StateType, NONE};
use crate::raft_log::{RaftLog, RaftLogError};
use crate::raftpb::raft::ConfChangeType::{ConfChangeAddNode, ConfChangeRemoveNode};
use crate::raftpb::raft::EntryType::EntryConfChange;
use crate::raftpb::raft::MessageType::{MsgReadIndex, MsgSnapStatus, MsgUnreachable};
use crate::raftpb::raft::{
    ConfChange, ConfState, Entry, EntryType, HardState, Message, MessageType,
};
use crate::raftpb::ConfChangeI;
use crate::status::{BaseStatus, Status};
use crate::storage::Storage;
use crate::tracker::progress::Progress;
use crate::util::{is_local_message, is_response_message};
use anyhow::{anyhow, Result};
use bytes::Bytes;
use protobuf::{Message as Msg, RepeatedField};
use serde_json::ser::State;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::marker::PhantomData;
use std::net::Shutdown::Read;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum RawRaftError {
    // ErrStepLocalMsg is returned when try to step a local raft message
    #[error("raft: cannot step raft local message")]
    StepLocalMsg,
    // ErrStepPeerNotFound is returned when try to step a response message
    // but there is no peer found in raft.Prs for that node.
    #[error("raft: cannot step as peer not found")]
    StepPeerNotFound,
    #[error("unknown error: {0}")]
    StepUnknown(String),
}

#[derive(Clone)]
pub struct SafeRawNode<S: Storage + Send> {
    pub(crate) core_node: Arc<RwLock<RawCoreNode<S>>>,
}

impl<S: Storage + Send> SafeRawNode<S> {
    pub fn new(code_node: RawCoreNode<S>) -> Self {
        SafeRawNode {
            core_node: Arc::new(RwLock::new(code_node)),
        }
    }
    pub fn new2(conf: Config, storage: S) -> Self {
        Self::new(RawCoreNode::new(conf, storage))
    }

    pub fn rl(&self) -> RwLockReadGuard<'_, RawCoreNode<S>> {
        self.core_node.read().unwrap()
    }

    pub fn wl(&self) -> RwLockWriteGuard<'_, RawCoreNode<S>> {
        self.core_node.write().unwrap()
    }
}

// RawNode is a thread-unsafe Node.
// The methods of this struct corresponds to the methods of Node and are described
// more fully there.
// #[derive(Clone)]
pub struct RawCoreNode<S: Storage> {
    pub raft: Raft<S>,
    prev_soft_st: Option<SoftState>,
    prev_hard_st: HardState,
}

trait AssertSend: Send {}
impl<T: Storage + Send> AssertSend for Raft<T> {}

impl<S: Storage> RawCoreNode<S> {
    /// NewRawNode instance a RawNode from the given configuration.
    ///
    /// See Bootstrap() for bootstrapping an initial state; this replaces the follower
    /// `peers` argument to this method (with identical behavior). However, It is
    /// recommended that instead of calling Bootstrap. application bootstrap their
    /// state manually by setting up a Storage that has a first index > 1 and which
    /// stores the described ConfState as its InitialState.
    pub fn new(config: Config, storage: S) -> RawCoreNode<S> {
        let mut raft = Raft::new(config, storage);
        let prev_soft_st = raft.soft_state();
        let prev_hard_st = raft.hard_state();
        RawCoreNode {
            raft,
            prev_soft_st: Some(prev_soft_st),
            prev_hard_st,
        }
    }

    pub fn boot_strap(&mut self, peers: Vec<Peer>) -> Result<(), String> {
        if peers.is_empty() {
            return Err("must provide at least one peer to Bootstrap".to_string());
        }
        let last_index = self
            .raft
            .raft_log
            .storage
            .last_index()
            .map_err(|err| format!("{:?}", err))?;
        if last_index != 0 {
            return Err("can't bootstrap a nonempty storage".to_string());
        }

        self.prev_hard_st = HardState::new();
        self.raft.become_follower(1, NONE);
        let mut ents = Vec::with_capacity(peers.len());
        for (i, peer) in peers.iter().enumerate() {
            let mut cc = ConfChange::new();
            cc.set_field_type(ConfChangeAddNode);
            cc.set_node_id(peer.id);
            cc.set_context(Bytes::from(peer.context.clone()));
            let data = cc.write_to_bytes().map_err(|err| format!("{}", err))?;

            let mut entry = Entry::new();
            entry.set_Type(EntryConfChange);
            entry.set_Term(1);
            entry.set_Index((i + 1) as u64);
            entry.set_Data(Bytes::from(data));
            ents.push(entry);
        }

        self.raft.raft_log.append(&ents);
        self.raft.raft_log.committed = ents.len() as u64;
        for peer in &peers {
            let mut cc = ConfChange::new();
            cc.set_node_id(peer.id);
            cc.set_field_type(ConfChangeAddNode);
            let mut v2 = cc.as_v2();
            self.raft.apply_conf_change(&mut v2);
        }
        return Ok(());
    }

    /// Tick advances the interval logical clock by a single tick.
    pub fn tick(&mut self) {
        self.raft.tick_election();
    }

    /// TickQuiesced advances the internal logical clock by a single tick without
    /// performing any other state machine processing. It allows the caller to avoid
    /// periodic heartbeats and elections when all of the peers in a Raft group are
    /// known to be at the same state. Expected usage is to periodically invoke Tick
    /// or TickQuiesced depending on whether the group is "active" or "quiesced".
    ///
    /// WARNING: Be very careful about using this method as it subverts the Raft
    /// state machine. You should probably be using Tick instead.
    pub fn tick_quiesced(&mut self) {
        self.raft.election_elapsed += 1;
    }

    /// Campaign causes this RawNode to transition to candidate state.
    pub fn campaign(&mut self) -> Result<()> {
        let mut msg = Message::new();
        msg.set_field_type(MessageType::MsgHup);
        self.raft.step(msg).map_err(|err| anyhow!("{}", err))
    }

    /// Propose propose data be appended to the raft log.
    pub fn propose(&mut self, data: Bytes) -> Result<()> {
        let mut ent = Entry::new();
        ent.set_Data(data);
        let mut msg = Message::new();
        msg.set_field_type(MessageType::MsgProp);
        msg.set_from(self.raft.id);
        msg.set_entries(RepeatedField::from_slice(&vec![ent]));
        self.raft.step(msg).map_err(|err| anyhow!("{}", err))
    }

    /// ProposeConfChange proposes a config message.
    pub fn propose_conf_change(&mut self, conf_change: &ConfChange) -> Result<()> {
        let data = Bytes::from(conf_change.write_to_bytes().unwrap());
        let mut entry = Entry::new();
        entry.set_Type(EntryType::EntryConfChange); // TODO: use x11EntryConfChangeV2
        entry.set_Data(data);
        let mut msg = Message::new();
        msg.set_field_type(MessageType::MsgProp);
        msg.set_entries(RepeatedField::from_slice(&vec![entry]));
        self.raft.step(msg).map_err(|err| anyhow!("{}", err))
    }

    /// ApplyConfChange applies a config change to the local node. The app must call
    /// this when it applies a configuration change, except when it decides to reject
    /// the configuration change, in which case no call must take place.
    pub fn apply_conf_change<T: ConfChangeI>(&mut self, conf_change: T) -> ConfState {
        let cs = self.raft.apply_conf_change(&mut conf_change.as_v2());
        cs
    }

    // Step advances the state machine using the given message.
    pub fn step(&mut self, m: Message) -> ::std::result::Result<(), RawRaftError> {
        // ignore unexpected local message receiving over the network
        if is_local_message(m.get_field_type()) {
            return Err(RawRaftError::StepLocalMsg);
        }
        if self.raft.prs.progress.contains_key(&m.get_from())
            || !is_response_message(m.get_field_type())
        {
            return self
                .raft
                .step(m)
                .map_err(|err| RawRaftError::StepUnknown(err.to_string()));
        }
        Err(RawRaftError::StepPeerNotFound)
    }

    // Ready returns the outstanding work that the application needs to handle. This
    // includes appending and applying entries or a snapshot, updating the HardState,
    // and sending messages. The returned Ready() *must* be handled and subsequently
    // passed back via Advance().
    pub fn ready(&mut self) -> Ready {
        let mut ready = self.ready_without_accept();
        self.accept_ready(&ready);
        ready
    }

    // readyWithoutAccept returns a Ready. This is a read-only operation, i.e. there
    // is no obligation that the Ready must be handled.
    pub(crate) fn ready_without_accept(&self) -> Ready {
        Ready::new(
            &self.raft,
            self.prev_soft_st.clone(),
            self.prev_hard_st.clone(),
        )
    }

    // acceptReady is called when the consumer of the RawNode has decided to go
    // ahead and handle a Ready. Nothing must alter the state of the RawNode between
    // this call and the prior call to Ready().
    pub(crate) fn accept_ready(&mut self, ready: &Ready) {
        debug!("accept ready, {:?}", ready);
        if ready.soft_state.is_some() {
            self.prev_soft_st = ready.soft_state.clone();
        }
        if !ready.read_states.is_empty() {
            self.raft.read_states.clear();
        }
        self.raft.msgs.clear();
    }

    /// HasReady called when RawNode user need to check if any Ready pending.
    /// Checking logic in this method should be consistent with Ready.containsUpdates().
    pub fn has_ready(&self) -> bool {
        if self
            .prev_soft_st
            .map_or_else(|| false, |soft_state| soft_state != self.raft.soft_state())
        {
            return true;
        }

        let hard_state = self.raft.hard_state();
        if !is_empty_hard_state(&hard_state) && hard_state != self.prev_hard_st {
            return true;
        }

        if self.raft.raft_log.has_pending_snapshot() {
            return true;
        }

        if !self.raft.msgs.is_empty()
            || !self.raft.raft_log.unstable_entries().is_empty()
            || self.raft.raft_log.has_next_entries()
        {
            return true;
        }

        if !self.raft.read_states.is_empty() {
            return true;
        }

        false
    }
    /// Advance notifies the RawNode that the application has applied and saved progress in the
    /// last Ready results.
    pub fn advance(&mut self, rd: &Ready) {
        if !is_empty_hard_state(&rd.hard_state) {
            self.prev_hard_st = rd.hard_state.clone();
        }
        self.raft.advance(rd);
    }

    /// Status returns the current status of the given group. This allocates, see
    /// BasicStatus and WithProgress for allocation-friendlier choices.
    pub fn status(&self) -> Status {
        Status::from(&self.raft)
    }

    /// BasicStatus returns a BasicStatus. Notably this does not contain the
    /// Progress map; see WithProgress for an allocation-free way to inspect it.
    pub fn base_status(&self) -> BaseStatus {
        BaseStatus::from(&self.raft)
    }

    /// WithProgress is a helper to introspect the Progress for this node and its
    /// peers.
    pub fn with_progress<F>(&mut self, mut visitor: F)
    where
        F: FnMut(u64, ProgressType, &mut Progress),
    {
        // self.raft.prs.visit()
        self.raft.prs.visit(|id, pr| {
            let mut typ = ProgressType::default();
            if pr.is_learner {
                typ = ProgressType::Learner;
            }
            let mut pr = pr.clone();
            pr.inflights.reset();
            visitor(id, typ, &mut pr);
        });
    }

    /// ReportUnreachable reports the given node is not reachable for the last send.
    pub fn report_unreachable(&mut self, id: u64) {
        let mut msg = Message::default();
        msg.set_field_type(MsgUnreachable);
        msg.set_from(id);
        self.raft.step(msg);
    }

    /// ReportSnapshot reports the status of the sent snapshot.
    pub fn report_snapshot(&mut self, id: u64, status: SnapshotStatus) {
        let rejected = status == SnapshotStatus::Failure;
        let mut msg = Message::default();
        msg.set_from(id);
        msg.set_field_type(MsgSnapStatus);
        msg.set_reject(rejected);
    }

    /// TransferLeader tris=es to transfer leadership to the given transferee.
    pub fn transfer_leader(&mut self, transferee: u64) {
        let mut msg = Message::new();
        msg.set_field_type(MessageType::MsgTransferLeader);
        msg.set_from(transferee);
        self.raft.step(msg);
    }

    /// ReadIndex requests a read state. The read state will be set in ready.
    /// Read State has a read index. Once the application advances further than the read
    /// index, any linearizable read requests issued before the read request can be
    /// processed safely. The read state will have the same rctx attached.
    pub fn read_index(&mut self, rctx: Vec<u8>) {
        let mut msg = Message::new();
        msg.set_field_type(MsgReadIndex);
        let mut entry = Entry::new();
        entry.set_Data(Bytes::from(rctx));
        msg.set_entries(RepeatedField::from(vec![entry]));
        self.raft.step(msg);
    }
}

#[derive(Copy, Clone, PartialEq, Debug)]
pub enum ProgressType {
    Peer,
    Learner,
}

impl Default for ProgressType {
    fn default() -> Self {
        ProgressType::Peer
    }
}

impl From<u8> for ProgressType {
    fn from(b: u8) -> Self {
        if b == 0 {
            ProgressType::Peer
        } else {
            ProgressType::Learner
        }
    }
}
