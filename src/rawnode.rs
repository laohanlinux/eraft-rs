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
use crate::raft::{Config, Raft, RaftError, StateType, NONE};
use crate::raft_log::{RaftLog, RaftLogError};
use crate::raftpb::raft::{
    ConfChange,
    ConfChangeType::{ConfChangeAddNode, ConfChangeRemoveNode},
    ConfState, Entry, EntryType,
    EntryType::EntryConfChange,
    HardState, Message, MessageType,
    MessageType::{MsgProp, MsgReadIndex, MsgSnapStatus, MsgUnreachable},
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

#[derive(Error, Clone, Debug, PartialEq, Eq)]
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
pub struct SafeRawNode<S: Storage> {
    pub(crate) core_node: Arc<RwLock<RawCoreNode<S>>>,
}

impl<S: Storage> SafeRawNode<S> {
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
    pub fn campaign(&mut self) -> Result<(), RaftError> {
        let mut msg = Message::new();
        msg.set_field_type(MessageType::MsgHup);
        self.raft.step(msg)
    }

    /// Propose propose data be appended to the raft log.
    pub fn propose(&mut self, data: Bytes) -> Result<(), RaftError> {
        let mut ent = Entry::new();
        ent.set_Data(data);
        let mut msg = Message::new();
        msg.set_field_type(MessageType::MsgProp);
        msg.set_from(self.raft.id);
        msg.set_entries(RepeatedField::from_slice(&vec![ent]));
        self.raft.step(msg)
    }

    /// ProposeConfChange proposes a config message.
    pub fn propose_conf_change(&mut self, conf_change: impl ConfChangeI) -> Result<(), RaftError> {
        let entry = conf_change.to_entry();
        let mut msg = Message::new();
        msg.set_field_type(MessageType::MsgProp);
        msg.set_entries(RepeatedField::from_slice(&vec![entry]));
        self.raft.step(msg)
    }

    /// ApplyConfChange applies a config change to the local node. The app must call
    /// this when it applies a configuration change, except when it decides to reject
    /// the configuration change, in which case no call must take place.
    pub fn apply_conf_change(&mut self, conf_change: Box<dyn ConfChangeI>) -> ConfState {
        let cs = self.raft.apply_conf_change(&mut conf_change.as_v2());
        cs
    }

    // Step advances the state machine using the given message.
    pub fn step(&mut self, m: Message) -> Result<(), RaftError> {
        // ignore unexpected local message receiving over the network
        if is_local_message(m.get_field_type()) {
            return Err(RaftError::FromRawRaft(RawRaftError::StepLocalMsg));
        }
        if self.raft.prs.progress.contains_key(&m.get_from())
            || !is_response_message(m.get_field_type())
        {
            return self.raft.step(m);
        }
        Err(RaftError::FromRawRaft(RawRaftError::StepPeerNotFound))
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
        self.raft.read_states.clear();
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

#[cfg(test)]
mod tests {
    use crate::mock::{new_log_with_storage, new_memory, new_test_conf, new_test_raw_node};
    use crate::node::{Node, Ready, SafeResult, SnapshotStatus};
    use crate::raft::{RaftError, NO_LIMIT};
    use crate::raftpb::raft::ConfChangeTransition::{
        ConfChangeTransitionAuto, ConfChangeTransitionJointExplicit,
        ConfChangeTransitionJointImplicit,
    };
    use crate::raftpb::raft::ConfChangeType::{ConfChangeAddLearnerNode, ConfChangeAddNode};
    use crate::raftpb::raft::EntryType::{EntryConfChange, EntryConfChangeV2};
    use crate::raftpb::raft::MessageType::{MsgHup, MsgPreVote, MsgPreVoteResp, MsgProp};
    use crate::raftpb::raft::{
        ConfChange, ConfChangeSingle, ConfChangeTransition, ConfChangeType, ConfChangeV2,
        ConfState, Entry, HardState, Message, MessageType, Snapshot, SnapshotMetadata,
    };
    use crate::raftpb::{cmp_conf_state, cmp_config_change_v2, ConfChangeI, ExtendConfChange};
    use crate::rawnode::{RawCoreNode, RawRaftError, SafeRawNode};
    use crate::status::Status;
    use crate::storage::{SafeMemStorage, Storage};
    use crate::util::{is_local_message, is_response_message};
    use async_channel::Receiver;
    use bytes::Bytes;
    use nom::ParseTo;
    use protobuf::{parse_from_bytes, Message as PbMessage, ProtobufEnum, RepeatedField};

    // rawNodeAdapter is essentially a lint that makes sure that RawNode implements
    // "most of" Node. The exceptions (some of which are easy to fix) are listed
    // below.
    struct RawNodeAdapter {
        raw_node: SafeRawNode<SafeMemStorage>,
    }

    impl Node for RawNodeAdapter {
        // Tick advances the internal logical clock by a single tick.
        fn tick(&mut self) {
            self.raw_node.wl().tick();
        }

        // Campaign causes this RawNode to transition to candidate state.
        fn campaign(&self) -> SafeResult<()> {
            self.raw_node.wl().campaign()
        }

        // Propose proposes data be appended to the raft log.
        fn propose(&self, data: &[u8]) -> SafeResult<()> {
            self.raw_node.wl().propose(Bytes::from(data.to_vec()))
        }

        // ProposeConfChange proposes a config change. See (Node).ProposeConfChange for
        // details.
        fn propose_conf_change(&self, cc: impl ConfChangeI) -> SafeResult<()> {
            self.raw_node.wl().propose_conf_change(cc)
        }

        fn step(&self, msg: Message) -> SafeResult<()> {
            self.raw_node.wl().step(msg)
        }

        fn ready(&self) -> Receiver<Ready> {
            unimplemented!()
        }

        fn advance(&self) {
            self.raw_node.wl().advance(&Ready::default());
        }

        // ApplyConfChange applies a config change to the local node. The app must call
        // this when it applies a configuration change, except when it decides to reject
        // the configuration change, in which case no call must take place.
        fn apply_conf_change(&self, cc: ConfChange) -> Option<ConfState> {
            Some(self.raw_node.wl().apply_conf_change(Box::new(cc)))
        }

        fn transfer_leader_ship(&self, _: u64, transferee: u64) {
            self.raw_node.wl().transfer_leader(transferee)
        }

        fn read_index(&self, rctx: Vec<u8>) -> SafeResult<()> {
            self.raw_node.wl().read_index(rctx);
            // RawNode swallowed the error in ReadIndex, it probably should not do that.
            return Ok(());
        }

        fn status(&self) -> Status {
            self.raw_node.rl().status()
        }

        fn report_unreachable(&self, id: u64) {
            self.raw_node.wl().report_unreachable(id)
        }

        fn report_snapshot(&self, id: u64, status: SnapshotStatus) {
            self.raw_node.wl().report_snapshot(id, status)
        }

        fn stop(&self) {}
    }

    #[test]
    fn t_raw_node_step() {
        flexi_logger::Logger::with_env().start();
        let msg_type = (0..MessageType::MsgPreVoteResp.value())
            .map(|id| MessageType::from_i32(id).unwrap())
            .collect::<Vec<_>>();
        for (_, msgt) in msg_type.iter().enumerate() {
            let mut s = new_memory();
            let mut hard_state = HardState::new();
            hard_state.set_term(1);
            hard_state.set_commit(1);
            {
                s.wl().set_hard_state(hard_state).unwrap();
            }
            {
                let mut snap_meta = SnapshotMetadata::new();
                snap_meta.set_term(1);
                snap_meta.set_index(1);
                let mut conf_state = ConfState::new();
                conf_state.set_voters(vec![1]);
                snap_meta.set_conf_state(conf_state);
                let mut snap = Snapshot::new();
                snap.set_metadata(snap_meta);
                assert!(s.wl().apply_snapshot(snap).is_ok());

                // Append an empty entry to make sure the non-local messages (like
                // vote requests) are ignored and don't trigger assertions.
                // TODO: Why?
                let mut raw_node = RawCoreNode::new(new_test_conf(1, vec![], 10, 1), s);
                let mut msg = Message::new();
                msg.set_field_type(*msgt);
                if let Err(err) = raw_node.step(msg) {
                    // LocalMsg should be ignored.
                    if is_local_message(*msgt) {
                        assert_eq!(
                            err,
                            RaftError::FromRawRaft(RawRaftError::StepLocalMsg),
                            "{:?}: step should ignored",
                            msgt,
                        );
                    }
                }
            }
        }
    }

    // TestNodeStepUnblock from node_test.go has no equivalent in rawNode because there is
    // no goroutine in RawNode.

    // TestRawNodeProposeAndConfChange tests the configuration change mechanism. Each
    // test case sends a configuration change which is either simple or joint, verifies
    // that it applies and that the resulting ConfState matches expectations, and for
    // joint configurations makes sure that they are exited successfully.
    #[test]
    fn t_raw_node_propose_and_conf_change() {
        flexi_logger::Logger::with_env().start();

        // (cc, exp, exp2)
        let mut tests: Vec<(Box<dyn ConfChangeI>, ConfState, Option<ConfState>)> = vec![
            // V1 config change.
            (
                new_conf_change(ConfChangeAddNode, 2),
                new_conf_state(vec![1, 2], vec![], vec![], None),
                None,
            ),
            // Proposing the same as a V2 change works just the same, without entering
            // a joint config.
            (
                new_conf_change(ConfChangeAddNode, 2),
                new_conf_state(vec![1, 2], vec![], vec![], None),
                None,
            ),
            // Ditto if we add it as a learner instead.
            (
                new_conf_change_v2(vec![(ConfChangeAddLearnerNode, 2)], None),
                new_conf_state(vec![1], vec![], vec![2], None),
                None,
            ),
            // We can ask explicitly for joint consensus if we want it.
            (
                new_conf_change_v2(
                    vec![(ConfChangeAddLearnerNode, 2)],
                    Some(ConfChangeTransitionJointExplicit),
                ),
                new_conf_state(vec![1], vec![1], vec![2], None),
                Some(new_conf_state(vec![1], vec![], vec![2], None)),
            ),
            // Ditto, but with implicit transition (the harness checks this).
            (
                new_conf_change_v2(
                    vec![(ConfChangeAddLearnerNode, 2)],
                    Some(ConfChangeTransitionJointImplicit),
                ),
                new_conf_state(vec![1], vec![1], vec![2], true),
                Some(new_conf_state(vec![1], vec![], vec![2], None)),
            ),
            // Add a new node and demote n1. This exercises the interesting case in
            // which we really need joint config changes and also need LearnersNext.
            (
                new_conf_change_v2(
                    vec![
                        (ConfChangeAddNode, 2),
                        (ConfChangeAddLearnerNode, 1),
                        (ConfChangeAddLearnerNode, 3),
                    ],
                    None,
                ),
                new_conf_state2(vec![2], vec![1], vec![3], vec![1], true),
                Some(new_conf_state(vec![2], vec![], vec![1, 3], None)),
            ),
            // Ditto explicit.
            (
                new_conf_change_v2(
                    vec![
                        (ConfChangeAddNode, 2),
                        (ConfChangeAddLearnerNode, 1),
                        (ConfChangeAddLearnerNode, 3),
                    ],
                    Some(ConfChangeTransitionJointExplicit),
                ),
                new_conf_state2(vec![2], vec![1], vec![3], vec![1], None),
                Some(new_conf_state(vec![2], vec![], vec![1, 3], None)),
            ),
            // Ditto implicit.
            (
                new_conf_change_v2(
                    vec![
                        (ConfChangeAddNode, 2),
                        (ConfChangeAddLearnerNode, 1),
                        (ConfChangeAddLearnerNode, 3),
                    ],
                    Some(ConfChangeTransitionJointImplicit),
                ),
                new_conf_state2(vec![2], vec![1], vec![3], vec![1], true),
                Some(new_conf_state(vec![2], vec![], vec![1, 3], None)),
            ),
        ];

        for (cc, exp, exp2) in tests {
            let mut s = new_memory();
            let raw_node = new_test_raw_node(1, vec![1], 10, 1, s.clone());
            {
                raw_node.wl().campaign().unwrap();
            }
            let mut proposed = false;
            let mut last_index = 0;
            let mut cc_data = vec![];
            // Propose the ConfChange, wait until it applies, save the resulting
            // ConfState.
            let mut cs = None;
            let mut core_node = raw_node.wl();
            while cs.is_none() {
                let ready = core_node.ready();
                s.wl().append(ready.entries.clone()).unwrap(); // persistent entries
                for ent in ready.committed_entries.iter() {
                    if ent.get_Type() == EntryConfChange {
                        let mut cc: ConfChange = parse_from_bytes(ent.get_Data()).unwrap();
                        cs = Some(core_node.apply_conf_change(Box::new(cc)));
                    } else if ent.get_Type() == EntryConfChangeV2 {
                        let mut cc: ConfChangeV2 = parse_from_bytes(ent.get_Data()).unwrap();
                        cs = Some(core_node.apply_conf_change(Box::new(cc)));
                    }
                }
                core_node.advance(&ready);
                // Once we are the leader, propose a command and a ConfChange.
                if !proposed && ready.soft_state.as_ref().unwrap().lead == core_node.raft.id {
                    assert!(core_node.propose(Bytes::from("somedata")).is_ok());
                    if let Some(ccv1) = cc.as_v1() {
                        cc_data = ccv1.write_to_bytes().unwrap();
                        core_node.propose_conf_change(ccv1.clone()).unwrap();
                    } else {
                        let ccv2 = cc.as_v2();
                        cc_data = ccv2.write_to_bytes().unwrap();
                        core_node.propose_conf_change(ccv2).unwrap();
                    }
                    proposed = true;
                }
            }

            // Check that the last index is exactly the conf change we put in,
            // down to the bits. Note that this comes from the Storage, which
            // will not reflect any unstable entries that we'll only be presented
            // with in the next Ready.
            let last_index = s.last_index();
            assert!(last_index.is_ok());
            let last_index = last_index.unwrap();
            let entries = s.entries(last_index - 1, last_index + 1, NO_LIMIT);
            assert!(entries.is_ok());
            let entries = entries.unwrap();
            assert_eq!(
                entries.len(),
                2,
                "len(entries) = len({}), want 2",
                entries.len()
            );
            //
            assert_eq!(
                entries[0].get_Data(),
                "somedata".as_bytes(),
                "type = {:?}, want {:?}",
                entries[1].get_Type(),
                EntryConfChangeV2
            );
            let mut typ = EntryConfChange;
            if cc.as_v1().is_none() {
                typ = EntryConfChangeV2;
            }
            assert_eq!(
                entries[1].get_Type(),
                typ,
                "type={:?}, want: {:?}",
                entries[1].get_Type(),
                typ
            );

            assert_eq!(
                entries[1].get_Data(),
                cc_data.as_slice(),
                "data={:?}, act: {:?}",
                entries[1].get_Data(),
                cc_data
            );

            let mut maybe_plugs_one = 0;
            let (auto_leave, ok) = cc.as_v2().enter_joint();
            if ok && auto_leave {
                // If this is an auto-leaving joint conf change, it will have
                // appended the entry that auto-leaves, so add one to the last
                // index that forms the basis of our expectations on
                // pendingConfIndex. (Recall that lastIndex was taken from stable
                // storage, but this auto-leaving entry isn't on stable storage
                // yet).
                maybe_plugs_one = 1;
            }

            {
                let (exp, act) = (
                    last_index + maybe_plugs_one,
                    core_node.raft.pending_config_index,
                );
                assert_eq!(
                    exp, act,
                    "pending_config_index: expectd {}, got: {}",
                    exp, act
                );
            }

            // Move the RawNode along. If the ConfChange was simple, nothing else
            // should happen. Otherwise, we're in a joint state, which is either
            // left automatically or not. If not, we add the proposal that leaves
            // it manually.
            let mut ready = core_node.ready();
            let mut context: &[u8] = &[];
            if !exp.get_auto_leave() {
                assert!(ready.entries.is_empty(), "expected no more entries");
                if exp2.is_none() {
                    continue;
                }

                context = "manual".as_bytes();
                info!("leaving joint state manually");
                let mut cc_v2 = ConfChangeV2::default();
                cc_v2.set_context(Bytes::from(context));
                assert!(core_node.propose_conf_change(cc_v2).is_ok());
                ready = core_node.ready();
            }
            // Check that the right ConfChange comes out.
            assert!(
                ready.entries.len() == 1 && ready.entries[0].get_Type() == EntryConfChangeV2,
                "expected exactly one more entry, got {:?}",
                ready
            );
            let mut cc = ConfChangeV2::default();
            cc.merge_from_bytes(ready.entries[0].get_Data()).unwrap();
            let mut expect_v2 = ConfChangeV2::new();
            expect_v2.set_context(Bytes::from(context));
            assert!(
                cmp_config_change_v2(&cc, &expect_v2),
                "expected zero ConfChangeV2, got {:?}",
                cc
            );
            // Lie and pretend the ConfChange applied. It won't do so because now
            // we require the joint quorum and we're only running one node.
            let mut cs = core_node.apply_conf_change(Box::new(cc));

            assert!(
                cmp_conf_state(exp2.as_ref().unwrap(), &cs),
                "exp: {:?}, got: {:?}",
                exp2.as_ref().unwrap(),
                &cs
            );
        }
    }

    fn new_conf_change(typ: ConfChangeType, node_id: u64) -> Box<dyn ConfChangeI> {
        let mut cc = ConfChange::new();
        cc.set_field_type(typ);
        cc.set_node_id(node_id);
        Box::new(cc)
    }

    fn new_conf_change_v2(
        op: Vec<(ConfChangeType, u64)>,
        ct: Option<ConfChangeTransition>,
    ) -> Box<dyn ConfChangeI> {
        let mut cc = ConfChangeV2::new();
        for (typ, node_id) in &op {
            let mut single = ConfChangeSingle::new();
            single.set_node_id(*node_id);
            single.set_field_type(*typ);
            cc.mut_changes().push(single);
        }
        if let Some(t) = ct {
            cc.set_transition(t);
        }
        Box::new(cc)
    }

    fn new_conf_state<T: Into<Option<bool>>>(
        voters: Vec<u64>,
        outgoing: Vec<u64>,
        learners: Vec<u64>,
        auto_leave: T,
    ) -> ConfState {
        let mut conf_state = ConfState::new();
        conf_state.set_voters(voters);
        conf_state.set_learners(learners);
        conf_state.set_voters_outgoing(outgoing);
        let auto_leave = auto_leave.into();
        if auto_leave.is_some() {
            conf_state.set_auto_leave(auto_leave.unwrap());
        }
        conf_state
    }

    fn new_conf_state2<T: Into<Option<bool>>>(
        voters: Vec<u64>,
        outgoing: Vec<u64>,
        learners: Vec<u64>,
        next_learners: Vec<u64>,
        auto_leave: T,
    ) -> ConfState {
        let mut conf_state = ConfState::new();
        conf_state.set_voters(voters);
        conf_state.set_learners(learners);
        conf_state.set_voters_outgoing(outgoing);
        conf_state.set_learners_next(next_learners);
        let auto_leave = auto_leave.into();
        if auto_leave.is_some() {
            conf_state.set_auto_leave(auto_leave.unwrap());
        }
        conf_state
    }
}
