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
use crate::node::SoftState;
use crate::node::{is_empty_hard_state, is_empty_snapshot, Ready};
use crate::quorum::quorum::{
    VoteResult,
    VoteResult::{VoteLost, VoteWon},
};
use crate::raft::ReadOnlyOption::{ReadOnlyLeaseBased, ReadOnlySafe};
use crate::raft_log::{RaftLog, RaftLogError};
use crate::raftpb::raft::EntryType::{EntryConfChange, EntryConfChangeV2, EntryNormal};
use crate::raftpb::raft::MessageType::{
    MsgApp, MsgAppResp, MsgBeat, MsgCheckQuorum, MsgHeartbeat, MsgHeartbeatResp, MsgHup,
    MsgPreVote, MsgPreVoteResp, MsgProp, MsgReadIndex, MsgReadIndexResp, MsgSnap, MsgSnapStatus,
    MsgTimeoutNow, MsgTransferLeader, MsgUnreachable, MsgVote, MsgVoteResp,
};
use crate::raftpb::raft::{
    ConfChangeV2, ConfState, Entry, HardState, Message, MessageType, Snapshot,
};
use crate::raftpb::{entry_to_conf_changei, equivalent, ConfChangeI, ExtendConfChange};
use crate::rawnode::RawRaftError;
use crate::read_only::{ReadIndexStatus, ReadOnly, ReadState};
use crate::storage::{Storage, StorageError, StorageError::SnapshotTemporarilyUnavailable};
use crate::tracker::{
    self, inflights::Inflights, progress::ProgressMap, state,
    state::StateType as ProgressStateType, ProgressTracker,
};
use crate::util::vote_resp_msg_type;
use bytes::Bytes;
use protobuf::{reflect::ProtobufValue, RepeatedField};
use rand::{thread_rng, Rng};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use thiserror::Error;

/// NONE is a placeholder node ID used when there is no leader.
pub const NONE: u64 = 0;
pub const NO_LIMIT: u64 = u64::MAX;

/// Represents the type of campaigning
/// the reason we use the type of enum instead of u64
/// is because it's simpler to compare and fill in raft entries.
#[derive(Clone, Debug, Copy, PartialEq)]
pub enum CampaignType {
    // CampaignPreElection represents the first phase of a normal election when
    // Config.PreVote is true.
    CampaignPreElection,
    // CampaignElection represents a normal (time-based) election (the second phase
    // of the election when Config.PreVote is true).
    CampaignElection,
    // CampaignTransfer represents the type of leader transfer.
    CampaignTransfer,
}

impl Display for CampaignType {
    fn fmt(&self, f: &mut Formatter<'_>) -> ::std::fmt::Result {
        match self {
            CampaignType::CampaignPreElection => write!(f, "CampaignPreElection"),
            CampaignType::CampaignElection => write!(f, "CampaignElection"),
            CampaignType::CampaignTransfer => write!(f, "CampaignTransfer"),
        }
    }
}

impl From<&[u8]> for CampaignType {
    fn from(b: &[u8]) -> Self {
        match String::from_utf8_lossy(b).as_ref() {
            "CampaignPreElection" => Self::CampaignPreElection,
            "CampaignElection" => Self::CampaignElection,
            "CampaignTransfer" => Self::CampaignTransfer,
            s => panic!("not unknown type {:?}", s),
        }
    }
}

/// State type represents the role of a node in a cluster.
#[derive(Clone, Debug, Copy, Eq, PartialEq)]
pub enum StateType {
    PreCandidate,
    Follower,
    Candidate,
    Leader,
}

impl Display for StateType {
    fn fmt(&self, f: &mut Formatter<'_>) -> ::std::fmt::Result {
        match self {
            StateType::PreCandidate => write!(f, "StatePreCandidate"),
            StateType::Follower => write!(f, "StateFollower"),
            StateType::Candidate => write!(f, "StateCandidate"),
            StateType::Leader => write!(f, "StateLeader"),
        }
    }
}

impl Default for StateType {
    fn default() -> Self {
        StateType::PreCandidate
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReadOnlyOption {
    // ReadOnlySafe guarantees the linearizability of the read only request by
    // communicating which the quorum. It is the default and suggested option.
    ReadOnlySafe,
    // ReadOnlyLeaseBased ensures linearizability of the read only request by
    // relying on the leader lease. It can be affected by clock drift.
    // If the clock drift is unbounded, leader might keep the lease longer than it
    // should (clock can move backward/pause without any bound). ReadIndex is not safe
    // in that case.
    ReadOnlyLeaseBased,
}

/// Possible values for `CompaignType`
/// `CAMPAIGN_PRE_ELECTION` represents the first phase of a normal election when
/// `Config.PreVote` is true.
pub const CAMPAIGN_PRE_ELECTION: &'static str = "CampaignPreElction";
/// `CAMPAIGN_ELECTION` represents a normal (time-based) election (the second phase
/// of the election when `Config.Prevote` is true).
pub const CAMPAIGN_ELECTION: &'static str = "CampaignElection";
/// `CAMPAIGN_TRANSFER` represents the type of leader transfer
pub const CAMPAIGN_TRANSFER: &'static str = "CompaignTransfer";

#[derive(Error, Clone, Debug, PartialEq)]
pub enum RaftError {
    // ErrProposalDropped is returned when the proposal is ignored by some cases,
    // so that the proposer can be notified and fail fast.
    #[error("raft proposal dropped")]
    ProposalDropped,
    #[error("raft: stopped")]
    Stopped,
    #[error("raft: the node is not voter")]
    NotIsVoter,
    #[error("raft: hasn't leader")]
    NoLeader,
    #[error("from raw_raft: {0}")]
    FromRawRaft(RawRaftError),
}

/// Config contains the parameters to start a raft.
#[derive(Clone)]
pub struct Config {
    // id is the identify of the local raft. id cannot be 0;
    pub id: u64,

    // peers contains the IDs of all nodes (including self) in the raft cluster. It
    // should only be set when starting a new raft cluster. Restarting raft from
    // previous configuration will panic if peers is set. peer is private and only
    // used for testing right now.
    pub peers: Vec<u64>,

    // learners contains the IDs of all learner nodes (including self if the
    // local node is a learner) in the raft cluster. learners only receives
    // entries from the leader node. It does not vote or promote itself.
    pub learners: Vec<u64>,

    // ElectionTick is the number of Node.Tick invocations that must pass between
    // elections. That is, if a follower does not receive any message from the
    // leader of current term before ElectionTick has elapsed, it will become
    // candidate and start an election. ElectionTick must be greater than
    // HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
    // unnecessary leader switching.
    pub election_tick: u64,

    // HeartbeatTick is the number of Node.Tick invocations that must pass between
    // heartbeats. That is, a leader sends heartbeat messages to maintain its
    // leadership every HeartbeatTick ticks.
    pub heartbeat_tick: u64,

    // Storage is the storage for raft. raft generates entries and states to be
    // stored in storage. raft reads the persisted entries and states out of
    // Storage when it needs. raft reads out the previous state and configuration
    // out of storage when restarting.
    // pub storage: S,

    // Applied is the last applied index. It should only be set when restarting
    // raft. raft will not return entries to the application smaller or equal to
    // Applied. If Applied is unset when restarting, raft might return previous
    // applied entries. This is a very application dependent configuration.
    pub applied: u64,

    // max_size_per_msg limits the max the byte size of each append message. Smaller
    // value lowers the raft recovery cost(initial probing and message lost
    // during normal operation). On the other side. it might affect the
    // throughput during normal replication. Note: math.MaxU64 for unlimited,
    // 0 from at most one entry per message.
    pub max_size_per_msg: u64,

    // max_committed_size_per_ready limits the size of the committed enables which
    // can be applied
    pub max_committed_size_per_ready: u64,

    // max_uncommitted_entries_size limits the aggregate byte size of the
    // uncommitted entries that may be appended to a leader's log. Once this
    // limit is exceeded, proposals will begin to return ErrProposalDropped
    // errors. Note: 0 for no limit.
    pub max_uncommitted_entries_size: u64,

    // max_inflight_msgs limits the max number of in-flight append messages during
    // optimistic replication phase. The application transportation layer usually
    // has its own sending buffer over TCP/UDP. Setting max_inflight_msgs to avoid
    // overflowing that sending buffer. TODO (xiangli): feedback to application to
    // limit the proposal rate?
    pub max_inflight_msgs: u64,

    // check_quorum specifies if the leader should check quorum activity. Leader
    // steps down when quorum is not active for an election_timeout.
    pub check_quorum: bool,

    // pre_vote enables the pre_vote algorithm described in raft thesis section
    // 9.6. This prevents disruption when a node that has been partitioned away
    // rejoins the cluster.
    pub pre_vote: bool,

    // ReadOnlyOption specifies how the read only request is processed.
    //
    // ReadOnlySafe guarantees the linearizability of the read only request by
    // communicating with the quorum. It is the default and suggested option.
    //
    // ReadOnlyLeaseBased ensures linearizability of the read only request by
    // replying on the leader lease. It can be affected by clock drift.
    // If the lock drift is unbounded, leader might keep the lease longer than it
    // should (clock and move backward/pause without any bound). ReadIndex is not safe
    // in that case.
    // check_quorum MUST be enables if ReadOnlyOption is ReadOnlyLeaseBased.
    pub read_only_option: ReadOnlyOption,

    // disable_proposal_forwarding set to true means that followers will drop
    // proposals, rather than forwarding them to the leader. One use case for
    // this feature would be in a situation where the Raft leader is used to
    // compute the data of a proposal, for example, adding a timestamp from a
    // hybrid logical clock to data in a monotonically increasing way. Forwarding
    // should be disable to prevent a follower with an inaccurate hybrid
    // logical clock from assigning the timestamp and then forwarding the data
    // to the leader.
    pub disable_proposal_forwarding: bool,
}

impl Config {
    pub fn validate(&mut self) -> Result<(), String> {
        if self.id == NONE {
            return Err("cannot be none as id".to_string());
        }
        if self.heartbeat_tick <= 0 {
            return Err("heartbeat tick must be greater than 0".to_string());
        }
        if self.election_tick <= self.heartbeat_tick {
            return Err("election tick must be greater than heartbeat tick".to_string());
        }

        if self.max_uncommitted_entries_size == 0 {
            self.max_uncommitted_entries_size = NO_LIMIT;
        }

        // default max_committed_size_per_ready to max_size_per_msg because they were
        // previously the same parameters.
        if self.max_committed_size_per_ready == 0 {
            self.max_committed_size_per_ready = self.max_size_per_msg;
        }

        if self.max_inflight_msgs <= 0 {
            return Err("max inflight messages must be greater than 0".to_string());
        }

        if self.read_only_option == ReadOnlyOption::ReadOnlyLeaseBased && !self.check_quorum {
            return Err(
                "check_quorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased"
                    .to_string(),
            );
        }

        Ok(())
    }
}

// Progress represents a follower's progress in the view of the leader. Leader maintains
// progress of all followers, and sends entries to the follower based on its progress.
#[derive(Clone, Default)]
pub struct Progress {
    pub _match: u64,
    pub next: u64,
}

pub struct Raft<S: Storage> {
    pub id: u64,
    pub term: u64,
    pub vote: u64,
    pub read_states: Vec<ReadState>,

    // the log
    pub raft_log: RaftLog<S>,

    pub max_msg_size: u64,
    pub max_uncommitted_size: u64,
    // TODO(tbg): rename to trk.
    // log replication progress of each peers.
    pub prs: ProgressTracker,
    // this peer's role
    pub state: StateType,
    // is_leader is true if the local raft node is a leaner
    pub is_learner: bool,

    // msgs need to send
    pub msgs: Vec<Message>,

    // the leader id
    pub lead: u64,
    // leadTransferee is id of the leader transfer target when its value is not zero.
    // Follow the procedure defined in section 3.10 of Raft phd thesis.
    // (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
    // (Used in 3A leader transfer)
    pub lead_transferee: u64,
    // Only one config change may be pending (in the log, but not yet
    // applied) at a time. This is enforced via PendingConfIndex, which
    // is set to a value >= the log index of the latest pending
    // configuration change (if any). Config changes are only allowed to
    // be proposed if the leader's applied index is greater than this
    // value.
    // (Used in 3A conf change)
    pub pending_config_index: u64,
    // an estimate of the size of the uncommitted tail of the Raft log. Used to
    // prevent unbounded log growth. Only maintained by the leader. Reset on
    // term changes.
    pub uncommitted_size: u64,

    pub read_only: ReadOnly,

    // number of ticks since it reached last election_timeout when it is leader
    // or candidate.
    // number of ticks since it reached last election_timeout or received a
    // valid message from current leader when it is a follower.
    pub election_elapsed: u64,

    // number of ticks sine it reached last heartbeat_timeout.
    // only leader keeps heartbeat_elapsed.
    pub heartbeat_elapsed: u64,

    pub check_quorum: bool,
    pub pre_vote: bool,

    // heartbeat interval, should send
    pub heartbeat_timeout: u64,
    // baseline of election interval
    pub election_timeout: u64,
    // randomized_election_timeout is a random number between
    // [election_timeout, 2*election_timeout-1]. It gets reset
    // when *raft changes its state to follower or candidate*.
    pub randomized_election_timeout: u64,
    pub disable_proposal_forwarding: bool,
}

// tick_election is run by followers and candidate after self.election_timeout
fn tick_election<S: Storage>(raft: &mut Raft<S>) {
    raft.election_elapsed += 1;
    if raft.promotable() && raft.past_election_timeout() {
        raft.election_elapsed = 0;
        let mut msg = Message::new();
        msg.set_from(raft.id);
        msg.set_field_type(MsgHup);
        raft.step(msg);
    }
}

impl<S: Storage> Raft<S> {
    // TODO:
    pub fn new(mut config: Config, storage: S) -> Self {
        assert!(
            config.validate().is_ok(),
            "{}",
            config.validate().unwrap_err()
        );
        let state_ret = storage.initial_state();
        assert!(state_ret.is_ok()); // TODO(bdarnell)
        let raft_log = RaftLog::new_log_with_size(storage, config.max_committed_size_per_ready);
        let (hs, mut cs) = state_ret.unwrap();
        if !config.peers.is_empty() || !config.learners.is_empty() {
            if !cs.voters.is_empty() || !cs.learners.is_empty() {
                // TODO(bdarnell): the peers argument is always nil except in
                // tests; the argument should be removed and these tests should be
                // updated to specify their nodes through a snapshot.
                panic!("cannot specify both new_raft(pees, learners) and ConfigState.(Voters, Learners)");
            }
            cs.set_voters(config.peers.clone());
            cs.set_learners(config.learners.clone());
        }

        let mut raft = Raft {
            id: config.id,
            term: 0,
            vote: 0,
            read_states: vec![],
            raft_log,
            max_msg_size: config.max_size_per_msg,
            max_uncommitted_size: config.max_uncommitted_entries_size,
            prs: ProgressTracker::new(config.max_inflight_msgs),
            state: StateType::Follower,
            is_learner: false,
            msgs: vec![],
            lead: NONE,
            lead_transferee: 0,
            pending_config_index: 0,
            uncommitted_size: 0,
            read_only: ReadOnly::new(config.read_only_option),
            election_elapsed: 0,
            heartbeat_elapsed: 0,
            check_quorum: config.check_quorum,
            pre_vote: config.pre_vote,
            heartbeat_timeout: config.heartbeat_tick,
            election_timeout: config.election_tick,
            randomized_election_timeout: 0,
            disable_proposal_forwarding: config.disable_proposal_forwarding,
        };
        let (cfg, prs) = restore(
            &mut Changer {
                tracker: raft.prs.clone(),
                last_index: 0,
            },
            &cs,
        )
        .unwrap();
        let s_tc = raft.switch_to_config(cfg, prs);
        assert!(equivalent(&cs, &s_tc).is_ok());

        if !is_empty_hard_state(&hs) {
            raft.load_state(&hs);
        }

        if config.applied > 0 {
            raft.raft_log.applied_to(config.applied);
        }

        raft.become_follower(raft.term, NONE);
        let nodes_str = raft
            .prs
            .voter_nodes()
            .iter()
            .map(|id| format!("{:#x}", id))
            .collect::<Vec<_>>()
            .join(",");
        info!(
            "new_raft {:#x} [peers: [{}], term: {}, commit: {}, applied: {}, last_index: {}, last_term: {}]",
            raft.id,
            nodes_str,
            raft.term,
            raft.raft_log.committed,
            raft.raft_log.applied,
            raft.raft_log.last_index(),
            raft.raft_log.last_term()
        );
        raft
    }

    pub(crate) fn has_leader(&self) -> bool {
        self.lead != NONE
    }

    pub(crate) fn soft_state(&self) -> SoftState {
        SoftState {
            raft_state: self.state,
            lead: self.lead,
        }
    }

    pub(crate) fn hard_state(&self) -> HardState {
        let mut hard_state = HardState::new();
        hard_state.set_term(self.term);
        hard_state.set_vote(self.vote);
        hard_state.set_commit(self.raft_log.committed);
        hard_state
    }

    // persists state to stable storage and then sends to its mailbox.
    fn send(&mut self, mut m: Message) {
        if m.from == NONE {
            m.from = self.id;
        }
        let msg_type = m.field_type;
        if msg_type == MsgVote
            || msg_type == MsgVoteResp
            || msg_type == MsgPreVote
            || msg_type == MsgPreVoteResp
        {
            // All {pre-, }campaign messages need to have the term set when
            // sending.
            // - MsgVote: m.Term is the term the node is campaigning for,
            //   non-zero as we increment the term when campaigning.
            // - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
            //   granted, non-zero for the same reason MsgVote is
            // - MsgPreVote: m.Term is the term the node will campaign,
            //   non-zero as we use m.Term to indicate the next term we'll be
            //   campaigning fro
            // - MsgPreVoteResp: m.Term is the term received in th original
            //   MsgPreVote if the pre-vote was granted, non-zero for the
            //   same reasons MsgPreVote is
            assert_ne!(
                m.term, 0,
                "term should be set when sending {:?}",
                m.field_type,
            );
        } else {
            if m.term != 0 {
                panic!(
                    "term should not be set when sending {:?} (was {})",
                    m.field_type, m.term
                );
            }
            // do not attach term to MsgProp, MsgReadIndex
            // proposals are a way to forward to the leader and
            // should be treated as local message.
            // MsgReadIndex is also forwarded to leader.
            if m.field_type != MsgProp && m.field_type != MsgReadIndex {
                m.term = self.term;
            }
        }

        self.msgs.push(m);
    }

    // send_append sends an append RPC with new entries (if any) and the
    // current commit index to the given peer. Returns true if a message was sent.
    fn send_append(&mut self, to: u64) -> bool {
        self.maybe_send_append(to, true)
    }

    // sends an append RPC with new entries to the given peer,
    // if necessary. Returns true if a message was sent. The sendIfEmpty
    // argument controls whether messages with no entries will be sent.
    // ("empty" messages are useful to convey updated Commit indexes, but
    // are undesirable when we're sending multiple messages in a batch).
    fn maybe_send_append(&mut self, to: u64, send_if_empty: bool) -> bool {
        debug!(
            "execute maybe_send_append, to: {}, send_if_empty: {}",
            to, send_if_empty
        );
        let pr = self.prs.progress.must_get_mut(&to);
        if pr.is_paused() {
            return false;
        }
        let mut m = Message {
            to,
            ..Default::default()
        };

        // get the last log term because `pr.next` is next index, so `lasted = next - 1`
        let term = self.raft_log.term(pr.next - 1);
        let ents = self.raft_log.entries(pr.next, self.max_msg_size);
        if ents.as_ref().map_or_else(|_| true, |ents| ents.is_empty()) && !send_if_empty {
            debug!(
                "ignore send append from, send_if_empty={}, to={:0x}",
                send_if_empty, m.to
            );
            return false;
        }
        if term.is_err() || ents.is_err() {
            // send snapshot if we failed to get term or entries
            if !pr.recent_active {
                debug!(
                    "ignore sending snapshot to {:#x} since it is not recently active",
                    to
                );
                return false;
            }

            m.field_type = MsgSnap;
            match self.raft_log.snapshot() {
                Ok(snapshot) => {
                    let metadata = snapshot.get_metadata();
                    let (s_index, s_term) = (metadata.get_index(), metadata.get_term());
                    assert!(s_index > 0, "need non-empty snapshot");
                    m.set_snapshot(snapshot);
                    debug!(
                        "{:#x} [first_index: {}, commit: {}] sent snapshot[index: {}, term: {}] to {:#x} [{:?}]",
                        self.id,
                        self.raft_log.first_index(),
                        self.raft_log.committed,
                        s_index,
                        s_term,
                        to,
                        pr
                    );
                    pr.become_snapshot(s_index);
                    debug!(
                        "{:#x} paused sending replication messages to {:#x} [{}]",
                        self.id, to, pr
                    );
                }
                Err(e)
                    if e == RaftLogError::FromStorage(
                        StorageError::SnapshotTemporarilyUnavailable,
                    ) =>
                {
                    debug!(
                            "{:#x} failed to send snapshot to {:#x} because snapshot is temporarily unvalidated",
                            self.id, to
                        );
                    return false;
                }
                Err(e) => panic!("{:?}", e), // TODO(bdarnell)
            }
        } else {
            let term = term.unwrap();
            let ents = ents.unwrap();
            m.field_type = MsgApp;
            // msg.index is the current node lasted index, so `sub - 1`
            m.index = pr.next - 1;
            // msg.logTerm is the current node lasted term.
            m.logTerm = term;
            m.entries = RepeatedField::from_vec(ents);
            m.commit = self.raft_log.committed;
            let n = m.entries.len();
            if n != 0 {
                match &pr.state {
                    state::StateType::Replicate => {
                        // optimistically increase the next when in StateReplicate
                        let last = m.entries.last().unwrap().get_Index();
                        pr.optimistic_update(last);
                        pr.inflights.add(last);
                        // debug!("#{:0x} progress at replication, append to it for log entry, {:?}, msg: {:?}", to, pr, m);
                    }
                    state::StateType::Probe => {
                        pr.probe_sent = true;
                    }
                    _ => panic!(
                        "{:#x} is sending append in unhandled state {}",
                        self.id, pr.state
                    ),
                }
            }
        }
        // debug!("send append succeeded, msg: {:#?}", m);
        self.send(m);
        true
    }

    // send_heartbeat sends a heartbeat RPC to the given peer.
    fn send_heartbeat<T: Into<Option<Vec<u8>>>>(&mut self, to: u64, ctx: T) {
        // Attach the commit as min(to.matched, self.committed).
        // When the leader sends out heartbeat message,
        // the receiver(follower) might not be matched with the leader.
        // or it might not have all the committed entries.
        // The leader MUST NOT forward the follower's commit to
        // an unmatched index.
        let commit = self
            .prs
            .progress
            .get(&to)
            .unwrap()
            ._match
            .min(self.raft_log.committed);
        let mut m = Message {
            to,
            field_type: MsgHeartbeat,
            commit,
            ..Default::default()
        };
        if let Some(ctx) = ctx.into() {
            m.context = Bytes::from(ctx);
        }
        self.send(m);
    }

    // sends RPC, with entries to all peers that are not up-to-date
    // according to the progress recorded in self.prs.
    fn bcast_append(&mut self) {
        let ids = self.prs.visit_nodes();
        let cur_id = self.id;
        for id in ids.iter().filter(|id| **id != cur_id) {
            if !self.send_append(*id) {
                debug!(
                    "message not to sent, {} inflights full: {}",
                    id,
                    self.prs.progress.must_get(id).inflights.full()
                );
            }
        }
    }

    // sends RPC, without entries to all the peers.
    fn bcast_heartbeat(&mut self) {
        let last_ctx = self.read_only.last_pending_request();
        self.bcast_heartbeat_with_ctx(last_ctx);
    }

    fn bcast_heartbeat_with_ctx(&mut self, ctx: Option<Vec<u8>>) {
        debug!("{:0x} bcast heartbeat with ctx", self.id);
        let nodes = self.prs.voter_nodes();
        for id in nodes
            .iter()
            .filter(|id| **id != self.id)
            .collect::<Vec<_>>()
        {
            self.send_heartbeat(*id, ctx.clone());
        }
    }

    pub(crate) fn advance(&mut self, rd: &Ready) {
        self.reduce_uncommitted_size(&rd.entries);
        // If entries were applied (or a snapshot), update our cursor for
        // the next `Ready`, Note that if the current `HardState` contains a
        // new `Commit` index, this does not mean that we're also applying
        // all of the new entries due to commit pagination by size.
        let new_applied = rd.appliedCursor();
        if new_applied > 0 {
            let old_applied = self.raft_log.applied;
            self.raft_log.applied_to(new_applied);
            if self.prs.config.auto_leave
                && old_applied <= self.pending_config_index
                && new_applied >= self.pending_config_index
                && self.state == StateType::Leader
            {
                // If the current (and most recent, at least for this leader's term)
                // configuration should be auto-left, initiate that now. We use a
                // nil Date which unmarshals into an empty `ConfChangeV2` and has the
                // benefit that appendEntry can never refuse it based on its size
                // (which registers as zero).
                let mut ent = Entry::new();
                ent.set_Type(EntryConfChangeV2);
                // There's no way in which this proposal should be able to be rejected.
                assert!(
                    self.append_entry(&mut [ent]),
                    "refused un-refusable auto-leaving ConfChangeV2"
                );
                self.pending_config_index = self.raft_log.last_index();
                info!(
                    "initiating automatic transition out of joint configuration at index({}): {:?}",
                    self.pending_config_index, self.prs.config
                );
            }
        }

        if !rd.entries.is_empty() {
            let e = rd.entries.last().unwrap();
            self.raft_log.stable_to(e.get_Index(), e.get_Term());
        }
        if !is_empty_snapshot(&rd.snapshot) {
            self.raft_log
                .stable_snap_to(rd.snapshot.get_metadata().get_index());
        }
    }

    fn reset(&mut self, term: u64) {
        if self.term != term {
            self.term = term;
            self.vote = NONE;
        }
        self.lead = NONE;

        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;
        self.reset_randomized_election_timeout();

        self.abort_leader_transferee();

        self.prs.reset_votes();
        let local_id = self.id;
        let last_index = self.raft_log.last_index();
        let max_inflight = self.prs.max_inflight;
        self.prs.visit(|id, pr| {
            let is_learner = pr.is_learner;
            *pr = tracker::progress::Progress::new(0, last_index + 1);
            pr.is_learner = is_learner;
            pr.inflights = Inflights::new(max_inflight);
            if local_id == id {
                pr._match = last_index;
            }
        });

        self.pending_config_index = 0;
        self.uncommitted_size = 0;
        self.read_only = ReadOnly::new(self.read_only.option);
    }

    pub fn append_entry(&mut self, es: &mut [Entry]) -> bool {
        let li = self.raft_log.last_index();
        for i in 0..es.len() {
            es[i].set_Term(self.term);
            es[i].set_Index(li + 1 + i as u64);
        }
        // Track the size of this uncommitted proposal.
        if !self.increase_uncommitted_size(es) {
            warn!(
                "{:#x} appending new entries to log would exceed uncommitted entry size limit; dropping proposal",
                self.id
            );
            // Drop the proposal.
            return false;
        }
        // use latest "last" index after truncate/append
        let li = self.raft_log.append(es);
        self.prs.progress.must_get_mut(&self.id).maybe_update(li);
        self.maybe_commit();
        true
    }

    /// maybe_commit attemps to advance the commit index. Returns true if
    /// the commit index changed (in which case the caller should call
    /// self.bcast_append)
    pub fn maybe_commit(&mut self) -> bool {
        let mci = self.prs.committed();
        self.raft_log.maybe_commit(mci, self.term)
    }

    /// Runs by followers and candidates after self.election_timeout
    pub fn tick_election(&mut self) {
        self.election_elapsed += 1;
        if self.promotable() && self.past_election_timeout() {
            self.election_elapsed = 0;
            let mut msg = Message::new();
            msg.set_from(self.id);
            msg.set_field_type(MsgHup);
            self.step(msg);
            debug!("trigger tick election");
        }
    }

    /// Runs by leaders to `send` a `MsgBeat` after self.heartbeat_timeout
    pub fn tick_heartbeat(&mut self) {
        self.heartbeat_elapsed += 1;
        self.election_elapsed += 1;

        if self.election_elapsed >= self.election_timeout {
            self.election_elapsed = 0;
            if self.check_quorum {
                self.step(Message {
                    from: self.id,
                    field_type: MsgCheckQuorum,
                    ..Default::default()
                });
            }
            // If current leader cannot transfer leadership in election_timeout, it becomes leader again.
            if self.state == StateType::Leader && self.lead_transferee != NONE {
                self.abort_leader_transferee();
            }
        }

        if self.state != StateType::Leader {
            return;
        }

        if self.heartbeat_elapsed >= self.heartbeat_timeout {
            self.heartbeat_elapsed = 0;
            self.step(Message {
                from: self.id,
                field_type: MsgBeat,
                ..Default::default()
            });
        }
    }

    // become_follower transform this peer's state to follower
    pub(crate) fn become_follower(&mut self, term: u64, lead: u64) {
        // self.step = Some(Box::new(Self::step_follower));
        self.reset(term);
        // self.tick = Some(Box::new(tick_election));
        self.lead = lead;
        self.state = StateType::Follower;
        info!("{:#x} became follower at term {}", self.id, self.term);
    }

    // become_candidate transform this peer's state to candidate
    pub(crate) fn become_candidate(&mut self) {
        assert_ne!(
            self.state,
            StateType::Leader,
            "invalid transition [leader -> candidate]"
        );
        // self.step = Some(Box::new(Self::step_candidate));
        self.reset(self.term + 1);
        // self.tick = Some(Box::new(tick_election));
        self.vote = self.id;
        self.state = StateType::Candidate;
        info!("{:#x} became candidate at term {}", self.id, self.term);
    }

    fn become_precandidate(&mut self) {
        assert_ne!(
            self.state,
            StateType::Leader,
            "invalid transition [leader->pre-candidate]"
        );

        // becoming a pre-candidate changes our step functions and state,
        // but doesn't change anything else. In particular it doesn't increase
        // self.term or change self.vote.
        // self.step = Some(Box::new(Self::step_candidate));
        self.prs.reset_votes();
        // self.tick = Some(Box::new(tick_election));
        self.lead = NONE;
        self.state = StateType::PreCandidate;
        info!("{:#x} became pre-candidate at term {}", self.id, self.term);
    }

    // become_leader transform this peer's state to leader
    pub(crate) fn become_leader(&mut self) {
        info!("{:#x} become leader at term {}", self.id, self.term);
        assert_ne!(
            self.state,
            StateType::Follower,
            "invalid transition [follower->leader]"
        );
        // self.step = Some(Box::new(Self::step_leader));
        self.reset(self.term);
        // self.tick = Some(Box::new(tick_election));
        self.lead = self.id;
        self.state = StateType::Leader;
        // Followers enter replicate mode when they've been successfully probed
        // (perhaps after having received a snapshot as a result). The leader is
        // trivially in this state. Note that r.reset() has initialized this
        // progress with the last index already.
        self.prs.progress.must_get_mut(&self.id).become_replicate();

        // conservatively set the pending_config_index to the last index in the
        // log. There may or may not be a pending config change. but it's
        // safe to delay any future proposals until we commit all our
        // pending log entries, and scanning the entire tail of the log
        // could be expensive.
        self.pending_config_index = self.raft_log.last_index();

        let empty_ent = Entry::new();
        // This won't happen because we just called reset() above.
        assert!(
            self.append_entry(&mut [empty_ent.clone()]),
            "empty entry was dropped"
        );
        // As a special case, don't count the initial empty entry towards the
        // uncommitted log quota. The is because we want to preseve the
        // behavior of allowing the one entry larger than quota if the current
        // usage is zero.
        self.reduce_uncommitted_size(&[empty_ent.clone()]);
    }

    fn hup(&mut self, t: CampaignType) {
        if self.state == StateType::Leader {
            debug!("{:#x} ignoring MsgHup because already leader", self.id);
            return;
        }

        if !self.promotable() {
            warn!("{:#x} is unprootable and can not campaign", self.id);
            return;
        }

        let ents = self.raft_log.slice(
            self.raft_log.applied + 1,
            self.raft_log.committed + 1,
            NO_LIMIT,
        );
        assert!(
            ents.is_ok(),
            "unexpected error getting unapplied entries ({})",
            ents.unwrap_err()
        );
        let n = Self::num_of_pending_conf(ents.unwrap().as_slice());
        if n != 0 && self.raft_log.committed > self.raft_log.applied {
            warn!(
                "{:#x} cannot campaign at term {} since there are still {} pending configuration changes to apply",
                self.id, self.term, n
            );
            return;
        }

        info!(
            "{:#x} is starting a new election at term {}",
            self.id, self.term
        );
        self.campaign(t);
    }

    // campaign transition the raft instance to candidate state. This must only be
    // called after verifying that this is a legitimate transition.
    fn campaign(&mut self, t: CampaignType) {
        if !self.promotable() {
            // This path should not be hit (callers are supposed to check), but
            // better safe than sorry.
            warn!(
                "{:#x} is unprootable; campaign() should have been called",
                self.id
            );
        }
        let mut term = 0;
        let mut vote_msg = MessageType::MsgHup;
        if t == CampaignType::CampaignPreElection {
            self.become_precandidate();
            vote_msg = MessageType::MsgPreVote;
            // PreVote RPCs are sent for the next term before we've incremented self.Term.
            term = self.term + 1;
        } else {
            self.become_candidate();
            vote_msg = MessageType::MsgVote;
            term = self.term;
        }

        let (_, _, res) = self.poll(self.id, vote_msg, true);
        if res == VoteWon {
            // We won the election after voting for ourselves (which must mean that
            // this is a *single-node* cluster). Advance to the next state.
            if t == CampaignType::CampaignPreElection {
                self.campaign(CampaignType::CampaignElection);
            } else {
                self.become_leader();
            }
            return;
        }
        let mut ids = self
            .prs
            .config
            .voters
            .ids()
            .iter()
            .map(|id| *id)
            .filter(|id| *id != self.id)
            .collect::<Vec<_>>();
        // TODO: why sort?
        ids.sort_by_key(|key| *key);
        ids.iter().for_each(|id| {
            info!(
                "{:#x} [logterm: {}, index: {}] sent {:?} request to {:#x} at term {}",
                self.id,
                self.raft_log.last_term(),
                self.raft_log.last_index(),
                vote_msg,
                id,
                self.term,
            );
            let mut msg = Message::new();
            msg.set_term(term);
            msg.set_index(self.raft_log.last_index());
            msg.set_to(*id);
            msg.set_field_type(vote_msg);
            msg.set_logTerm(self.raft_log.last_term());
            if t == CampaignType::CampaignTransfer {
                msg.set_context(Bytes::from(format!("{:?}", t)));
            }
            self.send(msg);
        });
    }

    pub(crate) fn poll(&mut self, id: u64, t: MessageType, v: bool) -> (usize, usize, VoteResult) {
        if v {
            debug!(
                "{:#x} received {:?} from {:#x} at term {}",
                self.id, t, id, self.term
            );
        } else {
            debug!(
                "{:#x} received {:?} rejection from {:#x} at term {}",
                self.id, t, id, self.term,
            );
        }
        self.prs.record_vote(id, v); // vote for itself.
        self.prs.tally_votes()
    }

    // Step the entrance of handle message, see `MessageType`
    // on `eraft.proto`. for what msgs should be handled
    pub fn step(&mut self, m: Message) -> Result<(), RaftError> {
        match m.term {
            0 => {
                // local message
                debug!("local message, {:?}", m);
            }
            term if term > self.term => {
                if m.field_type == MsgVote || m.field_type == MsgPreVote {
                    let force =
                        CampaignType::from(m.get_context()) == CampaignType::CampaignTransfer;
                    let in_lease = self.check_quorum
                        && self.lead != NONE
                        && self.election_elapsed < self.election_timeout;
                    if !force && in_lease {
                        // If a server receives a RequestVote request within the minimum election timeout
                        // of hearing from a current leader, it doesn't update its term or grant its vote
                        info!("{:#x} [logterm: {}, index: {}, vote: {:#x} ignored {:?} from {} [logterm: {}, index: {}] at term {}; lease is not expired (remaining ticks: {})",
                              self.id, self.raft_log.last_term(), self.raft_log.last_index(), self.vote, m.get_field_type(), m.get_from(), m.get_logTerm(), m.get_index(), self.term, self.election_timeout - self.election_elapsed);
                        return Ok(());
                    }
                }
                match m.field_type {
                    MsgPreVote => {} // Never change our term in response to a PreVote
                    typ if typ == MsgPreVoteResp && !m.reject => {
                        // We send pre-vote requests with a term in our future. If the
                        // pre-vote is granted, we will increment our term when we get a
                        // quorum. If it is not, the term comes from the node that
                        // rejected our vote so we should become a follower at the new
                        // term.
                    }
                    typ => {
                        info!(
                            "{:#x} [term: {}] received a {:?} message with higher term from {:#x} [term: {}]",
                            self.id,
                            self.term,
                            typ,
                            m.from,
                            m.term,
                        );

                        if typ == MsgApp || typ == MsgHeartbeat || typ == MsgSnap {
                            self.become_follower(m.term, m.from);
                        } else {
                            self.become_follower(m.term, NONE);
                        }
                    }
                }
            }
            term if term < self.term => {
                if (self.check_quorum || self.pre_vote)
                    && (m.field_type == MsgHeartbeat || m.field_type == MsgApp)
                {
                    // We have received messages from a leader at a lower term. It is possible
                    // that these messages were simply delayed in the network, but this could
                    // also mean that this node has advanced its term number during a network
                    // partition, and it is now unable to either win an election or to rejoin
                    // the majority on the old term. If checkQuorum is false, this will be
                    // handled by incrementing term numbers in response to MsgVote with a
                    // higher term, but if checkQuorum is true we may not advance the term on
                    // MsgVote and must generate other messages to advance the term. The net
                    // result of these two features is to minimize the disruption caused by
                    // nodes that have been removed from the cluster's configuration: a
                    // removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
                    // but it will not receive MsgApp or MsgHeartbeat, so it will not create
                    // disruptive term increases, by notifying leader of this node's activeness.
                    // The above comments also true for Pre-Vote
                    //
                    // When follower gets isolated, it soon starts an election ending
                    // up with a higher term than leader, although it won't receive enough
                    // votes to win the election. When it regains connectivity, this response
                    // with "pb.MsgAppResp" of higher term would force leader to step down.
                    // However, this disruption is inevitable to free this stuck node with
                    // fresh election. This can be prevented with Pre-Vote phase.
                    let mut resp_m = Message::new();
                    resp_m.set_field_type(MsgAppResp);
                    resp_m.set_to(m.from);
                    self.send(resp_m);
                } else if m.get_field_type() == MsgPreVote {
                    // Before Pre-Vote enabled, there may have candidate with higher term,
                    // but less log. After update to Pre-Vote, the cluster may deadlock if
                    // we drop messages with a lower term.
                    info!(
                        "{:#x} [logterm: {}, index: {}, vote: {:#x}] rejected {:?} from {:#x} [logterm: {}, index: {}] at term {}",
                        self.id,
                        self.raft_log.last_term(),
                        self.raft_log.last_index(),
                        self.vote,
                        m.field_type,
                        m.from,
                        m.logTerm,
                        m.index,
                        self.term
                    );

                    let mut msg = Message::new();
                    msg.set_to(m.get_from());
                    msg.set_term(self.term);
                    msg.set_field_type(MsgPreVoteResp);
                    msg.set_reject(true);
                } else {
                    // ignore other cases.
                    info!(
                        "{:#x} [term: {}] ignored a {:?} message with lower term from {:#x} [term: {}]",
                        self.id,
                        self.term,
                        m.field_type,
                        m.from,
                        m.logTerm,
                    );
                }
                return Ok(());
            }
            _ => {}
        }

        match m.field_type {
            MsgHup => {
                if self.pre_vote {
                    self.hup(CampaignType::CampaignPreElection)
                } else {
                    self.hup(CampaignType::CampaignElection);
                }
            }
            MsgVote | MsgPreVote => {
                let can_vote = self.vote == m.get_from() // We can vote if this is a repeat of a vote we've already cast...
                    || (self.vote == NONE && self.lead == NONE) // ...we haven't voted and we don't think there's a leader yet in this term...
                    || (m.get_field_type() == MsgPreVote && m.get_term() > self.term);
                // ...or this is a PreVote for a further ter

                // ... and we believe the candidate is up to date.
                if can_vote && self.raft_log.is_up_to_date(m.get_index(), m.get_logTerm()) {
                    // Note: it turns out that that learners must be allowed to cast votes.
                    // This seems counter- intuitive but is necessary in the situation in which
                    // a learner has been promoted (i.e. is now a voter) but has not learned
                    // about this yet.
                    // For example, consider a group in which id=1 is a learner and id=2 and
                    // id=3 are voters. A configuration change promoting 1 can be committed on
                    // the quorum `{2,3}` without the config change being appended to the
                    // learner's log. If the leader (say 2) fails, there are de facto two
                    // voters remaining. Only 3 can win an election (due to its log containing
                    // all committed entries), but to do so it will need 1 to vote. But 1
                    // considers itself a learner and will continue to do so until 3 has
                    // stepped up as leader, replicates the conf change to 1, and 1 applies it.
                    // Ultimately, by receiving a request to vote, the learner realizes that
                    // the candidate believes it to be a voter, and that it should act
                    // accordingly. The candidate's config may be stale, too; but in that case
                    // it won't win the election, at least in the absence of the bug discussed
                    // in:
                    // https://github.com/etcd-io/etcd/issues/7625#issuecomment-488798263.
                    info!(
                        "{:#x} [logterm: {}, index: {}, vote: {:#x}] cast {:?} for {:#x} [logterm: {}, index: {}]  at term {}",
                        self.id,
                        self.raft_log.last_term(),
                        self.raft_log.last_index(),
                        self.vote,
                        m.get_field_type(),
                        m.get_from(),
                        m.get_logTerm(),
                        m.get_index(),
                        self.term
                    );
                    // When responding to Msg{Pre,}Vote messages we include the term
                    // from the message, not the local term. To see why, consider the
                    // case where a single node was previously partitioned away and
                    // it's local term is now out of date. If we include the local term
                    // (recall that for pre-votes we don't update the local term), the
                    // (pre-)campaigning node on the other end will proceed to ignore
                    // the message (it ignores all out of date messages).
                    // The term in the original message and current local term are the
                    // same in the case of regular votes, but different for pre-votes.
                    let mut msg = Message::new();
                    msg.set_to(m.get_from());
                    msg.set_term(m.get_term());
                    msg.set_field_type(vote_resp_msg_type(m.get_field_type()));
                    self.send(msg);
                    if m.get_field_type() == MsgVote {
                        // only record real votes.
                        self.election_elapsed = 0;
                        self.vote = m.get_from();
                    }
                } else {
                    info!(
                        "{:#x} [logterm: {}, index: {}, vote: {:#x}] rejected {:?} from {:#x} [logterm: {}, index: {}] at term {}",
                        self.id,
                        self.raft_log.last_term(),
                        self.raft_log.last_index(),
                        self.vote,
                        m.get_field_type(),
                        m.get_from(),
                        m.get_logTerm(),
                        m.get_index(),
                        m.get_term()
                    );
                    let mut msg = Message::new();
                    msg.set_to(m.get_from());
                    msg.set_term(self.term);
                    msg.set_field_type(vote_resp_msg_type(m.get_field_type()));
                    msg.set_reject(true);
                    self.send(msg);
                }
            }

            _ => {
                self.execute_step_fn(m)?;
            }
        }
        Ok(())
    }

    // handle_append_entries handle append entries RPC request
    fn handle_append_entries(&mut self, m: Message) {
        if m.get_index() < self.raft_log.committed {
            let mut msg = Message::default();
            msg.set_to(m.get_from());
            msg.set_field_type(MsgAppResp);
            msg.set_index(self.raft_log.committed);
            self.send(msg);
            return;
        }

        if let Some(m_last_index) = self.raft_log.maybe_append(
            m.get_index(),
            m.get_logTerm(),
            m.get_commit(),
            m.get_entries(),
        ) {
            let mut msg = Message::default();
            msg.set_to(m.get_from());
            msg.set_field_type(MsgAppResp);
            msg.set_index(m_last_index);
            self.send(msg);
        } else {
            debug!(
                "{:x} [logterm: {}, index: {}] rejected MsgApp [logterm: {}, index: {}] from {:#x}",
                self.id,
                self.raft_log.zero_term_on_err_compacted(m.get_index()),
                m.get_index(),
                m.get_logTerm(),
                m.get_index(),
                m.get_from(),
            );
            let mut msg = Message::new();
            msg.set_to(m.get_from());
            msg.set_field_type(MsgAppResp);
            msg.set_index(m.get_index());
            msg.set_reject(true);
            msg.set_rejectHint(self.raft_log.last_index());
            self.send(msg);
        }
    }

    // handles heartbeat RPC request, only invoked by Follower or Candidate
    fn handle_heartbeat(&mut self, m: Message) {
        self.raft_log.commit_to(m.get_commit());
        let mut msg = Message::new();
        msg.set_from(m.get_from());
        msg.set_field_type(MsgHeartbeatResp);
        msg.set_context(Bytes::from(m.get_context().to_vec()));
        self.send(msg);
    }

    // handles Snapshot RPC request, only invoked by Follower or Candidate
    fn handle_snapshot(&mut self, m: Message) {
        let (sindex, sterm) = (
            m.get_snapshot().get_metadata().get_index(),
            m.get_snapshot().get_metadata().get_term(),
        );
        if self.restore(m.get_snapshot()) {
            info!(
                "{:#x} [commit: {}] restored snapshot [index: {}, term: {}]",
                self.id, self.raft_log.committed, sindex, sterm
            );
            let mut msg = Message::new();
            msg.set_to(m.get_from());
            msg.set_field_type(MsgAppResp);
            msg.set_index(self.raft_log.last_index());
            self.send(msg);
        } else {
            info!(
                "{:#x} [commit: {}] ignored snapshot [index: {}, term: {}]",
                self.id, self.raft_log.committed, sindex, sterm,
            );
            let mut msg = Message::new();
            msg.set_to(m.get_from());
            msg.set_field_type(MsgAppResp);
            msg.set_index(self.raft_log.committed);
            self.send(msg);
        }
    }

    // recovers the state machine from a snapshot. It restores the log and the
    // configuration of state machine. If this method returns false, the snapshot was
    // ignored, either because it was obsolete or because of an error.
    pub(crate) fn restore(&mut self, s: &Snapshot) -> bool {
        if s.get_metadata().get_index() <= self.raft_log.committed {
            return false;
        }

        if self.state != StateType::Follower {
            // This is defense-in-depth: if the leader somehow ended up applying a
            // snapshot, it could move into a new term without moving into a
            // follower state. This should never fire, but if it did, we'd have
            // prevented damage by returning early, so log only a loud warning.
            //
            // At the time of writing, the instance is guaranteed to be in follower
            // state when this method is called.
            warn!(
                "{:x} attempted to restore snapshot as leader; should never happen",
                self.id
            );
            self.become_follower(self.term + 1, NONE);
            return false;
        }

        // More defense-in-depth: throw away snapshot if recipient is not in the
        // config. This shouldn't ever happen (at the time of writing) but lots of
        // code here and there assumes that r.id is in the progress tracker.
        let cs = s.get_metadata().get_conf_state();
        let found = vec![cs.voters.clone(), cs.learners.clone()]
            .iter()
            .flatten()
            .any(|id| *id == self.id);
        if !found {
            warn!(
                "{:#x} attempted to restore snapshot but it is not in the ConfState {:?}; should never happen",
                self.id, cs
            );
            return false;
        }

        // Now go ahead and actually restore.
        if self
            .raft_log
            .match_term(s.get_metadata().get_index(), s.get_metadata().get_term())
        {
            info!(
                "{:#x} [commit: {}, last_index: {}, last_term: {}] fast-forwarded commit to snapshot [index: {}, term: {}]",
                self.id,
                self.raft_log.committed,
                self.raft_log.last_index(),
                self.raft_log.last_term(),
                s.get_metadata().get_index(),
                s.get_metadata().get_term()
            );
            return false;
        }

        // TODO:option
        self.raft_log.restore(s.clone());

        // Reset the configuration and add the (potentially updated) peers in anew.
        self.prs = ProgressTracker::new(self.prs.max_inflight);
        let (cfg, prs) = restore(
            &mut Changer {
                tracker: self.prs.clone(),
                last_index: self.raft_log.last_index(),
            },
            cs,
        )
        // This should never happen. Either there's a bug in our config change
        // handling or the client corrupted the conf change.
        .map_err(|err| panic!("unable to restore config {:?}: {}", cs, err))
        .unwrap();
        equivalent(cs, &self.switch_to_config(cfg, prs)).unwrap();
        let pr = self.prs.progress.get_mut(&self.id).unwrap();
        pr.maybe_update(pr.next - 1); // TODO(tbg): this is untested and likely unneeded

        info!(
            "{:#x} [commit: {}, last_index: {}, last_term: {}] restored snapshot [index: {}, term: {}]",
            self.id,
            self.raft_log.committed,
            self.raft_log.last_index(),
            self.raft_log.last_term(),
            s.get_metadata().get_index(),
            s.get_metadata().get_term()
        );

        true
    }

    // promotable indicates whether state machine can be promoted to leader.
    // which is true when its own id is in progress list.
    fn promotable(&mut self) -> bool {
        if let Some(progress) = self.prs.progress.get(&self.id) {
            !progress.is_learner && !self.raft_log.has_pending_snapshot()
        } else {
            false
        }
    }

    pub(crate) fn apply_conf_change(&mut self, cc: &mut ConfChangeV2) -> ConfState {
        let mut res;
        let mut changer = Changer {
            tracker: self.prs.clone(),
            last_index: self.raft_log.last_index(),
        };

        // TODO: optimized
        if cc.leave_joint() {
            res = changer.leave_joint().unwrap();
        } else {
            let (auto_leave, ok) = cc.enter_joint();
            info!("joint consensus, auto_leave: {}, ok: {}", auto_leave, ok);
            if ok {
                res = changer.enter_joint(auto_leave, cc.mut_changes()).unwrap();
            } else {
                info!("simple change");
                res = changer.simple(cc.mut_changes()).unwrap();
            }
        }
        self.switch_to_config(res.0, res.1)
    }

    // switch_to_config reconfigures this node to use the provided configuration. It
    // updates the in-memory state and, when necessary, carries out additional
    // actions such as reacting to the removal of nodes or changed quorum
    // requirements.
    //
    // The inputs usually result from restoring a ConfState or apply a ConfState
    pub(crate) fn switch_to_config(&mut self, cfg: tracker::Config, prs: ProgressMap) -> ConfState {
        self.prs.config = cfg;
        self.prs.progress = prs;
        info!("{} switched to configuration {}", self.id, self.prs.config);
        let cs = self.prs.config_state();
        let pr = self.prs.progress.get(&self.id);
        // Update
        self.is_learner = pr.is_some() && pr.as_ref().unwrap().is_learner;
        if (pr.is_none() || self.is_learner) && self.state == StateType::Leader {
            // This node is leader and was removed or demoted. We prevent demotions
            // at the time writing but hypothetically we handle them the same way as
            // removing the leader; stepping down into the next Term.
            //
            // TODO(tbg): step down (for sanity) and ask follower with largest Match
            // to TimeoutNow (to advoid interruption). This might still drop some
            // proposals but it's bette than nothing.
            //
            // TODO(tbg): test this branch. It is untested at the time of writing.
            return cs;
        }

        // The remaining steps only make sense if this node is the leader and there
        // are other nodes.
        if self.state != StateType::Leader || cs.get_voters().is_empty() {
            return cs;
        }

        if self.maybe_commit() {
            // If the configuration change means that more entries are committed now,
            // broadcast/append to everyone in the updated config.
            self.bcast_append();
        } else {
            // Otherwise, still probe the newly added replicas; there's no reason to
            // let them wait out a heartbeat interval (or the next incoming
            // proposal).
            // TODO:
            //
            self.prs.visit_nodes().iter_mut().for_each(|id| {
                self.maybe_send_append(*id, false);
            })
            //            self.prs.visit(|id, pr| {
            //                self.maybe_send_append(id, false);
            //            });
        }
        // If the leaderTransferee was removed or demoted, abort the leadership transfer.
        let t_ok = self.prs.config.voters.ids().contains(&self.lead_transferee);
        if !t_ok && self.lead_transferee != 0 {
            self.abort_leader_transferee();
        }
        cs
    }

    pub(crate) fn load_state(&mut self, state: &HardState) {
        let commit = state.get_commit();
        if commit < self.raft_log.committed || commit > self.raft_log.last_index() {
            panic!(
                "{:#x} state.commit {} is out of range [{}, {}]",
                self.id,
                commit,
                self.raft_log.committed,
                self.raft_log.last_index()
            );
        }
        self.raft_log.committed = commit;
        self.term = state.get_term();
        self.vote = state.get_vote();
    }

    // returns the if the peer has committed an entry in its term.
    fn committed_entry_in_current_term(&self) -> bool {
        self.raft_log.term(self.raft_log.committed).map_or_else(
            |err| {
                if err == RaftLogError::FromStorage(StorageError::Compacted) {
                    0
                } else {
                    panic!("unexpected error {}", err)
                }
            },
            |term| term,
        ) == self.term
    }

    // constructs a response from `req`. If `req` comes from the peer
    // itself, a blank value will be returned.
    fn response_to_read_index_req(&mut self, req: &Message, read_idx: u64) -> Message {
        if req.get_from() == NONE || req.get_from() == self.id {
            self.read_states.push(ReadState {
                index: read_idx,
                request_ctx: req.entries[0].get_Data().to_vec(),
            });
            return Message::default();
        }

        let mut msg = Message::default();
        msg.set_field_type(MsgReadIndexResp);
        msg.set_to(req.get_from());
        msg.set_index(read_idx);
        msg.set_entries(RepeatedField::from_vec(req.get_entries().to_vec()));
        msg
    }

    // increaseUncommittedSize computes the size of the proposed entries and
    // determines whether they would push leader over its maxUncommittedSize limit.
    // If the new entries would exceed the limit, the method returns false. If not,
    // the increase in uncommitted entry size is recorded and the method returns
    // true.
    //
    // Empty payloads are never refused. This is used both for appending an empty
    // entry at a new leader's term, as well as leaving a joint configuration.
    fn increase_uncommitted_size(&mut self, ents: &[Entry]) -> bool {
        let s = ents
            .iter()
            .fold(0, |acc, entry| acc + entry.get_Data().len() as u64);
        if self.uncommitted_size > 0
            && s > 0
            && self.uncommitted_size + s > self.max_uncommitted_size
        {
            // If the uncommitted tail of the Raft log is empty, allow any size
            // proposal. Otherwise, limit the size of the uncommitted tail of the
            // log and drop any proposal that would push the size over the limit.
            // Note the added requirement s>0 which is used to make sure that
            // appending single empty entries to the log always succeeds, used both
            // for replicating a new leader's initial empty entry, and for
            // auto-leaving joint configurations.
            return false;
        }
        self.uncommitted_size += s;
        true
    }

    // accounts for the newly committed entries by decreasing
    // the uncommitted entry size limit.
    fn reduce_uncommitted_size(&mut self, ents: &[Entry]) {
        if self.uncommitted_size == 0 {
            // Fast-path for followers, who do not track or enforce the limit.
            return;
        }
        let s = ents
            .iter()
            .fold(0, |acc, entry| acc + entry.get_Data().len());
        if s as u64 > self.uncommitted_size {
            self.uncommitted_size = 0;
        } else {
            self.uncommitted_size -= s as u64;
        }
    }

    // returns true if r.election_elapsed is greater than
    // than or equal to the randomized election timeout in
    // [election_timeout, 2*election_timeout-1].
    fn past_election_timeout(&self) -> bool {
        self.election_elapsed >= self.randomized_election_timeout
    }

    fn reset_randomized_election_timeout(&mut self) {
        self.randomized_election_timeout =
            self.election_timeout + thread_rng().gen_range(0, self.election_timeout);
    }

    fn send_timeout_now(&mut self, to: u64) {
        let mut msg = Message::default();
        msg.set_to(to);
        msg.set_field_type(MsgTimeoutNow);
        self.send(msg);
    }

    pub fn abort_leader_transferee(&mut self) {
        self.lead_transferee = NONE;
    }

    fn execute_step_fn(&mut self, m: Message) -> Result<(), RaftError> {
        match self.state {
            StateType::PreCandidate | StateType::Candidate => self.step_candidate(m),
            StateType::Follower => self.step_follower(m),
            StateType::Leader => self.step_leader(m),
        }
    }

    fn step_leader(&mut self, mut m: Message) -> Result<(), RaftError> {
        // These message types do not require any progress for m.From
        match m.field_type {
            MsgBeat => {
                self.bcast_heartbeat();
                return Ok(());
            }
            MsgCheckQuorum => {
                // The leader should always see itself as active. As a precaution, handle
                // the case in which the leader isn't in the configuration any more (for
                // example if it just removed itself).
                //
                // TODO:(tbg): I added a TODO in removeNode, it doesn't seem that the
                // leader steps down when removing itself. I might be missing something.
                if let Some(pr) = self.prs.progress.get_mut(&self.id) {
                    pr.recent_active = true;
                }
                if !self.prs.quorum_active() {
                    warn!(
                        "{:#x} stepped down to follower since quorum is not active",
                        self.id
                    );
                    self.become_follower(self.term, NONE);
                }
                // Mark everyone (but ourselves) as inactive in preparation for the next
                // CheckQuorum.
                let cur_id = self.id;
                self.prs.visit(|id, pr| {
                    if id != cur_id {
                        pr.recent_active = false;
                    }
                });
                return Ok(());
            }
            MsgProp => {
                assert!(
                    !m.entries.is_empty(),
                    "{:#x} stepped empty MsgProp",
                    self.id
                );
                if !self.prs.progress.contains_key(&self.id) {
                    // If we are not currently a member of the range (i.e. this node
                    // was removed from the configuration while serving as leader).
                    // drop any new proposals.
                    return Err(RaftError::ProposalDropped);
                }
                if self.lead_transferee != NONE {
                    debug!(
                        "{:#x} [term {}] transfer leadership to {:#x} is in progress; dropping proposal",
                        self.id, self.term, self.lead_transferee
                    );
                    return Err(RaftError::ProposalDropped);
                }

                for (i, entry) in m.mut_entries().iter_mut().enumerate() {
                    if let Some(cc) = entry_to_conf_changei(entry) {
                        let already_pending = self.pending_config_index > self.raft_log.applied;
                        let already_joint = !self.prs.config.voters.outgoing.is_empty();
                        let v2 = cc.as_v2();
                        let wants_leave_joint = v2.changes.is_empty();

                        let mut refused = String::new();
                        if already_pending {
                            refused = format!(
                                "possible unapplied conf change at index {} (applied to {})",
                                self.pending_config_index, self.raft_log.applied
                            );
                        } else if already_joint && !wants_leave_joint {
                            refused = format!("must transition out of the joint config first");
                        } else if !already_joint && wants_leave_joint {
                            refused = format!("not in joint state; refusing empty conf change");
                        }

                        if !refused.is_empty() {
                            info!(
                                "{:#x} ignoring conf change {} at config {:?}: {}",
                                self.id, v2, self.prs.config, refused
                            );
                            *entry = Entry::new();
                            entry.set_Type(EntryNormal);
                        } else {
                            self.pending_config_index = self.raft_log.last_index() + i as u64 + 1;
                        }
                    }
                }
                if !self.append_entry(m.mut_entries()) {
                    return Err(RaftError::ProposalDropped);
                }

                self.bcast_append();
                return Ok(());
            }

            MsgReadIndex => {
                // only one voting member (the leader) in the cluster.
                if self.prs.is_singleton() {
                    let resp = self.response_to_read_index_req(&m, self.raft_log.committed);
                    if resp.get_to() != NONE {
                        self.send(resp);
                    }
                    return Ok(());
                }
                // Reject read only request when this leader has not committed any log entry at its term.
                if !self.committed_entry_in_current_term() {
                    return Ok(());
                }

                // thinking: use an interally defined context instead of the user given context.
                // We can express this in terms of the term and index instead of a user-supplied value.
                // This would allow multiple reads to piggyback on the same message.
                if self.read_only.option == ReadOnlySafe {
                    self.read_only
                        .add_request(self.raft_log.committed, m.clone());
                    // The local node automatically acks the request.
                    self.read_only.recv_ack(
                        self.id,
                        m.get_entries().first().unwrap().get_Data().to_vec(),
                    );
                    self.bcast_heartbeat_with_ctx(Some(
                        m.get_entries().first().unwrap().get_Data().to_vec(),
                    ));
                } else if self.read_only.option == ReadOnlyLeaseBased {
                    let resp = self.response_to_read_index_req(&m, self.raft_log.committed);
                    if resp.get_to() != NONE {
                        self.send(resp);
                    }
                }
                return Ok(());
            }
            _ => {}
        }

        // All other message types require a progress for m.From (pr).
        if !self.prs.progress.contains_key(&m.from) {
            info!("{:#x} no progress available for {:#x}", self.id, m.from);
            return Ok(());
        }
        match m.field_type {
            MsgAppResp => self.callback_leader_app_resp(m),
            MsgHeartbeatResp => self.callback_heartbeat_resp(m),
            MsgSnapStatus => self.callback_snapshot_status(m),
            MsgUnreachable => self.callback_unreachable(m),
            MsgTransferLeader => self.callback_transfer_leader(m),
            _ => Ok(()),
        }
    }

    // step_candidate is shared by `StateCandidate` and `StatePreCandidate`; the difference is
    // whether they respond to `MsgVoteResp` or `MsgPreVoteResp`.
    fn step_candidate(&mut self, m: Message) -> Result<(), RaftError> {
        let mut my_vote_resp_type = MessageType::MsgVoteResp;
        if self.state == StateType::PreCandidate {
            my_vote_resp_type = MessageType::MsgPreVoteResp;
        }

        let typ = m.get_field_type();
        match typ {
            MsgProp => {
                info!(
                    "{:#x} no leader at term {}, dropping proposal",
                    self.id, self.term
                );
                return Err(RaftError::ProposalDropped);
            }
            MsgApp => {
                self.become_follower(m.get_term(), m.get_from()); // always self.term == self.term
                self.handle_append_entries(m);
            }
            MsgHeartbeat => {
                self.become_follower(m.get_term(), m.get_from()); // always self.term == self.term
                self.handle_heartbeat(m);
            }
            MsgSnap => {
                self.become_follower(m.get_term(), m.get_from()); // always self.term == self.term
                self.handle_snapshot(m);
            }
            MsgPreVoteResp | MsgVoteResp => {
                let (gr, rj, res) = self.poll(m.get_from(), my_vote_resp_type, !m.get_reject());
                info!(
                    "{:#x} has received {} {:?} votes and {} vote rejections",
                    self.id, gr, typ, rj
                );
                if res == VoteWon {
                    if self.state == StateType::PreCandidate {
                        self.campaign(CampaignType::CampaignElection);
                    } else {
                        self.become_leader();
                        self.bcast_append();
                    }
                } else if res == VoteLost {
                    // pb.MsgPreVoteResp contains future term of pre-candidate
                    // m.term > self.term; reuse self.term.
                    self.become_follower(self.term, NONE);
                }
            }
            MsgTimeoutNow => {
                debug!(
                    "{:#x} [term: {} state: {}] ignored MsgTimeoutNow from {:#x}",
                    self.id,
                    self.term,
                    self.state,
                    m.get_from()
                );
            }
            _ => {}
        }
        Ok(())
    }

    fn step_follower(&mut self, m: Message) -> Result<(), RaftError> {
        let typ = m.get_field_type();
        match typ {
            MsgProp => {
                if self.lead == NONE {
                    info!(
                        "{:#x} no leader at term {}; dropping proposal",
                        self.id, self.term
                    );
                    return Err(RaftError::ProposalDropped);
                } else if self.disable_proposal_forwarding {
                    info!(
                        "{:#x} not forwarding to leader {:x} at term {}; dropping proposal",
                        self.id, self.lead, self.term
                    );
                    return Err(RaftError::ProposalDropped);
                }
                let mut msg = m.clone();
                msg.set_to(self.lead);
                self.send(msg);
            }
            MsgApp => {
                self.election_elapsed = 0;
                self.lead = m.get_from();
                self.handle_append_entries(m);
            }
            MsgHeartbeat => {
                self.election_elapsed = 0;
                self.lead = m.get_from();
                self.handle_heartbeat(m);
            }
            MsgSnap => {
                self.election_elapsed = 0;
                self.lead = m.get_from();
                self.handle_snapshot(m);
            }
            MsgTransferLeader => {
                if self.lead == NONE {
                    info!(
                        "{:#x} no leader at term {}; dropping leader transfer msg",
                        self.id, self.term
                    );
                    return Ok(());
                }
                let mut msg = m.clone();
                msg.set_to(self.lead);
                self.send(msg);
            }
            MsgTimeoutNow => {
                info!(
                    "{:#x} [term {}] received MsgTimeoutNow from {:#x} and starts an election to get leadership.",
                    self.id,
                    self.term,
                    m.get_from()
                );
                self.hup(CampaignType::CampaignTransfer);
            }
            MsgReadIndex => {
                if self.lead == NONE {
                    info!(
                        "{:#x} no leader at term {}; dropping index reading msg",
                        self.id, self.term
                    );
                    return Ok(());
                }
                let mut msg = m.clone();
                msg.set_to(self.lead);
                self.send(msg);
            }
            MsgReadIndexResp => {
                if m.get_entries().len() != 1 {
                    error!(
                        "{:#x} invalid format of MsgReadIndexResp from {:#x}, entries count: {}",
                        self.id,
                        m.get_from(),
                        m.get_entries().len()
                    );
                    return Ok(());
                }
                self.read_states.push(ReadState {
                    index: m.get_index(),
                    request_ctx: m.entries[0].get_Data().to_vec(),
                });
            }
            _ => {}
        }
        Ok(())
    }

    fn callback_leader_app_resp(&mut self, m: Message) -> Result<(), RaftError> {
        debug!("call back leader app resp, from: {:?}", m);
        let pr = self.prs.progress.must_get_mut(&m.get_from());
        pr.recent_active = true;
        if m.get_reject() {
            info!(
                "{:#x} received MsgAppResp(MsgApp was rejected, last_index: {:x}) from {:#x} for index: {}",
                self.id,
                m.get_rejectHint(),
                m.get_from(),
                m.get_index()
            );
            let pr = self.prs.progress.get_mut(&m.get_from()).unwrap();
            if pr.maybe_decr_to(m.get_index(), m.get_rejectHint()) {
                info!(
                    "{:#x} decreased progress of {:#x} to [{}]",
                    self.id,
                    m.get_from(),
                    pr
                );
                if pr.state == ProgressStateType::Replicate {
                    pr.become_probe();
                }
                self.send_append(m.get_from());
            }
            return Ok(());
        }

        let old_paused = pr.is_paused();
        if !pr.maybe_update(m.get_index()) {
            return Ok(());
        }
        match pr.state {
            ProgressStateType::Probe => {
                pr.become_replicate();
            }
            ProgressStateType::Snapshot if pr._match >= self.pending_config_index => {
                // TODO(tbg): we should also enter this branch if a snapshot is
                // received that is below pr.PendingSnapshot but which makes it
                // possible to use the log again.
                info!(
                    "{:#x} recovered from needing snapshot, resumed sending replication messages to {:#x} [{}]",
                    self.id,
                    m.get_from(),
                    pr
                );
                // Transition back to replicating state via probing state
                // (which takes the snapshot into account). If we didn't
                // move to replicating state, that would only happen with
                // the next round of appends (but there may not be a next
                // round for a while, exposing an inconsistent RaftStatus).
                pr.become_probe();
                pr.become_replicate();
            }
            ProgressStateType::Replicate => {
                debug!("free inflights, id: {:0x} {:?}", self.id, m);
                pr.inflights.free_le(m.index);
            }
            _ => {}
        }

        // advoid compile failed
        if self.maybe_commit() {
            self.bcast_append();
        } else if old_paused {
            // If we were paused before, this node may be missing the
            // latest commit index, so send it.
            self.send_append(m.from);
        }
        // We've updated flow control information above, which may
        // allow us to send multiple (size-limited) in-flight messages
        // at once (such as when transitioning from probe to
        // replicate, or when freeTo() covers multiple messages). If
        // we have more entries to send, send as many messages as we
        // can (without sending empty messages for the commit index)
        while self.maybe_send_append(m.from, false) {
            warn!("trigger empty message at {:0x}", m.from);
        }
        let _match = self.prs.progress.must_get(&m.from)._match;
        // Transfer leadership is in progress.
        if m.from == self.lead_transferee && _match == self.raft_log.last_index() {
            info!(
                "{:#x} sent MsgTimeoutNow to {:#x} after received MsgAppResp",
                self.id,
                m.get_from()
            );
            self.send_timeout_now(m.from);
        }
        Ok(())
    }

    fn callback_heartbeat_resp(&mut self, m: Message) -> Result<(), RaftError> {
        info!("heartbeat call back, from:{:0x}, to: {:0x}", m.from, m.to);
        let pr = self.prs.progress.must_get_mut(&m.from);
        pr.recent_active = true;
        pr.probe_sent = false;
        // free on slot for the full inflights windows to allow progress
        if pr.state == ProgressStateType::Replicate && pr.inflights.full() {
            pr.inflights.free_first_one();
        }
        // check from's progress match, and try to catch up `Index`
        if pr._match < self.raft_log.last_index() {
            self.send_append(m.from);
        }
        if self.read_only.option != ReadOnlySafe || m.get_context().is_empty() {
            return Ok(());
        }
        if self.prs.config.voters.vote_result(
            self.read_only
                .recv_ack(m.from, m.get_context().to_vec())
                .unwrap(),
        ) != VoteWon
        {
            return Ok(());
        }

        self.read_only.advance(m).iter_mut().for_each(|rs| {
            let resp = self.response_to_read_index_req(&rs.req, rs.index);
            if resp.get_to() != NONE {
                self.send(resp);
            }
        });
        Ok(())
    }

    fn callback_unreachable(&mut self, m: Message) -> Result<(), RaftError> {
        // During optimistic replication, if the remote become unreachable,
        // there is huge probability that a MsgApp is lost.
        let pr = self.prs.progress.get_mut(&m.get_from()).unwrap();
        if pr.state == ProgressStateType::Replicate {
            pr.become_probe();
        }
        info!(
            "{:#x} failed to send message to {:#x} because it is unreachable [{}]",
            self.id,
            m.get_from(),
            pr
        );
        Ok(())
    }

    fn callback_snapshot_status(&mut self, m: Message) -> Result<(), RaftError> {
        let pr = self.prs.progress.must_get_mut(&m.get_from());
        if pr.state != ProgressStateType::Snapshot {
            return Ok(());
        }
        // TODO(tbg): this code is very similar to the snapshot handling in
        // MsgAppResp above. In fact, the code there is more correct than the
        // code here and should likely be updated to match (or even better, the
        // logic pulled into a newly created Progress state machine handler).
        if !m.reject {
            pr.become_probe();
            info!(
                "{:#x} snapshot succeeded, resumed sending replication messages to {:#x} [{}]",
                self.id,
                m.get_from(),
                pr
            );
        } else {
            // NB: the order here matters or we'll be probing erroneously from
            // the snapshot index, but the snapshot never applied.
            pr.pending_snapshot = 0;
            pr.become_probe();
            debug!(
                "{:0x} snapshot failed, resumed sending replication message to {:0x} [{:?}]",
                self.id, m.from, pr
            );
        }

        // If snapshot finished, wait for the MsgAppResp from the remote node before sending
        // out the next MsgApp.
        // If snapshot failure, wait for a heartbeat interval before next try
        pr.probe_sent = true;
        Ok(())
    }

    fn callback_transfer_leader(&mut self, m: Message) -> Result<(), RaftError> {
        let pr = self.prs.progress.get_mut(&m.get_from()).unwrap();
        if pr.is_learner {
            debug!("{:#x} is learner. Ignored transferring leadership", self.id);
            return Ok(());
        }
        let lead_transferee = m.get_from();
        let last_lead_transferee = self.lead_transferee;
        if last_lead_transferee != NONE {
            if last_lead_transferee == lead_transferee {
                info!(
                    "{:#x} [term {}] transfer leadership to {:#x} is in progress, ignores request to same node {:#x}",
                    self.id, self.term, lead_transferee, lead_transferee
                );
                return Ok(());
            }
            self.abort_leader_transferee();
            info!(
                "{:x} [term {}] abort previous transferring leadership to {:#x}",
                self.id, self.term, last_lead_transferee
            );
        }
        if lead_transferee == self.id {
            info!(
                "{:x} is already leader. Ignored transferring leadership to self",
                self.id
            );
            return Ok(());
        }
        // Transfer leadership to third party.
        info!(
            "{:#x} [term {}] starts to transfer leadership to {:#x}",
            self.id, self.term, lead_transferee
        );
        // Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
        self.election_elapsed = 0;
        self.lead_transferee = lead_transferee;
        let _match = self.prs.progress.get(&m.get_from()).unwrap()._match;
        if _match == self.raft_log.last_index() {
            self.send_timeout_now(lead_transferee);
            info!(
                "{:#x} sends MsgTimeoutNow to {:#x} immediately as {:#x} already has up-to-date log",
                self.id, lead_transferee, lead_transferee,
            );
        } else {
            self.send_append(lead_transferee);
        }

        Ok(())
    }

    fn num_of_pending_conf(ents: &[Entry]) -> usize {
        ents.iter().fold(0, |acc, entry| {
            if entry.get_Type() == EntryConfChange || entry.get_Type() == EntryConfChangeV2 {
                acc + 1
            } else {
                acc
            }
        })
    }
}
