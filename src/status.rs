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

use crate::node::SoftState;
use crate::raft::{Raft, StateType};
use crate::raftpb::raft::HardState;
use crate::storage::Storage;
use crate::tracker::progress::ProgressMap;
use crate::tracker::Config;
use std::fmt::{Display, Formatter};

/// Contains information about this Raft peer and its view of the system.
/// The Progress is only populated on the leader.
#[derive(Clone, Debug)]
pub struct Status {
    pub(crate) base_status: BaseStatus,
    pub config: Config,
    pub progress: ProgressMap,
}

impl Display for Status {
    fn fmt(&self, f: &mut Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// Contains basic information about the Raft peer. It does not allocate
#[derive(Clone, Debug)]
pub struct BaseStatus {
    id: u64,
    hard_state: HardState,
    soft_state: SoftState,
    applied: u64,
    lead_transferee: u64,
}

impl<S: Storage> From<&Raft<S>> for BaseStatus {
    fn from(raft: &Raft<S>) -> Self {
        BaseStatus {
            id: raft.id,
            hard_state: raft.hard_state(),
            soft_state: raft.soft_state(),
            applied: raft.raft_log.applied,
            lead_transferee: raft.lead_transferee,
        }
    }
}

impl<S: Storage> From<&Raft<S>> for Status {
    fn from(raft: &Raft<S>) -> Self {
        let mut s = Status {
            base_status: BaseStatus::from(raft),
            config: Default::default(),
            progress: Default::default(),
        };
        if s.base_status.soft_state.raft_state == StateType::Leader {
            s.progress = raft.prs.progress.clone();
        }
        s.config = raft.prs.config.clone();
        s
    }
}
