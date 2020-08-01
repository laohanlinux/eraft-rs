// Copyright 2019 The etcd Authors
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

use crate::quorum::majority::MajorityConfig;
use crate::quorum::quorum::VoteResult::{VoteLost, VotePending};
use crate::quorum::quorum::{AckedIndexer, Index, VoteResult};
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display, Error, Formatter};
use std::process::id;

/// JointConfig is a configuration of two groups of (possibly overlapping)
/// majority configurations. Decisions require the support of both majorities.
/// Here Thanks tikv
#[derive(Clone, PartialEq, Debug)]
pub struct JointConfig {
    pub(crate) incoming: MajorityConfig,
    pub(crate) outgoing: MajorityConfig,
}

impl JointConfig {
    pub fn new() -> Self {
        JointConfig {
            incoming: MajorityConfig::new(),
            outgoing: MajorityConfig::new(),
        }
    }
    pub fn new2(incoming: MajorityConfig, outgoing: MajorityConfig) -> Self {
        JointConfig { incoming, outgoing }
    }
}

impl Default for JointConfig {
    fn default() -> Self {
        JointConfig::new()
    }
}

impl Display for JointConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if !self.outgoing.is_empty() {
            write!(f, "{}&&{}", self.incoming, self.outgoing)
        } else {
            write!(f, "{}", self.incoming)
        }
    }
}

impl JointConfig {
    /// IDs returns a newly initialized map representing the set of voters present
    /// in the joint configuration.
    pub fn ids(&self) -> HashSet<u64> {
        let mut hash_set = HashSet::new();
        hash_set.extend(self.incoming.iter());
        hash_set.extend(self.outgoing.iter());
        hash_set
    }

    /// TODO
    /// Describe returns a (multi-line) representation of the commit indexes for the
    /// given lookuper.
    pub fn describe<T: AckedIndexer>(&self, l: &T) -> String {
        MajorityConfig::from(self.ids()).describe(l)
    }

    /// committed_index returns the largest committed index for the given joint
    /// quorum. An index is jointly committed if it is committed in both constituent
    /// majorities
    pub fn committed<T: AckedIndexer>(&self, l: &T) -> Index {
        let idx0 = self.incoming.committed_index(l);
        let idx1 = self.outgoing.committed_index(l);
        if idx0 < idx1 {
            return idx0;
        }
        idx1
    }

    pub fn vote_result(&self, votes: &HashMap<u64, bool>) -> VoteResult {
        let r1 = self.incoming.vote_result(votes);
        let r2 = self.outgoing.vote_result(votes);
        if r1 == r2 {
            return r1;
        }
        if r1 == VoteLost || r2 == VoteLost {
            // If either config has lost, loss is the only possible outcome.
            return VoteLost;
        }
        // TODO: Why?
        // One side won, the other one is pending, so the whole outcome is
        VotePending
    }

    /// clears all IDs.
    pub fn clear(&mut self) {
        self.incoming.clear();
        self.outgoing.clear();
    }

    /// Returns true if (and only if) there is only one voting member
    /// (i.e. the leader) in the current configuration.
    #[inline]
    pub fn is_singleton(&self) -> bool {
        self.outgoing.is_empty() && self.incoming.len() == 1
    }

    /// Check if an id is a voter.
    #[inline]
    pub fn contains(&self, id: u64) -> bool {
        self.incoming.contains(&id) || self.outgoing.contains(&id)
    }

    #[inline]
    pub fn joint(&self) -> bool {
        !self.outgoing.is_empty()
    }
}
