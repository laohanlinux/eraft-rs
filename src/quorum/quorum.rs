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

use std::collections::HashMap;

// Index is a Raft log position
pub type Index = u64;

pub fn to_string(index: Index) -> String {
    if index == u64::MAX {
        "∞".to_string()
    } else {
        index.to_string()
    }
}

/// AckedIndexer allows looking up a commit index for a given ID of a voter
/// from a correcsponding MajorityConfig.
pub trait AckedIndexer {
    fn acked_index(&self, voter_id: &u64) -> Option<&Index>;
}

pub(crate) type MapAckIndexer = HashMap<u64, Index>;

impl AckedIndexer for MapAckIndexer {
    fn acked_index(&self, voter_id: &u64) -> Option<&Index> {
        self.get(voter_id)
    }
}


/// VoteResult indicates the outcome of a vote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VoteResult {
    /// VotePending indicates that the decision of the vote depends on future
    /// votes, i.e. neither "yes" or "no" has reached quorum yet.
    VotePending,
    /// VoteLost indicates that the quorum has votes "no"
    VoteLost,
    /// VoteWon indicates that the quorum has voted "yes"
    VoteWon,
}