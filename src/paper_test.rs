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

#[cfg(test)]
mod tests {
    use crate::raft::{StateType, Raft};
    use crate::mock::new_test_inner_node;
    use crate::storage::SafeMemStorage;
    use crate::raftpb::raft::{Message, HardState};
    use crate::raftpb::raft::MessageType::MsgApp;
    use serde::de::Error;

    #[test]
    fn follower_update_term_from_message() {
        flexi_logger::Logger::with_env().start();
        test_update_term_from_message(StateType::Follower);
    }

    #[test]
    fn candidate_update_term_from_message() {
        flexi_logger::Logger::with_env().start();
        test_update_term_from_message(StateType::Candidate);
    }

    #[test]
    fn leader_update_term_from_message() {
        flexi_logger::Logger::with_env().start();
        test_update_term_from_message(StateType::Leader);
    }

    // tests that if one server's current term is
    // smaller than the other's, then it updates its current term to the larger
    // value. If a candidate or leader discovers that its term is out of date,
    // it immediately reverts to follower state.
    // References: section 5.1
    fn test_update_term_from_message(state: StateType) {
        let mut raft = new_test_inner_node(0x1, vec![0x1, 0x2, 0x3], 10, 1, SafeMemStorage::new());
        match state {
            StateType::Follower => raft.become_follower(1, 0x2),
            StateType::Candidate => raft.become_candidate(),
            StateType::Leader => {
                raft.become_candidate();
                raft.become_leader();
            }
            _ => {}
        }

        raft.step(Message { field_type: MsgApp, term: 2, ..Default::default() });

        assert_eq!(raft.term, 2, "term = {}, want = {}", raft.term, 2);
        assert_eq!(raft.state, StateType::Follower, "state = {}, want = {}", raft.state, StateType::Follower);
    }

    // if a server receives a request with
    // a stale term number, it rejects the request.
    // Our implementation ignores the request instead.
    // Reference: section 5.1
    #[test]
    fn reject_stale_term_message() {
        let mut called = false;
        let mut fake_step = |raft: &Raft<SafeMemStorage>, m: Message| -> Result<(), String> {
            called = true;
            Ok(())
        };

        let mut raft = new_test_inner_node(0x1, vec![0x1, 0x2, 0x3], 10, 1, SafeMemStorage::new());
        raft.load_state(&HardState { term: 2, ..Default::default() });
        raft.step(Message { field_type: MsgApp, term: raft.term - 1, ..Default::default() });

        assert!(!called, "step_func called = {}, want = {}", called, false);
    }
}
