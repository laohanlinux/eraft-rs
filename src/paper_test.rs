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
    use crate::mock::{
        init_console_log, new_empty_entry_set, new_entry_set, new_test_inner_node, read_message,
    };
    use crate::raft::{Raft, StateType};
    use crate::raftpb::raft::MessageType::{MsgApp, MsgHeartbeat, MsgVote};
    use crate::raftpb::raft::{Entry, HardState, Message};
    use crate::storage::{SafeMemStorage, Storage};
    use serde::de::Error;

    #[test]
    fn follower_update_term_from_message() {
        init_console_log();
        test_update_term_from_message(StateType::Follower);
    }

    #[test]
    fn candidate_update_term_from_message() {
        init_console_log();
        test_update_term_from_message(StateType::Candidate);
    }

    #[test]
    fn leader_update_term_from_message() {
        init_console_log();
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

        raft.step(Message {
            field_type: MsgApp,
            term: 2,
            ..Default::default()
        });

        assert_eq!(raft.term, 2, "term = {}, want = {}", raft.term, 2);
        assert_eq!(
            raft.state,
            StateType::Follower,
            "state = {}, want = {}",
            raft.state,
            StateType::Follower
        );
    }

    // TODO: FIXME: i don't found more than implementation
    // if a server receives a request with
    // a stale term number, it rejects the request.
    // Our implementation ignores the request instead.
    // Reference: section 5.1
    #[test]
    fn reject_stale_term_message() {
        init_console_log();
        let mut called = false;
        let mut fake_step = |raft: &Raft<SafeMemStorage>, m: Message| -> Result<(), String> {
            called = true;
            Ok(())
        };

        let mut raft = new_test_inner_node(0x1, vec![0x1, 0x2, 0x3], 10, 1, SafeMemStorage::new());
        raft.load_state(&HardState {
            term: 2,
            ..Default::default()
        });
        let res = raft.step(Message {
            field_type: MsgApp,
            term: raft.term - 1,
            ..Default::default()
        });
        info!("{:?}", res);
        assert!(!called, "step_func called = {}, want = {}", called, false);
    }

    // When server starts up, they begin as followers.
    // Reference: 5.2
    #[test]
    fn start_as_followers() {
        init_console_log();
        let mut raft = new_test_inner_node(0x1, vec![0x1, 0x2, 0x3], 10, 1, SafeMemStorage::new());
        assert_eq!(
            raft.state,
            StateType::Follower,
            "state={}, want={}",
            raft.state,
            StateType::Follower
        );
    }

    // If the leader receives a heartbeat tick,
    // It will send a `MsgHeartbeat` with m.index = 0, `m.log_term = 0` and empty entries
    // as heartbeat to all followers.
    // Reference: 5.2
    #[test]
    fn leader_bcast_beat() {
        init_console_log();
        // heartbeat interval
        let hi = 1;
        let mut raft = new_test_inner_node(0x1, vec![0x1, 0x2, 0x3], 10, hi, SafeMemStorage::new());
        raft.become_candidate();
        raft.become_leader();

        for i in 0..10 {
            let mut entry = new_entry_set(vec![(i + 1, 0)]);
            must_append_entry(&mut raft, entry);
        }

        for i in 0..hi {
            raft.tick_heartbeat();
        }

        let mut msgs = read_message(&mut raft);
        msgs.sort_by(|m1, m2| format!("{:?}", m1).cmp(&format!("{:?}", m2)));

        let w_msgs = vec![
            Message {
                from: 1,
                to: 2,
                term: 1,
                field_type: MsgHeartbeat,
                ..Default::default()
            },
            Message {
                from: 1,
                to: 3,
                term: 1,
                field_type: MsgHeartbeat,
                ..Default::default()
            },
        ];
        assert_eq!(msgs, w_msgs, "msgs={:?}, want={:?}", msgs, w_msgs);
    }

    #[test]
    fn follower_start_election() {
        init_console_log();
        test_non_leader_start_election(StateType::Follower);
    }

    #[test]
    fn candidate_start_new_election() {
        init_console_log();
        test_non_leader_start_election(StateType::Candidate);
    }

    fn must_append_entry<S: Storage>(raft: &mut Raft<S>, mut ents: Vec<Entry>) {
        let ok = raft.append_entry(&mut ents);
        assert!(ok, "entry unexpectedly dropped");
    }

    // if a follower receives no communication
    // over election timeout, it begins an election to choose a new leader. It
    // increments its current term and transitions to candidate state. It then
    // votes for itself and issues RequestVote RPCs in parallel to each of the
    // other servers in the cluster.
    // Reference: section 5.2
    // Also if a candidate fails to obtain a majority, it will time out and
    // start a new election by incrementing its term and initiating another
    // round of RequestVote RPCs.
    // Reference: section 5.2
    fn test_non_leader_start_election(state: StateType) {
        // election timeout
        let election_timeout = 10;
        let mut raft = new_test_inner_node(
            0x1,
            vec![0x1, 0x2, 0x3],
            election_timeout,
            1,
            SafeMemStorage::new(),
        );
        match state {
            StateType::Follower => raft.become_follower(1, 0x2),
            StateType::Candidate => raft.become_candidate(),
            _ => {}
        }
        for i in 0..2 * election_timeout {
            raft.tick_election();
        }

        assert_eq!(raft.term, 2, "term = {}, want = {}", raft.term, 2);
        assert_eq!(
            raft.state,
            StateType::Candidate,
            "state = {}, want = {}",
            raft.state,
            StateType::Candidate
        );
        let vote = raft.prs.votes.get(&raft.id).unwrap();
        assert!(*vote, "vote for self = false, want true");

        let mut msgs = read_message(&mut raft);
        msgs.sort_by(|m1, m2| format!("{:?}", m1).cmp(&format!("{:?}", m2)));
        let w_msgs = vec![
            Message {
                from: 1,
                to: 2,
                term: 2,
                field_type: MsgVote,
                ..Default::default()
            },
            Message {
                from: 1,
                to: 3,
                term: 2,
                field_type: MsgVote,
                ..Default::default()
            },
        ];
        assert_eq!(msgs, w_msgs, "msgs = {:?}, want = {:?}", msgs, w_msgs);
    }
}
