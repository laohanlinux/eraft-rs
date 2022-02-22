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
    use crate::tests_util::mock::{self, new_empty_entry_set, new_entry, new_entry_set, new_test_inner_node, read_message, MockEntry, MocksEnts, new_entry_set2};
    use crate::raft::{Raft, StateType, NONE};
    use crate::raftpb::raft::MessageType::{
        MsgApp, MsgAppResp, MsgHeartbeat, MsgHup, MsgProp, MsgVote, MsgVoteResp,
    };
    use bytes::Bytes;
    use maplit::hashmap;
    use nom::lib::std::collections::HashMap;

    use crate::raftpb::raft::{Entry, HardState, Message};
    use crate::storage::{SafeMemStorage, Storage};
    use protobuf::RepeatedField;
    use std::ptr::read;
    use crate::tests_util::try_init_log;

    #[test]
    fn follower_update_term_from_message() {
        try_init_log();
        test_update_term_from_message(StateType::Follower);
    }

    #[test]
    fn candidate_update_term_from_message() {
        try_init_log();
        test_update_term_from_message(StateType::Candidate);
    }

    #[test]
    fn leader_update_term_from_message() {
        try_init_log();
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
        try_init_log();
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
        try_init_log();
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
        try_init_log();
        // heartbeat interval
        let hi = 1;
        let mut raft = new_test_inner_node(0x1, vec![0x1, 0x2, 0x3], 10, hi, SafeMemStorage::new());
        raft.become_candidate();
        raft.become_leader();

        for i in 0..10 {
            let entry = new_entry_set(vec![(i + 1, 0)]);
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
        try_init_log();
        test_non_leader_start_election(StateType::Follower);
    }

    #[test]
    fn candidate_start_new_election() {
        try_init_log();
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
        for _ in 0..2 * election_timeout {
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

    // leader election during one round of `RequestVote` RPC:
    // a) it wins the election
    // b) it loses the election
    // c) it is unclear about the result
    // Reference: section 5.2
    #[test]
    fn leader_election_in_one_round_rpc() {
        try_init_log();
        let tests = vec![
            (1, hashmap! {}, StateType::Leader),
            (3, hashmap! {2 => true, 3 => true}, StateType::Leader),
            (3, hashmap! {2 => true}, StateType::Leader),
            (
                5,
                hashmap! {2 => true, 3 => true, 4 => true, 5 => true},
                StateType::Leader,
            ),
            (
                5,
                hashmap! {2 => true, 3 => true, 4 => true},
                StateType::Leader,
            ),
            (5, hashmap! {2 => true, 3 => true}, StateType::Leader),
            // return to follower state if it receives vote denial from a majority
            (3, hashmap! {2 => false, 3 => false}, StateType::Follower),
            (
                5,
                hashmap! {2 => false, 3 => false, 4 => false, 5 => false},
                StateType::Follower,
            ),
            (
                5,
                hashmap! {2 => true, 3 => false, 4 => false, 5 => false},
                StateType::Follower,
            ),
            // stay in candidate if it does not obtain the majority
            (3, hashmap! {}, StateType::Candidate),
            (5, hashmap! {2 => true}, StateType::Candidate),
            (5, hashmap! {2 => false, 3 => false}, StateType::Candidate),
            (5, hashmap! {}, StateType::Candidate),
        ];

        for (i, (size, votes, state)) in tests.iter().enumerate() {
            let mut raft =
                new_test_inner_node(0x1, mock::ids_by_size(*size), 10, 1, SafeMemStorage::new());
            raft.step(Message {
                from: 0x1,
                to: 0x1,
                field_type: MsgHup,
                ..Default::default()
            });

            for (id, vote) in votes {
                raft.step(Message {
                    from: *id,
                    to: 0x1,
                    field_type: MsgVoteResp,
                    reject: !*vote,
                    ..Default::default()
                });
            }
            assert_eq!(
                raft.state, *state,
                "#{}, state = {}, want {:?}",
                i, raft.state, *state
            );
            assert_eq!(raft.term, 1, "#{}, term = {}, want {}", i, raft.term, 1);
        }
    }

    // each follower will vote for at most one
    // candidate in a given term, on a first-come-first-served basis.
    // Reference: section 5.2
    #[test]
    fn follower_vote() {
        try_init_log();
        let tests = vec![
            (NONE, 1, false),
            (NONE, 1, false),
            (1, 1, false),
            (2, 2, false),
            (1, 2, true),
            (2, 1, true),
        ];
        for (i, (vote, n_vote, w_reject)) in tests.iter().enumerate() {
            let mut raft =
                new_test_inner_node(0x1, vec![0x1, 0x2, 0x3], 10, 1, SafeMemStorage::new());
            raft.load_state(&HardState {
                term: 1,
                vote: *vote,
                ..Default::default()
            });
            raft.step(Message {
                from: *n_vote,
                to: 0x1,
                term: 1,
                field_type: MsgVote,
                ..Default::default()
            });

            let msgs = read_message(&mut raft);
            let w_msgs = vec![Message {
                from: 0x1,
                to: *n_vote,
                term: 1,
                field_type: MsgVoteResp,
                reject: *w_reject,
                ..Default::default()
            }];
            assert_eq!(msgs, w_msgs, "#{}: msgs = {:?}, want {:?}", i, msgs, w_msgs);
        }
    }

    // tests that while waiting for votes,
    // if a candidate receives an AppendEntries RPC from another server claiming
    // to be leader whose term is at least as large as the candidate's current term,
    // it recognizes the leader as legitimate and returns to follower state.
    // Reference: section 5.2
    #[test]
    fn candidate_fallback() {
        let tests = vec![
            Message {
                from: 0x2,
                to: 0x1,
                term: 1,
                field_type: MsgApp,
                ..Default::default()
            },
            Message {
                from: 0x2,
                to: 0x1,
                term: 2,
                field_type: MsgApp,
                ..Default::default()
            },
        ];

        for (i, msg) in tests.iter().enumerate() {
            let mut raft =
                new_test_inner_node(0x1, vec![0x1, 0x2, 0x3], 10, 1, SafeMemStorage::new());
            raft.step(Message {
                from: 0x1,
                to: 0x1,
                field_type: MsgHup,
                ..Default::default()
            });
            assert_eq!(
                raft.state,
                StateType::Candidate,
                "unexpected state = {}, want {}",
                raft.state,
                StateType::Candidate
            );
            raft.step(msg.clone());
            assert_eq!(
                raft.state,
                StateType::Follower,
                "#{}: state = {}, want {}",
                i,
                raft.state,
                StateType::Follower
            );
            assert_eq!(
                raft.term, msg.term,
                "#{}: term = {}, want {}",
                i, raft.term, msg.term
            );
        }
    }

    #[test]
    fn follower_election_timeout_randomized() {
        try_init_log();
        test_non_leader_election_timeout_randomized(StateType::Follower);
    }

    #[test]
    fn candidate_election_timeout_randomized() {
        try_init_log();
        test_non_leader_election_timeout_randomized(StateType::Candidate);
    }

    // test that election timeout for follower or candidate is randomized.
    // Reference: section 5.2
    fn test_non_leader_election_timeout_randomized(state: StateType) {
        let et = 10;
        let mut raft = new_test_inner_node(0x1, vec![0x1, 0x2, 0x3], 10, 1, SafeMemStorage::new());
        let mut timeouts: HashMap<i32, bool> = hashmap! {};

        for round in 0..50 * et {
            match state {
                StateType::Follower => raft.become_follower(raft.term + 1, 0x2),
                StateType::Candidate => raft.become_candidate(),
                _ => {}
            }

            let mut time = 0;
            while read_message(&mut raft).is_empty() {
                raft.tick_election();
                time += 1;
            }
            timeouts.insert(time, true);
        }

        // Note: election time range
        for d in et + 1..2 * et {
            assert!(
                timeouts.contains_key(&d),
                "timeout in {} ticks should happen"
            );
        }
    }

    #[test]
    fn follower_election_timeout_non_conflict() {
        try_init_log();
        test_non_leader_election_timeout_non_conflict(StateType::Follower);
    }

    #[test]
    fn candidate_election_timeout_non_conflict() {
        try_init_log();
        test_non_leader_election_timeout_non_conflict(StateType::Candidate);
    }

    // tests that when receiving client proposals,
    // the leader appends the proposal to its log as a new entry, then issues
    // AppendEntries RPCs in parallel to each of the other servers to replicate
    // the entry. Also, when sending an AppendEntries RPC, the leader includes
    // the index and term of the entry in its log that immediately precedes
    // the new entries.
    // Also, it writes the new entry into stable storage.
    // Reference: section 5.3
    #[test]
    fn leader_start_replication() {
        try_init_log();
        let mut raft = new_test_inner_node(0x1, vec![0x1, 0x2, 0x3], 10, 1, SafeMemStorage::new());
        raft.become_candidate();
        raft.become_leader();
        commit_noop_entry(&mut raft);

        let li = raft.raft_log.last_index();
        let ents = mock::MocksEnts::from("some data").into();
        raft.step(Message {
            from: 0x1,
            to: 0x1,
            field_type: MsgProp,
            entries: ents,
            ..Default::default()
        })
            .unwrap();

        let g = raft.raft_log.last_index();
        assert_eq!(g, li + 1, "last_index={}, want={}", g, li + 1);

        let g = raft.raft_log.committed;
        assert_eq!(g, li, "committed={}, want={}", g, li);

        let mut msgs = read_message(&mut raft);
        msgs.sort_by_key(|key| format!("{:?}", key));
        let wents: Vec<Entry> = vec![Entry {
            Index: li + 1,
            Term: 1,
            Data: Bytes::from("some data"),
            ..Default::default()
        }];
        let w_msgs = vec![
            Message {
                from: 0x1,
                to: 0x2,
                term: 1,
                field_type: MsgApp,
                index: li,
                logTerm: 1,
                commit: li,
                entries: RepeatedField::from_vec(wents.clone()),
                ..Default::default()
            },
            Message {
                from: 0x1,
                to: 0x3,
                term: 1,
                field_type: MsgApp,
                index: li,
                logTerm: 1,
                commit: 1,
                entries: RepeatedField::from_vec(wents.clone()),
                ..Default::default()
            },
        ];
        assert_eq!(msgs, w_msgs, "msgs = {:?}, want = {:?}", msgs, w_msgs);
        let g = raft
            .raft_log
            .unstable_entries()
            .iter()
            .map(|entry| entry.clone())
            .collect::<Vec<_>>();
        assert_eq!(g, wents, "ents = {:?}, want = {:?}", g, wents);
    }

    // tests that when the entry has been safely replicated,
    // the leader gives out the applied entries, which can be applied to its state
    // machine.
    // Also, the leader keeps track of the highest index it knows to be committed,
    // and it includes that index in future AppendEntries RPCs so that the other
    // servers eventually find out.
    // Reference: section 5.3
    #[test]
    fn leader_commit_entry() {
        try_init_log();
        let mut raft = new_test_inner_node(0x1, vec![0x1, 0x2, 0x3], 10, 1, SafeMemStorage::new());
        raft.become_candidate();
        raft.become_leader();
        commit_noop_entry(&mut raft);
        let li = raft.raft_log.last_index();
        let ents = mock::MocksEnts::from("some data").into();

        // It just committed
        raft.step(Message {
            from: 0x1,
            to: 0x1,
            field_type: MsgProp,
            entries: ents,
            ..Default::default()
        });
        for m in read_message(&mut raft) {
            raft.step(accept_and_reply(m));
        }
        let g = raft.raft_log.committed;
        assert_eq!(g, li + 1, "committed = {}, want {}", g, li);
        let mut w_ents = vec![new_entry(li + 1, 1)];
        w_ents[0].set_Data(Bytes::from("some data"));
        let g = raft.raft_log.next_ents();
        assert_eq!(g, w_ents, "next_ents = {:?}, want {:?}", g, w_ents);

        let mut msgs = read_message(&mut raft);
        msgs.sort_by_key(|key| format!("{:?}", key));
        for (i, m) in msgs.iter().enumerate() {
            let w = i + 2;
            assert_eq!(m.to, w as u64, "to = {}, want {}", m.to, w);
            assert_eq!(
                m.field_type, MsgApp,
                "type = {:?}, want = {:?}",
                m.field_type, MsgProp
            );
            assert_eq!(m.commit, li + 1, "commit = {}, want = {}", m.commit, li + 1);
        }
    }

    // Test that a log entry is committed once the
    // leader that created the entry has replicated it on a majority of the servers.
    // Reference: section 5.3
    #[test]
    fn leader_acknowledge_commit() {
        let tests = vec![
            (1, hashmap! {}, true),
            (3, hashmap! {}, false),
            (3, hashmap! {2 => true}, true),
            (3, hashmap! {2 => true, 3 => true}, true),
            (5, hashmap! {}, false),
            (5, hashmap! {2 => true}, false),
            (5, hashmap! {2 => true, 3 => true}, true),
            (5, hashmap! {2 => true, 3 => true, 4 => true}, true),
            (
                5,
                hashmap! {2 => true, 3 => true, 4 => true, 5 => true},
                true,
            ),
        ];

        for (i, (size, acceptors, wack)) in tests.iter().enumerate() {
            let mut raft =
                new_test_inner_node(0x1, mock::ids_by_size(*size), 10, 1, SafeMemStorage::new());
            raft.become_candidate();
            raft.become_leader();
            commit_noop_entry(&mut raft);
            let li = raft.raft_log.last_index();
            let ents = MocksEnts::from("some data").into();
            raft.step(Message {
                from: 0x1,
                to: 0x1,
                field_type: MsgProp,
                entries: ents,
                ..Default::default()
            });

            for m in read_message(&mut raft) {
                if acceptors.contains_key(&m.to) {
                    raft.step(accept_and_reply(m));
                }
            }

            let g = raft.raft_log.committed > li;
            assert_eq!(g, *wack, "#{}: ack commit={}, want {}", i, g, wack);
        }
    }

    // tests that when leader commits a log entry
    // it also commits all preceding entries in the leader's log, including
    // entries created by previous leaders.
    // Also, it applies the entry to its local state machine (in log order).
    // Reference: section 5.3
    #[test]
    fn leader_commit_preceding_entries() {
        try_init_log();
        let tests = vec![
            new_entry_set(vec![]),
            new_entry_set(vec![(1, 2)]),
            new_entry_set(vec![(1, 1), (2, 2)]),
            new_entry_set(vec![(1, 1)]),
        ];

        for (i, tt) in tests.iter().enumerate() {
            let mut storage = SafeMemStorage::new();
            storage.wl().append(tt.clone());
            let mut raft = new_test_inner_node(0x1, vec![0x1, 0x2, 0x3], 10, 1, storage);
            raft.load_state(&HardState {
                term: 2,
                ..Default::default()
            });
            raft.become_candidate();
            raft.become_leader(); // generator a noo-op log
            raft.step(Message {
                from: 0x1,
                to: 0x1,
                field_type: MsgProp,
                entries: MocksEnts::from("some data").into(),
                ..Default::default()
            });

            for m in read_message(&mut raft) {
                raft.step(accept_and_reply(m));
            }
            let li = tt.len();
            let mut w_ents = tt.clone();
            w_ents.push(new_entry((li + 1) as u64, 3)); // leader noo-op log
            let mut entry = new_entry((li + 2) as u64, 3);
            entry.set_Data(Bytes::from(Vec::from("some data"))); // Props
            w_ents.push(entry);
            let g = raft.raft_log.next_ents();
            assert_eq!(g, w_ents, "#{}: ents = {:?}, want = {:?}", i, g, w_ents);
        }
    }

    // tests that once a follower learns that a log entry
    // is committed, it applies the entry to its local state machine (in log order).
    // Reference: section 5.3
    #[test]
    fn follower_commit_entry() {
        try_init_log();
        let tests = vec![
            (new_entry_set2(vec![(1, 1, "some data")]), 1),
            (new_entry_set2(vec![(1, 1, "some data"), (2, 1, "some data2")]), 2),
            (new_entry_set2(vec![(1, 1, "some data"), (2, 1, "some data2")]), 2),
            (new_entry_set2(vec![(1, 1, "some data"), (2, 1, "some data2")]), 1),
        ];

        for (i, (entries, commit)) in tests.iter().enumerate() {
            let mut raft = new_test_inner_node(0x1, vec![0x1, 0x2, 0x3], 10, 1, SafeMemStorage::new());
            raft.become_follower(1, 0x2);
            raft.step(Message { from: 0x2, to: 0x1, field_type: MsgApp, term: 1, entries: RepeatedField::from_vec(entries.clone()), commit: (*commit) as u64, ..Default::default() });
            let g = raft.raft_log.committed;
            assert_eq!(g, *commit, "#{}: committed={}, want {}", i, g, commit);
            let w_ents = &entries[..(*commit as usize)];
            let g = raft.raft_log.next_ents();
            assert_eq!(&g, w_ents, "#{}: next_ents={:?}, want {:?}", i, g, w_ents);
        }
    }

    // tests that if the follower does not find an
    // entry in its log with the same index and the term as the one in `AppendEntries` RPC,
    // then it refuses the new entries. Otherwise it replies that it accepts the 
    // append entries.
    // Reference: section 5.3 
    #[test]
    fn follower_check_msg_app() {
        try_init_log();
        let ents = new_entry_set(vec![(1, 1), (2, 2)]);
        let tests = vec![
            // match with committed entries
            (0, 0, 1, false, 0),
            (ents[0].Term, ents[0].Index, 1, false, 0),
            // match with uncommitted entries
            (ents[1].Term, ents[1].Index, 2, false, 0),

            // unmatch with existing entry
            (ents[0].Term, ents[1].Index, ents[1].Index, true, 2),
            // unexpecting entry
            (ents[1].Term + 1, ents[1].Index + 1, ents[1].Index + 1, true, 2),
        ];

        for (i, (term, index, w_index, w_reject, w_reject_hint)) in tests.iter().enumerate() {
            let mut storage = SafeMemStorage::new();
            storage.wl().append(ents.clone());
            let mut raft = new_test_inner_node(0x1, vec![0x1, 0x2, 0x3], 10, 1, storage);
            raft.load_state(&HardState { commit: 1, ..Default::default() });
            raft.become_follower(2, 2);

            raft.step(Message { from: 0x2, to: 0x1, field_type: MsgApp, term: 2, logTerm: *term, index: *index, ..Default::default() });

            let msgs = read_message(&mut raft);
            let w_msgs = vec![Message { from: 0x1, to: 0x2, field_type: MsgAppResp, term: 2, index: *w_index, reject: *w_reject, rejectHint: *w_reject_hint, ..Default::default() }];
            assert_eq!(w_msgs, msgs, "#{}: msgs={:?}, want {:?}", i, msgs, w_msgs);
        }
    }

    // tests that when AppendEntries RPC is valid,
    // the follower will delete the existing conflict entry and all that follow it,
    // and append any new entries not already in the log.
    // Also, it writes the new entry into stable storage.
    // Reference: section 5.3
    #[test]
    fn follower_append_entries() {
        try_init_log();
        let tests = vec![
            (2, 2,
             new_entry_set(vec![(3, 3)]),
             new_entry_set(vec![(1, 1), (2, 2), (3, 3)]),
             new_entry_set(vec![(3, 3)])),
            (1, 1,
             new_entry_set(vec![(2, 3), (3, 4)]),
             new_entry_set(vec![(1, 1), (2, 3), (3, 4)]),
             new_entry_set(vec![(2, 3), (3, 4)])),
            (0, 0,
             new_entry_set(vec![(1, 1)]),
             new_entry_set(vec![(1, 1), (2, 2)]),
             new_entry_set(vec![])),
            (0, 0,
             new_entry_set(vec![(1, 3)]),
             new_entry_set(vec![(1, 3)]),
             new_entry_set(vec![(1, 3)]))];

        for (i, (index, term, ents, w_ents, w_unstable)) in tests.iter().enumerate() {
            let storage = SafeMemStorage::new();
            storage.wl().append(new_entry_set(vec![(1, 1), (2, 2)])).unwrap();
            let mut raft = new_test_inner_node(0x1, vec![0x1, 0x2, 0x3], 10, 1, storage);
            raft.become_follower(2, 0x2);
            raft.step(Message { from: 0x2, to: 0x1, field_type: MsgApp, term: 2, logTerm: *term, index: *index, entries: RepeatedField::from_vec(ents.clone()), ..Default::default() });

            let g = raft.raft_log.all_entries();
            assert_eq!(&g, w_ents, "#{}, ents = {:?}, want = {:?}", i, &g, w_ents);

            let g = raft.raft_log.unstable_entries();
            assert_eq!(&g, w_unstable, "#{}, unstable_entries={:?}, want = {:?}", i, g, w_unstable);
        }
    }

    // tests that the leader could bring a follower's log
    // into consistency with its own.
    // Reference: section 5.3, figure 7
    #[test]
    fn leader_sync_follower() {
        let ents = new_entry_set(vec![
            (0, 0),
            (1, 1), (2, 1), (3, 1),
            (4, 4), (5, 4),
            (6, 5), (7, 5),
            (8, 6), (9, 6), (10, 6),
        ]);

        let term = 8;
        let tests = vec![
            new_entry_set(vec![
                (0, 0),
                (1, 1), (2, 1), (3, 1),
                (4, 4), (5, 4),
                (6, 5), (7, 5),
                (8, 6), (9, 6)]),
            new_entry_set(vec![
                (0, 0),
                (1, 1), (2, 1), (3, 1),
                (4, 4)]),
            new_entry_set(vec![
                (0, 0),
                (1, 1), (2, 1), (3, 1),
                (4, 4), (5, 4),
                (6, 5), (7, 5),
                (8, 6), (9, 6), (10, 6), (11, 6)]),
            new_entry_set(vec![
                (0, 0),
                (1, 1), (2, 1), (3, 1),
                (4, 4), (5, 4),
                (6, 5), (7, 5),
                (8, 6), (9, 6), (10, 6),
                (11, 7), (12, 7)]),
            new_entry_set(vec![
                (0, 0),
                (1, 1), (2, 1), (3, 1),
                (4, 4), (5, 4), (6, 4), (7, 4)]),
            new_entry_set(vec![
                (0, 0),
                (1, 1), (2, 1), (3, 1),
                (4, 2), (5, 2), (6, 2),
                (7, 3), (8, 3), (9, 3), (10, 3), (11, 3)])];

        for (i, entries) in tests.iter().enumerate() {
            let leader_storage = SafeMemStorage::new();
            leader_storage.wl().append(entries.clone());
            let mut lead = new_test_inner_node(0x1, vec![0x1, 0x2, 0x3], 10, 1, leader_storage);
            lead.load_state(&HardState { commit: lead.raft_log.last_index(), term, ..Default::default() });
            let follower_storage = SafeMemStorage::new();
            follower_storage.wl().append(entries.clone());
            let mut follow = new_test_inner_node(0x2, vec![0x1, 0x2, 0x3], 10, 1, follower_storage);
            follow.load_state(&HardState { term: term - 1, ..Default::default() });
        }
    }

    // tests that in most cases only a
    // single server(follower or candidate) will time out, which reduces the
    // likelihood of split vote in the new election.
    // Reference: section 5.2
    fn test_non_leader_election_timeout_non_conflict(state: StateType) {
        let et = 10;
        let size = 5;
        let ids = mock::ids_by_size(size as u64);
        let mut rafts: Vec<Raft<SafeMemStorage>> = Vec::with_capacity(size);
        for idx in ids {
            rafts.push(new_test_inner_node(
                idx,
                vec![0x1, 0x2, 0x3],
                et,
                1,
                SafeMemStorage::new(),
            ));
        }

        let mut conflicts = 0.0;

        for _ in 0..1000 {
            for raft in &mut rafts {
                match state {
                    StateType::Follower => raft.become_follower(raft.term + 1, NONE),
                    StateType::Candidate => raft.become_candidate(),
                    _ => {}
                }
            }

            let mut timeout_num = 0;
            while timeout_num == 0 {
                for mut raft in &mut rafts {
                    raft.tick_election();
                    if !read_message(&mut raft).is_empty() {
                        timeout_num += 1;
                    }
                }
            }
            // several rafts time out at the same tick
            if timeout_num > 1 {
                conflicts += 1.0;
            }
        }
        let g = conflicts / 1000.0;
        assert!(g <= 0.3, "probability of conflicts = {}, want <= 0.3", g);
    }

    // NOTE: the log has committed and applied
    fn commit_noop_entry(raft: &mut Raft<SafeMemStorage>) {
        assert_eq!(
            raft.state,
            StateType::Leader,
            "it should only be used when it is the leader"
        );
        raft.bcast_append();

        // simulate the response of msgApp
        let msgs = read_message(raft);
        for m in msgs {
            assert!(
                m.field_type == MsgApp
                    && m.entries.len() == 1
                    && m.entries.first().unwrap().Data.is_empty(),
                "not a message to append noop empty"
            );
            raft.step(accept_and_reply(m)).unwrap();
        }

        // ignore future messages to refresh followers' commit index
        read_message(raft);
        let unstable_entries = raft
            .raft_log
            .unstable_entries()
            .iter()
            .map(|entry| entry.clone())
            .collect::<Vec<_>>();
        raft.raft_log.storage.wl().append(unstable_entries).unwrap();
        raft.raft_log.applied_to(raft.raft_log.committed);
        raft.raft_log
            .stable_to(raft.raft_log.last_index(), raft.raft_log.last_term());
    }

    fn accept_and_reply(m: Message) -> Message {
        assert_eq!(m.field_type, MsgApp, "type should be MsgApp");
        Message {
            from: m.to,
            to: m.from,
            term: m.term,
            index: m.index + m.entries.len() as u64,
            field_type: MsgAppResp,
            ..Default::default()
        }
    }
}
