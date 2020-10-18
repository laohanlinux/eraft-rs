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
    use crate::mock::{new_test_raw_node, MockEntry, MocksEnts};
    use crate::raft::Raft;
    use crate::raftpb::raft::MessageType::{MsgAppResp, MsgProp};
    use crate::raftpb::raft::{Entry, Message};
    use crate::storage::{SafeMemStorage, Storage};
    use bytes::Bytes;
    use protobuf::RepeatedField;

    fn read_message<S: Storage>(raft: &mut Raft<S>) -> Vec<Message> {
        let msg = raft.msgs.clone();
        raft.msgs.clear();
        msg
    }

    // Ensures:
    // 1. `MsgApp` fill the sending windows until full
    // 2. when the windows is full, no more `MsgApp` can be sent.
    #[test]
    fn msg_app_flow_control_full() {
        flexi_logger::Logger::with_env().start();
        let raft = new_test_raw_node(1, vec![1, 2], 5, 1, SafeMemStorage::new());
        let mut wl_raft = raft.wl();
        wl_raft.raft.become_candidate();
        wl_raft.raft.become_leader();

        {
            let mut pr = wl_raft.raft.prs.progress.must_get_mut(&2);
            // force the progress to be in replicate state.
            pr.become_replicate();
        }
        // fill in the inflights windows
        {
            for i in 0..wl_raft.raft.prs.max_inflight {
                let mut msg = Message::new();
                msg.from = 1;
                msg.to = 1;
                msg.field_type = MsgProp;
                msg.entries = MocksEnts::from("somedata").into();
                assert!(wl_raft.step(msg).is_ok());
                let msg = read_message(&mut wl_raft.raft);
                assert_eq!(msg.len(), 1, "{}: len(ms) = {}, want: 1", i, msg.len());
            }
        }

        // ensure 1
        {
            let mut pr = wl_raft.raft.prs.progress.must_get_mut(&2);
            assert!(
                pr.inflights.full(),
                "inflights.full = {}, want: {}",
                pr.inflights.full(),
                true
            );
        }

        //ensure 2
        {
            for i in 0..10 {
                let mut msg = Message::new();
                msg.from = 1;
                msg.to = 1;
                msg.field_type = MsgProp;
                msg.entries = MocksEnts::from("somedata").into();
                assert!(wl_raft.step(msg).is_ok());
                let msg = read_message(&mut wl_raft.raft);
                assert_eq!(msg.len(), 0, "{}: len(ms) = {}, want: 1", i, msg.len());
            }
        }
    }

    // Ensures `MsgAppResp` can move
    // forward the sending windows correctly:
    // 1. valid `MsgAppResp.Index` moves the windows to pass all smaller or euqal index.
    // 2. out-of-dated `MsgAppResp` has no effect on the sliding windows.
    #[test]
    fn msg_app_flow_control_move_forward() {
        flexi_logger::Logger::with_env().start();
        let raft = new_test_raw_node(1, vec![1, 2], 5, 1, SafeMemStorage::new());
        let mut wl_raft = raft.wl();
        wl_raft.raft.become_candidate();
        wl_raft.raft.become_leader();
        {
            let mut pr2 = wl_raft.raft.prs.progress.must_get_mut(&2);
            // force the progress to be in replicate state
            pr2.become_replicate();
        }

        // fill in the inflights windows.
        {
            for i in 0..wl_raft.raft.prs.max_inflight {
                let mut msg = Message::new();
                msg.from = 1;
                msg.to = 1;
                msg.field_type = MsgProp;
                msg.set_entries(MocksEnts::from("somedata").into());
                assert!(wl_raft.step(msg).is_ok());
                let msg = read_message(&mut wl_raft.raft);
                assert_eq!(msg.len(), 1, "{}: len(ms) = {}, want: 1", i, msg.len());
            }
        }
        info!("pr1: {:?}", wl_raft.raft.prs.progress);
        // 1 is noop, 2 is the first proposal we just sent.
        // so we start with 2.
        {
            for tt in 2..wl_raft.raft.prs.max_inflight {
                // move forward the windows
                {
                    let mut msg = Message::new();
                    msg.from = 2;
                    msg.to = 1;
                    msg.field_type = MsgAppResp;
                    msg.index = tt;
                    assert!(wl_raft.step(msg).is_ok());
                    read_message(&mut wl_raft.raft);
                }

                // fill in the inflights windows again.
                {
                    let mut msg = Message::new();
                    msg.from = 1;
                    msg.to = 1;
                    msg.set_field_type(MsgProp);
                    msg.set_entries(MocksEnts::from("somedata").into());
                    assert!(wl_raft.raft.step(msg).is_ok());
                    let msg = read_message(&mut wl_raft.raft);
                    assert_eq!(msg.len(), 1, "{}: len={}, want 1", tt, msg.len());

                    // ensure 1
                    {
                        let pr2 = wl_raft.raft.prs.progress.get(&2).unwrap();
                        assert!(
                            pr2.inflights.full(),
                            "inflights.full = {}, want true",
                            pr2.inflights.full()
                        );
                    }

                    // ensure 2
                    {
                        for i in 0..tt {
                            let mut msg = Message::new();
                            msg.from = 2;
                            msg.to = 1;
                            msg.field_type = MsgAppResp;
                            msg.index = i;
                            assert!(wl_raft.step(msg).is_ok());
                            let pr2 = wl_raft.raft.prs.progress.must_get(&2);
                            assert!(
                                pr2.inflights.full(),
                                "{}: inflights.full={}, want {}",
                                tt,
                                pr2.inflights.full(),
                                true
                            );
                        }
                    }
                }
            }
        }
    }
}
