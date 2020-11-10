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
    use crate::mock::{new_test_raw_node, MocksEnts, read_message, new_test_core_node, new_test_inner_node};
    use crate::storage::SafeMemStorage;
    use crate::raftpb::raft::{Snapshot, SnapshotMetadata, ConfState, Message};
    use protobuf::{SingularField, SingularPtrField};
    use crate::raftpb::raft::MessageType::{MsgAppResp, MsgProp, MsgSnapStatus};
    use crate::tracker::state::StateType;

    #[test]
    fn sending_snapshot_set_pending_snapshot() {
        flexi_logger::Logger::with_env().start();
        let mut raft = new_test_inner_node(0x1, vec![1], 10, 1, SafeMemStorage::new());
        raft.restore(&new_testing_snap());

        raft.become_candidate();
        raft.become_leader();

        // force set the next of node 2, so that
        // node 2 needs a snapshot
        let first_index = raft.raft_log.first_index();
        raft.prs.progress.must_get_mut(&0x2).next = first_index;

        let index = raft.prs.progress.must_get(&0x2).next - 1;
        raft.step(Message { from: 0x2, to: 0x1, field_type: MsgAppResp, index, reject: true, ..Default::default() });

        let pending_snapshot = raft.prs.progress.must_get(&0x2).pending_snapshot;
        assert_eq!(pending_snapshot, 11, "pending_snapshot = {}, want 11", pending_snapshot);
    }

    #[test]
    fn pending_snapshot_pause_replication() {
        flexi_logger::Logger::with_env().start();
        let mut raft = new_test_inner_node(0x1, vec![0x1, 0x2], 10, 1, SafeMemStorage::new());
        raft.restore(&new_testing_snap());

        raft.become_candidate();
        raft.become_leader();

        raft.prs.progress.must_get_mut(&0x2).become_snapshot(11);

        raft.step(Message { from: 0x1, to: 0x1, field_type: MsgProp, entries: MocksEnts::from("somedata").into(), ..Default::default() });
        let msg = read_message(&mut raft);
        assert!(msg.is_empty(), "len(msgs) = {}, want 0", msg.len());
    }

    #[test]
    fn snapshot_failure() {
        flexi_logger::Logger::with_env().start();
        let mut raft = new_test_inner_node(0x1, vec![0x1, 0x2], 10, 1, SafeMemStorage::new());
        raft.restore(&new_testing_snap());

        raft.become_candidate();
        raft.become_leader();

        raft.prs.progress.must_get_mut(&0x2).next = 1;
        raft.prs.progress.must_get_mut(&0x2).become_snapshot(11);
        raft.step(Message { from: 0x2, to: 0x1, field_type: MsgSnapStatus, reject: true, ..Default::default() });
        assert_eq!(raft.prs.progress.must_get(&0x2).pending_snapshot, 0, "pending_snapshot = {}, want 0", raft.prs.progress.must_get(&0x2).pending_snapshot);
        assert_eq!(raft.prs.progress.must_get(&0x2).next, 1, "next = {}, want 1", raft.prs.progress.must_get(&0x2).next);
        assert!(raft.prs.progress.must_get(&0x2).probe_sent, "probe_sent = {}, want true", raft.prs.progress.must_get(&0x2).probe_sent);
    }

    fn new_testing_snap() -> Snapshot {
        let mut snap = Snapshot::new();
        let mut conf_state = ConfState::new();
        conf_state.set_voters(vec![1, 2]);
        snap.set_metadata(SnapshotMetadata { index: 11, term: 11, conf_state: SingularPtrField::from(Some(conf_state)), ..Default::default() });
        snap
    }
}