use eraft_rs::raftpb::raft::ConfState;

pub struct Record {
    typ: i64,
    crc: u32,
    data: Vec<u8>,
}

pub struct SnapShot {
    index: u64,
    term: u64,
    conf_state: ConfState,
}