use crate::raftpb::raft::{ConfChangeSingle, ConfChangeType};

pub mod conf_change;
mod datadriven_test;
mod quick_test;
pub mod restore;

pub(crate) fn new_conf_change_single(id: u64, typ: ConfChangeType) -> ConfChangeSingle {
    let mut ccs = ConfChangeSingle::new();
    ccs.set_node_id(id);
    ccs.set_field_type(typ);
    ccs
}
