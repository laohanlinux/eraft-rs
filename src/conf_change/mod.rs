use crate::raftpb::raft::{ConfChangeSingle, ConfChangeType};

pub mod restore;
pub mod conf_change;
mod quick_test;
mod datadriven_test;


pub(crate) fn new_conf_change_single(id: u64, typ: ConfChangeType) -> ConfChangeSingle{
    let mut ccs = ConfChangeSingle::new();
    ccs.set_node_id(id);
    ccs.set_field_type(typ);
    ccs
}