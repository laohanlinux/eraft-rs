use crate::raftpb::raft::ConfChangeTransition::{
    ConfChangeTransitionAuto, ConfChangeTransitionJointExplicit, ConfChangeTransitionJointImplicit,
};
use crate::raftpb::raft::ConfChangeType::{
    ConfChangeAddLearnerNode, ConfChangeAddNode, ConfChangeRemoveNode, ConfChangeUpdateNode,
};
use crate::raftpb::raft::EntryType::{EntryConfChange, EntryConfChangeV2};
use crate::raftpb::raft::{ConfChange, ConfChangeSingle, ConfChangeV2, ConfState, Entry};
use crate::util::vote_resp_msg_type;
use bytes::{Buf, Bytes};
use nom::lib::std::borrow::Cow;
use nom::lib::std::fmt::{Display, Formatter};
use protobuf::{Message, RepeatedField};

pub mod raft;
// pub mod gogoproto;

// returns a nil error if the inputs describe the same configuration.
// On mismatch, returns a descriptive error showing the difference.
pub fn equivalent(cs1: &ConfState, cs2: &ConfState) -> Result<(), String> {
    let orig1 = cs1.clone();
    let orig2 = cs2.clone();
    let mut cs1 = cs1.clone();
    let mut cs2 = cs2.clone();
    cs1.voters.sort();
    cs1.learners.sort();
    cs1.voters_outgoing.sort();
    cs1.learners_next.sort();
    if !cs1.get_auto_leave() {
        cs1.set_auto_leave(false);
    }

    cs2.voters.sort();
    cs2.learners.sort();
    cs2.voters_outgoing.sort();
    cs2.learners_next.sort();
    if !cs2.get_auto_leave() {
        cs2.set_auto_leave(false);
    }

    if cs1 != cs2 {
        info!("cs1: {:?}\ncs2:{:?}", cs1, cs2);
        return Err(format!(
            "ConfStates not equivalent after sorting:{:?}\n{:?}\nInputs were:\n{:?}\n{:?}",
            cs1, cs2, orig1, orig2
        ));
    }

    Ok(())
}

// ConfChangeI abstracts over ConfChangeV2 and (legacy) ConfChange to allow
// treating them in a unified manner.
pub trait ConfChangeI: Display + protobuf::Message {
    fn as_v2(&self) -> ConfChangeV2;
    fn as_v1(&self) -> Option<&ConfChange>;
    fn to_entry(&self) -> Entry;
}

impl ConfChangeI for ConfChange {
    #[inline]
    fn as_v2(&self) -> ConfChangeV2 {
        let mut cc2 = ConfChangeV2::new();
        cc2.set_context(self.get_context().to_bytes());
        let mut change = ConfChangeSingle::new();
        change.set_field_type(self.get_field_type());
        change.set_node_id(self.get_node_id());
        cc2.set_changes(RepeatedField::from(vec![change]));
        cc2
    }

    #[inline]
    fn as_v1(&self) -> Option<&ConfChange> {
        Some(&self)
    }

    #[inline]
    fn to_entry(&self) -> Entry {
        let data = self.write_to_bytes().unwrap();
        let mut entry = Entry::new();
        entry.set_Data(Bytes::from(data));
        entry.set_Type(EntryConfChange);
        entry
    }
}

impl Display for ConfChange {
    fn fmt(&self, f: &mut Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl ConfChangeI for ConfChangeV2 {
    #[inline]
    fn as_v2(&self) -> ConfChangeV2 {
        self.clone()
    }

    #[inline]
    fn as_v1(&self) -> Option<&ConfChange> {
        None
    }

    #[inline]
    fn to_entry(&self) -> Entry {
        let data = self.write_to_bytes().unwrap();
        let mut entry = Entry::new();
        entry.set_Data(Bytes::from(data));
        entry.set_Type(EntryConfChangeV2);
        entry
    }
}

impl Display for ConfChangeV2 {
    fn fmt(&self, f: &mut Formatter<'_>) -> ::std::fmt::Result {
        write!(f, "{}", self)
    }
}

pub trait ExtendConfChange {
    fn leave_joint(&self) -> bool;
    fn enter_joint(&self) -> (bool, bool);
}

impl ExtendConfChange for ConfChangeV2 {
    fn leave_joint(&self) -> bool {
        let mut cp = self.clone();
        cp.clear_context();
        let empty = ConfChangeV2::default();
        cp.eq(&empty)
    }
    // EnterJoint returns two bools. The second bool is true if and only if this
    // config change will use Joint Consensus, which is the case if it contains more
    // than one change or if the use of Joint Consensus was requested explicitly.
    // The first bool can only be true if second one is, and indicates whether the
    // Joint State will be left automatically.
    fn enter_joint(&self) -> (bool, bool) {
        // NB: in theory, more config changes could qualify for the "simple"
        // protocol but it depends on the config on top of which the changes apply.
        // For example, adding two learners is not OK if both nodes are part of the
        // base config (i.e. two voters are turned into learners in the process of
        // applying the conf change). In practice, these distinctions should not
        // matter, so we keep it simple and use Joint Consensus liberally.
        if self.get_transition() != ConfChangeTransitionAuto || self.changes.len() > 1 {
            // Use Joint Consensus.
            let mut auto_leave = false;
            match self.get_transition() {
                ConfChangeTransitionAuto | ConfChangeTransitionJointImplicit => auto_leave = true,
                ConfChangeTransitionJointExplicit => {}
            }
            return (auto_leave, true);
        }
        (false, false)
    }
}

pub fn cmp_conf_state(a: &ConfState, b: &ConfState) -> bool {
    let mut a = a.clone();
    let mut b = b.clone();
    a.voters.sort();
    b.voters.sort();
    a.learners.sort();
    b.learners.sort();
    a.voters_outgoing.sort();
    b.voters_outgoing.sort();
    a.learners_next.sort();
    b.learners_next.sort();

    a.get_auto_leave() == b.get_auto_leave()
        && a.get_voters() == b.get_voters()
        && a.get_voters_outgoing() == b.get_voters_outgoing()
        && a.get_learners() == b.get_learners()
}

pub fn cmp_config_change_v2(a: &ConfChangeV2, b: &ConfChangeV2) -> bool {
    a.get_transition() == b.get_transition()
        && a.get_changes() == b.get_changes()
        && a.get_context() == b.get_context()
}

pub fn entry_to_conf_changei(entry: &Entry) -> Option<Box<dyn ConfChangeI>> {
    if entry.get_Type() == EntryConfChange {
        let mut cc = ConfChange::default();
        assert!(cc.merge_from_bytes(entry.get_Data()).is_ok());
        return Some(Box::new(cc));
    } else if entry.get_Type() == EntryConfChangeV2 {
        let mut cc = ConfChangeV2::default();
        assert!(cc.merge_from_bytes(entry.get_Data()).is_ok());
        return Some(Box::new(cc));
    }
    None
}

// ConfChangesFromString parses a Space-delimited sequence of operations into a
// slice of ConfChangeSingle. The supported operations are:
// - vn: make n a voter,
// - ln: make n a learner,
// - rn: remove n, and
// - un: update n.
pub fn conf_changes_from_string(s: &str) -> Result<Vec<ConfChangeSingle>, String> {
    let mut ccs = Vec::<ConfChangeSingle>::new();
    for tok in &mut s
        .split_ascii_whitespace()
        .map(|s| s.chars())
        .collect::<Vec<_>>()
    {
        if tok.count() < 2 {
            return Err(format!(
                "unknown token {}",
                tok.into_iter().collect::<String>()
            ));
        }
        let mut cc = ConfChangeSingle::new();
        match tok.nth(0).unwrap() {
            'v' => cc.set_field_type(ConfChangeAddNode),
            'l' => cc.set_field_type(ConfChangeAddLearnerNode),
            'r' => cc.set_field_type(ConfChangeRemoveNode),
            'u' => cc.set_field_type(ConfChangeUpdateNode),
            _ => {
                return Err(format!(
                    "unknown token {}",
                    tok.into_iter().collect::<String>()
                ));
            }
        }
        let id = tok.skip(0).into_iter().collect::<String>();
        cc.set_node_id(id.parse().unwrap());
        ccs.push(cc);
    }
    Ok(ccs)
}

#[cfg(test)]
mod tests {
    use crate::raftpb::raft::ConfChangeV2;
    use bytes::Bytes;
    use protobuf::Message;

    #[test]
    fn it_works() {
        let mut cc = ConfChangeV2::new();
        cc.set_context(Bytes::from("manual"));
        let data = cc.write_to_bytes().unwrap();
        let mut expect = ConfChangeV2::default();
        expect.merge_from_bytes(data.as_slice()).unwrap();
        assert_eq!(expect.get_context(), "manual".as_bytes());
    }
}
