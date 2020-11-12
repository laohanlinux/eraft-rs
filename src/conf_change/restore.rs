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

use crate::raftpb::raft::{ConfState, ConfChangeSingle, ConfChange, ConfChangeType};
use crate::raftpb::raft::ConfChangeType::{ConfChangeAddNode, ConfChangeRemoveNode, ConfChangeAddLearnerNode};
use crate::conf_change::new_conf_change_single;
use crate::tracker::Config;
use crate::tracker::progress::ProgressMap;
use crate::conf_change::conf_change::Changer;
use std::array::FixedSizeArray;

// toConfChangeSingle translates a conf state into 1) a slice of operations creating
// first the config that will become the outgoing one, and then the incoming one, and 
// b) another slice that, when applied to the config resulted from 1), respresents the 
// ConfState.
fn to_conf_change_single(cs: &ConfState) -> (Vec<ConfChangeSingle>, Vec<ConfChangeSingle>) {
    // Example to follow along this code:
    // voters=(1 2 3) learners=(5) outgoing=(1 2 4 6) learners_next=(4)
    //
    // This means that before entering the joint config, the configuration
    // had voters (1 2 4 6) and perhaps some learners that are already gone.
    // The new set of voters is (1 2 3), i.e. (1 2) were kept around, and (4 6)
    // are no longer voters; however 4 is poised to become a learner upon leaving
    // the joint state.
    // We can't tell whether 5 was a learner before entering the joint config,
    // but it doesn't matter (we'll pretend that it wasn't)
    //
    // The code below will construct
    // outgoing = add 1; add 2; add 4; add 6
    // incoming = remove 1; remove 2; remove 4; remove 6
    // outgoing    add 1; add 2; add 3;
    // incoming    add-learner 5;
    //             add-learner 4;
    // So, when starting with an empty config, after applying 'outgoing' we have
    //
    //  quorum=(1 2 4 6)
    //
    // From which we enter a joint state via 'incoming'
    //  quorum=(1 2 3)&&(1 2 4 6) learner=(5) learners_next=(4)
    //
    // as desired.

    let mut outgoing = Vec::new();
    let mut incoming = Vec::new();
    for id in cs.get_voters_outgoing() {
        // If there are outgoing voters, first add them one by one so that the
        // (non-joint) config has them all.
        outgoing.push(new_conf_change_single(*id, ConfChangeType::ConfChangeAddNode));
    }
    // We're done constructing the outgoing slice, now on to the incoming one
    // (which will apply on top of the config created by the outgoing slice).

    // First, we'll remove all of the outgoing voters.
    for id in cs.get_voters_outgoing() {
        incoming.push(new_conf_change_single(*id, ConfChangeType::ConfChangeRemoveNode));
    }
    // Then we'll add the incoming voters and learners.
    for id in cs.get_voters() {
        incoming.push(new_conf_change_single(*id, ConfChangeType::ConfChangeAddNode));
    }
    for id in cs.get_learners() {
        incoming.push(new_conf_change_single(*id, ConfChangeType::ConfChangeAddLearnerNode));
    }
    // Same for LeanersNext; these are nodes we want to be learners but which
    // are currently voters in the outgoing config.
    for id in cs.get_learners_next() {
        incoming.push(new_conf_change_single(*id, ConfChangeType::ConfChangeAddLearnerNode))
    }
    (outgoing, incoming)
}

// pub fn chain(chg ConfChange, ops: impl )

/// takes a Changer (which must represent an empty configuration), and
/// runs a sequence of changes enacting the configuration described in the
/// ConfState
///
/// TODO(tbg) it's silly that this takes a Changer. Unravel this by making sure
/// the Changer only needs a ProgressMap (not a whole Tracker) at which point
/// this can just take last_index and max_inflight directly instead and cook up
/// the results from that alone.
pub fn restore(chg: &mut Changer, cs: &ConfState) -> Result<(Config, ProgressMap), String> {
    warn!("execute restore ");
    let (outgoing, mut incoming) = to_conf_change_single(cs);
    if outgoing.is_empty() {
        // No outgoing config, so just apply the incoming changes one by one.
        for cc in incoming.iter() {
            let cc = &mut vec![cc.clone()];
            let (cfg, progress) = chg.simple(cc)?;
            chg.tracker.config = cfg;
            chg.tracker.progress = progress;
        }
    } else {
        // The ConfState describes a joint configuration.
        //
        // First, apply all of the changes of the outgoing config one by one, so
        // that it temporarily becomes the incoming active config. For example,
        // if the config is (1 2 3)&(2 3 4), this will establish (2 3 4)&().
        for cc in outgoing.iter() {
            let cc = &mut vec![cc.clone()];
            let (cfg, progress) = chg.simple(cc)?;
            chg.tracker.config = cfg;
            chg.tracker.progress = progress;
        }

        // Now enter the joint state, which rotates the above additions into the
        // outgoing config, and adds the incoming config in. Continuing the
        // example above. we'd get (1 2 3)&(2 3 4), i.e. the incoming operations
        // would be removing 2,3,4 and then adding in 1,2,3 while transitioning
        // into a joint state.
        let (cfg, progress) = chg.enter_joint(cs.get_auto_leave(), &mut *incoming)?;
        chg.tracker.config = cfg;
        chg.tracker.progress = progress;
    }

    Ok((chg.tracker.config.clone(), chg.tracker.progress.clone()))
}


#[cfg(test)]
mod tests {
    use rand::Rng;
    use rand::prelude::SliceRandom;
    use crate::raftpb::raft::ConfState;
    use crate::conf_change::conf_change::Changer;
    use crate::tracker::ProgressTracker;
    use crate::conf_change::restore::restore;
    use protobuf::reflect::ProtobufValue;
    use crate::mock::init_console_log;

    #[test]
    fn t_restore() {
        init_console_log();
        let count = 1000;
        let f = |cs: &mut ConfState| -> bool {
            let mut chg = Changer { tracker: ProgressTracker::new(10), last_index: 0 };
            let (cfg, prs) = {
                match restore(&mut chg, cs) {
                    Ok((cfg, prs)) => (cfg, prs),
                    Err(e) => {
                        error!("{}", e);
                        return false;
                    }
                }
            };

            chg.tracker.config = cfg;
            chg.tracker.progress = prs;

            cs.voters.sort();
            cs.learners.sort();
            cs.voters_outgoing.sort();
            cs.learners_next.sort();
            let mut cs2 = chg.tracker.config_state();
            cs2.voters.sort();
            cs2.learners.sort();
            cs2.voters_outgoing.sort();
            cs2.learners_next.sort();
            // NB: cs.Equivalent does the same "sorting" dance internally, but let's
            // test it a bit here instead of relying on it.
            if cs.get_auto_leave() == false {
                cs.set_auto_leave(false);
            }
            if cs2.get_auto_leave() == false {
                cs2.set_auto_leave(false);
            }
            if *cs == cs2 {
                return true;
            }
            false
        };


        let new_conf_state = |voters: Option<Vec<u64>>, learners: Option<Vec<u64>>, voters_outgoing: Option<Vec<u64>>, learners_next: Option<Vec<u64>>, auto_leave: bool| -> ConfState{
            let mut cs = ConfState::new();
            if voters.is_some() {
                cs.set_voters(voters.unwrap());
            }
            if learners.is_some() {
                cs.set_learners(learners.unwrap());
            }
            if voters_outgoing.is_some() {
                cs.set_voters_outgoing(voters_outgoing.unwrap());
            }
            if learners_next.is_some() {
                cs.set_learners_next(learners_next.unwrap());
            }
            cs.set_auto_leave(auto_leave);
            cs
        };
        for mut cs in vec![
            ConfState::new(),
            new_conf_state(Some(vec![1, 2, 3]), None, None, None, false),
            new_conf_state(Some(vec![1, 2, 3]), Some(vec![4, 5, 6]), None, None, false),
            new_conf_state(Some(vec![1, 2, 3]), Some(vec![5]), Some(vec![1, 2, 4, 6]), Some(vec![4]), false)
        ].iter_mut() {
            assert!(f(&mut cs));
        }

        for _ in 0..count {
            let mut cs = generate_rnd_conf_change();
            println!("{:?}", cs);
            assert!(f(&mut cs));
        }
    }

    // Generate create a random (valid) ConfState for use with quickcheck.
    fn generate_rnd_conf_change() -> ConfState {
        let conv = |sl: &Vec<u64>| -> Vec<u64> {
            // We want IDs but the incoming slice is zero-indexed, so add one to
            // each.
            let mut out = [0].repeat(sl.len());
            for i in 0..sl.len() {
                out[i] = sl[i] + 1;
            }
            out
        };

        let mut r = rand::thread_rng();
        // NB: never generate the empty ConfState, that one should be unit tested.
        let n_voters = r.gen_range(0, 5) + 1;
        let n_learners = r.gen_range(0, 5);

        // The number of voters that are in the outgoing config but not in the
        // incoming one. (We'll additionally retain a random number of the
        // incoming voters below).
        let n_removed_voters = r.gen_range(0, 3);

        // Voters, learners, and removed voters must not overlap. A "removed voter"
        // is one that we have in the outgoing config but not the incoming one.
        let mut ids = (1..=2 * (n_voters + n_learners + n_removed_voters) as u64).collect::<Vec<_>>();
        ids.shuffle(&mut r);
        // println!("ids {:?}, {}", ids, 2 * (n_voters + n_learners + n_removed_voters));
        let mut cs = ConfState::new();
        cs.voters = ids.drain(..n_voters).collect();

        if n_learners > 0 {
            cs.learners = ids.drain(..n_learners).collect::<Vec<_>>();
        }

        // Roll the dice on how many of the incoming voters we decide were also
        // previously voters.
        //
        // NB: this code avoids creating non-nil empty slices (here and below).
        let n_outgoing_retained_voters = r.gen_range(0, n_voters + 1);
        if n_outgoing_retained_voters > 0 || n_removed_voters > 0 {
            cs.voters_outgoing.extend_from_slice(&cs.voters[..n_outgoing_retained_voters]);
            cs.voters_outgoing.extend_from_slice(&ids[..n_removed_voters]);
        }

        // Only outgoing voters that are not also incoming voters can be in
        // learners_next (they represent demotions).
        if n_removed_voters > 0 {
            let n_learners = r.gen_range(0, n_removed_voters + 1);
            if n_learners > 0 {
                cs.learners_next = ids[..n_learners].to_vec();
            }
        }

        cs.set_auto_leave(cs.voters_outgoing.len() > 0 && r.gen_range(0, 2) == 1);
        cs
    }
}
