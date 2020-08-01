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

use crate::quorum::joint::JointConfig;
use crate::quorum::majority::MajorityConfig;
use crate::raftpb::raft::{ConfChangeSingle, ConfChangeType};
use crate::tracker::inflights::Inflights;
use crate::tracker::progress::{Progress, ProgressMap};
use crate::tracker::{Config, ProgressTracker};
use bytes::BytesMut;
use std::collections::{HashMap, HashSet};
use std::fmt::Write;

/// Changer facilitates configuration changes. It exposes methods to handle
/// simple and joint consensus while performing the proper validation that allows
/// refusing invalid configuration changes before they affect the active
/// configuration.
#[derive(Debug, Clone, PartialEq)]
pub struct Changer {
    pub tracker: ProgressTracker,
    pub last_index: u64,
}

impl Changer {
    /// verifies that the outgoing (=right) majority config of the joint
    /// config is empty and initializes it with a copy of the incoming (=left)
    /// majority config. That is, it transactions from
    /// ```text
    ///          (1 2 3) && ()
    /// ```
    /// to
    /// ``` text
    ///          (1 2 3) && (1 2 3)
    /// ```.
    /// The supplied changes are then applied to the incoming majority config,
    /// resulting in a joint configuration that in terms of the Raft thesis[1]
    /// (Section 4.3) corresponds to `c_{new, old}`.
    ///
    /// [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
    pub fn enter_joint(
        &mut self,
        auto_leave: bool,
        ccs: &mut [ConfChangeSingle],
    ) -> Result<(Config, ProgressMap), String> {
        info!("enter joint, auto_leave={}", auto_leave);
        check_invariants(&self.tracker.config, &self.tracker.progress)?;
        let mut cfg = self.tracker.config.clone();
        let mut prs = ProgressMap::default();
        prs.extend(self.tracker.progress.clone());
        if cfg.voters.joint() {
            return Err("config is already joint".to_string());
        }

        if cfg.voters.incoming.is_empty() {
            // we allow adding nodes to an empty config for convenience (testing and
            // bootstrap), but you can't enter a joint state.
            return Err("can't make a zero-voter config joint".to_string());
        }
        // clear the outgoing config.
        cfg.voters.outgoing.clear();
        // copy incoming to outgoing
        cfg.voters.outgoing.extend(&cfg.voters.incoming);
        self.apply(&mut cfg, &mut prs, ccs)?;
        cfg.auto_leave = auto_leave;
        Ok((cfg, prs))
    }

    /// leave_joint transactions out of a joint configuration. It is an error to call
    /// this method if the configuration is not joint, i.e. if the outgoing majority
    /// config voters[1] is empty.
    ///
    /// The outgoing majority config of the joint configuration will be removed,
    /// that is, the incoming config is promoted as the sole decision maker. In the
    /// notation of the Raft thesis[1] (Section 4.3), this method transactions from
    /// `C_{new, old}` to `C_new`.
    ///
    /// At the same time, any staged learners (learners_next) the addition of which
    /// was held back by an overlapping voter in the former outgoing config will be
    /// inserted into learners.
    ///
    /// [1]: https://github.com/ongardie/dissertation/blob/master/online-trim.pdf
    pub fn leave_joint(&mut self) -> Result<(Config, ProgressMap), String> {
        let mut cfg = self.tracker.config.clone();
        let mut prs = self.tracker.progress.clone();
        if !cfg.voters.joint() {
            return Err("can't leave a non-joint config".to_string());
        }
        if cfg.voters.outgoing.is_empty() {
            return Err(format!("configuration is not joint: {}", cfg));
        }
        for id in &cfg.learners_next {
            cfg.learners.insert(*id);
            let pr = prs.get_mut(&id).unwrap();
            pr.is_learner = true;
        }
        cfg.learners_next.clear();

        for id in cfg.voters.outgoing.iter() {
            let is_voter = cfg.voters.incoming.contains(id);
            let is_learner = cfg.learners.contains(id);
            if !is_voter && !is_learner {
                prs.remove(id);
            }
        }
        cfg.voters.outgoing.clear();
        cfg.auto_leave = false;
        Ok((cfg, prs))
    }

    /// carries out a series of configuration changes that (in aggregate)
    /// mutates the incoming majority config voters[0] by at most one. This
    /// method will return an error if that is not the case, if the resulting quorum is
    /// zero, or if the configuration is in a joint state (i.e. if there is an outgoing configuration).
    pub fn simple(
        &mut self,
        ccs: &mut [ConfChangeSingle],
    ) -> Result<(Config, ProgressMap), String> {
        info!("exec simple change config");
        let mut cfg = self.tracker.config.clone();
        let mut prs = self.tracker.progress.clone();
        if cfg.voters.joint() {
            return Err("can't apply simple config change in joint config".to_string());
        }
        self.apply(&mut cfg, &mut prs, ccs)?;
        let count = self
            .tracker
            .config
            .voters
            .incoming
            .votes
            .symmetric_difference(&cfg.voters.incoming.votes)
            .count();
        if count > 1 {
            return Err("more than one voter changed without entering joint config".to_string());
        }
        Ok((cfg, prs))
    }

    // apply a change to the configuration. By convention, changes to voters are
    // always made to the incoming majority config voters[0]. Voters[1] is either
    // empty or preserves the outgoing majority configuration while in a joint state.
    fn apply(
        &mut self,
        cfg: &mut Config,
        prs: &mut ProgressMap,
        ccs: &mut [ConfChangeSingle],
    ) -> Result<(), String> {
        info!("execute change.apply");
        for cc in ccs {
            if cc.get_node_id() == 0 {
                // etcd replace the nodeID with zero if it decides (downstream of
                // raft) to not apply a change, so we have to have explicit code
                // here to ignore these.
                continue;
            }
            match cc.get_field_type() {
                ConfChangeType::ConfChangeAddNode => {
                    self.make_voter(cfg, prs, cc.get_node_id());
                }
                ConfChangeType::ConfChangeAddLearnerNode => {
                    self.make_learner(cfg, prs, cc.get_node_id());
                }
                ConfChangeType::ConfChangeRemoveNode => {
                    self.remove(cfg, prs, cc.get_node_id());
                }
                ConfChangeType::ConfChangeUpdateNode => info!("execute conf change update node"),
                _ => return Err(format!("unexpected conf type {:?}", cc.get_field_type())),
            }
        }
        if cfg.voters.incoming.is_empty() {
            return Err("removed all voters".to_string());
        }
        Ok(())
    }

    // adds or promotes the given ID to be a voter in the incoming
    fn make_voter(&mut self, cfg: &mut Config, prs: &mut ProgressMap, id: u64) {
        info!("make a voter, id: {}", id);
        match prs.get_mut(&id) {
            Some(pr) => {
                pr.is_learner = false;
                cfg.learners.remove(&id);
                // TODO: why?
                cfg.learners_next.remove(&id);
                cfg.voters.incoming.insert(id);
            }
            None => {
                // TODO: why?
                self.init_progress(cfg, prs, id, false);
            }
        }
    }

    // make_learner makes the given ID a learner or stages it to be a leaner once
    // an active joint configuration is existed.
    //
    // The former happens when the peers is not a part of the outgoing config, in
    // which case we either add a new learner or demote a voter in the incoming
    // config.
    // The latter case occurs when the configuration is joint and the peer is a
    // voter in the outgoing config. In that case, wo do not want to add the peer
    // as a learner because then we'd have to track a peer as a voter and learner
    // simultaneously. Insted, we add the learner to LeanersNext. so that it will
    // be added to learners the moment the outgoing config is removed by leave_joint().
    fn make_learner(&mut self, cfg: &mut Config, prs: &mut ProgressMap, id: u64) {
        info!("add a learner, id={}", id);
        let pro = prs.get(&id);
        if pro.is_none() {
            self.init_progress(cfg, prs, id, true);
            return;
        }
        let pro = pro.unwrap().clone();
        if pro.is_learner {
            return;
        }

        self.remove(cfg, prs, id);
        // ... but save the progress
        prs.insert(id, pro);

        // NB: Use LeanersNext if we can't add the learner to learners directly, i.e.
        // if the peer is still tracked as a voter in the outgoing config. It will
        // be turned into a learner in leave_joint().
        //
        // Otherwise, add a regular learner right away.
        if cfg.voters.outgoing.contains(&id) {
            cfg.learners_next.insert(id);
        } else {
            prs.get_mut(&id).map(|pro| pro.is_learner = true);
            cfg.learners.insert(id);
        }
    }

    // remove this peer as a voter or leaner from the incoming config.
    fn remove(&mut self, cfg: &mut Config, prs: &mut ProgressMap, id: u64) {
        info!("remove node, id: {}", id);
        if !prs.contains_key(&id) {
            return;
        }
        cfg.voters.incoming.remove(&id);
        cfg.learners.remove(&id);
        cfg.learners_next.remove(&id);

        // If the peer is still a voter in the outgoing config, keep the Progress.
        if !cfg.voters.outgoing.contains(&id) {
            prs.remove(&id);
        }
    }

    // initializes a new progress for the given node or learner
    fn init_progress(
        &mut self,
        cfg: &mut Config,
        prs: &mut ProgressMap,
        id: u64,
        is_learner: bool,
    ) {
        if !is_learner {
            cfg.voters.incoming.insert(id);
        } else {
            cfg.learners.insert(id);
        }
        // TODO: why?
        // initializing the Progress with the last index means that the follower
        // can be probed (with the last index).
        //
        // TODO(tbg): seems awfully optimistic. Using the first index would be
        // better. The general expectation here is that the follower has no log
        // at all (and will thus likely need a snapshot), through the app may
        // have applied a snapshot out of band before adding the replica (thus
        // making the first index the better choice).
        let mut progress = Progress::new(0, self.last_index);
        progress.is_learner = is_learner;
        progress.inflights = Inflights::new(self.tracker.max_inflight);
        // When a node is first added, we should mark it as recently active.
        // Otherwise, check_quorum may cause use to step down if it is invoked
        // before the added node has had a chance to communicate with us.
        progress.recent_active = true;
        prs.insert(id, progress);
    }
}

// makes sure that the config and progress are compatible with
// each other. This is used to check both what the Changer is initialized with.
// as well as it returns.
fn check_invariants(cfg: &Config, prs: &ProgressMap) -> Result<(), String> {
    // NB: intentionally allow the empty config. In production we'll never see a
    // non-empty config(we prevent it from being created) but we will need to
    // be able *create* an initial config, for example during bootstrap (or
    // during tests). Instead of having to hand-code this, we allow
    // transaction from an empty config into any other legal and no-empty
    // config.
    let mut sets = HashSet::new();
    sets.extend(cfg.voters.incoming.votes.iter());
    sets.extend(cfg.voters.outgoing.votes.iter());
    sets.extend(cfg.learners.iter());
    sets.extend(cfg.learners_next.iter());
    for id in sets.iter() {
        if !prs.contains_key(id) {
            return Err(format!("no progress for {}", id));
        }
    }

    // TODO: Why?
    // Any staged learner was staged because it could not be directly added due
    // to a conflicting voter in the outgoing config.
    for id in cfg.learners_next.iter() {
        if !cfg.voters.outgoing.contains(id) {
            return Err(format!("{} is in learners_next, but not outgoing", id));
        }
        if prs.get(id).as_ref().unwrap().is_learner {
            return Err(format!(
                "{} is in learners_next, but is already marked as learner",
                id
            ));
        }
    }

    // conversely leaners and voters doesn't intersect at all.
    for id in cfg.learners.iter() {
        if cfg.voters.outgoing.contains(id) {
            return Err(format!("{} is in Leaners and outgoing", id));
        }
        if cfg.voters.incoming.contains(id) {
            return Err(format!("{} is in Leaners and incoming", id));
        }
        if !prs.get(id).as_ref().unwrap().is_learner {
            return Err(format!(
                "{} is in Learners, but is not marked as leaner",
                id
            ));
        }
    }

    if !cfg.voters.joint() {
        // We enforce that empty maps are nil instead of zero.
        if !cfg.voters.outgoing.is_empty() {
            return Err("outgoing must be nil when not joint".to_string());
        }
        if !cfg.learners_next.is_empty() {
            return Err("learners_next must be nil when not joint".to_string());
        }
        if cfg.auto_leave {
            return Err("auto_leave must be false when not joint".to_string());
        }
    }

    Ok(())
}

/// prints the type and node_id of the configuration changes as a
/// space-delimited string.
pub fn describe(ccs: Vec<ConfChangeSingle>) -> String {
    let mut buf = BytesMut::new();
    for cc in ccs {
        if !buf.is_empty() {
            buf.write_char(' ').unwrap();
        }
        buf.write_str(&format!("{:?}({})", cc.get_field_type(), cc.get_node_id())).unwrap();
    }
    String::from_utf8(buf.to_vec()).unwrap()
}
