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


#[cfg(test)]
mod tests {
    use crate::raftpb::raft::{ConfChangeType, ConfChangeSingle, ConfChange};
    use crate::conf_change::conf_change::Changer;
    use std::fmt::Error;
    use rand::Rng;
    use protobuf::ProtobufEnum;
    use crate::tracker::ProgressTracker;

    // uses quick_check to verify that simple and joint config
    // changes arrive at the same result.
    #[test]
    fn t_conf_change_quick() {
        flexi_logger::Logger::with_env().start();
        let count = 1000;
        // log the first couple of runs of give some indication of things working
        // as intended.
        const info_count: usize = 5;

        for i in 0..count {
            let (simple_change, mut ccs) = wrapper().unwrap();
            let mut epoch_cc = ccs.drain(..1).collect::<Vec<_>>();
            let mut tr = ProgressTracker::new(10);
            let mut c = Changer { tracker: tr, last_index: 10 };
            let ret = c.simple(&mut epoch_cc);
            assert!(ret.is_ok());
            c.tracker.config = ret.as_ref().unwrap().0.clone();
            c.tracker.progress = ret.as_ref().unwrap().1.clone();
            let ret = with_joint(&mut c, &mut ccs);
            assert!(ret.is_ok());
            assert_eq!(simple_change, c);
        }
    }

    fn gen_cc(num: impl Fn() -> usize, id: impl Fn() -> u64, typ: impl Fn() -> ConfChangeType) -> Vec<ConfChangeSingle> {
        let mut ccs = Vec::new();
        let n = num();
        for i in 0..n {
            let mut cc = ConfChangeSingle::new();
            cc.set_field_type(typ());
            cc.set_node_id(id());
            ccs.push(cc);
        }
        ccs
    }

    fn wrapper() -> Result<(Changer, Vec<ConfChangeSingle>), String> {
        let mut ccs = gen_cc(|| -> usize {
            let mut r = rand::thread_rng();
            r.gen_range(1, 9) + 1
        }, || -> u64 {
            let mut r = rand::thread_rng();
            r.gen_range(1, 9) + 1
        }, || -> ConfChangeType{
            let mut r = rand::thread_rng();
            let n = ConfChangeType::values().len();
            let em = r.gen_range(0, n);
            ConfChangeType::from_i32(em as i32).unwrap()
        });
        let mut epoch_cc = ConfChangeSingle::new();
        epoch_cc.set_node_id(1);
        epoch_cc.set_field_type(ConfChangeType::ConfChangeAddNode);
        ccs.push(epoch_cc);
        ccs.reverse();

        let ccs_copy = ccs.clone();

        let mut tr = ProgressTracker::new(10);
        let mut c = Changer { tracker: tr, last_index: 10 };
        with_simple(&mut c, &mut ccs).map(|_| (c, ccs_copy))
    }

    fn with_simple(c: &mut Changer, ccs: &mut [ConfChangeSingle]) -> Result<(), String> {
        for cc in ccs.iter() {
            let mut ccs = Vec::new();
            ccs.push(cc.clone());
            let (cfg, prs) = c.simple(&mut ccs)?;
            c.tracker.config = cfg;
            c.tracker.progress = prs;
        }
        Ok(())
    }

    fn with_joint(c: &mut Changer, ccs: &mut [ConfChangeSingle]) -> Result<(), String> {
        let (cfg, prs) = c.enter_joint(false, ccs)?;
        // Also do this with auto_leave on, just to check that we'd get the same
        // result.
        let (mut cfg2a, mut prs2a) = c.enter_joint(true, ccs)?;
        cfg2a.auto_leave = false;
        assert_eq!(cfg, cfg2a);
        assert_eq!(prs, prs2a);

        c.tracker.config = cfg.clone();
        c.tracker.progress = prs.clone();
        let (mut cfg2b, mut prs2b) = c.leave_joint()?;
        // Reset back to the main branch with auto_leave = false.
        c.tracker.config = cfg.clone();
        c.tracker.progress = prs.clone();
        let (cfg, prs) = c.leave_joint()?;
        assert_eq!(cfg, cfg2b);
        assert_eq!(prs, prs2b);

        c.tracker.config = cfg.clone();
        c.tracker.progress = prs.clone();

        Ok(())
    }
}
