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
    use crate::nom_data_test::{execute_test, walk, TestData};
    use crate::quorum::joint::JointConfig;
    use crate::quorum::majority::MajorityConfig;
    use crate::quorum::quick_test::alternative_majority_committed_index;
    use crate::quorum::quorum::{to_string, AckedIndexer, Index, MapAckIndexer};
    use std::collections::{HashMap, HashSet};
    use std::fmt::Write;
    use std::iter::FromIterator;

    // parses and executes and the test cases in ./testdata/*. An entry
    // in such a file specifies the command, which is either of "committed" to check
    // committed_index or "vote" to verify a VoteResult. The underlying configuration
    // and inputs are specified via the arguments 'cfg' and 'cfgj' (for the majority
    // config and, optionally, majority config joint to the first one) and `idx`
    // (for CommittedIndex) and 'votes' (for VoteResult).
    //
    // Internally, the harness runs some additional checks on each test case for
    // which it is known that the result shouldn't change. For example,
    // interchanging the majority c configurations of a joint quorum must not
    // influence the result; if it does, this is noted in the test's output.
    #[test]
    fn t_data_driven() {
        // flexi_logger::Logger::with_env().start();
        walk("src/quorum/testdata", |p| {
            execute_test(p, "--------------------------------", |data| -> String {
                // Two majority configs. The first one is always used (though it may
                // be empty) and the second one is used if used iff joint is true.
                let mut joint = false;
                let mut ids = Vec::<u64>::new();
                let mut idsj = Vec::<u64>::new();
                // The committed indexes for the nodes in the config in the order in
                // which they appear in (ids,idsj), without repetition. An underscore
                // denotes an omission (i.e. no information for this voter); this is
                // different from 0, For example,
                //
                // cfg=(1,2) cfgj=(2,3,4) idx=(_,5,_7) initializes the idx for voter 2
                // to 5 and that for voter 4 to 7 (and no others).
                //
                // cfgj=zero is specified to instruct the test harness to treat cfgj
                // as zero instead of not specified (i.e. it will trigger a joint
                // quorum test instead of a majority quorum test for cfg only).
                let mut idxs = Vec::<Index>::new();
                // votes. these are initialized similar to idxs except the only values
                // used are 1 (voted against) and 2 (voted for). This looks awkward,
                // but it convenient because it allows sharing code between the two.
                let mut votes = Vec::<Index>::new();

                // parse the args.
                for cmd_arg in &data.cmd_args {
                    for val in &cmd_arg.vals {
                        match cmd_arg.key.as_str() {
                            "cfg" => {
                                ids.push(val.parse().unwrap());
                            }
                            "cfgj" => {
                                joint = true;
                                if val == &"zero" {
                                    assert_eq!(cmd_arg.vals.len(), 1);
                                } else {
                                    idsj.push(val.parse().unwrap());
                                }
                            }
                            "idx" => {
                                // register placeholders as zeros.
                                if val != &"_" {
                                    idxs.push(val.parse().unwrap());
                                    // This is a restriction caused by the above
                                    // special-casing for _.
                                    assert_ne!(idxs.last().unwrap(), &0, "cannot use 0 as idx");
                                }
                            }
                            "votes" => {
                                if val == &"y" {
                                    votes.push(2);
                                } else if val == &"n" {
                                    votes.push(1);
                                } else if val == &"_" {
                                    votes.push(0);
                                } else {
                                    panic!(format!("unknown vote: {}", val));
                                }
                            }
                            other => panic!(format!("unknown arg {:?}", cmd_arg)),
                        }
                    }
                }

                // Build the two majority configs.
                let mut c = MajorityConfig {
                    votes: HashSet::from_iter(ids.clone().into_iter()),
                };
                let mut cj = MajorityConfig {
                    votes: HashSet::from_iter(idsj.clone().into_iter()),
                };

                // Helper that returns an AckedIndexer which has the specified indexes
                // mapped to the right IDs.
                let make_lookuper =
                    |idxs: &Vec<Index>, ids: &Vec<u64>, idsj: &Vec<u64>| -> MapAckIndexer {
                        let mut l: HashMap<u64, u64> = HashMap::new();
                        let mut p = 0;
                        let mut _ids: Vec<Index> = Vec::new();
                        _ids.extend(ids);
                        _ids.extend(idsj);
                        for id in &_ids {
                            if l.contains_key(id) {
                                continue;
                            }
                            if p < idxs.len() {
                                // NB: this creates zero entries for placeholders that we remove later.
                                // The upshot of doing it that way is to avoid having to specify placeholders
                                // multiple times when omitting voters present in both halves of
                                // a joint config.
                                l.insert(*id, idxs[p]);
                                p += 1;
                            }
                        }

                        // zero entries are created by _ placeholders; we don't want
                        // them in the lookuper because "no entry" is different from
                        // "zero entry". Note that we prevent tests from specifying
                        // zero commit Indexes, so that there's no confusion between
                        // the two concepts.
                        l.retain(|_, val| *val != 0);
                        l
                    };

                if data.cmd == "vote" {
                    let mut joint_config = JointConfig::new();
                    joint_config.incoming = c.clone();
                    joint_config.outgoing = cj.clone();
                    let voters = joint_config.ids();
                    assert_eq!(
                        voters.len(),
                        votes.len(),
                        "mismatch input (explicit for _) fro votes {:?}: {:?}",
                        voters,
                        votes
                    );
                }

                let mut buf = String::new();
                match data.cmd.as_str() {
                    "committed" => {
                        let l = make_lookuper(&idxs, &ids, &idsj);
                        // branch based on wether this is a majority or joint quorum.
                        // test case.
                        if !joint {
                            let idx = c.committed_index(&l);
                            buf.write_str(c.describe(&l).as_str());
                            println!("MapAckIndexer {:?}, ack_id:{}", l, idx);
                            // These alternative computations should return the same
                            // result. If not, print to the output.
                            let a_idx = alternative_majority_committed_index(c.clone(), &l);
                            if a_idx != idx {
                                buf.write_str(
                                    format!("{} <-- via alternative computation\n", a_idx).as_str(),
                                );
                            }
                            // Joining a majority with the empty majority should give same result.
                            let a_idx =
                                JointConfig::new2(c.clone(), MajorityConfig::new()).committed(&l);
                            if a_idx != idx {
                                buf.write_str(
                                    format!("{} >-- via zero-joint quorum\n", a_idx).as_str(),
                                );
                            }
                            // Joining a majority with it self should give the same result.
                            let a_idx = JointConfig::new2(c.clone(), c.clone()).committed(&l);
                            if a_idx != idx {
                                buf.write_str(
                                    format!("{} >-- via self-joint quorum\n", a_idx).as_str(),
                                );
                            }

                            let overlay = |c: MajorityConfig,
                                           l: &dyn AckedIndexer,
                                           id: u64,
                                           idx: Index|
                             -> MapAckIndexer {
                                let mut ll = MapAckIndexer::new();
                                for iid in c.iter() {
                                    if *iid == id {
                                        ll.insert(*iid, idx);
                                    } else if let Some(idx) = l.acked_index(iid) {
                                        ll.insert(*iid, *idx);
                                    }
                                }
                                ll
                            };
                            for id in c.iter() {
                                let iidx = l.acked_index(id).map(|idx| *idx).unwrap_or_else(|| 0);
                                if idx > iidx && iidx > 0 {
                                    // If the committed index was definitely above the currently
                                    // inspected idx, the result shouldn't change if we lower it
                                    // further.
                                    let lo = overlay(c.clone(), &l, *id, iidx - 1);
                                    let a_idx = c.committed_index(&lo);
                                    if a_idx != idx {
                                        buf.write_str(
                                            format!("{} <-- overlaying {}-->{}", a_idx, id, iidx)
                                                .as_str(),
                                        );
                                    }

                                    let lo = overlay(c.clone(), &l, *id, 0);
                                    let a_idx = c.committed_index(&lo);
                                    if a_idx != idx {
                                        buf.write_str(
                                            format!("{} <-- overlaying {}-->0", a_idx, id).as_str(),
                                        );
                                    }
                                }
                            }
                            buf.write_str(to_string(idx).as_str());
                        } else {
                            let mut cc = JointConfig::new2(c.clone(), cj.clone());
                            buf.write_str(cc.describe(&l).as_str());
                            let idx = cc.committed(&l);
                            // Interchanging the majority shouldn't make a difference. If it does, print.
                            let a_idx = JointConfig::new2(c.clone(), cj.clone()).committed(&l);
                            if a_idx != idx {
                                buf.write_str(format!("{} <-- via symmetry\n", a_idx).as_str());
                            }
                            buf.write_str(to_string(idx).as_str());
                        }
                    }
                    "vote" => {
                        let ll = make_lookuper(&votes, &ids, &idsj);
                        println!(
                            "ids: {:?}, idsj: {:?}, votes: {:?}, ll: {:?}",
                            ids, idsj, votes, ll
                        );
                        let mut l = HashMap::new();
                        for (id, v) in ll.iter() {
                            l.insert(*id, *v != 1); // NB: 1 == false, 2 == true
                        }
                        if !joint {
                            // Test a majority quorum
                            buf.write_str(&format!("{:?}", c.vote_result(&l)));
                        } else {
                            // Run a joint quorum test case.
                            let r = JointConfig::new2(c.clone(), cj.clone()).vote_result(&l);
                            // Interchanging the majorities shouldn't make a difference. If it does, print.
                            let ar = JointConfig::new2(cj.clone(), c.clone()).vote_result(&l);
                            assert_eq!(r, ar);
                            buf.write_str(format!("{:?}", r).as_str());
                        }
                    }
                    _ => {}
                }
                buf
            });
        });
    }
}
