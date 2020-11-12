#[cfg(test)]
mod test {
    use crate::nom_data_test::{walk, execute_test};
    use crate::tracker::{ProgressTracker, Config};
    use crate::conf_change::conf_change::Changer;
    use crate::raftpb::raft::{ConfChange, ConfChangeType, ConfChangeSingle};
    use protobuf::ProtobufEnum;
    use crate::tracker::progress::ProgressMap;
    use std::convert::AsMut;
    use env_logger::init;
    use crate::mock::init_console_log;

    #[test]
    fn t_conf_data_driven() {
        init_console_log();
        walk("src/conf_change/testdata", |p| {
            let mut tr = ProgressTracker::new(10);
            let mut c = Changer {
                tracker: tr,
                last_index: 0, // incremented in this test with each cmd
            };
            execute_test(p, "--------------------------------", |data| -> String {
                // The test files use the commands
                // - simple: run a simple conf change (i.e. no joint consensus),
                // - enter-joint: enter a joint config, and
                // - leave-joint: leave a joint config
                // The first two take a list of config changes, which have the following
                // syntax:
                // - vn: make a voter,
                // - ln: make n a learner,
                // - rn: remove n, and
                // - un: update n
                let mut ccs: Vec<ConfChangeSingle> = vec![];
                let mut auto_leave = false;
                for cmd_arg in data.cmd_args.iter() {
                    let mut cc = ConfChangeSingle::new();
                    match cmd_arg.key.as_str() {
                        "v" => {
                            cc.set_field_type(ConfChangeType::ConfChangeAddNode);
                        }
                        "l" => {
                            cc.set_field_type(ConfChangeType::ConfChangeAddLearnerNode)
                        }
                        "r" => {
                            cc.set_field_type(ConfChangeType::ConfChangeRemoveNode)
                        }
                        "u" => {
                            cc.set_field_type(ConfChangeType::ConfChangeUpdateNode)
                        }
                        "autoleave" => {
                            auto_leave = cmd_arg.vals[0].parse().unwrap();
                        }
                        u => panic!("unknown input: {}", u)
                    }
                    if cmd_arg.key.as_str() != "autoleave" {
                        let id = cmd_arg.vals[0].parse().unwrap();
                        cc.set_node_id(id);
                        ccs.push(cc);
                    }
                }

                let mut cfg = Config::default();
                let mut prs = ProgressMap::default();
                match data.cmd.as_str() {
                    "simple" => {
                        match c.simple(&mut ccs) {
                            Ok((new_cfg, new_prs)) => {
                                cfg = new_cfg;
                                prs = new_prs;
                            }
                            e => {
                                c.last_index += 1;
                                return e.unwrap_err();
                            }
                        }
                    }
                    "enter-joint" => {
                        match c.enter_joint(auto_leave, &mut ccs) {
                            Ok((new_cfg, new_prs)) => {
                                cfg = new_cfg;
                                prs = new_prs;
                            }
                            e => {
                                c.last_index += 1;
                                return e.unwrap_err();
                            }
                        }
                    }
                    "leave-joint" => {
                        info!("ccs {:?}", ccs);
                        if !ccs.is_empty() {
                            return "this command takes no input".to_owned();
                        }
                        match c.leave_joint() {
                            Ok((new_cfg, new_prs)) => {
                                cfg = new_cfg;
                                prs = new_prs;
                            }
                            e => {
                                c.last_index += 1;
                                return e.unwrap_err();
                            }
                        }
                    }
                    u => panic!("unknown command: {}", u)
                }
                c.tracker.config = cfg;
                c.tracker.progress = prs;
                c.last_index += 1;
                format!("{}\n{}", c.tracker.config, c.tracker.progress)
            })
        });
    }
}
