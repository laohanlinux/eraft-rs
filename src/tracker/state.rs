use std::fmt::{self, Display, Error, Formatter};

// StateType is the state of a tracked follower.
#[derive(Clone, Debug, PartialEq)]
pub enum StateType {
    // StateProbe indicates that a follower whose last index isn't known. Such a
    // follower is "probe" (i.e. an append sent periodically) to narrow down
    // its last index. In the ideal (and common) case, only one round of probing
    // is necessary as the follower will react with a hint. Followers that are
    // probed over extend periods of time are often offline.
    Probe,
    // StateReplicate is the steady in which a follower eagerly receives
    // log entries to append to its log.
    Replicate,
    // StateSnapshot indicates a follower that needs log entries not avaliable
    // from the leader's Raft log. Such a follower needs a full snapshot to
    // return a StateReplicate
    Snapshot,
}

impl Default for StateType {
    fn default() -> Self {
        StateType::Probe
    }
}

impl Display for StateType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            StateType::Probe => write!(f, "StateProbe"),
            StateType::Replicate => {
                write!(f, "StateReplicate")
            }
            StateType::Snapshot => write!(f, "StateSnapshot"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tracker::state::StateType;
    #[test]
    fn it_works() {
        assert_eq!(format!("{}", StateType::Probe), "StateProbe");
    }
}
