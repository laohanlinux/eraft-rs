use std::collections::{HashSet, HashMap};
use std::fmt::{self, Formatter, Display, Write};
use std::process::id;
use std::cmp::Ordering;
use crate::quorum::quorum::{AckedIndexer, Index, VoteResult};
use std::collections::hash_set::Iter;

/// MajorityConfig is a set of IDs that uses majority quorums to make decisions.
#[derive(Clone, PartialEq, Debug)]
pub struct MajorityConfig {
    pub(crate) votes: HashSet<u64>,
}

impl From<HashSet<u64>> for MajorityConfig {
    fn from(h: HashSet<u64>) -> Self {
        MajorityConfig { votes: h }
    }
}

impl MajorityConfig {
    pub fn new() -> Self {
        MajorityConfig { votes: HashSet::new() }
    }

    /// returns a (multi-line) representation of the commit indexes for the
    /// given lookuper.
    pub fn describe<T: AckedIndexer>(&self, l: &T) -> String {
        if self.votes.is_empty() {
            return "<empty majority quorum>".to_string();
        }

        #[derive(Default, Clone, Copy)]
        struct Tup {
            id: u64,
            idx: Index,
            // idx found?
            ok: bool,
            // length of bar displayed for this up
            bar: usize,
        }

        // Below, populate .bar so that the i-th largest commit index has bar i (we
        // plot this as sort of a progress bar). The actual code is a bit more
        // complicated and also makes sure that equal index => equal bar.
        let n = self.votes.len();
        let mut info: Vec<Tup> = vec![Tup::default()].repeat(n);
        for (i, id) in self.iter().enumerate() {
            let idx = l.acked_index(id);
            info[i].id = *id;
            info[i].idx = *idx.or_else(|| Some(&0)).unwrap();
            info[i].ok = idx.is_some();
        }
        // sort by index
        info.sort_by(|a, b| {
            if a.idx == b.idx {
                a.id.cmp(&b.id)
            } else {
                a.idx.cmp(&b.idx)
            }
        });

        // Populate .bar.
        for i in 0..info.len() {
            if i > 0 && info[i - 1].idx < info[i].idx {
                info[i].bar = i;
            }
        }

        // sort by id
        info.sort_by(|a, b| a.id.cmp(&b.id));

        let mut buf = String::new();
        // print

        buf.write_str((" ".repeat(n) + "    idx\n").as_str()).unwrap();

        for i in 0..info.len() {
            let bar = info[i].bar;
            if !info[i].ok {
                buf.write_str("?").unwrap();
                buf.write_str(" ".repeat(n).as_str()).unwrap();
            } else {
                buf.write_str(&*("x".repeat(bar) + ">" + " ".repeat(n - bar).as_str())).unwrap();
            }
            buf.write_str(format!(" {:>5}    (id={})\n", info[i].idx, info[i].id).as_str()).unwrap();
        }
        buf
    }

    /// commit_index computes the committed index from those supplied via the
    /// provide acked_index (for the active config).
    pub fn committed_index<T: AckedIndexer>(&self, l: &T) -> Index {
        if self.is_empty() {
            // This plays well with joint quorum which, when one of half is the zero
            // MajorityConfig, should behave like the other half.
            return u64::max_value();
        }
        // Use a on-stack slice to collect the committed indexes when n <= 7
        // (otherwise we alloc). The alternative is to stash a slice on
        // MajorityConfig, but this impairs usability (as is, MajorityConfig is just
        // a map, and that's nice). The assumption is that running with a
        // performance is a lesser concern (additionally the performance
        // implication of an allocation here are far from drastic).
        // TODO: optimized use stack
        let n = self.len();
        let mut srt: Vec<u64> = [0].repeat(n);
        let mut i = 0;
        for id in self.iter() {
            if let Some(idx) = l.acked_index(&id) {
                srt[i as usize] = *idx;
                i += 1;
            }
        }

        srt.sort_by_key(|key| *key);
        let pos = n - (n / 2 + 1);
        srt[pos]
    }

    /// VoteResult takes a mapping of voters to yes/no (true/false) votes and returns
    /// a result indicating whether the vote is pending (i.e. neither a quorum of
    /// yes/no has been reached), won (a quorum of yes has been reached), or lost (a
    /// quorum of no has been reached).
    pub fn vote_result(&self, votes: &HashMap<u64, bool>) -> VoteResult {
        if self.is_empty() {
            // By convention, the elections on an empty config win. This comes in
            // handy with joint quorums because it'll make a half-populated joint
            // quorum behave like a majority quorum
            return VoteResult::VoteWon;
        }
        let (against, agree, missing) = self.votes.iter().fold((0, 0, 0), |(mut against, mut agree, mut missing), id| {
            if let Some(v) = votes.get(id) {
                if *v { agree += 1 } else { against += 1 }
            } else {
                missing += 1;
            }
            (against, agree, missing)
        });
        // vote counts for no and yes, responsibility
        let q = self.len() / 2 + 1;
        debug!("agree:{}, missing:{}, q:{}", agree, missing, q);
        if agree >= q {
            return VoteResult::VoteWon;
        }
        if agree + missing >= q {
            return VoteResult::VotePending;
        }
        VoteResult::VoteLost
    }

    #[inline]
    pub fn as_slice(&self) -> Vec<u64> {
        let mut s1: Vec<u64> = self.iter().map(|v| *v).collect();
        s1.sort_by_key(|v| *v);
        s1
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.votes.len()
    }

    #[inline]
    pub(crate) fn get(&self, id: &u64) -> Option<&u64> {
        self.votes.get(id)
    }

    #[inline]
    pub(crate) fn insert(&mut self, id: u64) {
        self.votes.insert(id);
    }

    #[inline]
    pub(crate) fn remove(&mut self, id: &u64) -> bool {
        self.votes.remove(id)
    }

    #[inline]
    pub(crate) fn contains(&self, id: &u64) -> bool {
        self.votes.contains(id)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.votes.is_empty()
    }

    #[inline]
    pub(crate) fn clear(&mut self) {
        self.votes.clear();
    }

    #[inline]
    pub(crate) fn extend(&mut self, other: &Self) {
        self.votes.extend(other.iter())
    }

    #[inline]
    pub fn iter(&self) -> Iter<'_, u64> {
        self.votes.iter()
    }
}

impl From<&Vec<u64>> for MajorityConfig {
    fn from(v: &Vec<u64>) -> Self {
        let mut config = MajorityConfig {
            votes: HashSet::new(),
        };
        for item in v.iter() {
            config.votes.insert(*item);
        }
        config
    }
}

impl From<Vec<u64>> for MajorityConfig {
    fn from(v: Vec<u64>) -> Self {
        let mut config = MajorityConfig {
            votes: HashSet::new(),
        };
        for item in v.iter() {
            config.votes.insert(*item);
        }
        config
    }
}

impl Display for MajorityConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut votes: Vec<u64> = self.votes.iter().map(|v| *v).collect();
        votes.sort();
        let votes: Vec<String> = votes.iter().map(|v| format!("{}", v)).collect();
        let s: String = votes.join(" ");
        write!(f, "({})", s)
    }
}

#[cfg(test)]
mod tests {
    use crate::quorum::majority::MajorityConfig;
    use crate::tracker::MatchAckIndexer;
    use std::collections::HashMap;
    use crate::quorum::quorum::VoteResult::{VoteLost, VotePending, VoteWon};
    use crate::quorum::quorum::AckedIndexer;
    use crate::tracker::progress::Progress;

    #[test]
    fn t_majority() {
        let mut majority = MajorityConfig::new();
        majority.votes.insert(0);
        majority.votes.insert(1);
        assert_eq!("(0 1)", format!("{}", majority));
        let mut majority = MajorityConfig::new();
        assert_eq!("()", format!("{}", majority));

        let v = &vec![0, 1, 2];
        let majority: MajorityConfig = v.into();
        assert_eq!("(0 1 2)", format!("{}", majority));
        let majority: MajorityConfig = v.into();
        assert_eq!("(0 1 2)", format!("{}", majority));

        let mut majority = MajorityConfig::new();
        majority.votes.insert(0);
        assert_eq!(vec![0], majority.as_slice());
    }

    #[test]
    fn t_majority_vote_result() {
        let mut majority = MajorityConfig::new();
        for id in 0..5 {
            majority.votes.insert(id);
        }
        let mut votes = HashMap::new();
        assert_eq!(majority.vote_result(&votes), VotePending);
        for id in 0..2 {
            votes.insert(id, true);
            assert_eq!(majority.vote_result(&votes), VotePending);
        }
        votes.insert(3, true);
        assert_eq!(majority.vote_result(&votes), VoteWon);
        for id in 0..3 {
            votes.insert(id, false);
        }
        assert_eq!(majority.vote_result(&votes), VoteLost);
    }

    #[test]
    fn t_majority_committed_index() {
        let mut majority = MajorityConfig::new();
        let n = 5;
        let tests = vec![
            (vec![(3, 3), (4, 4), (5, 5)], 3),
            (vec![(4, 4), (3, 3), (5, 5)], 3),
            (vec![(5, 5), (4, 4), (3, 3)], 3),
            (vec![(3, 3), (4, 4), (5, 5), (4, 4), (3, 3)], 4),
            (vec![(3, 3), (6, 6), (5, 5), (7, 7), (3, 3)], 5),
            (vec![(3, 3), (6, 6), (6, 6), (6, 6), (6, 6)], 6),
        ];
        for id in 0..n {
            majority.votes.insert(id);
        }
        for (set, w_commit) in tests {
            let match_ack_indexer = new_match_ack_indexer(set.clone());
            let index = majority.committed_index(&match_ack_indexer);
            assert_eq!(index, w_commit);
        }
    }

    fn new_match_ack_indexer(v: Vec<(u64, u64)>) -> MatchAckIndexer {
        let mut match_ack_indexer = MatchAckIndexer::new();
        v.iter().fold(0, |acc, (m, n)| {
            let mut progress = Progress::new(*m, *n);
            match_ack_indexer.insert(acc, progress);
            acc + 1
        });
        match_ack_indexer
    }
}
