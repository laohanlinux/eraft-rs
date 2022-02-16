use crate::quorum::majority::MajorityConfig;
use crate::quorum::quorum::{AckedIndexer, Index};
use std::collections::HashMap;

#[cfg(test)]
mod tests {
    use crate::quorum::majority::MajorityConfig;
    use crate::quorum::quick_test::alternative_majority_committed_index;
    use crate::quorum::quorum::{AckedIndexer, Index};
    use rand::prelude::*;
    use rand::Rng;
    use std::cmp::Ordering;
    use std::collections::{HashMap, HashSet};

    type IdxMap = HashMap<u64, Index>;

    fn new_idx_map() -> IdxMap {
        small_ran_idx_map(0)
    }

    type MemberMap = HashSet<u64>;

    fn convert_idx_map_to_member_map(idx_map: &IdxMap) -> MemberMap {
        let mut m = HashSet::new();
        idx_map.iter().for_each(|(k, v)| {
            m.insert(*k);
        });
        m
    }

    fn new_member_map() -> MemberMap {
        let mut m = HashSet::new();
        small_ran_idx_map(0).iter().for_each(|(k, v)| {
            m.insert(*k);
        });
        m
    }

    // returns a reasonably sized map of ids to commit indexes.
    fn small_ran_idx_map(size: usize) -> HashMap<u64, Index> {
        // Hard-code a reasonably small here (quick will hard-code 50, which
        // is not usefull here).
        let size = 10;
        let mut rng = rand::thread_rng();
        let n: usize = rng.gen_range(0..size);
        let mut ids: Vec<usize> = (1..size).collect();
        ids.shuffle(&mut rng);
        ids.drain(n..);
        let mut idxs = [0].repeat(ids.len());
        for idx in idxs.iter_mut() {
            *idx = rng.gen_range(0..n);
        }
        let mut m = HashMap::new();
        for (i, v) in ids.iter().enumerate() {
            m.insert(*v as u64, *idxs.get(i).unwrap() as Index);
        }
        m
    }

    #[test]
    fn tt_majority() {
        let count = 5000;
        for i in 0..count {
            let idx_map = new_idx_map();
            let member_map = convert_idx_map_to_member_map(&idx_map);
            let mut majority = MajorityConfig::new();
            majority.votes = member_map.clone();
            let idx = majority.committed_index(&idx_map);
            let expect_idx = alternative_majority_committed_index(majority.clone(), &idx_map);
            assert_eq!(idx, expect_idx);
        }
    }
}

// This is an alternative implmentation of (MajorityConfig).CommittedIndex(l).
pub(crate) fn alternative_majority_committed_index<T: AckedIndexer>(
    c: MajorityConfig,
    l: &T,
) -> Index {
    if c.is_empty() {
        return u64::MAX;
    }
    let mut id_to_idx = HashMap::new();
    c.votes.iter().for_each(|node| {
        if let Some(idx) = l.acked_index(node) {
            id_to_idx.insert(node, idx);
        }
    });

    // Build a map from index to voters who have acked that or any higher index.
    let mut idx_to_votes = HashMap::new();
    id_to_idx.iter().for_each(|(id, idx)| {
        idx_to_votes.insert(idx, 0);
    });

    for (_, idx) in id_to_idx.iter() {
        for (idy, v) in idx_to_votes.iter_mut() {
            if ***idy > **idx {
                continue;
            }
            *v += 1;
        }
    }

    // Find the maximum index that has achieved quorum.
    let q = c.len() / 2 + 1;
    let mut max_quorum_index = Index::default();
    for (idx, n) in idx_to_votes.clone() {
        if n >= q as u64 && *idx > &max_quorum_index {
            max_quorum_index = **idx;
        }
    }
    // println!("---->{:?}, {:?}, quorum: {}, max_quorum_index: {:?}", id_to_idx, idx_to_votes, q, max_quorum_index);
    max_quorum_index
}
