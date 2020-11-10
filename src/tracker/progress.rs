use crate::tracker::inflights::Inflights;
use crate::tracker::state::StateType;
use crate::tracker::state::StateType::{Probe, Replicate, Snapshot};

use nom::lib::std::option::IterMut;
use std::collections::hash_map::{Iter, Keys};
use std::collections::HashMap;
use std::fmt::{self, Display, Error, Formatter};

// Progress represents a follower's progress in the view of the leader. Leader
// maintains progresses of all followers, and sends entries to the follower
// based on its progress.
//
// NB(tg): Progress is basically a state machine whose transactions are mostly
// strewn around `*raft.raft`. Additionally, some fields are only used when in a
// certain State. All of this isn't ideal
#[derive(Clone, PartialEq, Debug)]
pub struct Progress {
    pub(crate) _match: u64,
    pub(crate) next: u64,

    // State defines how the leader should interact with the follower.
    //
    // When in StateProbe, leader sends at most one replication message
    // per heartbeat interval. It also probes actual progress of the follower.
    //
    // When in StateReplicate, leader optimistically increase next
    // to the latest entry sent after sending replication message. This is
    // an optimized state for fast replicating log entries to the follower.
    //
    // When in StateSnapshot, leader should have sent out snapshot
    // before and stops sending any replication message.
    pub(crate) state: StateType,

    // PendingSnapshot is used in StateSnapshot.
    // If there is a pending snapshot, the pendingSnapshot will be set to the
    // index of the snapshot. If pendingSnapshot is set, the replication process of
    // this Progress will be paused. raft will not resend snapshot until the pending one
    // is reported to be failed.
    pending_snapshot: u64,

    // recent_active is true if the progress is recently active. Receiving any messages
    // from the corresponds follower indicates the progress is active.
    // recent_active can be reset to false after an election timeout.
    //
    // TODO(tbg): the leader should always have this set to true.
    pub(crate) recent_active: bool,

    // ProbeSent is used while this follower is in StateProbe. When ProbeSent is
    // true, raft should pause sending replication message to this peer until
    // ProbeSent is reset. See ProbeAcked() and IsPaused().
    pub(crate) probe_sent: bool,

    // Inflights is a sliding window for the inflight messages.
    // Each inflight message contains one or mre log entries.
    // The max number of entries per message is defined in raft config as MaxSizePerMsg.
    // Thus inflight effectively limits both the number of inflight messages
    // and the bandwidth each process can use.
    // When inflights is Full, no more message should be sent.
    // When a leader sends out a message, the index of the last
    // entry should be added to inflights. The index MUST be added
    // into inflights in order.
    // When a leader receives a reply, the previous inflights should
    // be freed by calling inflights.FreeLe with the index of the last
    // received entry.
    pub(crate) inflights: Inflights,

    // is_learner is true if this progress is tracked for a leader.
    pub(crate) is_learner: bool,
}

impl Progress {
    pub fn new(_match: u64, next: u64) -> Self {
        Progress {
            _match,
            next,
            state: Default::default(),
            pending_snapshot: 0,
            recent_active: false,
            probe_sent: false,
            is_learner: false,
            inflights: Default::default(),
        }
    }
    // ResetState moves that Progress into the specified State, resetting ProbeSent,
    // PendingSnapshot, and inflight
    pub fn reset_state(&mut self, state: StateType) {
        self.probe_sent = false;
        self.pending_snapshot = 0;
        self.state = state;
        self.inflights.reset();
    }

    // probe_acked is called when this peer has accepted an append. It resets
    // probe_sent to signal that additional append messages should be sent without
    // further delay.
    pub fn probe_acked(&mut self) {
        self.probe_sent = false;
    }

    // BecomeProbe transaction into StateProbe. Next is reset to Match+1 or,
    // optionally and if larger, the index of the pending snapshot.
    pub fn become_probe(&mut self) {
        // If the original state is StateSnapshot, Progress knows that
        // the pending snapshot has been sent to this peer Successfully, then
        // probes from pendingSnapshot + 1.
        if self.state == StateType::Snapshot {
            let pending_snapshot = self.pending_snapshot;
            self.reset_state(StateType::Probe);
            self.next = (self._match + 1).max(pending_snapshot + 1);
        } else {
            self.reset_state(StateType::Probe);
            self.next = self._match + 1;
        }
    }

    /// Become Replicate transaction into StateReplicate, resetting Next to _match + 1
    pub fn become_replicate(&mut self) {
        self.reset_state(StateType::Replicate);
        self.next = self._match + 1;
        info!("become replicate");
    }

    /// BecomeSnapshot moves that Progress to StateSnapshot with the specified pending
    /// snapshot
    pub fn become_snapshot(&mut self, snapshot: u64) {
        self.reset_state(StateType::Snapshot);
        self.pending_snapshot = snapshot;
    }

    /// maybe_update is called when an MsgAppResp arrives from the follower, with the
    /// index acked by it. The method returns false if the given n index comes of from
    /// an outdated message. Otherwise it updates the progress and return true.
    pub fn maybe_update(&mut self, n: u64) -> bool {
        let mut update = false;
        if self._match < n {
            self._match = n;
            update = true;
            self.probe_acked();
        }
        if self.next < n + 1 {
            self.next = n + 1;
        }
        update
    }

    /// OptimisticUpdate signals the appends all the way up to and including index n
    /// are in-flight. As a result. `Next` is increased to `n + 1`
    pub fn optimistic_update(&mut self, n: u64) {
        self.next = n + 1;
    }

    /// MaybeDecrTo adjust the Progress to the receipt of a MsgApp rejection. The
    /// arguments are the index the follower rejected to append to its log, and its
    /// last index.
    ///
    /// rejecteds can happen spuriously as messages are sent out of order or
    /// duplicated. In such cases, the rejection pertains to an index that the
    /// Progress already knows were previously acknowledged, and false is returned
    /// without changing the Progress.
    ///
    /// If the rejection is genuine, Next is lowered sensibly, and the Progress is
    /// cleared for sending log entries
    pub fn maybe_decr_to(&mut self, rejected: u64, last: u64) -> bool {
        if self.state == Replicate {
            // The rejected must be stale if the progress has matched and "rejected"
            // is smaller than "match".
            if rejected <= self._match {
                return false;
            }
            // Directly decrease next to match + 1
            //
            // TODO(tbg): Why not use last if it's larger?
            self.next = self._match + 1;
            return true;
        }

        // TODO: add by me
        assert!(self.next > 0);
        // The rejection must be stale if "rejected" does not match next - 1. This
        // is because non-replicating followers are probed one entry at a time.
        if self.next - 1 != rejected {
            return false;
        }

        self.next = rejected.min(last + 1);
        if self.next < 1 {
            self.next = 1;
        }
        self.probe_sent = false;
        true
    }

    /// Return whether sending log entries to this node has been throttled.
    /// This is done when a node has rejected recent MsgApp, is currently waiting
    /// for a snapshot, or has reached the MaxInflightMsgs limit. In normal
    /// operation, this is false. A throttled node will be contacted less frequently
    /// until it has reached a state in which it's able to accept a steady stream of
    /// log entries again.
    pub fn is_paused(&self) -> bool {
        match self.state {
            StateType::Probe => self.probe_sent,
            StateType::Replicate => self.inflights.full(),
            StateType::Snapshot => true,
        }
    }
}

impl Display for Progress {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} match={} next={}", self.state, self._match, self.next)?;
        if self.is_learner {
            write!(f, " learner")?;
        }
        if self.is_paused() {
            write!(f, " paused")?;
        }
        if self.pending_snapshot > 0 {
            write!(f, " pendingSnap={}", self.pending_snapshot)?;
        }
        if !self.recent_active {
            write!(f, " inactive")?;
        }
        let n = self.inflights.count();
        if n > 0 {
            write!(f, " inflight={}", n)?;
            if self.inflights.full() {
                write!(f, "[full]")?;
            }
        }
        Ok(())
    }
}

// ProgressMap is a map of *Progress
#[derive(Default, Clone, PartialEq, Debug)]
pub struct ProgressMap {
    pub(crate) map: HashMap<u64, Progress>,
}

impl Display for ProgressMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut keys: Vec<u64> = self.map.keys().map(|uid| *uid).collect();
        keys.sort_by_key(|k| *k);
        for (i, id) in keys.iter().enumerate() {
            if i + 1 < keys.len() {
                writeln!(f, "{}: {}", id, self.map.get(id).unwrap())?;
            } else {
                write!(f, "{}: {}", id, self.map.get(id).unwrap())?;
            }
        }
        Ok(())
    }
}

impl ProgressMap {
    pub fn new(map: HashMap<u64, Progress>) -> Self {
        ProgressMap { map }
    }

    // TODO: Optz
    #[inline]
    pub fn ids(&self) -> Vec<u64> {
        self.map.keys().map(|id| *id).collect()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[inline]
    pub fn insert(&mut self, id: u64, progress: Progress) {
        self.map.insert(id, progress);
    }

    #[inline]
    pub fn remove(&mut self, id: &u64) -> Option<Progress> {
        self.map.remove(id)
    }

    #[inline]
    pub fn contains_key(&self, id: &u64) -> bool {
        self.map.contains_key(id)
    }

    #[inline]
    pub fn get(&self, id: &u64) -> Option<&Progress> {
        self.map.get(id)
    }

    #[inline]
    pub fn must_get(&self, id: &u64) -> &Progress {
        self.map.get(id).unwrap()
    }

    #[inline]
    pub fn keys(&self) -> Keys<'_, u64, Progress> {
        self.map.keys()
    }

    #[inline]
    pub fn get_mut(&mut self, id: &u64) -> Option<&mut Progress> {
        self.map.get_mut(id)
    }

    #[inline]
    pub fn must_get_mut(&mut self, id: &u64) -> &mut Progress {
        self.map.get_mut(id).unwrap()
    }

    #[inline]
    pub fn extend(&mut self, other: Self) {
        self.map.extend(other.map);
    }

    #[inline]
    pub fn to_map(&self) -> &HashMap<u64, Progress> {
        &self.map
    }

    #[inline]
    pub fn iter(&self) -> Iter<'_, u64, Progress> {
        self.map.iter()
    }

    //    #[inline]
    //    pub fn iter_mut(&mut self) -> IterMut<'_, u64, Progress> {
    //        self.map.iter_mut()
    //    }
}

#[cfg(test)]
mod tests {
    use crate::tracker::inflights::Inflights;
    use crate::tracker::progress::Progress;
    use crate::tracker::state::StateType;
    use crate::tracker::state::StateType::{Probe, Replicate, Snapshot};

    #[test]
    fn it_process_string() {
        let mut ins = Inflights::new(1);
        ins.add(123);
        let mut pr = Progress {
            _match: 1,
            next: 2,
            state: StateType::Snapshot,
            pending_snapshot: 123,
            recent_active: false,
            probe_sent: true,
            is_learner: true,
            inflights: ins,
        };
        let exp =
            "StateSnapshot match=1 next=2 learner paused pendingSnap=123 inactive inflight=1[full]";
        assert_eq!(format!("{}", pr), exp);
    }

    #[test]
    pub fn t_process_is_paused() {
        // (state, paused, w)
        let tests = vec![
            (Probe, false, false),
            (Probe, true, true),
            (Replicate, false, false),
            (Snapshot, false, true),
            (Replicate, true, false),
        ];
        for (state, paused, w) in tests {
            let p = Progress {
                _match: 0,
                next: 0,
                state,
                pending_snapshot: 0,
                recent_active: false,
                probe_sent: paused,
                is_learner: false,
                inflights: Inflights::new(256),
            };
            assert_eq!(w, p.is_paused());
        }
    }

    // ensures that maybe_update and maybe_update and maybe_decr_to will reset
    // probe_sent
    #[test]
    fn it_progress_resume() {
        let mut p = Progress {
            _match: 0,
            next: 2,
            state: StateType::Probe,
            pending_snapshot: 0,
            recent_active: false,
            probe_sent: true,
            is_learner: false,
            inflights: Default::default(),
        };
        p.maybe_decr_to(1, 1);
        assert!(!p.probe_sent);

        p.probe_sent = true;
        p.maybe_update(2);
        assert!(!p.probe_sent);
    }

    #[test]
    fn it_progress_become_probe() {
        let _match = 1;
        // (p, w_next)
        let tests = vec![
            (
                Progress {
                    _match,
                    next: 5,
                    state: Replicate,
                    pending_snapshot: 0,
                    recent_active: false,
                    probe_sent: false,
                    is_learner: false,
                    inflights: Default::default(),
                },
                2,
            ),
            // snapshot finish
            (
                Progress {
                    _match,
                    next: 5,
                    state: Snapshot,
                    pending_snapshot: 10,
                    recent_active: false,
                    probe_sent: false,
                    is_learner: false,
                    inflights: Inflights::new(1 << 8),
                },
                11,
            ),
            // snapshot failure
            (
                Progress {
                    _match,
                    next: 5,
                    state: Snapshot,
                    pending_snapshot: 0,
                    recent_active: false,
                    probe_sent: false,
                    is_learner: false,
                    inflights: Inflights::new(1 << 8),
                },
                2,
            ),
        ];

        for (mut p, w_next) in tests {
            p.become_probe();
            assert_eq!(p.state, Probe);
            assert_eq!(p._match, _match);
            assert_eq!(p.next, w_next);
        }
    }

    #[test]
    fn it_progress_become_replicate() {
        let mut p = Progress {
            _match: 1,
            next: 5,
            state: Probe,
            pending_snapshot: 0,
            recent_active: false,
            probe_sent: false,
            is_learner: false,
            inflights: Inflights::new(1 << 8),
        };
        p.become_replicate();
        assert_eq!(p.state, Replicate);
        assert_eq!(p._match, 1);
        assert_eq!(p.next, p._match + 1);
    }

    #[test]
    fn it_progress_become_snapshot() {
        let mut p = Progress {
            _match: 1,
            next: 5,
            state: Probe,
            pending_snapshot: 0,
            recent_active: false,
            probe_sent: false,
            is_learner: false,
            inflights: Inflights::new(1 << 8),
        };
        p.become_snapshot(10);
        assert_eq!(p.state, Snapshot);
        assert_eq!(p._match, 1);
        assert_eq!(p.pending_snapshot, 10);
    }

    #[test]
    fn it_progress_update() {
        let prev_m = 3;
        let prev_n = 5;
        //(update, w_m, w_n, w_ok)
        let tests = vec![
            // do not decrease match, next
            (prev_m - 1, prev_m, prev_n, false),
            // do not decrease next
            (prev_m, prev_m, prev_n, false),
            // increase match, do not decrease next
            (prev_m + 1, prev_m + 1, prev_n, true),
            // increase match, next
            (prev_m + 2, prev_m + 2, prev_n + 1, true),
        ];
        for (update, w_m, w_n, w_ok) in tests {
            let mut p = Progress {
                _match: prev_m,
                next: prev_n,
                state: Default::default(),
                pending_snapshot: 0,
                recent_active: false,
                probe_sent: false,
                is_learner: false,
                inflights: Default::default(),
            };
            assert_eq!(w_ok, p.maybe_update(update));
            assert_eq!(p._match, w_m);
            assert_eq!(p.next, w_n);
        }
    }

    #[test]
    fn it_progress_maybe_decr() {
        // (state, m, n, rejected, last, w, w_n)
        let tests = vec![
            // state replicate and rejected is not greater than match
            (Replicate, 5, 10, 5, 5, false, 10),
            // state replicate and rejected is not greater that match, it is stale message
            (Replicate, 5, 10, 4, 4, false, 10),
            // state replicate and rejected is greater than match
            // directly decrease to match+1
            (Replicate, 5, 10, 9, 9, true, 6),
            // next - 1 != rejected is always false
            (Probe, 0, 10, 0, 0, false, 10),
            // next - 1 != rejected is always false
            (Probe, 0, 10, 5, 5, false, 10),
            // next > 1 = decremented by 1
            (Probe, 0, 10, 9, 9, true, 9),
            // next > 1 = decremented by 1
            (Probe, 0, 2, 1, 1, true, 1),
            // next <= 1 = reset to 1
            (Probe, 0, 1, 0, 0, true, 1),
            // decrease to min(rejected, last+1)
            (Probe, 0, 10, 9, 2, true, 3),
            // rejected < 1, reset to 1
            (Probe, 0, 10, 9, 0, true, 1),
        ];

        for (state, m, n, rejected, last, w, w_n) in tests {
            let mut p = Progress {
                _match: m,
                next: n,
                state,
                pending_snapshot: 0,
                recent_active: false,
                probe_sent: false,
                is_learner: false,
                inflights: Default::default(),
            };
            assert_eq!(w, p.maybe_decr_to(rejected, last));
            assert_eq!(m, p._match);
            assert_eq!(w_n, p.next);
        }
    }
}
