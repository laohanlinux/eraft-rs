// Copyright 2016 The etcd Authors
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

use crate::raft::ReadOnlyOption;
use crate::raftpb::raft::{Message, MessageType};
use std::borrow::Cow;
use std::collections::HashMap;

// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready. it's also caller's duty to differentiate if this
// state is what it requests through request_ctx, eg. given a unique id as
// request_ctx
#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct ReadState {
    pub index: u64,
    pub request_ctx: Vec<u8>,
}

#[derive(Default, Debug, Clone)]
pub(crate) struct ReadIndexStatus {
    pub req: Message,
    pub index: u64,
    // NB: this never records 'false', but it's more convenient to use this
    // instead of a HashMap<u64,bool> due to the API of quorum.VoteResult. If
    // this becomes performance sensitive enough (doubtful), quorum.VoteResult
    // can change to an API that is closer to that of CommittedIndex.
    pub acks: HashMap<u64, bool>,
}

#[derive(Clone)]
pub struct ReadOnly {
    pub(crate) option: ReadOnlyOption,
    pub(crate) pending_read_index: HashMap<Vec<u8>, ReadIndexStatus>,
    pub(crate) read_index_queue: Vec<Vec<u8>>,
}

impl ReadOnly {
    pub(crate) fn new(option: ReadOnlyOption) -> Self {
        ReadOnly {
            option,
            pending_read_index: Default::default(),
            read_index_queue: vec![],
        }
    }

    // add_request adds a record only request into readonly struct.
    // `index` is the commit index of the raft state machine when it received
    // the read only request.
    // `m` is the original read only request message from the local or remote node.
    pub(crate) fn add_request(&mut self, index: u64, m: Message) {
        let s = m.get_entries()[0].get_Data().to_vec();
        let read_index_status = ReadIndexStatus {
            req: m,
            index,
            acks: Default::default(),
        };
        self.pending_read_index
            .entry(s.clone())
            .or_insert(read_index_status);
        self.read_index_queue.push(s);
    }

    // recv_ack notifies the read_only struct that the raft state machine received
    // an acknowledgment of the heartbeat that attached with the read only request
    // context.
    pub(crate) fn recv_ack(&mut self, id: u64, context: Vec<u8>) -> Option<&HashMap<u64, bool>> {
        if let Some(mut entry) = self.pending_read_index.get_mut(&context) {
            entry.acks.insert(id, true);
            return Some(&entry.acks);
        }
        None
    }

    // advances advances the read only request queue kept by the read_only struct.
    // It dequeues the requests until it finds the read only request that has
    // the same context as the given `m`.
    pub(crate) fn advance(&mut self, m: Message) -> Vec<ReadIndexStatus> {
        let mut rss: Vec<ReadIndexStatus> = vec![];
        let mut i = 0;
        let mut found = false;
        for ok_ctx in &self.read_index_queue {
            i += 1;
            let rs = self.pending_read_index.get(ok_ctx);
            if rs.is_none() {
                panic!("cannot find corresponding read state from pending map");
            }
            let rs = rs.unwrap();
            rss.push(rs.clone());
            if ok_ctx.as_slice() == m.get_context() {
                found = true;
                break;
            }
        }
        if found {
            self.read_index_queue.drain(..i);
            rss.iter().for_each(|rs| {
                self.pending_read_index
                    .remove(rs.req.get_entries()[0].get_Data());
            });
            return rss;
        }
        vec![]
    }

    // last_pending_request returns the context of the last pending read only
    // request in readonly struct
    pub(crate) fn last_pending_request(&self) -> Option<Vec<u8>> {
        self.read_index_queue.last().map(|v| v.clone())
    }
}
