use crate::{
    async_ch::MsgWithResult,
    node::{InnerChan, Ready},
    raftpb::raft::{ConfChangeV2, ConfState, Message},
};

#[derive(Clone)]
pub(crate) enum Event {
    Prop(MsgWithResult),
    Msg(Message),
    Conf(ConfChangeV2),
    ConfState(ConfState),
    Ready(Ready),
    Advance,
    Ticker,
    Status,
}

#[derive(Default)]
pub(crate) struct EventChannel {
    inner: InnerChan<Event>,
}

impl EventChannel {
    pub(crate) fn channel(&self) -> InnerChan<Event> {
        self.inner.clone()
    }
}
