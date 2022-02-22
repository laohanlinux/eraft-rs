use std::sync::Arc;
use std::time::Duration;
use futures::SinkExt;
use async_channel::{bounded, Sender, Receiver, SendError, RecvError, TryRecvError};
use env_logger::Env;
use futures::task::SpawnExt;
use tokio::select;
use crate::node::SafeResult;
use crate::raftpb::raft::Message;

#[derive(Clone)]
pub(crate) struct Channel<T> {
    rx: Option<Receiver<T>>,
    tx: Option<Sender<T>>,
}

impl<T> Channel<T> {
    pub(crate) fn new(n: usize) -> Self {
        let (tx, rx) = bounded(n);
        Channel {
            rx: Some(rx),
            tx: Some(tx),
        }
    }
    async fn try_send(&self, msg: T) -> Result<(), SendError<T>> {
        if let Some(tx) = &self.tx {
            return tx.send(msg).await;
        }
        Ok(())
    }

    pub(crate) async fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(rx) = &self.rx {
            return rx.try_recv();
        }
        Err(TryRecvError::Empty)
    }

    pub(crate) async fn recv(&self) -> Result<T, async_channel::RecvError> {
        let rx = self.rx.as_ref().unwrap();
        rx.recv().await
    }

    pub(crate) async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        let tx = self.tx.as_ref().unwrap();
        tx.send(msg).await
    }

    pub(crate) fn tx(&self) -> Sender<T> {
        self.tx.as_ref().unwrap().clone()
    }

    pub(crate) fn take_tx(&mut self) -> Option<Sender<T>> {
        self.tx.take()
    }
}

#[derive(Clone)]
pub(crate) struct MsgWithResult {
    m: Option<Message>,
    ch: Option<Sender<SafeResult<()>>>,
}

impl Default for MsgWithResult {
    fn default() -> Self {
        MsgWithResult {
            m: None,
            ch: None,
        }
    }
}

impl MsgWithResult {
    pub fn new() -> Self {
        MsgWithResult {
            m: None,
            ch: None,
        }
    }

    pub fn new_with_msg(msg: Message) -> Self {
        MsgWithResult {
            m: Some(msg),
            ch: None,
        }
    }

    pub fn new_with_channel(tx: Sender<SafeResult<()>>, msg: Message) -> Self {
        MsgWithResult {
            m: Some(msg),
            ch: Some(tx),
        }
    }

    pub fn get_msg(&self) -> Option<&Message> {
        self.m.as_ref()
    }

    pub(crate) async fn notify(&self, msg: SafeResult<()>) {
        if let Some(sender) = &self.ch {
            sender.send(msg).await;
        }
    }

    pub(crate) async fn notify_and_close(&mut self, msg: SafeResult<()>) {
        if let Some(sender) = self.ch.take() {
            sender.send(msg).await;
            sender.close();
        }
    }
}
