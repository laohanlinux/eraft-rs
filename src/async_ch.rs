use std::sync::Arc;
use std::sync::mpsc::RecvError;
use futures::SinkExt;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::mpsc::error::{SendError, TryRecvError};
use crate::node::SafeResult;

pub(crate) struct Channel<T> {
    rx: Option<Receiver<T>>,
    tx: Option<Sender<T>>,
}

impl<T> Channel<T> {
    fn new(n: usize) -> Self {
        let (tx, rx) = channel(n);
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

    async fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if let Some(rx) = &mut self.rx {
            return rx.try_recv();
        }
        Err(TryRecvError::Empty)
    }

    async fn recv(&mut self) -> Option<T> {
        let rx = self.rx.as_mut().unwrap();
        rx.recv().await
    }

    fn tx(&self) -> Sender<T> {
        self.tx.as_ref().unwrap().clone()
    }
}


pub(crate) struct FutChannels {
    ch: Channel<Sender<SafeResult<()>>>,
}

impl FutChannels {
    fn new(n: usize) -> Self {
        FutChannels {
            ch: Channel::new(n),
        }
    }

    async fn recv(&mut self) -> Option<Sender<SafeResult<()>>> {
        self.ch.recv().await
    }

    fn tx(&self) -> Sender<Sender<SafeResult<()>>> {
        self.ch.tx()
    }
}


#[tokio::test]
async fn it_works() {
    let mut ch = Channel::new(1);
    let tx = ch.tx();

    let mut pm = Channel::new(1);
    let mut pm_tx = pm.tx();
    tokio::spawn(async move {
        pm_tx.send(tx).await;
    });
    let tx_ch = pm.recv().await.unwrap();
    tx_ch.send(100).await;

    let last = ch.recv().await.unwrap();
    println!("last: {}", last);
}