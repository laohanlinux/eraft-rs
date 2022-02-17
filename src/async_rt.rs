use std::future::Future;
use tokio::time::error::Elapsed;
use tokio::task;
use tokio::time::{self, Duration};
use tokio_global::tokio::runtime::Handle;

pub(crate) fn wait_timeout<F>(d: Duration, fut: F) -> Result<F::Output, Elapsed>
    where F: Future + Send + 'static, F::Output: Send + 'static
{
    task::block_in_place(move || {
        Handle::current().block_on(async move {
            time::timeout(d, fut).await
        })
    })
}

pub(crate) fn sleep(d: Duration) {
    task::block_in_place(move || {
        Handle::current().block_on(async move {
            time::sleep(d).await
        });
    });
}

pub(crate) fn wait<F>(fut: F) -> F::Output
    where F: Future + Send + 'static, F::Output: Send + 'static {
    tokio::task::block_in_place(move || {
        Handle::current().block_on(async move {
            fut.await
        })
    })
}

#[test]
fn it_works() {}