use std::{
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{
    FutureExt, Sink,
    future::{BoxFuture, Shared},
};
use surrealdb::{Surreal, engine::any::Any};

use crate::{
    CompactType, Config, SurrealError, SurrealStorage, SurrealTask, queries::push_tasks::push_tasks,
};

type FlushFuture = BoxFuture<'static, Result<(), Arc<SurrealError>>>;

/// Buffered sink that flushes queued tasks into the SurrealDB backend
#[pin_project::pin_project]
#[derive(Debug)]
pub struct SurrealSink<Args, Compact, Codec> {
    conn: Arc<Surreal<Any>>,
    config: Config,
    buffer: Vec<SurrealTask<Compact>>,
    #[pin]
    flush_future: Option<Shared<FlushFuture>>,
    _marker: PhantomData<(Args, Codec)>,
}

impl<Args, Compact, Codec> SurrealSink<Args, Compact, Codec> {
    /// Create a sink that flushes tasks to the given queue
    #[must_use]
    pub fn new(conn: &Arc<Surreal<Any>>, config: &Config) -> Self {
        Self {
            conn: conn.clone(),
            config: config.clone(),
            buffer: Vec::new(),
            flush_future: None,
            _marker: PhantomData,
        }
    }
}

impl<Args, Compact, Codec> Clone for SurrealSink<Args, Compact, Codec> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            config: self.config.clone(),
            buffer: Vec::new(),
            flush_future: None,
            _marker: PhantomData,
        }
    }
}

impl<Args, C, Fetcher> Sink<SurrealTask<CompactType>> for SurrealStorage<Args, C, Fetcher>
where
    Args: Send + Sync + 'static,
{
    type Error = SurrealError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: SurrealTask<CompactType>) -> Result<(), Self::Error> {
        self.project().sink.buffer.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();

        if this.sink.flush_future.is_none() && this.sink.buffer.is_empty() {
            return Poll::Ready(Ok(()));
        }

        if this.sink.flush_future.is_none() && !this.sink.buffer.is_empty() {
            let conn = this.sink.conn.clone();
            let config = this.sink.config.clone();
            let buffer = std::mem::take(&mut this.sink.buffer);
            let flush = async move { push_tasks(&conn, &config, buffer).await.map_err(Arc::new) };
            this.sink.flush_future = Some((Box::pin(flush) as FlushFuture).shared());
        }

        if let Some(mut flush) = this.sink.flush_future.take() {
            match flush.poll_unpin(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                Poll::Ready(Err(err)) => Poll::Ready(Err(Arc::into_inner(err).unwrap_or_else(
                    || {
                        SurrealError::Database(surrealdb::Error::internal(
                            "push flush future was unexpectedly shared".to_owned(),
                        ))
                    },
                ))),
                Poll::Pending => {
                    this.sink.flush_future = Some(flush);
                    Poll::Pending
                }
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}
