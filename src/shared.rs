use std::{
    collections::{HashMap, hash_map::Entry},
    fmt,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{Backend, BackendExt, TaskStream, codec::Codec, queue::Queue, shared::MakeShared},
    layers::Stack,
    timer::Delay,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use futures::{
    FutureExt, SinkExt, Stream, StreamExt,
    channel::mpsc::{self, Receiver, Sender},
    future::{BoxFuture, Shared},
    lock::Mutex,
    stream::{self, BoxStream, select},
};
use surrealdb::{Surreal, engine::any::Any};
use ulid::Ulid;

use crate::{
    CompactType, Config, LockTaskLayer, SurrealAck, SurrealContext, SurrealError,
    SurrealLiveFetcher, SurrealPollFetcher, SurrealStorage, SurrealTask,
    queries::{fetch_next_shared::fetch_next_shared, keep_alive::initial_heartbeat},
    sink::SurrealSink,
};

type Registry = Arc<Mutex<HashMap<String, Sender<SurrealTask<CompactType>>>>>;

/// A SurrealDB storage whose single connection is shared across workers of different queues
pub struct SharedSurrealStorage<Decode> {
    conn: Arc<Surreal<Any>>,
    registry: Registry,
    drive: Shared<BoxFuture<'static, ()>>,
    _marker: PhantomData<Decode>,
}

impl<Decode> Clone for SharedSurrealStorage<Decode> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            registry: self.registry.clone(),
            drive: self.drive.clone(),
            _marker: PhantomData,
        }
    }
}

impl<Decode> fmt::Debug for SharedSurrealStorage<Decode> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedSurrealStorage")
            .field("conn", &self.conn)
            .field("codec", &std::any::type_name::<Decode>())
            .finish()
    }
}

impl<Decode> SharedSurrealStorage<Decode> {
    /// Get the SurrealDB client shared across the registered workers
    #[must_use]
    pub fn client(&self) -> &Surreal<Any> {
        &self.conn
    }
}

impl SharedSurrealStorage<JsonCodec<CompactType>> {
    /// Create a shared storage over an existing SurrealDB connection
    #[must_use]
    pub fn new(conn: &Arc<Surreal<Any>>) -> Self {
        Self::new_with_codec(conn)
    }

    /// Create a shared storage over an existing connection using a specific codec
    #[must_use]
    pub fn new_with_codec<C>(conn: &Arc<Surreal<Any>>) -> SharedSurrealStorage<C> {
        let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
        let fallback = *Config::default().keep_alive();
        let drive = drive_registry(conn.clone(), registry.clone(), fallback)
            .boxed()
            .shared();
        SharedSurrealStorage {
            conn: conn.clone(),
            registry,
            drive,
            _marker: PhantomData,
        }
    }
}

// fan claimed tasks out to the perqueue channels, waking on live notifications or a fallback tick
async fn drive_registry(conn: Arc<Surreal<Any>>, registry: Registry, fallback: Duration) {
    let live = SurrealLiveFetcher::new(&conn);
    let ticks = stream::unfold((), move |()| async move {
        Delay::new(fallback).await;
        Some(((), ()))
    });
    let chunk = registry.try_lock().map_or(1, |r| r.len().max(1));
    select(live, ticks)
        .ready_chunks(chunk)
        .for_each(move |_| {
            let conn = conn.clone();
            let registry = registry.clone();
            async move {
                let queues: Vec<String> = registry.lock().await.keys().cloned().collect();
                if queues.is_empty() {
                    return;
                }
                let limit = std::cmp::max(10, queues.len()) as i64;
                match fetch_next_shared(&conn, &queues, limit).await {
                    Ok(tasks) => {
                        let mut registry = registry.lock().await;
                        for task in tasks {
                            let Some(queue) = task.parts.ctx.queue().clone() else {
                                continue;
                            };
                            // a task whose channel is gone is dropped, its queue is no longer registered
                            if let Some(tx) = registry.get_mut(&queue) {
                                let _ = tx.send(task).await;
                            }
                        }
                    }
                    Err(err) => log::error!("Shared driver failed to claim tasks: {err}"),
                }
            }
        })
        .await;
}

#[derive(Debug, thiserror::Error)]
pub enum SharedSurrealError {
    #[error("Namespace {0} already exists")]
    NamespaceExists(String),
    #[error("Could not acquire registry lock")]
    RegistryLocked,
}

impl<Args, Decode: Codec<Args, Compact = CompactType>> MakeShared<Args>
    for SharedSurrealStorage<Decode>
{
    type Backend = SurrealStorage<Args, Decode, SharedFetcher<CompactType>>;
    type Config = Config;
    type MakeError = SharedSurrealError;

    fn make_shared(&mut self) -> Result<Self::Backend, Self::MakeError>
    where
        Self::Config: Default,
    {
        self.make_shared_with_config(Config::new(std::any::type_name::<Args>()))
    }

    fn make_shared_with_config(
        &mut self,
        config: Self::Config,
    ) -> Result<Self::Backend, Self::MakeError> {
        let (tx, rx) = mpsc::channel(config.buffer_size());
        let mut registry = self
            .registry
            .try_lock()
            .ok_or(SharedSurrealError::RegistryLocked)?;
        // a plain insert would clobber the registered channel before the duplicate is rejected
        match registry.entry(config.queue().to_string()) {
            Entry::Occupied(_) => {
                return Err(SharedSurrealError::NamespaceExists(
                    config.queue().to_string(),
                ));
            }
            Entry::Vacant(entry) => {
                entry.insert(tx);
            }
        }
        let sink = SurrealSink::new(&self.conn, &config);
        Ok(SurrealStorage {
            conn: self.conn.clone(),
            job_type: PhantomData,
            codec: PhantomData,
            config,
            sink,
            fetcher: SharedFetcher {
                poller: self.drive.clone(),
                receiver: Arc::new(std::sync::Mutex::new(rx)),
            },
        })
    }
}

/// Fetcher that drives the shared registry and yields the tasks routed to one queue
pub struct SharedFetcher<Compact> {
    poller: Shared<BoxFuture<'static, ()>>,
    receiver: Arc<std::sync::Mutex<Receiver<SurrealTask<Compact>>>>,
}

impl<Compact> Clone for SharedFetcher<Compact> {
    fn clone(&self) -> Self {
        Self {
            poller: self.poller.clone(),
            receiver: self.receiver.clone(),
        }
    }
}

impl<Compact> fmt::Debug for SharedFetcher<Compact> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedFetcher").finish()
    }
}

impl<Compact> Stream for SharedFetcher<Compact> {
    type Item = SurrealTask<Compact>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // polling the shared driver keeps it running for every registered queue
        let _ = this.poller.poll_unpin(cx);
        match this.receiver.lock() {
            Ok(mut receiver) => receiver.poll_next_unpin(cx),
            Err(_) => Poll::Ready(None),
        }
    }
}

impl<Args, Decode> Backend for SurrealStorage<Args, Decode, SharedFetcher<CompactType>>
where
    Args: Send + Sync + Unpin + 'static,
    Decode: Codec<Args, Compact = CompactType> + Send + Sync + Unpin + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;
    type IdType = Ulid;
    type Context = SurrealContext;
    type Error = SurrealError;
    type Stream = TaskStream<SurrealTask<Args>, SurrealError>;
    type Beat = BoxStream<'static, Result<(), SurrealError>>;
    // ack must wrap lock so it snapshots the lock_by the lock layer sets, the shared claim leaves it unset
    type Layer = Stack<AcknowledgeLayer<SurrealAck>, LockTaskLayer>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        self.heartbeat_stream(worker)
    }

    fn middleware(&self) -> Self::Layer {
        let lock = LockTaskLayer::new(self.conn.clone());
        let ack = AcknowledgeLayer::new(SurrealAck::new(self.conn.clone()));
        Stack::new(ack, lock)
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        self.poll_shared(worker)
            .map(|a| match a {
                Ok(Some(task)) => Ok(Some(
                    task.try_map(|t| Decode::decode(&t))
                        .map_err(|e| SurrealError::Decode(e.into()))?,
                )),
                Ok(None) => Ok(None),
                Err(e) => Err(e),
            })
            .boxed()
    }
}

impl<Args, Decode: Send + 'static> BackendExt
    for SurrealStorage<Args, Decode, SharedFetcher<CompactType>>
where
    Self: Backend<Args = Args, IdType = Ulid, Context = SurrealContext, Error = SurrealError>,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
    Args: Send + Unpin + 'static,
{
    type Codec = Decode;
    type Compact = CompactType;
    type CompactStream = TaskStream<SurrealTask<CompactType>, SurrealError>;

    fn get_queue(&self) -> Queue {
        self.config.queue().clone()
    }

    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        self.poll_shared(worker).boxed()
    }
}

impl<Args, Decode: Send + 'static> SurrealStorage<Args, Decode, SharedFetcher<CompactType>> {
    fn poll_shared(
        self,
        worker: &WorkerContext,
    ) -> impl Stream<Item = Result<Option<SurrealTask<CompactType>>, SurrealError>> + Send + 'static
    {
        let conn = self.conn.clone();
        let config = self.config.clone();
        let registered = worker.clone();
        let register = stream::once(async move {
            initial_heartbeat(&conn, &config, &registered, "SharedSurrealStorage")
                .await
                .map(|()| None::<SurrealTask<CompactType>>)
        });
        let eager = SurrealPollFetcher::<CompactType, Decode>::new(&self.conn, &self.config, worker)
            .boxed();
        let lazy = self.fetcher.map(|task| Ok(Some(task))).boxed();
        register.chain(select(lazy, eager))
    }
}
