#![doc = include_str!("../README.md")]

use std::{fmt, marker::PhantomData};

use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{Backend, BackendExt, TaskStream, codec::Codec, queue::Queue},
    layers::Stack,
    task::Task,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use apalis_sql::context::SqlContext;
use futures::{
    Stream, StreamExt, TryStreamExt,
    stream::{self, BoxStream, select},
};
use ulid::Ulid;

pub use apalis_sql::config::Config;
pub use apalis_sql::ext::TaskBuilderExt;
pub use surrealdb::{
    Surreal,
    engine::any::{Any, connect},
};

pub use crate::ack::{LockTaskLayer, SurrealAck};
pub use crate::errors::SurrealError;
pub use crate::fetcher::{SurrealFetcher, SurrealLiveFetcher, SurrealPollFetcher};
use crate::{
    queries::{
        fetch_next::fetch_next,
        keep_alive::{initial_heartbeat, keep_alive_stream},
        reenqueue_orphaned::reenqueue_orphaned_stream,
    },
    sink::SurrealSink,
};

mod ack;
mod errors;
pub mod fetcher;
mod from_row;
pub mod queries;
pub mod sink;

const SCHEMA: &str = include_str!("schema.surql");

const SCHEMA_VERSION: i64 = 1;

pub const JOB_TABLE: &str = "job";

pub const WORKER_TABLE: &str = "worker";

/// The task context stored alongside every job in SurrealDB
pub type SurrealContext = SqlContext<Surreal<Any>>;

/// A task as stored and retrieved from the SurrealDB backend
pub type SurrealTask<Args> = Task<Args, SurrealContext, Ulid>;

/// The compact representation used when serializing task arguments to `bytes`
pub type CompactType = Vec<u8>;

/// A storage backend for apalis backed by SurrealDB
#[pin_project::pin_project]
pub struct SurrealStorage<T, C, Fetcher> {
    conn: Surreal<Any>,
    job_type: PhantomData<T>,
    codec: PhantomData<C>,
    config: Config,
    #[pin]
    sink: SurrealSink<T, CompactType, C>,
    #[pin]
    fetcher: Fetcher,
}

impl<T, C, F> fmt::Debug for SurrealStorage<T, C, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SurrealStorage")
            .field("conn", &self.conn)
            .field("job_type", &"PhantomData<T>")
            .field("codec", &std::any::type_name::<C>())
            .field("config", &self.config)
            .finish()
    }
}

impl<T, C, F: Clone> Clone for SurrealStorage<T, C, F> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            job_type: PhantomData,
            codec: self.codec,
            config: self.config.clone(),
            sink: self.sink.clone(),
            fetcher: self.fetcher.clone(),
        }
    }
}

impl SurrealStorage<(), (), ()> {
    /// Define the tables, fields and indexes required by the backend.
    pub async fn setup(conn: &Surreal<Any>) -> Result<(), SurrealError> {
        conn.query(SCHEMA).await?.check()?;
        conn.query("UPSERT apalis_meta:schema SET version = $version")
            .bind(("version", SCHEMA_VERSION))
            .await?
            .check()?;
        Ok(())
    }
}

impl<T> SurrealStorage<T, (), ()> {
    /// Create a new storage using the queue named after the task type
    #[must_use]
    pub fn new(conn: &Surreal<Any>) -> SurrealStorage<T, JsonCodec<CompactType>, SurrealFetcher> {
        Self::new_with_config(conn, &Config::new(std::any::type_name::<T>()))
    }

    /// Create a new storage bound to a specific queue
    #[must_use]
    pub fn new_in_queue(
        conn: &Surreal<Any>,
        queue: &str,
    ) -> SurrealStorage<T, JsonCodec<CompactType>, SurrealFetcher> {
        Self::new_with_config(conn, &Config::new(queue))
    }

    /// Create a new storage from an explicit [`Config`]
    #[must_use]
    pub fn new_with_config(
        conn: &Surreal<Any>,
        config: &Config,
    ) -> SurrealStorage<T, JsonCodec<CompactType>, SurrealFetcher> {
        SurrealStorage {
            conn: conn.clone(),
            job_type: PhantomData,
            codec: PhantomData,
            config: config.clone(),
            sink: SurrealSink::new(conn, config),
            fetcher: SurrealFetcher,
        }
    }

    /// Create a storage that reacts to live-query notifications, needs a ws or embedded connection
    #[must_use]
    pub fn new_with_events(
        conn: &Surreal<Any>,
        config: &Config,
    ) -> SurrealStorage<T, JsonCodec<CompactType>, SurrealLiveFetcher> {
        SurrealStorage {
            conn: conn.clone(),
            job_type: PhantomData,
            codec: PhantomData,
            config: config.clone(),
            sink: SurrealSink::new(conn, config),
            fetcher: SurrealLiveFetcher::new(conn),
        }
    }
}

impl<T, C, F> SurrealStorage<T, C, F> {
    /// Change the codec used to serialize and deserialize task arguments
    pub fn with_codec<D>(self) -> SurrealStorage<T, D, F> {
        let sink = SurrealSink::new(&self.conn, &self.config);
        SurrealStorage {
            conn: self.conn,
            job_type: PhantomData,
            codec: PhantomData,
            config: self.config,
            sink,
            fetcher: self.fetcher,
        }
    }

    /// Get the config used by the storage
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get the SurrealDB client used by the storage
    #[must_use]
    pub fn client(&self) -> &Surreal<Any> {
        &self.conn
    }
}

impl<Args, Decode, F> SurrealStorage<Args, Decode, F> {
    fn heartbeat_stream(&self, worker: &WorkerContext) -> BoxStream<'static, Result<(), SurrealError>> {
        let keep_alive = keep_alive_stream(self.conn.clone(), self.config.clone(), worker.clone());
        let reenqueue = reenqueue_orphaned_stream(
            self.conn.clone(),
            self.config.clone(),
            *self.config.keep_alive(),
        )
        .map_ok(|_| ());
        select(keep_alive, reenqueue).boxed()
    }

    fn middleware_stack(&self) -> Stack<LockTaskLayer, AcknowledgeLayer<SurrealAck>> {
        let lock = LockTaskLayer::new(self.conn.clone());
        let ack = AcknowledgeLayer::new(SurrealAck::new(self.conn.clone()));
        Stack::new(lock, ack)
    }
}

impl<Args, Decode: Send + 'static, F> SurrealStorage<Args, Decode, F> {
    fn poll_default(
        self,
        worker: &WorkerContext,
    ) -> impl Stream<Item = Result<Option<SurrealTask<CompactType>>, SurrealError>> + Send + 'static
    {
        let conn = self.conn.clone();
        let config = self.config.clone();
        let registered = worker.clone();
        let register = stream::once(async move {
            initial_heartbeat(&conn, &config, &registered, "SurrealStorage")
                .await
                .map(|()| None::<SurrealTask<CompactType>>)
        });
        register.chain(SurrealPollFetcher::<CompactType, Decode>::new(
            &self.conn,
            &self.config,
            worker,
        ))
    }
}

impl<Args, Decode: Send + 'static> SurrealStorage<Args, Decode, SurrealLiveFetcher> {
    fn poll_with_listener(
        self,
        worker: &WorkerContext,
    ) -> impl Stream<Item = Result<Option<SurrealTask<CompactType>>, SurrealError>> + Send + 'static
    {
        let worker = worker.clone();
        let reg_conn = self.conn.clone();
        let reg_config = self.config.clone();
        let reg_worker = worker.clone();
        let register = stream::once(async move {
            initial_heartbeat(&reg_conn, &reg_config, &reg_worker, "SurrealStorageWithEvents")
                .await
                .map(|()| None::<SurrealTask<CompactType>>)
        });

        let eager_fetcher: SurrealPollFetcher<CompactType, Decode> =
            SurrealPollFetcher::new(&self.conn, &self.config, &worker);

        let fetch_conn = self.conn.clone();
        let fetch_config = self.config.clone();
        let buffer_size = self.config.buffer_size();
        let lazy_fetcher = self
            .fetcher
            .ready_chunks(buffer_size)
            .then(move |_| {
                let conn = fetch_conn.clone();
                let config = fetch_config.clone();
                let worker = worker.clone();
                async move { fetch_next(&conn, &config, &worker).await }
            })
            .flat_map(|res| match res {
                Ok(tasks) => stream::iter(tasks).map(Ok).boxed(),
                Err(e) => stream::iter(vec![Err(e)]).boxed(),
            })
            .map(|res| res.map(Some));

        register.chain(select(lazy_fetcher, eager_fetcher))
    }
}

impl<Args, Decode> Backend for SurrealStorage<Args, Decode, SurrealFetcher>
where
    Args: Send + Unpin + 'static,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;
    type IdType = Ulid;
    type Context = SurrealContext;
    type Error = SurrealError;
    type Stream = TaskStream<SurrealTask<Args>, SurrealError>;
    type Beat = BoxStream<'static, Result<(), SurrealError>>;
    type Layer = Stack<LockTaskLayer, AcknowledgeLayer<SurrealAck>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        self.heartbeat_stream(worker)
    }

    fn middleware(&self) -> Self::Layer {
        self.middleware_stack()
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        self.poll_default(worker)
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

impl<Args, Decode: Send + 'static> BackendExt for SurrealStorage<Args, Decode, SurrealFetcher>
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
        self.poll_default(worker).boxed()
    }
}

impl<Args, Decode> Backend for SurrealStorage<Args, Decode, SurrealLiveFetcher>
where
    Args: Send + Unpin + 'static,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;
    type IdType = Ulid;
    type Context = SurrealContext;
    type Error = SurrealError;
    type Stream = TaskStream<SurrealTask<Args>, SurrealError>;
    type Beat = BoxStream<'static, Result<(), SurrealError>>;
    type Layer = Stack<LockTaskLayer, AcknowledgeLayer<SurrealAck>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        self.heartbeat_stream(worker)
    }

    fn middleware(&self) -> Self::Layer {
        self.middleware_stack()
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        self.poll_with_listener(worker)
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

impl<Args, Decode: Send + 'static> BackendExt for SurrealStorage<Args, Decode, SurrealLiveFetcher>
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
        self.poll_with_listener(worker).boxed()
    }
}
