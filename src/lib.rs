#![doc = include_str!("../README.md")]

use std::{fmt, marker::PhantomData};

use apalis_codec::json::JsonCodec;
use apalis_core::task::Task;
use apalis_sql::context::SqlContext;
use ulid::Ulid;

pub use apalis_sql::config::Config;
pub use apalis_sql::ext::TaskBuilderExt;
pub use surrealdb::{
    Surreal,
    engine::any::{Any, connect},
};

pub use crate::errors::SurrealError;

mod errors;
mod from_row;

const SCHEMA: &str = include_str!("schema.surql");

pub const JOBS_TABLE: &str = "jobs";

pub const WORKERS_TABLE: &str = "workers";

/// The task context stored alongside every job in SurrealDB
pub type SurrealContext = SqlContext<Surreal<Any>>;

/// A task as stored and retrieved from the SurrealDB backend
pub type SurrealTask<Args> = Task<Args, SurrealContext, Ulid>;

/// The compact representation used when serializing task arguments to `bytes`
pub type CompactType = Vec<u8>;

/// Marker fetcher that drives the polling backend for [`SurrealStorage`]
#[derive(Clone, Debug)]
pub struct SurrealFetcher;

/// A storage backend for apalis backed by SurrealDB
#[pin_project::pin_project]
pub struct SurrealStorage<T, C, Fetcher> {
    conn: Surreal<Any>,
    job_type: PhantomData<T>,
    codec: PhantomData<C>,
    config: Config,
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
            fetcher: self.fetcher.clone(),
        }
    }
}

impl SurrealStorage<(), (), ()> {
    /// Define the tables, fields and indexes required by the backend.
    pub async fn setup(conn: &Surreal<Any>) -> Result<(), SurrealError> {
        conn.query(SCHEMA).await?.check()?;
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
            fetcher: SurrealFetcher,
        }
    }
}

impl<T, C, F> SurrealStorage<T, C, F> {
    /// Change the codec used to serialize and deserialize task arguments
    pub fn with_codec<D>(self) -> SurrealStorage<T, D, F> {
        SurrealStorage {
            conn: self.conn,
            job_type: PhantomData,
            codec: PhantomData,
            config: self.config,
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
