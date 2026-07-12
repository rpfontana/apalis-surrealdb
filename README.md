# apalis-surrealdb

[Background task processing for Rust](https://apalis.dev/), powered by [`apalis`](https://crates.io/crates/apalis) and [SurrealDB](https://surrealdb.com/)


## Features

- **Persistent job queue**: jobs live in SurrealDB, so a restart never loses work and no external broker is needed
- **Atomic claims**: each poll claims a batch of due jobs in one transaction; optimistic-concurrency conflicts are retried, so two workers never run the same job
- **Realtime fetching**: subscribe to SurrealDB live queries with `new_with_events` to pick up new jobs as they land, with polling as a fallback
- **Scheduled & delayed jobs**: use `run_after` to enqueue work that only becomes due at or after a future point in time
- **Priority queues**: assign an integer priority so high-urgency jobs are claimed first
- **Idempotency keys**: attach a key to deduplicate jobs; a duplicate is skipped inside the insert transaction
- **Automatic retries**: a failed job with attempts left is picked up again on the next claim; no separate scheduler
- **Heartbeat & orphan recovery**: workers heartbeat periodically, and jobs stranded by a dead worker are re-enqueued
- **Shared storage**: multiplex several job types over a single SurrealDB connection with `SharedSurrealStorage`
- **Custom codecs**: swap the payload codec with `with_codec`; JSON is the default
- **Observability**: inspect jobs, workers, queues and metrics through the apalis board traits


## Storage Types

| Type                                                                                                                | Description                                                            |
| ------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| [`SurrealStorage`](https://docs.rs/apalis-surrealdb/latest/apalis_surrealdb/struct.SurrealStorage.html)             | Polling-based storage; the default built by `new`                    |
| [`SurrealStorage::new_with_events`](https://docs.rs/apalis-surrealdb/latest/apalis_surrealdb/struct.SurrealStorage.html#method.new_with_events) | Realtime storage driven by SurrealDB live queries, with polling fallback |
| [`SharedSurrealStorage`](https://docs.rs/apalis-surrealdb/latest/apalis_surrealdb/shared/struct.SharedSurrealStorage.html) | Shared storage supporting multiple job types over one connection     |


## Examples

### Quickstart

Connect, run `setup` once to define the schema, push a job and run a worker:

```rust,no_run
use apalis_core::backend::TaskSink;
use apalis_core::error::BoxDynError;
use apalis_core::worker::builder::WorkerBuilder;
use apalis_core::worker::context::WorkerContext;
use apalis_surrealdb::{SurrealStorage, connect};

#[tokio::main]
async fn main() {
    let conn = connect("mem://").await.unwrap();
    conn.use_ns("apalis").use_db("apalis").await.unwrap();
    SurrealStorage::setup(&conn).await.unwrap();

    let mut backend = SurrealStorage::new(&conn);
    backend.push(42u32).await.unwrap();

    async fn email(job: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
        assert_eq!(job, 42);
        worker.stop()?;
        Ok(())
    }

    let worker = WorkerBuilder::new("worker-1").backend(backend).build(email);
    worker.run().await.unwrap();
}
```


### Realtime Fetching

`new_with_events` drives the worker from a SurrealDB live query on the `job` table and falls back to
polling when a notification is missed. Live queries run over the WebSocket and embedded engines, not
over the HTTP protocol

```rust,no_run
use apalis_core::error::BoxDynError;
use apalis_core::worker::builder::WorkerBuilder;
use apalis_core::worker::context::WorkerContext;
use apalis_surrealdb::{Config, SurrealStorage, connect};

#[tokio::main]
async fn main() {
    let conn = connect("ws://localhost:8000").await.unwrap();
    conn.use_ns("apalis").use_db("apalis").await.unwrap();
    SurrealStorage::setup(&conn).await.unwrap();

    let backend = SurrealStorage::new_with_events(&conn, &Config::new("emails"));

    async fn email(job: u32, worker: WorkerContext) -> Result<(), BoxDynError> {
        worker.stop()?;
        Ok(())
    }

    let worker = WorkerBuilder::new("worker-1").backend(backend).build(email);
    worker.run().await.unwrap();
}
```

### Shared Storage

`SharedSurrealStorage` fans a single connection out to several job types, each with its own worker:

```rust,no_run
use apalis_core::backend::{TaskSink, shared::MakeShared};
use apalis_core::error::BoxDynError;
use apalis_core::task::task_id::TaskId;
use apalis_core::worker::builder::WorkerBuilder;
use apalis_core::worker::context::WorkerContext;
use apalis_surrealdb::{SharedSurrealStorage, SurrealStorage, connect};

#[tokio::main]
async fn main() {
    let conn = connect("mem://").await.unwrap();
    conn.use_ns("apalis").use_db("apalis").await.unwrap();
    SurrealStorage::setup(&conn).await.unwrap();

    let mut shared = SharedSurrealStorage::new(&conn);
    let mut emails = shared.make_shared().unwrap();
    let mut reports = shared.make_shared().unwrap();

    emails.push("hello@example.com".to_owned()).await.unwrap();
    reports.push(42u32).await.unwrap();

    async fn run<T, I>(_job: T, _id: TaskId<I>, worker: WorkerContext) -> Result<(), BoxDynError> {
        worker.stop()?;
        Ok(())
    }

    let emails_worker = WorkerBuilder::new("emails").backend(emails).build(run);
    let reports_worker = WorkerBuilder::new("reports").backend(reports).build(run);
    tokio::try_join!(emails_worker.run(), reports_worker.run()).unwrap();
}
```

## Observability

You can track your jobs using [apalis-board](https://github.com/apalis-dev/apalis-board).
![Task](https://github.com/apalis-dev/apalis-board/raw/main/screenshots/task.png)


## License

Licensed under the [MIT License](https://github.com/apalis-dev/apalis-surrealdb/blob/main/LICENSE).
