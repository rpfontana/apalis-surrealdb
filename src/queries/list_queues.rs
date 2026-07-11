use std::collections::BTreeMap;

use apalis_core::backend::{BackendExt, ListQueues, QueueInfo};
use surrealdb::types::{Number, SurrealValue};
use ulid::Ulid;

use crate::{
    CompactType, SurrealContext, SurrealError, SurrealStorage,
    queries::metrics::{OverviewRow, build_statistics},
};

const LIST_QUEUES: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/list_queues.surql"
));

const SECS_PER_DAY: i64 = 86_400;

const ACTIVITY_DAYS: i64 = 7;

#[derive(Debug, SurrealValue)]
struct QueueCountsRow {
    queue: String,
    total: i64,
    pending: i64,
    running: i64,
    failed: i64,
    done: i64,
    killed: i64,
    backlog: i64,
    past_hour: i64,
    finished: i64,
    dur_secs: i64,
}

impl From<QueueCountsRow> for OverviewRow {
    fn from(row: QueueCountsRow) -> Self {
        OverviewRow {
            total: row.total,
            pending: row.pending,
            running: row.running,
            failed: row.failed,
            done: row.done,
            killed: row.killed,
            backlog: row.backlog,
            past_hour: row.past_hour,
            finished: row.finished,
            dur_secs: row.dur_secs,
        }
    }
}

// math::min can come back as a float, so decode through Number
#[derive(Debug, SurrealValue)]
struct QueueOldestRow {
    queue: String,
    oldest_unix: Option<Number>,
}

#[derive(Debug, SurrealValue)]
struct ActivityRow {
    queue: String,
    day: i64,
    done_count: i64,
}

#[derive(Debug, SurrealValue)]
struct QueueWorkerRow {
    name: String,
    queue: String,
}

impl<Args, D, F> ListQueues for SurrealStorage<Args, D, F>
where
    Self: BackendExt<
            Context = SurrealContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = SurrealError,
        >,
{
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Self::Error>> + Send {
        let conn = self.conn.clone();
        async move {
            let mut response = conn.query(LIST_QUEUES).await?;
            let counts: Vec<QueueCountsRow> = response.take(0)?;
            let oldest: Vec<QueueOldestRow> = response.take(1)?;
            let activity: Vec<ActivityRow> = response.take(2)?;
            let workers: Vec<QueueWorkerRow> = response.take(3)?;

            let oldest: BTreeMap<String, Option<i64>> = oldest
                .into_iter()
                .map(|row| (row.queue, row.oldest_unix.and_then(|n| n.to_int())))
                .collect();
            let today = chrono::Utc::now().timestamp() / SECS_PER_DAY;
            let mut daily: BTreeMap<String, Vec<usize>> = BTreeMap::new();
            for row in activity {
                let offset = today - row.day / SECS_PER_DAY;
                if (0..ACTIVITY_DAYS).contains(&offset) {
                    let days = daily
                        .entry(row.queue)
                        .or_insert_with(|| vec![0; ACTIVITY_DAYS as usize]);
                    days[(ACTIVITY_DAYS - 1 - offset) as usize] = row.done_count as usize;
                }
            }

            let mut queues: BTreeMap<String, QueueInfo> = counts
                .into_iter()
                .map(|row| {
                    let name = row.queue.clone();
                    let stats = build_statistics(&row.into(), oldest.get(&name).copied().flatten());
                    let activity = daily
                        .remove(&name)
                        .unwrap_or_else(|| vec![0; ACTIVITY_DAYS as usize]);
                    (
                        name.clone(),
                        QueueInfo {
                            name,
                            stats,
                            workers: Vec::new(),
                            activity,
                        },
                    )
                })
                .collect();

            for row in workers {
                queues
                    .entry(row.queue.clone())
                    .or_insert_with(|| QueueInfo {
                        name: row.queue,
                        stats: build_statistics(&OverviewRow::default(), None),
                        workers: Vec::new(),
                        activity: vec![0; ACTIVITY_DAYS as usize],
                    })
                    .workers
                    .push(row.name);
            }

            Ok(queues.into_values().collect())
        }
    }
}
