use apalis_core::backend::{BackendExt, Metrics, Statistic};
use apalis_sql::stat_type_from_string;
use surrealdb::types::{Number, SurrealValue};
use ulid::Ulid;

use crate::{CompactType, SurrealContext, SurrealError, SurrealStorage};

const OVERVIEW: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/overview.surql"
));

const OVERVIEW_BY_QUEUE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/overview_by_queue.surql"
));

#[derive(Debug, Default, SurrealValue)]
pub(crate) struct OverviewRow {
    pub(crate) total: i64,
    pub(crate) pending: i64,
    pub(crate) running: i64,
    pub(crate) failed: i64,
    pub(crate) done: i64,
    pub(crate) killed: i64,
    pub(crate) backlog: i64,
    pub(crate) past_hour: i64,
    pub(crate) finished: i64,
    pub(crate) dur_secs: i64,
}

// math::min can come back as a float, so decode through Number
#[derive(Debug, SurrealValue)]
struct OldestRow {
    oldest_unix: Option<Number>,
}

fn stat(title: &str, stat_type: &str, value: String, priority: u64) -> Statistic {
    Statistic {
        title: title.to_owned(),
        stat_type: stat_type_from_string(stat_type),
        value,
        priority: Some(priority),
    }
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

/// Turn raw per-scope aggregates into the core board statistics
pub(crate) fn build_statistics(counts: &OverviewRow, oldest_unix: Option<i64>) -> Vec<Statistic> {
    let success_rate = if counts.total > 0 {
        round2(100.0 * counts.done as f64 / counts.total as f64)
    } else {
        0.0
    };
    let avg_duration = if counts.finished > 0 {
        round2(counts.dur_secs as f64 / counts.finished as f64 / 60.0)
    } else {
        0.0
    };
    vec![
        stat("PENDING_JOBS", "Number", counts.pending.to_string(), 1),
        stat("RUNNING_JOBS", "Number", counts.running.to_string(), 1),
        stat("FAILED_JOBS", "Number", counts.failed.to_string(), 2),
        stat("JOBS_PAST_HOUR", "Number", counts.past_hour.to_string(), 3),
        stat("TOTAL_JOBS", "Number", counts.total.to_string(), 4),
        stat("DONE_JOBS", "Number", counts.done.to_string(), 4),
        stat("KILLED_JOBS", "Number", counts.killed.to_string(), 4),
        stat("SUCCESS_RATE", "Percentage", success_rate.to_string(), 4),
        stat("AVG_JOB_DURATION_MINS", "Decimal", avg_duration.to_string(), 5),
        stat("QUEUE_BACKLOG", "Number", counts.backlog.to_string(), 5),
        stat(
            "OLDEST_PENDING_JOB",
            "Timestamp",
            oldest_unix.unwrap_or_default().to_string(),
            8,
        ),
    ]
}

impl<Args, D, F> Metrics for SurrealStorage<Args, D, F>
where
    Self: BackendExt<
            Context = SurrealContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = SurrealError,
        >,
{
    fn global(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let conn = self.conn.clone();
        async move {
            let mut response = conn.query(OVERVIEW).await?;
            let counts = response
                .take::<Vec<OverviewRow>>(0)?
                .into_iter()
                .next()
                .unwrap_or_default();
            let oldest = response
                .take::<Vec<OldestRow>>(1)?
                .into_iter()
                .next()
                .and_then(|row| row.oldest_unix)
                .and_then(|n| n.to_int());
            Ok(build_statistics(&counts, oldest))
        }
    }

    fn fetch_by_queue(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        let conn = self.conn.clone();
        let queue = self.config().queue().to_string();
        async move {
            let mut response = conn.query(OVERVIEW_BY_QUEUE).bind(("queue", queue)).await?;
            let counts = response
                .take::<Vec<OverviewRow>>(0)?
                .into_iter()
                .next()
                .unwrap_or_default();
            let oldest = response
                .take::<Vec<OldestRow>>(1)?
                .into_iter()
                .next()
                .and_then(|row| row.oldest_unix)
                .and_then(|n| n.to_int());
            Ok(build_statistics(&counts, oldest))
        }
    }
}
