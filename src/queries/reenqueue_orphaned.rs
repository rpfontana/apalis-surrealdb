use std::sync::Arc;
use std::time::Duration;

use apalis_core::timer::Delay;
use futures::{Stream, stream};
use surrealdb::{Surreal, engine::any::Any, types::Value};

use crate::{Config, SurrealError};

const REENQUEUE_ORPHANED: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/reenqueue_orphaned.surql"
));

/// Return tasks held by timed-out workers to the queue and report how many
pub async fn reenqueue_orphaned(conn: &Arc<Surreal<Any>>, config: &Config) -> Result<u64, SurrealError> {
    let dead_for = config.reenqueue_orphaned_after().as_secs() as i64;
    let mut response = conn
        .query(REENQUEUE_ORPHANED)
        .bind(("queue", config.queue().to_string()))
        .bind(("dur", dead_for))
        .await?;
    let reenqueued: Vec<Value> = response.take(1)?;
    let count = reenqueued.len() as u64;
    if count > 0 {
        log::info!("Re-enqueued {count} orphaned tasks that were being processed by dead workers");
    }
    Ok(count)
}

/// Re-enqueue orphaned tasks at a fixed interval
pub fn reenqueue_orphaned_stream(
    conn: Arc<Surreal<Any>>,
    config: Config,
    interval: Duration,
) -> impl Stream<Item = Result<u64, SurrealError>> + Send {
    stream::unfold((), move |()| {
        let conn = conn.clone();
        let config = config.clone();
        async move {
            Delay::new(interval).await;
            Some((reenqueue_orphaned(&conn, &config).await, ()))
        }
    })
}
