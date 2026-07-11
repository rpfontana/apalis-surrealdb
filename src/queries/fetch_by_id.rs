use apalis_core::{
    backend::{BackendExt, FetchById, codec::Codec},
    task::task_id::TaskId,
};
use apalis_sql::TaskRow;
use surrealdb::{Surreal, engine::any::Any, types::RecordId};
use ulid::Ulid;

use crate::{
    CompactType, JOB_TABLE, SurrealContext, SurrealError, SurrealStorage, SurrealTask,
    from_row::SurrealTaskRow,
};

const FETCH_BY_ID: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/fetch_by_id.surql"
));

impl<Args, D, F> FetchById<Args> for SurrealStorage<Args, D, F>
where
    Self: BackendExt<
            Context = SurrealContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = SurrealError,
        >,
    D: Codec<Args, Compact = CompactType>,
    D::Error: std::error::Error + Send + Sync + 'static,
    Args: 'static,
{
    fn fetch_by_id(
        &mut self,
        id: &TaskId<Ulid>,
    ) -> impl Future<Output = Result<Option<SurrealTask<Args>>, Self::Error>> + Send {
        let conn = self.conn.clone();
        let id = RecordId::new(JOB_TABLE, id.to_string());
        async move {
            let mut response = conn.query(FETCH_BY_ID).bind(("id", id)).await?;
            let Some(row) = response.take::<Vec<SurrealTaskRow>>(0)?.into_iter().next() else {
                return Ok(None);
            };
            let task_row: TaskRow = row.try_into()?;
            let task = task_row
                .try_into_task_compact::<Ulid, Surreal<Any>>()?
                .try_map(|a| D::decode(&a))
                .map_err(|e| SurrealError::Decode(e.into()))?;
            Ok(Some(task))
        }
    }
}
