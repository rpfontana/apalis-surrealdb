use apalis_core::backend::{BackendExt, Vacuum};
use surrealdb::types::Value;
use ulid::Ulid;

use crate::{CompactType, SurrealContext, SurrealError, SurrealStorage};

const VACUUM: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/vacuum.surql"
));

impl<Args, D, F> Vacuum for SurrealStorage<Args, D, F>
where
    Self: BackendExt<
            Context = SurrealContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = SurrealError,
        >,
{
    fn vacuum(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send {
        let conn = self.conn.clone();
        async move {
            let mut response = conn.query(VACUUM).await?;
            let deleted: Vec<Value> = response.take(0)?;
            Ok(deleted.len())
        }
    }
}
