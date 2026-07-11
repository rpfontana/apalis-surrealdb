DEFINE TABLE IF NOT EXISTS jobs SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS args ON jobs TYPE bytes;
DEFINE FIELD IF NOT EXISTS queue ON jobs TYPE string;
DEFINE FIELD IF NOT EXISTS status ON jobs TYPE string DEFAULT 'Pending'
    ASSERT $value INSIDE ['Pending', 'Queued', 'Running', 'Done', 'Failed', 'Killed'];
DEFINE FIELD IF NOT EXISTS attempts ON jobs TYPE int DEFAULT 0;
DEFINE FIELD IF NOT EXISTS max_attempts ON jobs TYPE int DEFAULT 25;
DEFINE FIELD IF NOT EXISTS run_at ON jobs TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS last_result ON jobs TYPE option<object> FLEXIBLE;
DEFINE FIELD IF NOT EXISTS lock_at ON jobs TYPE option<datetime>;
DEFINE FIELD IF NOT EXISTS lock_by ON jobs TYPE option<string>;
DEFINE FIELD IF NOT EXISTS done_at ON jobs TYPE option<datetime>;
DEFINE FIELD IF NOT EXISTS priority ON jobs TYPE int DEFAULT 0;
DEFINE FIELD IF NOT EXISTS metadata ON jobs TYPE option<object> FLEXIBLE;
DEFINE FIELD IF NOT EXISTS idempotency_key ON jobs TYPE option<string>;
DEFINE INDEX IF NOT EXISTS jobs_fetch ON jobs COLUMNS queue, status, run_at;
DEFINE INDEX IF NOT EXISTS jobs_lock_by ON jobs COLUMNS lock_by;
DEFINE INDEX IF NOT EXISTS jobs_done ON jobs COLUMNS queue, done_at;
DEFINE INDEX IF NOT EXISTS jobs_idempotency ON jobs COLUMNS queue, idempotency_key;

DEFINE TABLE IF NOT EXISTS workers SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS queue ON workers TYPE string;
DEFINE FIELD IF NOT EXISTS storage_name ON workers TYPE string;
DEFINE FIELD IF NOT EXISTS layers ON workers TYPE option<string>;
DEFINE FIELD IF NOT EXISTS started_at ON workers TYPE datetime DEFAULT time::now();
DEFINE FIELD IF NOT EXISTS last_seen ON workers TYPE datetime DEFAULT time::now();
DEFINE INDEX IF NOT EXISTS workers_queue ON workers COLUMNS queue;

DEFINE TABLE IF NOT EXISTS apalis_meta SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS version ON apalis_meta TYPE int;
