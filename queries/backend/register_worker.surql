BEGIN;
LET $existing = SELECT VALUE last_seen FROM $worker;
IF array::len($existing) > 0 AND $existing[0] > time::now() - duration::from_secs($keep_alive) {
    THROW 'WORKER_ALREADY_EXISTS';
};
UPSERT $worker CONTENT {
    queue: $queue,
    storage_name: $storage,
    layers: $layers,
    started_at: time::now(),
    last_seen: time::now()
};
COMMIT;
