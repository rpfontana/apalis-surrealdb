BEGIN;
LET $previous = SELECT VALUE last_seen FROM $worker;
LET $live = array::len($previous) > 0
    AND $previous[0] > time::now() - duration::from_secs($keep_alive);
UPSERT $worker CONTENT {
    queue: $queue,
    storage_name: $storage,
    layers: $layers,
    instance: $instance,
    started_at: time::now(),
    last_seen: time::now()
};
RETURN $live;
COMMIT;
