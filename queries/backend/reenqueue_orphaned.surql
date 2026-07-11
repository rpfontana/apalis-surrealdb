LET $stale = SELECT VALUE record::id(id) FROM workers
    WHERE queue = $queue AND last_seen < time::now() - duration::from_secs($dur);
UPDATE jobs SET
    status = 'Pending',
    attempts = attempts + 1,
    lock_by = NONE,
    lock_at = NONE,
    done_at = NONE,
    last_result = { Err: 'Re-enqueued due to worker heartbeat timeout.' }
    WHERE queue = $queue AND status IN ['Queued', 'Running'] AND lock_by IN $stale;
