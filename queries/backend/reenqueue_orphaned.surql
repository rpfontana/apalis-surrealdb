LET $live = SELECT VALUE instance FROM worker
    WHERE queue = $queue AND last_seen >= time::now() - duration::from_secs($dur);
UPDATE job SET
    status = 'Pending',
    attempts = attempts + 1,
    lock_by = NONE,
    lock_at = NONE,
    done_at = NONE,
    last_result = { Err: 'Re-enqueued: locking worker instance is no longer live.' }
    WHERE queue = $queue
      AND lock_at != NONE
      AND lock_at <= time::now() - duration::from_secs($dur)
      AND ((status IN ['Queued', 'Running'] AND lock_by != NONE AND lock_by NOTINSIDE $live)
           OR (status = 'Queued' AND lock_by IS NONE));

DELETE worker WHERE queue = $queue
    AND last_seen < time::now() - duration::from_secs($dur * 4);
