BEGIN;
LET $due = SELECT VALUE id FROM job
    WHERE queue IN $queues
      AND ((status = 'Pending' AND lock_by IS NONE)
           OR (status = 'Failed' AND attempts < max_attempts))
      AND run_at <= time::now()
    ORDER BY priority DESC, run_at ASC, id ASC
    LIMIT $limit;
UPDATE $due SET status = 'Queued', lock_at = time::now() RETURN AFTER;
COMMIT;
