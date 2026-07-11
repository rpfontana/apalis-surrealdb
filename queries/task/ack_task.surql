UPDATE $id SET status = $status, attempts = $attempts, last_result = $result, done_at = time::now()
    WHERE lock_by = $worker
    RETURN AFTER;
