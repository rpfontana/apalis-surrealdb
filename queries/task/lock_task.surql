UPDATE $id SET status = 'Running', lock_at = time::now(), lock_by = $worker
    WHERE (lock_by = $worker OR lock_by IS NONE) AND status IN ['Queued', 'Pending', 'Failed']
    RETURN AFTER;
