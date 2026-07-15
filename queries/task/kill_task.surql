UPDATE $id SET
    status = 'Killed',
    last_result = { Err: $reason },
    done_at = time::now(),
    lock_by = NONE,
    lock_at = NONE
    RETURN NONE;
