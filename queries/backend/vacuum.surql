DELETE job
    WHERE status IN ['Done', 'Killed'] OR (status = 'Failed' AND attempts >= max_attempts)
    RETURN BEFORE;
