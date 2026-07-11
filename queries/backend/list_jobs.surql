SELECT * FROM job
    WHERE queue = $queue AND status = $status
    ORDER BY run_at DESC, id DESC
    LIMIT $limit START $offset;
