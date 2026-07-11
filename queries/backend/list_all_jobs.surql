SELECT * FROM job
    WHERE status = $status
    ORDER BY run_at DESC, id DESC
    LIMIT $limit START $offset;
