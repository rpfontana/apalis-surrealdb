SELECT (name ?? record::id(id)) AS name, queue, storage_name, layers, started_at, last_seen FROM worker
    WHERE queue = $queue
    ORDER BY last_seen DESC
    LIMIT $limit START $offset;
