SELECT
    count() AS total,
    count(status = 'Pending') AS pending,
    count(status = 'Running') AS running,
    count(status = 'Failed') AS failed,
    count(status = 'Done') AS done,
    count(status = 'Killed') AS killed,
    count(status = 'Pending' AND run_at <= time::now()) AS backlog,
    count(run_at >= time::now() - 1h) AS past_hour,
    count(status IN ['Done', 'Failed', 'Killed'] AND done_at != NONE AND lock_at != NONE) AS finished,
    math::sum(IF status IN ['Done', 'Failed', 'Killed'] AND done_at != NONE AND lock_at != NONE THEN duration::secs(done_at - lock_at) ELSE 0 END) AS dur_secs
FROM job WHERE queue = $queue GROUP ALL;
SELECT math::min(time::unix(run_at)) AS oldest_unix FROM job
    WHERE queue = $queue AND status = 'Pending' AND run_at <= time::now() GROUP ALL;
