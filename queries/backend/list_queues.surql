SELECT
    queue,
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
FROM job GROUP BY queue;
SELECT queue, math::min(time::unix(run_at)) AS oldest_unix FROM job
    WHERE status = 'Pending' AND run_at <= time::now() GROUP BY queue;
SELECT queue, time::unix(time::floor(done_at, 1d)) AS day, count() AS done_count FROM job
    WHERE status = 'Done' AND done_at != NONE AND done_at >= time::now() - 7d
    GROUP BY queue, day;
SELECT record::id(id) AS name, queue FROM worker;
